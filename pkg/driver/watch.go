/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

// Every staged volume is watched: the node plugin listens to its bucket
// (MinIO's ListenBucketNotification, with the volume's own credentials) and
// drops the mount's cache where objects were created or removed (cache.go).
//
// A connection that starts — the first, or any after a break — drops the
// whole cache first: what changed while nobody was listening was never
// heard. An endpoint that refuses to be listened to (not MinIO, or
// credentials without the permission) gets its cache dropped every
// refusedRefresh instead, what a --stat-cache-ttl that short would do.

import (
	"context"
	"errors"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"

	"github.com/yandex-cloud/k8s-csi-s3/pkg/s3"
)

const (
	// changeWindow gathers the changes of a burst (a tree being uploaded)
	// into one drop.
	changeWindow = 250 * time.Millisecond
	// maxDirs: a burst touching more directories than this drops the whole
	// cache.
	maxDirs = 64
	// reconnectMin and reconnectMax bound the wait between connections.
	reconnectMin = time.Second
	reconnectMax = 30 * time.Second
	// refusedRefresh and refusedRetry: the fallback of a bucket that cannot
	// be listened to, and how often listening is tried again.
	refusedRefresh = time.Minute
	refusedRetry   = 5 * time.Minute
)

// listener follows the changes of a bucket: s3's Listen.
type listener interface {
	Listen(ctx context.Context, bucket, prefix string, connected func(), changed func(key string)) error
}

// watch keeps the cache of one staged volume true to its bucket.
type watch struct {
	sup      *supervisor
	volumeID string
	stage    string
	rewarm   bool // walk the cache again after every drop
	ctx      context.Context
	cancel   context.CancelFunc

	// set once the volume's credentials resolve
	client listener
	bucket string
	prefix string // the mount's prefix in the bucket, "" or ending in /

	mu       sync.Mutex
	pending  map[string]bool // directories, relative to the mount, that changed
	flushing bool
}

// startWatch (re)starts the watch of a staged volume.
func (s *supervisor) startWatch(volumeID string) {
	s.mu.Lock()
	v := s.vols[volumeID]
	if v == nil {
		s.mu.Unlock()
		return
	}
	if old := s.watches[volumeID]; old != nil {
		old.cancel()
	}
	w := &watch{sup: s, volumeID: volumeID, stage: v.StagePath, rewarm: v.Rewarm, pending: map[string]bool{}}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	s.watches[volumeID] = w
	volumeContext, secrets := v.VolumeContext, v.Secrets
	s.mu.Unlock()
	go w.run(volumeContext, secrets)
}

// ensureWatch starts the watch of a staged volume that has none.
func (s *supervisor) ensureWatch(volumeID string) {
	s.mu.Lock()
	running := s.watches[volumeID] != nil
	s.mu.Unlock()
	if !running {
		s.startWatch(volumeID)
	}
}

// ensureWatches covers every staged volume this plugin knows.
func (s *supervisor) ensureWatches() {
	s.mu.Lock()
	ids := make([]string, 0, len(s.vols))
	for id := range s.vols {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	for _, id := range ids {
		s.ensureWatch(id)
	}
}

// stopWatch ends the watch of a volume being unstaged.
func (s *supervisor) stopWatch(volumeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if w := s.watches[volumeID]; w != nil {
		w.cancel()
		delete(s.watches, volumeID)
	}
}

func (w *watch) run(volumeContext, secrets map[string]string) {
	delay := reconnectMin
	for w.client == nil {
		_, client, meta, err := w.sup.driver.ns.volumeTarget(w.ctx, w.volumeID, volumeContext, secrets)
		if err == nil {
			w.client, w.bucket, w.prefix = client, meta.BucketName, listenPrefix(meta.Prefix)
			break
		}
		glog.Warningf("watch: volume %s: %v — trying again in %s", w.volumeID, err, delay)
		if !sleep(w.ctx, delay) {
			return
		}
		delay = min(2*delay, reconnectMax)
	}

	delay = reconnectMin
	refused := false
	for {
		var connectedAt time.Time
		err := w.client.Listen(w.ctx, w.bucket, w.prefix, func() {
			connectedAt = time.Now()
			refused = false
			glog.Infof("watch: volume %s: listening to %s/%s", w.volumeID, w.bucket, w.prefix)
			w.dropAll()
		}, w.changed)
		if w.ctx.Err() != nil {
			return
		}
		var r *s3.ListenRefused
		if errors.As(err, &r) {
			if !refused {
				glog.Warningf("watch: volume %s: %v — its cache is dropped every %s instead", w.volumeID, err, refusedRefresh)
				refused = true
			}
			if !w.dropEvery(refusedRefresh, refusedRetry) {
				return
			}
			continue
		}
		if !connectedAt.IsZero() && time.Since(connectedAt) > reconnectMax {
			delay = reconnectMin
		}
		glog.Warningf("watch: volume %s: %v — listening again in %s", w.volumeID, err, delay)
		if !sleep(w.ctx, delay) {
			return
		}
		delay = min(2*delay, reconnectMax)
	}
}

// listenPrefix is the key prefix of a mount of bucket:prefix.
func listenPrefix(prefix string) string {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return ""
	}
	return prefix + "/"
}

// changedDir is the directory, relative to the mount, whose listing a change
// of key alters; ok is false for a key outside the mount.
func changedDir(prefix, key string) (string, bool) {
	if !strings.HasPrefix(key, prefix) {
		return "", false
	}
	rel := strings.Trim(strings.TrimPrefix(key, prefix), "/")
	if rel == "" {
		return "", false
	}
	return path.Dir(rel), true // a "dir/" marker object changes its parent's listing too
}

// changed takes one created or removed object: its directory is dropped at the
// end of the burst it belongs to.
func (w *watch) changed(key string) {
	dir, ok := changedDir(w.prefix, key)
	if !ok {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pending[dir] = true
	if !w.flushing {
		w.flushing = true
		time.AfterFunc(changeWindow, w.flush)
	}
}

// flush drops the cache of the directories a burst changed.
func (w *watch) flush() {
	w.mu.Lock()
	dirs := make([]string, 0, len(w.pending))
	for d := range w.pending {
		dirs = append(dirs, d)
	}
	w.pending = map[string]bool{}
	w.flushing = false
	w.mu.Unlock()
	if w.ctx.Err() != nil {
		return
	}

	targets := outermost(dirs)
	if len(targets) > maxDirs || (len(targets) == 1 && targets[0] == ".") {
		w.dropAll()
		return
	}
	var dropped []string
	for _, d := range targets {
		p, err := invalidateNearest(w.stage, d)
		if err != nil {
			glog.Warningf("watch: volume %s: dropping the cache of %s: %v", w.volumeID, p, err)
			continue
		}
		dropped = append(dropped, p)
	}
	glog.V(4).Infof("watch: volume %s: cache dropped for %v", w.volumeID, targets)
	if w.rewarm && len(dropped) > 0 {
		w.sup.warm(w.volumeID, dropped...)
	}
}

// dropAll drops the whole cache of the mount, walking it again if read-only.
func (w *watch) dropAll() {
	w.sup.stopWarm(w.volumeID)
	if err := invalidate(w.stage); err != nil {
		glog.Errorf("watch: volume %s: dropping the cache of %s: %v", w.volumeID, w.stage, err)
		return
	}
	glog.Infof("watch: volume %s: cache dropped", w.volumeID)
	if w.rewarm {
		w.sup.warm(w.volumeID, w.stage)
	}
}

// dropEvery drops the whole cache every interval for d: the fallback of a
// bucket that cannot be listened to. False when the watch ended.
func (w *watch) dropEvery(interval, d time.Duration) bool {
	for end := time.Now().Add(d); time.Now().Before(end); {
		if !sleep(w.ctx, interval) {
			return false
		}
		w.sup.stopWarm(w.volumeID)
		if err := invalidate(w.stage); err != nil {
			glog.Errorf("watch: volume %s: dropping the cache of %s: %v", w.volumeID, w.stage, err)
		}
	}
	return true
}

// outermost keeps the directories no other one contains: dropping the cache
// of a directory drops its whole subtree's.
func outermost(dirs []string) []string {
	sort.Slice(dirs, func(i, j int) bool {
		if len(dirs[i]) != len(dirs[j]) {
			return len(dirs[i]) < len(dirs[j])
		}
		return dirs[i] < dirs[j]
	})
	var out []string
	for _, d := range dirs {
		covered := false
		for _, o := range out {
			if o == "." || d == o || strings.HasPrefix(d, o+"/") {
				covered = true
				break
			}
		}
		if !covered {
			out = append(out, d)
		}
	}
	return out
}

// sleep waits d, or less if ctx ends first; false when it did.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
