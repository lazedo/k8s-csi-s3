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

// The metadata cache of a mount, kept true to its bucket.
//
// geesefs keeps what it listed for --stat-cache-ttl, and the StorageClass sets
// it in years: a cold directory listing costs seconds on this storage, and a
// pod (FreeSWITCH opening a sound) should never pay one. A cache that never
// expires has to be told what changed, so the node plugin listens to the
// bucket of every volume it stages (watch.go) and drops the cache of the
// directories whose objects were created or removed: the .invalidate xattr,
// set on the staging path, which is writable here (the pods' binds are
// read-only, and setxattr on a read-only mount is refused by the VFS before
// FUSE sees it). A read-only volume is walked again after each drop, so its
// pods only ever see warm listings; a writable one re-lists lazily.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"golang.org/x/sys/unix"
)

const (
	// invalidateAttr is geesefs's --refresh-attr default: setting the xattr
	// drops the inode's cache, recursively for a directory.
	invalidateAttr = ".invalidate"
	// warmParallel bounds the concurrent directory listings of one walk.
	warmParallel = 8
)

// warmUp lists every directory under each root once, warmParallel at a time,
// until done or ctx ends. Returns the directories listed.
func warmUp(ctx context.Context, roots ...string) int {
	var dirs atomic.Int64
	sem := make(chan struct{}, warmParallel)
	var wg sync.WaitGroup
	var walk func(dir string)
	walk = func(dir string) {
		defer wg.Done()
		if ctx.Err() != nil {
			return // a select with both ready picks either
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		entries, err := os.ReadDir(dir)
		<-sem
		if err != nil {
			return
		}
		dirs.Add(1)
		for _, e := range entries {
			if ctx.Err() != nil {
				return
			}
			if e.IsDir() {
				wg.Add(1)
				go walk(filepath.Join(dir, e.Name()))
			}
		}
	}
	for _, root := range roots {
		wg.Add(1)
		go walk(root)
	}
	wg.Wait()
	return int(dirs.Load())
}

// invalidate drops geesefs's cache of path, recursively for a directory: it
// is listed again from the server on the next access.
func invalidate(path string) error {
	return unix.Setxattr(path, invalidateAttr, []byte{}, 0)
}

// invalidateNearest drops the cache of dir (relative to the mount at root), or
// of its nearest ancestor that exists: a removed directory is no longer in
// its parent's listing, and a new one is not in it yet. Returns the path
// whose cache was dropped.
func invalidateNearest(root, dir string) (string, error) {
	for {
		p := filepath.Join(root, filepath.FromSlash(dir))
		err := invalidate(p)
		if err == nil || dir == "." || !errors.Is(err, unix.ENOENT) {
			return p, err
		}
		dir = filepath.Dir(dir)
	}
}

// warm walks paths of a staged volume in the background. Walks of the same
// volume run side by side; stopWarm ends them all.
func (s *supervisor) warm(volumeID string, paths ...string) {
	s.mu.Lock()
	if s.vols[volumeID] == nil {
		s.mu.Unlock()
		return
	}
	parent := s.warming[volumeID]
	if parent == nil {
		parent = &warming{}
		parent.ctx, parent.cancel = context.WithCancel(context.Background())
		s.warming[volumeID] = parent
	}
	ctx := parent.ctx
	s.mu.Unlock()
	go func() {
		start := time.Now()
		n := warmUp(ctx, paths...)
		if ctx.Err() == nil {
			glog.Infof("warm-up: volume %s, %d directories listed in %s", volumeID, n, time.Since(start).Round(time.Millisecond))
		}
	}()
}

// warming is the context of a volume's walks.
type warming struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// stopWarm ends the walks of a volume (its cache is about to be dropped, or
// the volume is being unstaged or remounted).
func (s *supervisor) stopWarm(volumeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if w := s.warming[volumeID]; w != nil {
		w.cancel()
		delete(s.warming, volumeID)
	}
}
