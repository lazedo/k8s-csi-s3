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

// Metadata warm-up and refresh for immutable-content mounts.
//
// A StorageClass whose geesefs keeps its metadata for the life of the mount
// (--stat-cache-ttl in years) serves a cold directory listing exactly once —
// seconds on a large bucket, paid by whoever opens the first file in it. With
// the driver-level --warm-up option the node plugin walks the whole tree in
// the background right after the stage mount (and after a heal), so the
// pods only ever see warm listings.
//
// Such a mount never notices objects written to the bucket afterwards. The
// writer says so instead: a new value of the csi.lazedo.dev/refresh-at
// annotation on the PVC makes every node plugin that stages the volume drop
// geesefs's cache — the .invalidate xattr, set on the staging path, which is
// writable here (the pods' binds are read-only, and setxattr on a read-only
// mount is refused by the VFS before FUSE sees it) — and warm it again.
// Nothing restarts.

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"golang.org/x/sys/unix"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"

	"github.com/yandex-cloud/k8s-csi-s3/pkg/mounter"
)

const (
	// refreshAnnotation on a PVC: a new value refreshes the volume's mounts.
	refreshAnnotation = "csi.lazedo.dev/refresh-at"
	// refreshAttr is geesefs's --refresh-attr default: setting the xattr
	// refreshes the inode's cache, recursively for a directory.
	refreshAttr = ".invalidate"
	// warmParallel bounds the concurrent directory listings of one walk.
	warmParallel = 8
)

// wantsWarmUp says whether the volume's mount options ask for the walk.
func wantsWarmUp(volumeContext map[string]string) bool {
	for _, opt := range getMeta("", "", volumeContext).MountOptions {
		if opt == mounter.WarmUpOption {
			return true
		}
	}
	return false
}

// warmUp lists every directory under root once, warmParallel at a time,
// until done or ctx ends. Returns the directories listed.
func warmUp(ctx context.Context, root string) int {
	var dirs atomic.Int64
	sem := make(chan struct{}, warmParallel)
	var wg sync.WaitGroup
	var walk func(dir string)
	walk = func(dir string) {
		defer wg.Done()
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
	wg.Add(1)
	go walk(root)
	wg.Wait()
	return int(dirs.Load())
}

// invalidate drops geesefs's cache under path: the tree is re-listed lazily
// from the server from here on.
func invalidate(path string) error {
	return unix.Setxattr(path, refreshAttr, []byte{}, 0)
}

// warmIfOptedIn walks only when the volume asked for it with --warm-up: the
// pre-walk at stage/heal is an opt-in cost. A refresh always re-warms (warm),
// because dropping the cache without re-warming is strictly worse than not
// refreshing — it reintroduces the cold-listing latency the mount exists to
// avoid.
func (s *supervisor) warmIfOptedIn(volumeID string) {
	s.mu.Lock()
	opted := s.vols[volumeID] != nil && s.vols[volumeID].WarmUp
	s.mu.Unlock()
	if opted {
		s.warm(volumeID)
	}
}

// warm starts the tree walk of a staged volume, replacing a walk still running.
func (s *supervisor) warm(volumeID string) {
	s.mu.Lock()
	v := s.vols[volumeID]
	if v == nil {
		s.mu.Unlock()
		return
	}
	if cancel := s.warming[volumeID]; cancel != nil {
		cancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.warming[volumeID] = cancel
	path := v.StagePath
	s.mu.Unlock()
	go func() {
		start := time.Now()
		n := warmUp(ctx, path)
		if ctx.Err() == nil {
			glog.Infof("warm-up: volume %s, %d directories listed in %s", volumeID, n, time.Since(start).Round(time.Millisecond))
		}
		s.mu.Lock()
		if s.warming[volumeID] != nil && ctx.Err() == nil {
			delete(s.warming, volumeID)
		}
		s.mu.Unlock()
	}()
}

// stopWarm ends a walk (the volume is being unstaged or remounted).
func (s *supervisor) stopWarm(volumeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cancel := s.warming[volumeID]; cancel != nil {
		cancel()
		delete(s.warming, volumeID)
	}
}

// refresh drops the volume's metadata cache and warms it again, recording
// the request it answered.
func (s *supervisor) refresh(volumeID, at string) {
	s.mu.Lock()
	v := s.vols[volumeID]
	if v == nil {
		s.mu.Unlock()
		return
	}
	path := v.StagePath
	s.mu.Unlock()
	s.stopWarm(volumeID)
	if err := invalidate(path); err != nil {
		glog.Errorf("refresh: volume %s at %s: %v", volumeID, path, err)
		return
	}
	glog.Infof("refresh: volume %s cache dropped (%s=%s)", volumeID, refreshAnnotation, at)
	s.mu.Lock()
	if v := s.vols[volumeID]; v != nil {
		v.RefreshedAt = at
		s.persist()
	}
	s.mu.Unlock()
	s.warm(volumeID)
}

// needsRefresh compares a PVC's request with the last one answered.
func needsRefresh(requested, answered string) bool {
	return requested != "" && requested != answered
}

// watchRefreshRequests follows the PVCs of the cluster for refresh
// requests on volumes staged here. A PVC names its PV, the PV its
// volumeHandle — the id the volume was staged with.
func (s *supervisor) watchRefreshRequests(ctx context.Context) {
	if s.driver.k8s == nil {
		return
	}
	factory := informers.NewSharedInformerFactory(s.driver.k8s.typed, 0)
	inf := factory.Core().V1().PersistentVolumeClaims().Informer()
	handle := func(obj interface{}) {
		pvc, ok := obj.(*corev1.PersistentVolumeClaim)
		if !ok {
			return
		}
		s.onPVC(ctx, pvc)
	}
	if _, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    handle,
		UpdateFunc: func(_, obj interface{}) { handle(obj) },
	}); err != nil {
		glog.Errorf("refresh: PVC watch: %v", err)
		return
	}
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())
	glog.Infof("refresh: watching PVCs for %s", refreshAnnotation)
}

// onPVC answers a refresh request on a volume staged here.
func (s *supervisor) onPVC(ctx context.Context, pvc *corev1.PersistentVolumeClaim) {
	at := pvc.Annotations[refreshAnnotation]
	if at == "" || pvc.Spec.VolumeName == "" {
		return
	}
	handle := s.volumeHandleOf(ctx, pvc.Spec.VolumeName)
	if handle == "" {
		return
	}
	s.mu.Lock()
	v := s.vols[handle]
	answered := ""
	if v != nil {
		answered = v.RefreshedAt
	}
	s.mu.Unlock()
	if v == nil || !needsRefresh(at, answered) {
		return
	}
	s.refresh(handle, at)
}

// volumeHandleOf resolves a PV of this driver to its volumeHandle; "" for
// another driver's or an unknown PV. PVs never change their handle: cached.
func (s *supervisor) volumeHandleOf(ctx context.Context, pvName string) string {
	s.mu.Lock()
	if h, ok := s.pvHandles[pvName]; ok {
		s.mu.Unlock()
		return h
	}
	s.mu.Unlock()
	pv, err := s.driver.k8s.typed.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		glog.Warningf("refresh: PV %s: %v", pvName, err)
		return ""
	}
	handle := ""
	if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == s.driver.name {
		handle = pv.Spec.CSI.VolumeHandle
	}
	s.mu.Lock()
	s.pvHandles[pvName] = handle
	s.mu.Unlock()
	return handle
}
