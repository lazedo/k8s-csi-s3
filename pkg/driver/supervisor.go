package driver

// Mount supervisor. FUSE mounters (geesefs and friends) are userspace
// daemons: when one dies, its mountpoint stays in the mount table as a dead
// endpoint — every access returns ENOTCONN ("Socket not connected") — and
// nothing ever remounts it. The supervisor health-checks every staged volume
// this node plugin knows about and, on a dead endpoint, lazily unmounts the
// corpse (publish binds first, then the staging mount), remounts the staging
// path with the same parameters NodeStageVolume used, and re-binds the
// publish targets.
//
// Scope note: a container that mounted the volume with mountPropagation None
// (the default) captured the dead superblock at start and only sees the healed
// mount after a container restart; HostToContainer propagation picks the heal
// up live. Either way kubelet, new pods and restarts are healthy immediately.
//
// State survives plugin restarts via a JSON file in the plugin dir (root-only,
// same trust domain as the CSI socket — for COSI-handle volumes it carries no
// credentials at all, they re-resolve in-cluster at heal time).

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/golang/glog"
)

const (
	superviseIntervalEnv    = "CSI_S3_SUPERVISE_INTERVAL"
	defaultSuperviseSeconds = 30
	stateFileName           = "supervisor-state.json"
)

type stagedVolume struct {
	VolumeID      string            `json:"volumeId"`
	StagePath     string            `json:"stagePath"`
	VolumeContext map[string]string `json:"volumeContext,omitempty"`
	// Secrets are the kubelet-provided stage secrets (empty for COSI-handle
	// volumes, which re-resolve in-cluster). The state file is 0600.
	Secrets   map[string]string `json:"secrets,omitempty"`
	Publishes []string          `json:"publishes,omitempty"`
}

type supervisor struct {
	mu        sync.Mutex
	driver    *driver
	stateFile string
	interval  time.Duration
	vols      map[string]*stagedVolume
}

func newSupervisor(d *driver) *supervisor {
	dir := os.Getenv("PLUGIN_DIR")
	if dir == "" {
		dir = "/var/lib/kubelet/plugins/csi.lazedo.dev"
	}
	interval := time.Duration(defaultSuperviseSeconds) * time.Second
	if v := os.Getenv(superviseIntervalEnv); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			interval = time.Duration(n) * time.Second // 0 disables
		}
	}
	s := &supervisor{
		driver:    d,
		stateFile: filepath.Join(dir, stateFileName),
		interval:  interval,
		vols:      map[string]*stagedVolume{},
	}
	s.load()
	return s
}

func (s *supervisor) load() {
	b, err := os.ReadFile(s.stateFile)
	if err != nil {
		return
	}
	var vols map[string]*stagedVolume
	if err := json.Unmarshal(b, &vols); err != nil {
		glog.Errorf("supervisor: corrupt state file %s: %v", s.stateFile, err)
		return
	}
	s.vols = vols
	glog.Infof("supervisor: recovered %d staged volume(s) from %s", len(vols), s.stateFile)
}

// persist writes the state file; callers hold s.mu.
func (s *supervisor) persist() {
	b, err := json.MarshalIndent(s.vols, "", "  ")
	if err != nil {
		return
	}
	tmp := s.stateFile + ".tmp"
	if err := os.WriteFile(tmp, b, 0600); err != nil {
		glog.Errorf("supervisor: writing %s: %v", tmp, err)
		return
	}
	if err := os.Rename(tmp, s.stateFile); err != nil {
		glog.Errorf("supervisor: renaming state file: %v", err)
	}
}

func (s *supervisor) recordStage(volumeID, stagePath string, volumeContext, secrets map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := s.vols[volumeID]
	v := &stagedVolume{VolumeID: volumeID, StagePath: stagePath, VolumeContext: volumeContext, Secrets: secrets}
	if prev != nil {
		v.Publishes = prev.Publishes
	}
	s.vols[volumeID] = v
	s.persist()
}

func (s *supervisor) forgetStage(volumeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.vols, volumeID)
	s.persist()
}

func (s *supervisor) recordPublish(volumeID, target string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.vols[volumeID]
	if v == nil {
		return
	}
	for _, t := range v.Publishes {
		if t == target {
			return
		}
	}
	v.Publishes = append(v.Publishes, target)
	sort.Strings(v.Publishes)
	s.persist()
}

func (s *supervisor) forgetPublish(volumeID, target string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.vols[volumeID]
	if v == nil {
		return
	}
	out := v.Publishes[:0]
	for _, t := range v.Publishes {
		if t != target {
			out = append(out, t)
		}
	}
	v.Publishes = out
	s.persist()
}

// run is the health loop; a zero interval disables supervision.
func (s *supervisor) run(ctx context.Context) {
	if s.interval <= 0 {
		glog.Infof("supervisor: disabled (%s=0)", superviseIntervalEnv)
		return
	}
	glog.Infof("supervisor: checking staged mounts every %s", s.interval)
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.checkAll(ctx)
		}
	}
}

func (s *supervisor) checkAll(ctx context.Context) {
	s.mu.Lock()
	snapshot := make([]*stagedVolume, 0, len(s.vols))
	for _, v := range s.vols {
		c := *v
		c.Publishes = append([]string{}, v.Publishes...)
		snapshot = append(snapshot, &c)
	}
	s.mu.Unlock()

	for _, v := range snapshot {
		dead, gone := mountDead(v.StagePath)
		if gone {
			continue // path no longer exists — kubelet cleaned it up
		}
		if !dead {
			continue
		}
		glog.Errorf("supervisor: dead mount endpoint at %s (volume %s) — remounting", v.StagePath, v.VolumeID)
		s.heal(ctx, v)
	}
}

// mountDead statfs-probes a mountpoint: (dead, gone).
func mountDead(path string) (bool, bool) {
	var st syscall.Statfs_t
	err := syscall.Statfs(path, &st)
	if err == nil {
		return false, false
	}
	if errors.Is(err, syscall.ENOENT) {
		return false, true
	}
	if errors.Is(err, syscall.ENOTCONN) || errors.Is(err, syscall.EIO) || errors.Is(err, syscall.ECONNABORTED) {
		return true, false
	}
	glog.Warningf("supervisor: statfs %s: %v", path, err)
	return false, false
}

// lazyUnmount detaches a (possibly dead) mountpoint.
func lazyUnmount(path string) {
	if err := syscall.Unmount(path, syscall.MNT_DETACH); err != nil && !errors.Is(err, syscall.EINVAL) && !errors.Is(err, syscall.ENOENT) {
		glog.Warningf("supervisor: lazy unmount %s: %v", path, err)
	}
}

// heal replaces a dead staging mount and its publish binds.
func (s *supervisor) heal(ctx context.Context, v *stagedVolume) {
	// publish binds reference the dead superblock — detach them first.
	for _, t := range v.Publishes {
		lazyUnmount(t)
	}
	lazyUnmount(v.StagePath)

	if err := s.driver.ns.mountStaged(ctx, v.VolumeID, v.StagePath, v.VolumeContext, v.Secrets); err != nil {
		glog.Errorf("supervisor: remount of %s (volume %s) failed: %v", v.StagePath, v.VolumeID, err)
		return
	}
	for _, t := range v.Publishes {
		cmd := exec.Command("mount", "--bind", v.StagePath, t)
		if out, err := cmd.CombinedOutput(); err != nil {
			glog.Errorf("supervisor: re-bind %s -> %s: %v (%s)", v.StagePath, t, err, out)
			continue
		}
		glog.Infof("supervisor: re-bound %s", t)
	}
	glog.Infof("supervisor: volume %s healed at %s", v.VolumeID, v.StagePath)
}
