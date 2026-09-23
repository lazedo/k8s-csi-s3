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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/golang/glog"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	// WarmUp: the mount options asked for the tree walk (refresh.go).
	WarmUp bool `json:"warmUp,omitempty"`
	// RefreshedAt is the last refresh request answered (the PVC annotation).
	RefreshedAt string `json:"refreshedAt,omitempty"`
}

type supervisor struct {
	mu        sync.Mutex
	driver    *driver
	stateFile string
	interval  time.Duration
	vols      map[string]*stagedVolume
	// warming: the running warm-up walk of a volume (refresh.go).
	warming map[string]context.CancelFunc
	// pvHandles caches PV name → volumeHandle for the refresh watch.
	pvHandles map[string]string
}

func newSupervisor(d *driver) *supervisor {
	// the plugin dir is bind-mounted at /csi inside the container (the host
	// path only exists host-side); state must go through the container view.
	dir := "/csi"
	if st, err := os.Stat(dir); err != nil || !st.IsDir() {
		dir = os.TempDir()
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
		warming:   map[string]context.CancelFunc{},
		pvHandles: map[string]string{},
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
	v := &stagedVolume{VolumeID: volumeID, StagePath: stagePath, VolumeContext: volumeContext, Secrets: secrets,
		WarmUp: wantsWarmUp(volumeContext)}
	if prev != nil {
		v.Publishes = prev.Publishes
		v.RefreshedAt = prev.RefreshedAt
	}
	s.vols[volumeID] = v
	s.persist()
}

func (s *supervisor) forgetStage(volumeID string) {
	s.stopWarm(volumeID)
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
	s.discover(ctx)
	go s.watchRefreshRequests(ctx)
	t := time.NewTicker(s.interval)
	defer t.Stop()
	n := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n++; n%10 == 0 {
				s.discover(ctx) // pick up mounts staged by older plugin versions
			}
			s.checkAll(ctx)
		}
	}
}

// discover adopts mounts this plugin did not stage itself (volumes staged by
// an older plugin version, or state lost): it scans /proc/mounts for our FUSE
// mounts, reads the volumeHandle from kubelet's vol_data.json next to each
// mountpoint, and resolves the mount parameters from the PV (volumeAttributes
// + nodeStageSecretRef). Once adopted, the health loop covers them.
func (s *supervisor) discover(ctx context.Context) {
	staged, published := scanOwnMounts(s.driver.name)
	if len(staged) == 0 && len(published) == 0 {
		return
	}
	s.mu.Lock()
	knownStage := map[string]bool{}
	for _, v := range s.vols {
		knownStage[v.StagePath] = true
	}
	s.mu.Unlock()

	var pvs []pvInfo
	for _, sp := range staged {
		if knownStage[sp] {
			continue
		}
		handle := volHandleFor(filepath.Dir(sp))
		if handle == "" {
			glog.Warningf("supervisor: no vol_data.json next to %s — cannot adopt", sp)
			continue
		}
		if pvs == nil {
			pvs = s.listPVs(ctx)
		}
		var info *pvInfo
		for i := range pvs {
			if pvs[i].handle == handle {
				info = &pvs[i]
				break
			}
		}
		if info == nil {
			glog.Warningf("supervisor: no PV with volumeHandle %s — cannot adopt %s", handle, sp)
			continue
		}
		secrets := map[string]string{}
		if info.secretName != "" && s.driver.k8s != nil {
			sec, err := s.driver.k8s.typed.CoreV1().Secrets(info.secretNS).Get(ctx, info.secretName, metav1.GetOptions{})
			if err != nil {
				glog.Warningf("supervisor: stage secret %s/%s for %s: %v", info.secretNS, info.secretName, handle, err)
				continue
			}
			for k, v := range sec.Data {
				secrets[k] = string(v)
			}
		}
		glog.Infof("supervisor: adopted pre-existing mount %s (volume %s)", sp, handle)
		s.recordStage(handle, sp, info.attributes, secrets)
	}

	// publish targets: kubelet pod dirs whose vol_data.json names a handle we track.
	s.mu.Lock()
	byHandle := map[string]bool{}
	for id := range s.vols {
		byHandle[id] = true
	}
	s.mu.Unlock()
	for _, pp := range published {
		handle := volHandleFor(filepath.Dir(pp))
		if handle == "" || !byHandle[handle] {
			continue
		}
		s.recordPublish(handle, pp)
	}
}

type pvInfo struct {
	handle     string
	attributes map[string]string
	secretName string
	secretNS   string
}

func (s *supervisor) listPVs(ctx context.Context) []pvInfo {
	if s.driver.k8s == nil {
		return nil
	}
	list, err := s.driver.k8s.typed.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		glog.Warningf("supervisor: listing PVs: %v", err)
		return nil
	}
	var out []pvInfo
	for i := range list.Items {
		csi := list.Items[i].Spec.CSI
		if csi == nil || csi.Driver != s.driver.name {
			continue
		}
		info := pvInfo{handle: csi.VolumeHandle, attributes: csi.VolumeAttributes}
		if ref := csi.NodeStageSecretRef; ref != nil {
			info.secretName, info.secretNS = ref.Name, ref.Namespace
		}
		out = append(out, info)
	}
	return out
}

// scanOwnMounts returns this driver's FUSE mountpoints from /proc/mounts,
// split into staging (…/plugins/kubernetes.io/csi/<driver>/…/globalmount)
// and publish (…/pods/…/volumes/kubernetes.io~csi/…/mount) paths. Dead
// endpoints are still listed in /proc/mounts, which is the point.
func scanOwnMounts(driverName string) (staged, published []string) {
	b, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return nil, nil
	}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) < 3 || !strings.HasPrefix(f[2], "fuse") {
			continue
		}
		mp := f[1]
		switch {
		case strings.Contains(mp, "/plugins/kubernetes.io/csi/"+driverName+"/") && strings.HasSuffix(mp, "/globalmount"):
			staged = append(staged, mp)
		case strings.Contains(mp, "/volumes/kubernetes.io~csi/") && strings.HasSuffix(mp, "/mount"):
			published = append(published, mp)
		}
	}
	return staged, published
}

// volHandleFor reads kubelet's vol_data.json in dir.
func volHandleFor(dir string) string {
	b, err := os.ReadFile(filepath.Join(dir, "vol_data.json"))
	if err != nil {
		return ""
	}
	var v struct {
		VolumeHandle string `json:"volumeHandle"`
		DriverName   string `json:"driverName"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return ""
	}
	return v.VolumeHandle
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

	s.stopWarm(v.VolumeID)
	if err := s.driver.ns.mountStaged(ctx, v.VolumeID, v.StagePath, v.VolumeContext, v.Secrets); err != nil {
		glog.Errorf("supervisor: remount of %s (volume %s) failed: %v", v.StagePath, v.VolumeID, err)
		return
	}
	s.warm(v.VolumeID)
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
