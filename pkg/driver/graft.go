package driver

// Grafting a healed mount into the containers that still hold the dead one.
// When a mounter dies -- a plugin restart takes every geesefs with it -- the
// supervisor remounts the volume on the node, but a running container keeps
// the mount it captured at start: the dead superblock, ENOTCONN, whatever its
// mountPropagation (heal's detach and re-bind reach no container that already
// holds the mount). So the dead superblock's device is remembered and, with
// the node's processes in view (hostPID), every other mount namespace that
// still holds it gets the healed mount grafted over each such mount point by
// nsmount-heal (cmd/nsmount-heal: setns needs a single-threaded caller, so
// not Go).

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/glog"
)

// nsmountHeal is the graft helper the image ships.
const nsmountHeal = "/usr/bin/nsmount-heal"

var missingHelper sync.Once

// mountEntry is one line of a /proc/<pid>/mountinfo.
type mountEntry struct {
	dev        string // major:minor of the superblock
	root       string // the mount's root inside its filesystem (a subPath)
	mountPoint string
	opts       string // per-mount options: rw/ro, nosuid, nodev, noexec...
	fstype     string
}

// parseMountinfo reads mountinfo lines, e.g.
// "36 35 0:52 / /mnt rw,nosuid,nodev,relatime shared:1 - fuse.geesefs bucket rw,user_id=0".
func parseMountinfo(b []byte) []mountEntry {
	var out []mountEntry
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		sep := -1
		for i := 6; i < len(f); i++ {
			if f[i] == "-" {
				sep = i
				break
			}
		}
		if sep < 0 || sep+1 >= len(f) {
			continue
		}
		out = append(out, mountEntry{dev: f[2], root: unescapeMount(f[3]), mountPoint: unescapeMount(f[4]),
			opts: f[5], fstype: f[sep+1]})
	}
	return out
}

// unescapeMount undoes the kernel's octal escapes in mountinfo paths
// (\040 space, \011 tab, \012 newline, \134 backslash).
func unescapeMount(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+4 <= len(s) {
			if n, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(n))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// mountDevice is the device of the mount at path in the namespace of the
// given mountinfo -- the topmost one when several are stacked; "" if none.
func mountDevice(mountinfo, path string) string {
	b, err := os.ReadFile(mountinfo)
	if err != nil {
		return ""
	}
	dev := ""
	for _, m := range parseMountinfo(b) {
		if m.mountPoint == path {
			dev = m.dev
		}
	}
	return dev
}

// deadMountDevice is the device of path's mount when that mount is dead, "" otherwise.
func deadMountDevice(path string) string {
	if dead, _ := mountDead(path); !dead {
		return ""
	}
	return mountDevice("/proc/self/mountinfo", path)
}

// holder is a mount of a dead superblock in another mount namespace.
type holder struct {
	pid int
	mountEntry
}

// findHolders lists the FUSE mounts of the dead devices that mount
// namespaces other than the node's (pid 1) and this plugin's hold, read
// through one process of each namespace.
func findHolders(proc string, dead map[string]bool) []holder {
	seen := map[string]bool{}
	for _, p := range []string{"self", "1"} {
		if ns, err := os.Readlink(filepath.Join(proc, p, "ns", "mnt")); err == nil {
			seen[ns] = true
		}
	}
	entries, err := os.ReadDir(proc)
	if err != nil {
		return nil
	}
	var out []holder
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		ns, err := os.Readlink(filepath.Join(proc, e.Name(), "ns", "mnt"))
		if err != nil || seen[ns] {
			continue
		}
		b, err := os.ReadFile(filepath.Join(proc, e.Name(), "mountinfo"))
		if err != nil {
			continue // gone meanwhile: another process of its namespace may speak for it
		}
		seen[ns] = true
		for _, m := range parseMountinfo(b) {
			if dead[m.dev] && strings.HasPrefix(m.fstype, "fuse") {
				out = append(out, holder{pid: pid, mountEntry: m})
			}
		}
	}
	return out
}

// graftFlags are the per-mount flags a graft keeps from the mount it
// replaces: a readOnly volumeMount stays read-only.
func graftFlags(opts string) string {
	var keep []string
	for _, o := range strings.Split(opts, ",") {
		switch o {
		case "ro", "nosuid", "nodev", "noexec":
			keep = append(keep, o)
		}
	}
	return strings.Join(keep, ",")
}

// markDead remembers a dead superblock of a volume until no container holds it.
func (s *supervisor) markDead(volumeID, dev string) {
	if dev == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.vols[volumeID]
	if v == nil {
		return
	}
	for _, d := range v.Dead {
		if d == dev {
			return
		}
	}
	v.Dead = append(v.Dead, dev)
	s.persist()
}

// graftHolders grafts each volume's healed staging mount over the mounts
// other namespaces still hold of its dead superblocks. A device stays
// remembered while a graft into it failed or its staging is not healthy yet;
// the next interval tries again.
func (s *supervisor) graftHolders() {
	s.mu.Lock()
	dead := map[string]bool{}
	stage := map[string]string{}
	for _, v := range s.vols {
		for _, d := range v.Dead {
			dead[d] = true
			stage[d] = v.StagePath
		}
	}
	s.mu.Unlock()
	if len(dead) == 0 {
		return
	}
	if _, err := os.Stat(nsmountHeal); err != nil {
		missingHelper.Do(func() {
			glog.Errorf("supervisor: %s missing -- running containers keep their dead mounts", nsmountHeal)
		})
		return
	}
	keep := map[string]bool{}
	for d := range dead {
		if isDead, _ := mountDead(stage[d]); isDead {
			keep[d] = true
		}
	}
	for _, h := range findHolders("/proc", dead) {
		if keep[h.dev] {
			continue
		}
		src := filepath.Join(stage[h.dev], h.root)
		args := []string{strconv.Itoa(h.pid), src, h.mountPoint}
		if f := graftFlags(h.opts); f != "" {
			args = append(args, f)
		}
		out, err := exec.Command(nsmountHeal, args...).CombinedOutput()
		if err != nil {
			glog.Errorf("supervisor: graft of %s into pid %d at %s: %v (%s)", src, h.pid, h.mountPoint, err,
				strings.TrimSpace(string(out)))
			keep[h.dev] = true
			continue
		}
		glog.Infof("supervisor: grafted %s into pid %d at %s", src, h.pid, h.mountPoint)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	changed := false
	for _, v := range s.vols {
		left := v.Dead[:0]
		for _, d := range v.Dead {
			if keep[d] {
				left = append(left, d)
			} else {
				changed = true
			}
		}
		v.Dead = left
	}
	if changed {
		s.persist()
	}
}
