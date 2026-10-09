package driver

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

const sampleMountinfo = `22 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw
120 22 0:52 / /var/lib/kubelet/plugins/kubernetes.io/csi/csi.lazedo.dev/abc/globalmount rw,nosuid,nodev,relatime shared:60 - fuse.geesefs kazoo-sounds: rw,user_id=0,group_id=0
121 22 0:52 / /var/lib/kubelet/pods/p1/volumes/kubernetes.io~csi/pv1/mount rw,nosuid,nodev,relatime shared:60 - fuse.geesefs kazoo-sounds: rw,user_id=0,group_id=0
122 22 0:52 /Call\040Trace /etc/dash\040boards ro,nosuid,nodev,relatime master:60 - fuse.geesefs obs: rw
`

func TestParseMountinfo(t *testing.T) {
	got := parseMountinfo([]byte(sampleMountinfo))
	if len(got) != 4 {
		t.Fatalf("got %d entries, want 4", len(got))
	}
	want := mountEntry{dev: "0:52", root: "/Call Trace", mountPoint: "/etc/dash boards",
		opts: "ro,nosuid,nodev,relatime", fstype: "fuse.geesefs"}
	if got[3] != want {
		t.Errorf("subPath mount with escapes: got %+v, want %+v", got[3], want)
	}
	if got[0].fstype != "ext4" || got[1].dev != "0:52" {
		t.Errorf("plain lines: %+v %+v", got[0], got[1])
	}
}

func TestMountDevice(t *testing.T) {
	f := filepath.Join(t.TempDir(), "mountinfo")
	stacked := sampleMountinfo +
		"130 120 0:61 / /var/lib/kubelet/plugins/kubernetes.io/csi/csi.lazedo.dev/abc/globalmount rw - fuse.geesefs kazoo-sounds: rw\n"
	if err := os.WriteFile(f, []byte(stacked), 0o600); err != nil {
		t.Fatal(err)
	}
	if d := mountDevice(f, "/var/lib/kubelet/plugins/kubernetes.io/csi/csi.lazedo.dev/abc/globalmount"); d != "0:61" {
		t.Errorf("the topmost of a stack: got %q, want 0:61", d)
	}
	if d := mountDevice(f, "/nowhere"); d != "" {
		t.Errorf("no mount there: got %q", d)
	}
}

// One process speaks for each mount namespace; the node's (pid 1) and our
// own are left alone, and only FUSE mounts of the dead devices count.
func TestFindHolders(t *testing.T) {
	proc := t.TempDir()
	add := func(pid, ns, mountinfo string) {
		d := filepath.Join(proc, pid)
		if err := os.MkdirAll(filepath.Join(d, "ns"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(ns, filepath.Join(d, "ns", "mnt")); err != nil {
			t.Fatal(err)
		}
		if mountinfo != "" {
			if err := os.WriteFile(filepath.Join(d, "mountinfo"), []byte(mountinfo), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	dead := "0:52"
	holding := "300 1 0:52 / /data rw,nosuid,nodev - fuse.geesefs b: rw\n" +
		"301 1 0:52 /sub /cfg ro,nosuid,nodev - fuse.geesefs b: rw\n" +
		"302 1 0:99 / /other rw - fuse.geesefs c: rw\n" +
		"303 1 0:52 / /notfuse rw - ext4 /dev/x rw\n"
	add("1", "mnt:[1]", holding)    // the node
	add("self", "mnt:[2]", holding) // this plugin
	add("10", "mnt:[2]", holding)   // a thread of the plugin's namespace
	add("20", "mnt:[3]", holding)   // a container
	add("21", "mnt:[3]", holding)   // the same container again
	add("30", "mnt:[4]", "")        // gone before its mountinfo was read
	add("31", "mnt:[4]", holding)   // ... the same namespace through another process

	got := findHolders(proc, map[string]bool{dead: true})
	byPID := map[int][]string{}
	for _, h := range got {
		byPID[h.pid] = append(byPID[h.pid], h.mountPoint+"|"+h.root+"|"+h.opts)
	}
	want := map[int][]string{
		20: {"/data|/|rw,nosuid,nodev", "/cfg|/sub|ro,nosuid,nodev"},
		31: {"/data|/|rw,nosuid,nodev", "/cfg|/sub|ro,nosuid,nodev"},
	}
	if !reflect.DeepEqual(byPID, want) {
		t.Errorf("holders: got %v, want %v", byPID, want)
	}
}

func TestGraftFlags(t *testing.T) {
	for in, want := range map[string]string{
		"ro,nosuid,nodev,relatime":       "ro,nosuid,nodev",
		"rw,relatime":                    "",
		"rw,nosuid,nodev,noexec,noatime": "nosuid,nodev,noexec",
	} {
		if got := graftFlags(in); got != want {
			t.Errorf("graftFlags(%q) = %q, want %q", in, got, want)
		}
	}
}
