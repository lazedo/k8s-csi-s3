//go:build lab

package driver

// The reconnect lab: the real NodeStage/NodePublish mount a bucket and bind
// it into two running "containers" (private mount namespaces, one of them
// read-only); the mounter dies as in a plugin restart; a new driver -- the
// restarted plugin, reading the same state file -- heals the node mount and
// grafts it into both, without restarting them. Needs root, FUSE, geesefs on
// PATH and nsmount-heal at /usr/bin, and the bucket in LAB_S3_*:
//
//   go test -c -tags lab -o driver.test ./pkg/driver
//   ./driver.test -test.run TestLabReconnect -test.v

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"

	"github.com/yandex-cloud/k8s-csi-s3/pkg/mounter"
)

func TestLabReconnect(t *testing.T) {
	endpoint := os.Getenv("LAB_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("LAB_S3_ENDPOINT unset")
	}
	ctx := context.Background()
	volumeID := os.Getenv("LAB_S3_BUCKET")
	secrets := map[string]string{"accessKeyID": os.Getenv("LAB_S3_KEY"),
		"secretAccessKey": os.Getenv("LAB_S3_SECRET"), "endpoint": endpoint}
	volCtx := map[string]string{mounter.TypeKey: "geesefs",
		mounter.OptionsKey: "--no-systemd --memory-limit 256 --dir-mode 0777 --file-mode 0777 --stat-cache-ttl 87600h --list-type 2"}
	capability := &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
	}
	newDriver := func() *driver {
		d := &driver{name: driverName, nodeID: "lab"}
		d.ns = &nodeServer{driver: d}
		d.sup = newSupervisor(d)
		return d
	}
	_ = os.Remove(filepath.Join(os.TempDir(), stateFileName))

	root := "/var/lib/kubelet"
	stage := filepath.Join(root, "plugins/kubernetes.io/csi", driverName, "lab/globalmount")
	targets := []string{filepath.Join(root, "pods/rw/volumes/kubernetes.io~csi/pv/mount"),
		filepath.Join(root, "pods/ro/volumes/kubernetes.io~csi/pv/mount")}
	if err := os.MkdirAll(stage, 0o750); err != nil {
		t.Fatal(err)
	}

	d1 := newDriver()
	if _, err := d1.ns.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{VolumeId: volumeID,
		StagingTargetPath: stage, VolumeCapability: capability, Secrets: secrets, VolumeContext: volCtx}); err != nil {
		t.Fatalf("NodeStageVolume: %v", err)
	}
	for _, target := range targets {
		if _, err := d1.ns.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{VolumeId: volumeID,
			StagingTargetPath: stage, TargetPath: target, VolumeCapability: capability, Secrets: secrets,
			VolumeContext: volCtx}); err != nil {
			t.Fatalf("NodePublishVolume %s: %v", target, err)
		}
	}

	// the "containers": a private mount namespace each, holding its target
	type consumer struct {
		cmd *exec.Cmd
		at  string
	}
	start := func(target, at string, ro bool) consumer {
		script := "mkdir -p " + at + " && mount --bind " + target + " " + at
		if ro {
			script += " && mount -o remount,bind,ro " + at
		}
		cmd := exec.Command("unshare", "-m", "--propagation", "private", "sh", "-c", script+" && exec sleep 600")
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
		return consumer{cmd: cmd, at: at}
	}
	in := func(c consumer, sh string) (string, error) {
		out, err := exec.Command("nsenter", "-t", strconv.Itoa(c.cmd.Process.Pid), "-m", "sh", "-c", sh).CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}
	read := func(c consumer) string {
		out, _ := in(c, "cat "+c.at+"/hello.txt")
		return out
	}
	rw, ro := start(targets[0], "/c-rw", false), start(targets[1], "/c-ro", true)
	for i := 0; i < 50 && (read(rw) != "hello" || read(ro) != "hello"); i++ {
		time.Sleep(100 * time.Millisecond)
	}
	if read(rw) != "hello" || read(ro) != "hello" {
		t.Fatalf("before: rw %q, ro %q", read(rw), read(ro))
	}

	// the plugin restart: its mounter dies with it
	p, err := mounter.FindFuseMountProcess(stage)
	if err != nil || p == nil {
		t.Fatalf("the mounter of %s: %v %v", stage, p, err)
	}
	if err := p.Kill(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	if got := read(rw); !strings.Contains(got, "not connected") {
		t.Fatalf("after the mounter died the container should see a dead mount, got %q", got)
	}
	t.Logf("mounter killed: the containers see %q", read(rw))

	// the restarted plugin: a new driver on the same state file
	d2 := newDriver()
	d2.sup.checkAll(ctx)

	if got := read(rw); got != "hello" {
		t.Errorf("rw container after the heal: %q", got)
	}
	if got := read(ro); got != "hello" {
		t.Errorf("ro container after the heal: %q", got)
	}
	if out, err := in(rw, "echo grafted > "+rw.at+"/written-after-graft.txt && sync"); err != nil {
		t.Errorf("rw container cannot write after the graft: %v %s", err, out)
	}
	if out, err := in(ro, "echo nope > "+ro.at+"/nope.txt"); err == nil || !strings.Contains(out, "Read-only") {
		t.Errorf("ro container must stay read-only after the graft: %v %q", err, out)
	}
	d2.sup.mu.Lock()
	left := d2.sup.vols[volumeID].Dead
	d2.sup.mu.Unlock()
	if len(left) != 0 {
		t.Errorf("dead devices still remembered after every holder was grafted: %v", left)
	}
}
