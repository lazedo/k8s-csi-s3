package driver

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	corev1 "k8s.io/api/core/v1"
)

// The key prefix of a mount is its prefix as a directory.
func TestListenPrefix(t *testing.T) {
	for in, want := range map[string]string{"": "", "/": "", "pcaps": "pcaps/", "pcaps/": "pcaps/", "/a/b/": "a/b/"} {
		if got := listenPrefix(in); got != want {
			t.Errorf("listenPrefix(%q) = %q, want %q", in, got, want)
		}
	}
}

// A change alters the listing of the directory holding the object, relative
// to the mount; a key outside the mount alters nothing.
func TestChangedDir(t *testing.T) {
	cases := []struct {
		prefix, key, dir string
		ok               bool
	}{
		{"", "kazoo-flow-panel-1.0.124.zip", ".", true},
		{"", "en/us/callie/digits/8000/1.wav", "en/us/callie/digits/8000", true},
		{"", "en/us/", "en", true}, // a directory marker
		{"pcaps/", "pcaps/2026/10/08/a.pcap", "2026/10/08", true},
		{"pcaps/", "pcaps/a.pcap", ".", true},
		{"pcaps/", "recordings/a.mp3", "", false},
		{"pcaps/", "pcaps/", "", false},
	}
	for _, c := range cases {
		dir, ok := changedDir(c.prefix, c.key)
		if dir != c.dir || ok != c.ok {
			t.Errorf("changedDir(%q, %q) = %q, %v; want %q, %v", c.prefix, c.key, dir, ok, c.dir, c.ok)
		}
	}
}

// Dropping a directory's cache drops its subtree's: only the outermost
// directories of a burst are dropped.
func TestOutermost(t *testing.T) {
	cases := []struct{ in, want []string }{
		{[]string{"en/us/callie", "en/us", "fr/ca"}, []string{"en/us", "fr/ca"}},
		{[]string{"a-b", "a/b", "a"}, []string{"a", "a-b"}},
		{[]string{"music/8000", ".", "en"}, []string{"."}},
		{[]string{"x", "x"}, []string{"x"}},
		{nil, nil},
	}
	for _, c := range cases {
		if got := outermost(append([]string(nil), c.in...)); !reflect.DeepEqual(got, c.want) {
			t.Errorf("outermost(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}

// Read-only volumes are the ones only read wherever they are used.
func TestReadOnly(t *testing.T) {
	mode := func(m csi.VolumeCapability_AccessMode_Mode) *csi.VolumeCapability {
		return &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{Mode: m}}
	}
	if !readOnlyCapability(mode(csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY)) ||
		!readOnlyCapability(mode(csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY)) {
		t.Error("reader-only capabilities are read-only")
	}
	if readOnlyCapability(mode(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)) || readOnlyCapability(nil) {
		t.Error("a writer capability, or none, is not read-only")
	}
	if !readOnlyModes([]corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany}) {
		t.Error("ReadOnlyMany is read-only")
	}
	if readOnlyModes([]corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany, corev1.ReadWriteMany}) || readOnlyModes(nil) {
		t.Error("a writer mode, or none, is not read-only")
	}
}

// Drops asked while one runs make one more, not one each, and the runner
// ends once nothing more was asked.
func TestDropAllCoalesces(t *testing.T) {
	w := &watch{pending: map[string]bool{}}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.cancel() // a drop of an ended watch does nothing: only the runner is tested
	for range 100 {
		w.dropAll()
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		w.mu.Lock()
		busy := w.dropping || w.dropAgain
		w.mu.Unlock()
		if !busy {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the drop runner never ended")
		}
		time.Sleep(time.Millisecond)
	}
}
