package driver

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/yandex-cloud/k8s-csi-s3/pkg/mounter"
)

// The walk lists every directory of the tree once and stops with its context.
func TestWarmUp(t *testing.T) {
	root := t.TempDir()
	for _, d := range []string{"en/us/callie/digits/8000", "en/us/callie/digits/48000", "en/us/allison/time/8000", "kazoo-core/en/us", "music/8000"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, d, "1.wav"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// root, en, en/us, callie, digits, 8000, 48000, allison, time, 8000, kazoo-core, kazoo-core/en, kazoo-core/en/us, music, music/8000
	if n := warmUp(context.Background(), root); n != 15 {
		t.Fatalf("listed %d directories, want 15", n)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if n := warmUp(ctx, root); n != 0 {
		t.Fatalf("cancelled walk listed %d", n)
	}
	if n := warmUp(context.Background(), filepath.Join(root, "missing")); n != 0 {
		t.Fatalf("missing root listed %d", n)
	}
}

// A request is answered once; the same value again is not a request.
func TestNeedsRefresh(t *testing.T) {
	for _, tc := range []struct {
		requested, answered string
		want                bool
	}{
		{"", "", false},
		{"2026-09-23T16:00:00Z", "", true},
		{"2026-09-23T16:00:00Z", "2026-09-23T16:00:00Z", false},
		{"2026-09-23T17:00:00Z", "2026-09-23T16:00:00Z", true},
	} {
		if got := needsRefresh(tc.requested, tc.answered); got != tc.want {
			t.Errorf("needsRefresh(%q, %q) = %v", tc.requested, tc.answered, got)
		}
	}
}

// --warm-up is read off the StorageClass options in the volume context.
func TestWantsWarmUp(t *testing.T) {
	if !wantsWarmUp(map[string]string{mounter.OptionsKey: "--no-systemd --stat-cache-ttl 87600h --list-type 2 --warm-up"}) {
		t.Fatal("--warm-up not seen")
	}
	if wantsWarmUp(map[string]string{mounter.OptionsKey: "--no-systemd --memory-limit 256"}) || wantsWarmUp(nil) {
		t.Fatal("warm-up without the option")
	}
}
