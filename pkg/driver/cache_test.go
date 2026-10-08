package driver

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// The walk lists every directory under its roots once and stops with its
// context.
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
	// two subtrees: music, music/8000, kazoo-core/en, kazoo-core/en/us
	if n := warmUp(context.Background(), filepath.Join(root, "music"), filepath.Join(root, "kazoo-core/en")); n != 4 {
		t.Fatalf("listed %d directories under two roots, want 4", n)
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
