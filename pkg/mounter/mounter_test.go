package mounter

import (
	"os"
	"path/filepath"
	"testing"
)

// A target that is gone, or a directory that is no longer a mount, is already
// unpublished: Unmount succeeds and leaves nothing behind.
func TestUnmountIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	if err := Unmount(filepath.Join(dir, "gone")); err != nil {
		t.Errorf("a target that is gone: %v", err)
	}
	stale := filepath.Join(dir, "mount")
	if err := os.Mkdir(stale, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := Unmount(stale); err != nil {
		t.Errorf("a directory that is no longer a mount: %v", err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("the directory stays behind: %v", err)
	}
}
