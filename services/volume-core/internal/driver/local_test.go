package driver

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalWriteReadAndDigestGuard(t *testing.T) {
	backend, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.CreateVolume("vol_test"); err != nil {
		t.Fatal(err)
	}
	digest, err := backend.Write("vol_test", "dir/file.txt", []byte("first"), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Write("vol_test", "dir/file.txt", []byte("second"), "sha256:wrong"); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("expected digest mismatch, got %v", err)
	}
	newDigest, err := backend.Write("vol_test", "dir/file.txt", []byte("second"), digest)
	if err != nil || newDigest == digest {
		t.Fatalf("digest=%q err=%v", newDigest, err)
	}
	content, total, readDigest, err := backend.Read("vol_test", "dir/file.txt", 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "eco" || total != 6 || readDigest != newDigest {
		t.Fatalf("content=%q total=%d digest=%q", content, total, readDigest)
	}
}

func TestLocalRejectsEscapesAndSymlinks(t *testing.T) {
	root := t.TempDir()
	backend, err := NewLocal(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.CreateVolume("vol_test"); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"../outside", "/absolute", `dir\file`} {
		if _, err := backend.Write("vol_test", path, []byte("x"), ""); err == nil {
			t.Fatalf("path %q was accepted", path)
		}
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(root, "vol_test", "link")); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Write("vol_test", "link/file", []byte("x"), ""); err == nil {
		t.Fatal("symlink path was accepted")
	}
}
