package driver

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

var ErrDigestMismatch = errors.New("digest mismatch")

type Local struct{ root string }

type Entry struct {
	Path      string
	Directory bool
	Size      int64
	Modified  int64
}

func NewLocal(root string) (*Local, error) {
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("create volume root: %w", err)
	}
	return &Local{root: root}, nil
}

func (d *Local) CreateVolume(volumeID string) error {
	return os.Mkdir(d.volumeRoot(volumeID), 0o750)
}

func (d *Local) DeleteVolume(volumeID string) error {
	return os.RemoveAll(d.volumeRoot(volumeID))
}

func (d *Local) CreateDirectory(volumeID, name string) error {
	target, err := d.resolve(volumeID, name)
	if err != nil {
		return err
	}
	return os.MkdirAll(target, 0o750)
}

func (d *Local) Write(volumeID, name string, content []byte, expectedDigest string) (string, error) {
	target, err := d.resolve(volumeID, name)
	if err != nil {
		return "", err
	}
	if expectedDigest != "" {
		current, err := digestFile(target)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		if current != expectedDigest {
			return "", ErrDigestMismatch
		}
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		return "", err
	}
	temporary, err := os.CreateTemp(filepath.Dir(target), ".harnessx-write-*")
	if err != nil {
		return "", err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o640); err != nil {
		temporary.Close()
		return "", err
	}
	if _, err := temporary.Write(content); err != nil {
		temporary.Close()
		return "", err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return "", err
	}
	if err := temporary.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(temporaryName, target); err != nil {
		return "", err
	}
	sum := sha256.Sum256(content)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func (d *Local) Read(volumeID, name string, offset, limit int64) ([]byte, int64, string, error) {
	target, err := d.resolve(volumeID, name)
	if err != nil {
		return nil, 0, "", err
	}
	file, err := os.Open(target)
	if err != nil {
		return nil, 0, "", err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, 0, "", err
	}
	if info.IsDir() {
		return nil, 0, "", fmt.Errorf("path is a directory")
	}
	if offset < 0 || offset > info.Size() {
		return nil, 0, "", fmt.Errorf("offset is outside the file")
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return nil, 0, "", err
	}
	if limit <= 0 || limit > 16<<20 {
		limit = 16 << 20
	}
	content, err := io.ReadAll(io.LimitReader(file, limit))
	if err != nil {
		return nil, 0, "", err
	}
	digest, err := digestFile(target)
	return content, info.Size(), digest, err
}

func (d *Local) List(volumeID, name string) ([]Entry, error) {
	target, err := d.resolve(volumeID, name)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(target)
	if err != nil {
		return nil, err
	}
	result := make([]Entry, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		relative := filepath.ToSlash(filepath.Join(name, entry.Name()))
		result = append(result, Entry{Path: relative, Directory: entry.IsDir(), Size: info.Size(), Modified: info.ModTime().UnixNano()})
	}
	return result, nil
}

func (d *Local) Delete(volumeID, name string, recursive bool) error {
	target, err := d.resolve(volumeID, name)
	if err != nil {
		return err
	}
	if name == "" {
		return fmt.Errorf("volume root cannot be deleted as a path")
	}
	if recursive {
		return os.RemoveAll(target)
	}
	return os.Remove(target)
}

func (d *Local) resolve(volumeID, name string) (string, error) {
	if strings.ContainsRune(name, '\x00') || strings.Contains(name, "\\") {
		return "", fmt.Errorf("invalid path")
	}
	clean := filepath.Clean(filepath.FromSlash(name))
	if clean == "." {
		clean = ""
	}
	if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes the volume")
	}
	root := d.volumeRoot(volumeID)
	target := filepath.Join(root, clean)
	if target != root && !strings.HasPrefix(target, root+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes the volume")
	}
	// The API never creates symlinks. Reject any operator-created symlink in an
	// existing path component so it cannot redirect access outside the root.
	current := root
	for _, component := range strings.Split(clean, string(filepath.Separator)) {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err == nil && info.Mode()&fs.ModeSymlink != 0 {
			return "", fmt.Errorf("symbolic links are not allowed in volume paths")
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
	}
	return target, nil
}

func (d *Local) volumeRoot(volumeID string) string { return filepath.Join(d.root, volumeID) }

func digestFile(name string) (string, error) {
	file, err := os.Open(name)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}
