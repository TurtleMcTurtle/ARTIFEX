// Package archivefs keeps portable archive IO relative to pinned directories.
package archivefs

import (
	"crypto/rand"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Check rejects pre-existing symlinks, including nested output parents. It is
// not the confinement boundary: every subsequent operation must use root, whose
// descriptor prevents an ancestor swap after the check from escaping the tree.
func Check(root *os.Root, name string) error {
	if !filepath.IsLocal(name) {
		return fmt.Errorf("archive path is not local: %q", name)
	}
	current := "."
	for _, part := range strings.Split(filepath.Clean(name), string(filepath.Separator)) {
		current = filepath.Join(current, part)
		info, err := root.Lstat(current)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("archive path contains a symlink: %s", current)
		}
	}
	return nil
}

func OpenFile(root *os.Root, name string, flag int, perm fs.FileMode) (*os.File, error) {
	if err := Check(root, name); err != nil {
		return nil, err
	}
	return root.OpenFile(name, flag, perm)
}

func ReadFile(root *os.Root, name string) ([]byte, error) {
	if err := Check(root, name); err != nil {
		return nil, err
	}
	return root.ReadFile(name)
}

func WriteFile(root *os.Root, name string, data []byte, perm fs.FileMode) error {
	if err := Check(root, name); err != nil {
		return err
	}
	return root.WriteFile(name, data, perm)
}

func MkdirAll(root *os.Root, name string, perm fs.FileMode) error {
	if err := Check(root, name); err != nil {
		return err
	}
	return root.MkdirAll(name, perm)
}

// Open pins a caller-owned directory for legacy path-based entrypoints. Task
// archive callers instead derive their roots from the already pinned data root,
// so even the opening of an archive or hot-store ancestor is confined.
func Open(path string, create bool, perm fs.FileMode) (*os.Root, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	ancestor := path
	for {
		info, err := os.Lstat(ancestor)
		if os.IsNotExist(err) && create {
			ancestor = filepath.Dir(ancestor)
			continue
		}
		if err != nil {
			return nil, err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("archive root is not a real directory: %s", ancestor)
		}
		root, err := os.OpenRoot(ancestor)
		if err != nil {
			return nil, err
		}
		pinned, statErr := root.Stat(".")
		if statErr != nil || !os.SameFile(info, pinned) {
			root.Close()
			return nil, fmt.Errorf("archive root changed while opening: %s", ancestor)
		}
		rel, err := filepath.Rel(ancestor, path)
		if err == nil && create {
			err = MkdirAll(root, rel, perm)
		}
		if err == nil {
			err = Check(root, rel)
		}
		if err != nil {
			root.Close()
			return nil, err
		}
		child, err := root.OpenRoot(rel)
		root.Close()
		return child, err
	}
}

func CreateTemp(root *os.Root, parent, prefix string, perm fs.FileMode) (*os.File, string, error) {
	if err := MkdirAll(root, parent, 0700); err != nil {
		return nil, "", err
	}
	for i := 0; i < 10; i++ {
		name := filepath.Join(parent, prefix+rand.Text())
		file, err := OpenFile(root, name, os.O_CREATE|os.O_EXCL|os.O_RDWR, perm)
		if os.IsExist(err) {
			continue
		}
		return file, name, err
	}
	return nil, "", fmt.Errorf("could not create archive staging file")
}
