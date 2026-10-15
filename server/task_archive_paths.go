package server

import (
	"crypto/rand"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// taskArchiveFS pins the configured data directory. Checks reject symlinked
// managed paths; os.Root also confines IO if a parent changes after a check.
// A database maintenance lock is still required for cooperating workers.
type taskArchiveFS struct {
	root *os.Root
	dir  string
}

func openTaskArchiveFS(dataDir string) (*taskArchiveFS, error) {
	dir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("task maintenance root is not a real directory")
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	pinned, err := root.Stat(".")
	if err != nil || !os.SameFile(info, pinned) {
		root.Close()
		return nil, fmt.Errorf("task maintenance root changed while opening: %w", err)
	}
	return &taskArchiveFS{root: root, dir: dir}, nil
}

func (f *taskArchiveFS) Close() error { return f.root.Close() }

func (f *taskArchiveFS) relative(path string) (string, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(f.dir, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("task maintenance path is outside the managed directory")
	}
	current := "."
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		current = filepath.Join(current, part)
		info, err := f.root.Lstat(current)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return "", err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("task maintenance path contains a symlink: %s", current)
		}
	}
	return rel, nil
}

func (f *taskArchiveFS) check(paths ...string) error {
	for _, path := range paths {
		if _, err := f.relative(path); err != nil {
			return err
		}
	}
	return nil
}

func (f *taskArchiveFS) Lstat(path string) (os.FileInfo, error) {
	rel, err := f.relative(path)
	if err != nil {
		return nil, err
	}
	return f.root.Lstat(rel)
}

func (f *taskArchiveFS) ReadFile(path string) ([]byte, error) {
	rel, err := f.relative(path)
	if err != nil {
		return nil, err
	}
	return f.root.ReadFile(rel)
}

func (f *taskArchiveFS) ReadDir(path string) ([]os.DirEntry, error) {
	rel, err := f.relative(path)
	if err != nil {
		return nil, err
	}
	return fs.ReadDir(f.root.FS(), rel)
}

func (f *taskArchiveFS) OpenFile(path string, flag int, perm os.FileMode) (*os.File, error) {
	rel, err := f.relative(path)
	if err != nil {
		return nil, err
	}
	return f.root.OpenFile(rel, flag, perm)
}

// OpenRoot derives a pinned subtree without reopening an absolute path. The
// returned root remains confined even if a managed ancestor is replaced later.
func (f *taskArchiveFS) OpenRoot(path string) (*os.Root, error) {
	rel, err := f.relative(path)
	if err != nil {
		return nil, err
	}
	return f.root.OpenRoot(rel)
}

func (f *taskArchiveFS) MkdirAll(path string, perm os.FileMode) error {
	rel, err := f.relative(path)
	if err != nil {
		return err
	}
	return f.root.MkdirAll(rel, perm)
}

func (f *taskArchiveFS) MkdirTemp(parent, prefix string) (string, error) {
	rel, err := f.relative(parent)
	if err != nil {
		return "", err
	}
	for i := 0; i < 10; i++ {
		name := prefix + rand.Text()
		if err := f.root.Mkdir(filepath.Join(rel, name), archiveDirMode); os.IsExist(err) {
			continue
		} else if err != nil {
			return "", err
		}
		return filepath.Join(parent, name), nil
	}
	return "", fmt.Errorf("could not allocate task archive extraction directory")
}

func (f *taskArchiveFS) Rename(source, destination string) error {
	src, err := f.relative(source)
	if err != nil {
		return err
	}
	dst, err := f.relative(destination)
	if err != nil {
		return err
	}
	return f.root.Rename(src, dst)
}

func (f *taskArchiveFS) RemoveAll(path string) error {
	rel, err := f.relative(path)
	if err != nil {
		return err
	}
	if rel == "." {
		return fmt.Errorf("refuse to remove the task maintenance root")
	}
	return f.root.RemoveAll(rel)
}

func (f *taskArchiveFS) Remove(path string) error {
	rel, err := f.relative(path)
	if err != nil {
		return err
	}
	return f.root.Remove(rel)
}
