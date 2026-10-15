package evidence

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"

	"github.com/sebastian93921/artifex/internal/archivefs"
	"github.com/sebastian93921/artifex/locale"
)

// Install a verified body using only rooted operations. Replacing an existing
// verified body with the same bytes refreshes the restore grace period without
// a path-based Chtimes operation that could follow a swapped ancestor/link.
func installArchiveBody(root *os.Root, input io.Reader, length int64, hash string) error {
	target, err := hashPath(".", hash)
	if err != nil {
		return err
	}
	if err := archivefs.Check(root, target); err != nil {
		return err
	}
	f, staged, err := archivefs.CreateTemp(root, ".staging", "body-", 0600)
	if err != nil {
		return err
	}
	defer func() { f.Close(); root.Remove(staged) }()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), input)
	if err != nil {
		return err
	}
	if n != length || hex.EncodeToString(h.Sum(nil)) != hash {
		return locale.Errorf("Evidence body verification failed: %s", hash)
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := archivefs.Check(root, target); err != nil {
		return err
	}
	if _, err := root.Stat(target); err == nil {
		existing, err := openArchiveBody(root, hash, length)
		if err != nil {
			return err
		}
		if err := existing.Close(); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := archivefs.MkdirAll(root, filepath.Dir(target), 0700); err != nil {
		return err
	}
	if err := archivefs.Check(root, target); err != nil {
		return err
	}
	if err := root.Rename(staged, target); err != nil {
		return err
	}
	dir, err := archivefs.OpenFile(root, filepath.Dir(target), os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
