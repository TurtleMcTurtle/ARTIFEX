package server

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sebastian93921/artifex/locale"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/klauspost/compress/zstd"
	pgdb "github.com/sebastian93921/artifex/db"
)

const (
	archiveDirMode  = 0o700
	archiveFileMode = 0o600
	maxArchiveFiles = 1_000_000
	maxArchiveBytes = int64(1 << 47) // 128 TiB safety ceiling for corrupt headers.
)

type archiveFileMove struct {
	Source   string `json:"source"`
	Relative string `json:"relative"`
}

type archiveStageJournal struct {
	ArchiveID int64             `json:"archive_id"`
	TaskID    string            `json:"task_id"`
	Moves     []archiveFileMove `json:"moves"`
}

type archiveRestoreJournal struct {
	ArchiveID int64                 `json:"archive_id"`
	TaskID    int64                 `json:"task_id"`
	Moves     []restoredArchivePath `json:"moves"`
}

type taskArchiveFileStage struct {
	dataDir string
	root    string
	payload string
	journal archiveStageJournal
	done    bool
}

type restoredArchivePath struct {
	Source      string
	Destination string
}

type taskArchiveRestoreFiles struct {
	dataDir   string
	taskID    string
	extracted string
	moves     []restoredArchivePath
	done      bool
}

func writeArchiveJournal(path string, value any) error {
	return writeArchiveJournalFile(func() (*os.File, error) {
		return os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, archiveFileMode)
	}, value)
}

func writeManagedArchiveJournal(f *taskArchiveFS, path string, value any) error {
	return writeArchiveJournalFile(func() (*os.File, error) {
		return f.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, archiveFileMode)
	}, value)
}

func writeArchiveJournalFile(open func() (*os.File, error), value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	file, err := open()
	if err != nil {
		return err
	}
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// Validate the entire journal before applying or reversing even its first move.
func (s *taskArchiveFileStage) validate(f *taskArchiveFS) error {
	id, err := strconv.ParseInt(s.journal.TaskID, 10, 64)
	if err != nil || id <= 0 || strconv.FormatInt(id, 10) != s.journal.TaskID || s.journal.ArchiveID <= 0 {
		return locale.NewError("Invalid task archive staging journal identity")
	}
	expected := filepath.Join(taskArchiveRoot(f.dir), ".staging", strconv.FormatInt(s.journal.ArchiveID, 10))
	if s.root != expected || s.payload != filepath.Join(expected, "payload") {
		return locale.NewError("Task archive staging journal directory mismatch")
	}
	if err := f.check(s.root, s.payload, filepath.Join(s.root, "journal.json"),
		filepath.Join(s.payload, "files"), filepath.Join(s.payload, "tables"),
		filepath.Join(s.payload, "traffic"), filepath.Join(s.payload, "evidence")); err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, move := range s.journal.Moves {
		if err := validateArchiveFileMove(f, s.payload, move.Source, move.Relative, s.journal.TaskID); err != nil {
			return err
		}
		if seen[move.Relative] {
			return locale.NewError("Duplicate task archive journal path")
		}
		seen[move.Relative] = true
	}
	return nil
}

func validateArchiveFileMove(f *taskArchiveFS, payload, source, relative, taskID string) error {
	if relative == "" || filepath.IsAbs(relative) || filepath.Clean(relative) != relative {
		return locale.NewError("Invalid task archive journal relative path")
	}
	parts := strings.Split(relative, string(filepath.Separator))
	if len(parts) != 3 || parts[0] != "files" ||
		!(parts[1] == "tasks" && parts[2] == taskID || parts[1] == "transcripts" && parts[2] != "" && parts[2] != "." && parts[2] != "..") {
		return locale.NewError("Task archive journal path is not a task workspace or transcript")
	}
	if source != filepath.Join(f.dir, parts[1], parts[2]) {
		return locale.NewError("Task archive journal source does not match its payload path")
	}
	return f.check(source, filepath.Join(payload, relative))
}

func (s *taskArchiveRestoreFiles) validate(f *taskArchiveFS) error {
	id, err := strconv.ParseInt(s.taskID, 10, 64)
	if err != nil || id <= 0 || strconv.FormatInt(id, 10) != s.taskID || s.extracted == f.dir {
		return locale.NewError("Invalid task archive restore journal identity")
	}
	if err := f.check(s.extracted, filepath.Join(s.extracted, "restore-journal.json")); err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, move := range s.moves {
		relative, err := filepath.Rel(s.extracted, move.Source)
		if err != nil {
			return err
		}
		if err := validateArchiveFileMove(f, s.extracted, move.Destination, relative, s.taskID); err != nil {
			return err
		}
		if seen[relative] {
			return locale.NewError("Duplicate task restore journal path")
		}
		seen[relative] = true
	}
	return nil
}

func taskArchiveRoot(dataDir string) string {
	return filepath.Join(dataDir, "archives", "tasks")
}

func taskArchivePath(dataDir string, archiveID int64, taskID string) string {
	return filepath.Join(taskArchiveRoot(dataDir), fmt.Sprintf("task-%s-%d.tar.zst", taskID, archiveID))
}

func stageTaskArchiveFiles(dataDir string, archiveID int64, taskID string, explorationID int64) (*taskArchiveFileStage, error) {
	f, err := openTaskArchiveFS(dataDir)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dataDir = f.dir
	root := filepath.Join(taskArchiveRoot(dataDir), ".staging", strconv.FormatInt(archiveID, 10))
	stage := &taskArchiveFileStage{dataDir: dataDir, root: root, payload: filepath.Join(root, "payload")}
	journalPath := filepath.Join(root, "journal.json")
	if err := f.check(root, journalPath, filepath.Join(dataDir, "tasks", taskID), filepath.Join(dataDir, "transcripts")); err != nil {
		return nil, err
	}
	if raw, err := f.ReadFile(journalPath); err == nil {
		if err := json.Unmarshal(raw, &stage.journal); err != nil {
			return nil, locale.Errorf("read task archive staging journal: %w", err)
		}
		if stage.journal.ArchiveID != archiveID || stage.journal.TaskID != taskID {
			return nil, locale.NewError("Task archive staging journal identity mismatch")
		}
		if err := stage.validate(f); err != nil {
			return nil, err
		}
		// A complete validated plan precedes all moves, so replay can safely
		// resume a prefix interrupted before compaction.
		if err := f.MkdirAll(filepath.Join(stage.payload, "files"), archiveDirMode); err != nil {
			return nil, err
		}
		if err := stage.applyMoves(); err != nil {
			_ = stage.rollback()
			return nil, err
		}
		return stage, nil
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	stage.journal = archiveStageJournal{ArchiveID: archiveID, TaskID: taskID}
	taskDir := filepath.Join(dataDir, "tasks", taskID)
	if _, err := f.Lstat(taskDir); err == nil {
		stage.journal.Moves = append(stage.journal.Moves, archiveFileMove{Source: taskDir, Relative: filepath.Join("files", "tasks", taskID)})
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	transcriptDir := filepath.Join(dataDir, "transcripts")
	entries, err := f.ReadDir(transcriptDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	prefix := fmt.Sprintf("exp%d-", explorationID)
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) || (!entry.IsDir() && !strings.HasSuffix(name, ".jsonl")) {
			continue
		}
		stage.journal.Moves = append(stage.journal.Moves, archiveFileMove{Source: filepath.Join(transcriptDir, name), Relative: filepath.Join("files", "transcripts", name)})
	}
	if err := stage.validate(f); err != nil {
		return nil, err
	}
	if err := f.MkdirAll(filepath.Join(stage.payload, "files"), archiveDirMode); err != nil {
		return nil, err
	}
	if err := writeManagedArchiveJournal(f, journalPath, stage.journal); err != nil {
		_ = stage.rollback()
		return nil, err
	}
	if err := stage.applyMoves(); err != nil {
		_ = stage.rollback()
		return nil, err
	}
	return stage, nil
}

// applyMoves performs the renames recorded in the journal. Entries already sitting
// in the payload are skipped, so an interrupted staging round can be resumed in
// place without moving anything twice.
func (s *taskArchiveFileStage) applyMoves() error {
	f, err := openTaskArchiveFS(s.dataDir)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := s.validate(f); err != nil {
		return err
	}
	for _, move := range s.journal.Moves {
		destination := filepath.Join(s.payload, move.Relative)
		if _, err := f.Lstat(destination); err == nil {
			continue
		} else if !os.IsNotExist(err) {
			return err
		}
		if _, err := f.Lstat(move.Source); err != nil {
			if os.IsNotExist(err) {
				// Neither side exists: the path was removed outside the archive flow
				// after the journal was written. Nothing can be staged for it.
				continue
			}
			return err
		}
		if err := f.MkdirAll(filepath.Dir(destination), archiveDirMode); err != nil {
			return err
		}
		if err := f.Rename(move.Source, destination); err != nil {
			return locale.Errorf("stage task archive path %s: %w", move.Source, err)
		}
	}
	return nil
}

func (s *taskArchiveFileStage) rollback() error {
	if s == nil || s.done {
		return nil
	}
	f, err := openTaskArchiveFS(s.dataDir)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := s.validate(f); err != nil {
		return err
	}
	var errs []error
	for i := len(s.journal.Moves) - 1; i >= 0; i-- {
		move := s.journal.Moves[i]
		staged := filepath.Join(s.payload, move.Relative)
		if _, err := f.Lstat(staged); os.IsNotExist(err) {
			continue
		} else if err != nil {
			errs = append(errs, err)
			continue
		}
		if _, err := f.Lstat(move.Source); err == nil {
			errs = append(errs, locale.Errorf("archive rollback destination exists: %s", move.Source))
			continue
		} else if !os.IsNotExist(err) {
			errs = append(errs, err)
			continue
		}
		if err := f.MkdirAll(filepath.Dir(move.Source), 0o755); err != nil {
			errs = append(errs, err)
			continue
		}
		if err := f.Rename(staged, move.Source); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		if err := f.RemoveAll(s.root); err != nil {
			errs = append(errs, err)
		}
	}
	s.done = true
	return errors.Join(errs...)
}

func (s *taskArchiveFileStage) commit() error {
	if s == nil || s.done {
		return nil
	}
	f, err := openTaskArchiveFS(s.dataDir)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := s.validate(f); err != nil {
		return err
	}
	s.done = true
	return f.RemoveAll(s.root)
}

func installTaskArchiveFiles(dataDir, extractedDir, taskID string, archiveID int64) (*taskArchiveRestoreFiles, error) {
	f, err := openTaskArchiveFS(dataDir)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dataDir = f.dir
	extractedDir, err = filepath.Abs(extractedDir)
	if err != nil {
		return nil, err
	}
	if err := f.check(extractedDir, filepath.Join(extractedDir, "files", "tasks", taskID), filepath.Join(extractedDir, "files", "transcripts")); err != nil {
		return nil, err
	}
	stage := &taskArchiveRestoreFiles{dataDir: dataDir, taskID: taskID, extracted: extractedDir}
	numericTaskID, err := strconv.ParseInt(taskID, 10, 64)
	if err != nil || numericTaskID <= 0 || strconv.FormatInt(numericTaskID, 10) != taskID || archiveID <= 0 {
		return nil, locale.Errorf("invalid restore identity %q/%d", taskID, archiveID)
	}
	workspace := filepath.Join(extractedDir, "files", "tasks", taskID)
	if _, err := f.Lstat(workspace); err == nil {
		stage.moves = append(stage.moves, restoredArchivePath{Source: workspace, Destination: filepath.Join(dataDir, "tasks", taskID)})
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	transcripts := filepath.Join(extractedDir, "files", "transcripts")
	entries, err := f.ReadDir(transcripts)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	for _, entry := range entries {
		stage.moves = append(stage.moves, restoredArchivePath{Source: filepath.Join(transcripts, entry.Name()), Destination: filepath.Join(dataDir, "transcripts", entry.Name())})
	}
	if err := stage.validate(f); err != nil {
		return nil, err
	}
	for _, move := range stage.moves {
		if _, err := f.Lstat(move.Destination); err == nil {
			return nil, locale.Errorf("restore destination already exists: %s", move.Destination)
		} else if !os.IsNotExist(err) {
			return nil, err
		}
	}
	journal := archiveRestoreJournal{ArchiveID: archiveID, TaskID: numericTaskID, Moves: stage.moves}
	if err := writeManagedArchiveJournal(f, filepath.Join(extractedDir, "restore-journal.json"), journal); err != nil {
		return nil, err
	}
	for _, move := range stage.moves {
		if err := f.MkdirAll(filepath.Dir(move.Destination), 0o755); err != nil {
			_ = stage.rollback()
			return nil, err
		}
		if err := f.Rename(move.Source, move.Destination); err != nil {
			_ = stage.rollback()
			return nil, err
		}
	}
	return stage, nil
}

func (s *taskArchiveRestoreFiles) rollback() error {
	if s == nil || s.done {
		return nil
	}
	f, err := openTaskArchiveFS(s.dataDir)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := s.validate(f); err != nil {
		return err
	}
	var errs []error
	for i := len(s.moves) - 1; i >= 0; i-- {
		move := s.moves[i]
		if _, err := f.Lstat(move.Destination); os.IsNotExist(err) {
			continue
		} else if err != nil {
			errs = append(errs, err)
			continue
		}
		if _, err := f.Lstat(move.Source); err == nil {
			errs = append(errs, locale.Errorf("restore rollback source exists: %s", move.Source))
			continue
		} else if !os.IsNotExist(err) {
			errs = append(errs, err)
			continue
		}
		if err := f.MkdirAll(filepath.Dir(move.Source), archiveDirMode); err != nil {
			errs = append(errs, err)
			continue
		}
		if err := f.Rename(move.Destination, move.Source); err != nil {
			errs = append(errs, err)
		}
	}
	s.done = true
	return errors.Join(errs...)
}

func (s *taskArchiveRestoreFiles) commit() error {
	if s == nil || s.done {
		return nil
	}
	f, err := openTaskArchiveFS(s.dataDir)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := s.validate(f); err != nil {
		return err
	}
	s.done = true
	return f.RemoveAll(s.extracted)
}

// recoverTaskArchiveRestoreStages resolves file installs left by an interrupted
// restore. If PostgreSQL committed, installed files are authoritative and only
// the extraction directory is stale. Otherwise all completed renames are moved
// back so the persistent restore job can retry from a clean destination.
func recoverTaskArchiveRestoreStages(dataDir string, pg *pgdb.DB) error {
	f, err := openTaskArchiveFS(dataDir)
	if err != nil {
		return err
	}
	defer f.Close()
	parent := filepath.Join(taskArchiveRoot(f.dir), ".restore")
	entries, err := f.ReadDir(parent)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var errs []error
	for _, entry := range entries {
		root := filepath.Join(parent, entry.Name())
		if err := f.check(root, filepath.Join(root, "restore-journal.json")); err != nil {
			errs = append(errs, err)
			continue
		}
		if !entry.IsDir() {
			continue
		}
		raw, err := f.ReadFile(filepath.Join(root, "restore-journal.json"))
		if os.IsNotExist(err) {
			// Extraction had no install plan, so no destination rename ran.
			if err := f.RemoveAll(root); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		var journal archiveRestoreJournal
		if err := json.Unmarshal(raw, &journal); err != nil {
			errs = append(errs, err)
			continue
		}
		stage := &taskArchiveRestoreFiles{dataDir: f.dir, taskID: strconv.FormatInt(journal.TaskID, 10), extracted: root, moves: journal.Moves}
		if journal.ArchiveID <= 0 || !strings.HasPrefix(entry.Name(), strconv.FormatInt(journal.ArchiveID, 10)+"-") {
			errs = append(errs, locale.NewError("Task restore journal directory identity mismatch"))
			continue
		}
		if err := stage.validate(f); err != nil {
			errs = append(errs, err)
			continue
		}
		archive, err := pg.GetTaskArchive(journal.ArchiveID)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if archive != nil && archive.TaskID != journal.TaskID {
			errs = append(errs, locale.NewError("Task restore journal database identity mismatch"))
			continue
		}
		restored, err := pg.IsTaskArchiveRestored(journal.ArchiveID)
		if errors.Is(err, pgdb.ErrTaskArchiveNotFound) {
			task, taskErr := pg.GetTask(journal.TaskID)
			if taskErr != nil {
				errs = append(errs, taskErr)
				continue
			}
			restored, err = task != nil, nil
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !restored {
			if err := stage.rollback(); err != nil {
				errs = append(errs, err)
				continue
			}
		}
		if err := f.RemoveAll(root); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// recoverTaskArchiveStages resolves file renames left by an interrupted archive.
// A committed cold task already has a verified package, so stale staging can be
// discarded; otherwise files are moved back before the persistent job retries.
func recoverTaskArchiveStages(dataDir string, pg *pgdb.DB) error {
	f, err := openTaskArchiveFS(dataDir)
	if err != nil {
		return err
	}
	defer f.Close()
	parent := filepath.Join(taskArchiveRoot(f.dir), ".staging")
	entries, err := f.ReadDir(parent)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var errs []error
	for _, entry := range entries {
		root := filepath.Join(parent, entry.Name())
		if err := f.check(root, filepath.Join(root, "journal.json")); err != nil {
			errs = append(errs, err)
			continue
		}
		if !entry.IsDir() {
			continue
		}
		raw, err := f.ReadFile(filepath.Join(root, "journal.json"))
		if os.IsNotExist(err) {
			// The durable plan is written before renaming any hot path.
			if err := f.RemoveAll(root); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		var journal archiveStageJournal
		if err := json.Unmarshal(raw, &journal); err != nil {
			errs = append(errs, err)
			continue
		}
		stage := &taskArchiveFileStage{dataDir: f.dir, root: root, payload: filepath.Join(root, "payload"), journal: journal}
		if err := stage.validate(f); err != nil {
			errs = append(errs, err)
			continue
		}
		archive, err := pg.GetTaskArchive(journal.ArchiveID)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if archive != nil && strconv.FormatInt(archive.TaskID, 10) != journal.TaskID {
			errs = append(errs, locale.NewError("Task archive journal database identity mismatch"))
			continue
		}
		// Check even the package cleanup path before discarding any stage.
		if archive != nil && archive.ArchivePath != "" {
			if err := validateArchivePath(f.dir, archive.ArchivePath); err != nil {
				errs = append(errs, err)
				continue
			}
		}
		if archive != nil && archive.State == pgdb.ArchiveReady {
			if err := f.RemoveAll(root); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		if err := stage.rollback(); err != nil {
			errs = append(errs, err)
			continue
		}
		if archive != nil && archive.ArchivePath != "" {
			if err := f.Remove(archive.ArchivePath); err != nil && !os.IsNotExist(err) {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func recoverTaskArchiveDeletePackages(dataDir string, pg *pgdb.DB) error {
	f, err := openTaskArchiveFS(dataDir)
	if err != nil {
		return err
	}
	defer f.Close()
	root := taskArchiveRoot(f.dir)
	entries, err := f.ReadDir(root)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var errs []error
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		marker := strings.LastIndex(name, ".deleting-")
		if marker < 0 {
			continue
		}
		archiveID, err := strconv.ParseInt(name[marker+len(".deleting-"):], 10, 64)
		if err != nil || archiveID <= 0 {
			continue
		}
		staged := filepath.Join(root, name)
		original := filepath.Join(root, name[:marker])
		if err := f.check(staged, original); err != nil {
			errs = append(errs, err)
			continue
		}
		archive, err := pg.GetTaskArchive(archiveID)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if archive == nil {
			if err := f.Remove(staged); err != nil && !os.IsNotExist(err) {
				errs = append(errs, err)
			}
			continue
		}
		if archive.State == pgdb.DeleteQueued || archive.State == pgdb.Deleting || archive.State == pgdb.DeleteFailed {
			// The idempotent delete worker consumes the staged path directly.
			continue
		}
		if _, err := f.Lstat(original); err == nil {
			errs = append(errs, locale.Errorf("archive delete recovery destination exists: %s", original))
			continue
		} else if !os.IsNotExist(err) {
			errs = append(errs, err)
			continue
		}
		if err := f.Rename(staged, original); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func stageTaskArchivePackageDelete(dataDir, archivePath string, archiveID int64) (string, bool, error) {
	f, err := openTaskArchiveFS(dataDir)
	if err != nil {
		return "", false, err
	}
	defer f.Close()
	staged := archivePath + fmt.Sprintf(".deleting-%d", archiveID)
	if err := f.check(archivePath, staged); err != nil {
		return staged, false, err
	}
	if _, err := f.Lstat(staged); err == nil {
		if _, originalErr := f.Lstat(archivePath); originalErr == nil {
			return staged, false, locale.NewError("Both the original archive package and its deletion-stage file exist")
		} else if !os.IsNotExist(originalErr) {
			return staged, false, originalErr
		}
		return staged, true, nil
	} else if !os.IsNotExist(err) {
		return staged, false, err
	}
	if err := f.Rename(archivePath, staged); err == nil {
		return staged, true, nil
	} else if !os.IsNotExist(err) {
		return staged, false, err
	}
	return staged, false, nil
}

func writeTaskArchivePackage(dataDir, path, payloadDir string, snapshot *pgdb.TaskArchiveSnapshot) (originalSize, compressedSize int64, checksum string, err error) {
	f, err := openTaskArchiveFS(dataDir)
	if err != nil {
		return 0, 0, "", err
	}
	defer f.Close()
	payloadDir, err = filepath.Abs(payloadDir)
	if err != nil {
		return 0, 0, "", err
	}
	if err := f.check(path, path+".partial", payloadDir, filepath.Join(payloadDir, "manifest.json")); err != nil {
		return 0, 0, "", err
	}
	if snapshot == nil {
		return 0, 0, "", locale.NewError("nil task archive snapshot")
	}
	if err = f.MkdirAll(filepath.Dir(path), archiveDirMode); err != nil {
		return 0, 0, "", err
	}
	manifestFile, err := f.OpenFile(
		filepath.Join(payloadDir, "manifest.json"),
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
		archiveFileMode,
	)
	if err != nil {
		return 0, 0, "", err
	}
	encodeErr := json.NewEncoder(manifestFile).Encode(snapshot)
	var syncErr error
	if encodeErr == nil {
		syncErr = manifestFile.Sync()
	}
	if err := errors.Join(encodeErr, syncErr, manifestFile.Close()); err != nil {
		return 0, 0, "", err
	}
	temporary := path + ".partial"
	_ = f.Remove(temporary)
	file, err := f.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, archiveFileMode)
	if err != nil {
		return 0, 0, "", err
	}
	committed := false
	defer func() {
		_ = file.Close()
		if !committed {
			_ = f.Remove(temporary)
		}
	}()
	hasher := sha256.New()
	multi := io.MultiWriter(file, hasher)
	encoder, err := zstd.NewWriter(multi,
		zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(10)),
		zstd.WithEncoderCRC(true),
	)
	if err != nil {
		return 0, 0, "", err
	}
	tarWriter := tar.NewWriter(encoder)
	payloadRelative, err := f.relative(payloadDir)
	if err != nil {
		_ = tarWriter.Close()
		_ = encoder.Close()
		return 0, 0, "", err
	}
	walkErr := fs.WalkDir(f.root.FS(), payloadRelative, func(current string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(payloadRelative, current)
		if err != nil || relative == "." {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			// The archive format supports only regular files and directories; extraction rejects other types.
			// Symlinks cannot be restored. Skip them without failing the package: inspect with lstat without following targets,
			// and write no symlink entry. Targets within the tree are still visited and archived independently.
			log.Printf(locale.Text(locale.ServerDefault(), "[task-archive] Skipping unsupported symlink; other files are unaffected: %s"), current)
			return nil
		}
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = filepath.ToSlash(relative)
		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		originalSize += info.Size()
		input, err := f.OpenFile(filepath.Join(f.dir, current), os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(tarWriter, input)
		closeErr := input.Close()
		return errors.Join(copyErr, closeErr)
	})
	if walkErr != nil {
		_ = tarWriter.Close()
		_ = encoder.Close()
		return 0, 0, "", walkErr
	}
	if err := tarWriter.Close(); err != nil {
		_ = encoder.Close()
		return 0, 0, "", err
	}
	if err := encoder.Close(); err != nil {
		return 0, 0, "", err
	}
	stat, err := file.Stat()
	if err != nil {
		return 0, 0, "", err
	}
	if err := file.Chmod(archiveFileMode); err != nil {
		return 0, 0, "", err
	}
	if err := file.Sync(); err != nil {
		return 0, 0, "", err
	}
	if err := file.Close(); err != nil {
		return 0, 0, "", err
	}
	compressedSize = stat.Size()
	checksum = hex.EncodeToString(hasher.Sum(nil))
	if err := f.Rename(temporary, path); err != nil {
		return 0, 0, "", err
	}
	committed = true
	return originalSize, compressedSize, checksum, nil
}

func extractTaskArchivePackage(dataDir, path, expectedSHA, destination string) error {
	f, err := openTaskArchiveFS(dataDir)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.check(path, destination); err != nil {
		return err
	}
	file, err := f.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	if expectedSHA != "" {
		hasher := sha256.New()
		if _, err := io.Copy(hasher, file); err != nil {
			return err
		}
		if hex.EncodeToString(hasher.Sum(nil)) != strings.ToLower(expectedSHA) {
			return locale.NewError("task archive checksum mismatch")
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return err
		}
	}
	decoder, err := zstd.NewReader(file)
	if err != nil {
		return err
	}
	defer decoder.Close()
	tarReader := tar.NewReader(decoder)
	if err := f.MkdirAll(destination, archiveDirMode); err != nil {
		return err
	}
	var entries int
	var total int64
	for {
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		entries++
		if entries > maxArchiveFiles || header.Size < 0 || total+header.Size > maxArchiveBytes {
			return locale.NewError("task archive exceeds extraction safety limits")
		}
		total += header.Size
		clean := filepath.Clean(filepath.FromSlash(header.Name))
		if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			return locale.Errorf("unsafe task archive path %q", header.Name)
		}
		target := filepath.Join(destination, clean)
		if relative, err := filepath.Rel(destination, target); err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return locale.Errorf("unsafe task archive target %q", header.Name)
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := f.MkdirAll(target, archiveDirMode); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := f.MkdirAll(filepath.Dir(target), archiveDirMode); err != nil {
				return err
			}
			output, err := f.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, fs.FileMode(header.Mode)&0o777)
			if err != nil {
				return err
			}
			_, copyErr := io.CopyN(output, tarReader, header.Size)
			closeErr := output.Close()
			if err := errors.Join(copyErr, closeErr); err != nil {
				return err
			}
		default:
			return locale.Errorf("unsupported task archive entry type %d", header.Typeflag)
		}
	}
	return nil
}
