package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pgdb "github.com/sebastian93921/artifex/db"
	"github.com/sebastian93921/artifex/traffic"
)

func reviewWriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func reviewWriteJournal(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	reviewWriteFile(t, path, string(raw))
}

func reviewRequireFile(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil || string(got) != want {
		t.Errorf("protected file %s changed: got=%q err=%v", path, got, err)
	}
}

func TestTaskRetentionReviewNestedSymlinkConfinement(t *testing.T) {
	for _, parent := range []string{".staging", ".restore"} {
		t.Run("startup/"+parent, func(t *testing.T) {
			s := retentionTestServer(t)
			outside := t.TempDir()
			victim := filepath.Join(outside, "77-interrupted", "protected.txt")
			reviewWriteFile(t, victim, "outside must survive")
			managedParent := filepath.Join(taskArchiveRoot(s.m.dir), parent)
			if err := os.MkdirAll(filepath.Dir(managedParent), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, managedParent); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			s.ctx = ctx
			s.startTaskArchiveWorker()
			cancel()
			s.archiveWG.Wait()
			reviewRequireFile(t, victim, "outside must survive")
		})
	}
	t.Run("archive-stage", func(t *testing.T) {
		dataDir, outside := t.TempDir(), t.TempDir()
		parent := filepath.Join(taskArchiveRoot(dataDir), ".staging")
		if err := os.MkdirAll(filepath.Dir(parent), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, parent); err != nil {
			t.Fatal(err)
		}
		_, err := stageTaskArchiveFiles(dataDir, 77, "42", 73)
		if err == nil {
			t.Error("archive staging accepted symlinked nested parent")
		}
		entries, err := os.ReadDir(outside)
		if err != nil || len(entries) != 0 {
			t.Errorf("archive staging wrote outside root: entries=%v err=%v", entries, err)
		}
	})
	t.Run("restore-install", func(t *testing.T) {
		dataDir, outside := t.TempDir(), t.TempDir()
		parent := filepath.Join(taskArchiveRoot(dataDir), ".restore")
		if err := os.MkdirAll(filepath.Dir(parent), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, parent); err != nil {
			t.Fatal(err)
		}
		extracted := filepath.Join(parent, "77-test")
		source := filepath.Join(outside, "77-test", "files", "tasks", "42", "protected.txt")
		reviewWriteFile(t, source, "outside restore source")
		_, err := installTaskArchiveFiles(dataDir, extracted, "42", 77)
		if err == nil {
			t.Error("restore accepted symlinked extraction parent")
		}
		reviewRequireFile(t, source, "outside restore source")
		if _, err := os.Lstat(filepath.Join(outside, "77-test", "restore-journal.json")); !os.IsNotExist(err) {
			t.Errorf("restore wrote journal outside root: %v", err)
		}
	})
}

func TestTaskRetentionReviewRejectsMaliciousStageJournalBeforeMoves(t *testing.T) {
	for _, corruption := range []string{"outside-source", "escaping-relative", "symlink-parent"} {
		t.Run(corruption, func(t *testing.T) {
			dataDir, outside := t.TempDir(), t.TempDir()
			root := filepath.Join(taskArchiveRoot(dataDir), ".staging", "77")
			owned := filepath.Join(dataDir, "tasks", "42", "owned.txt")
			reviewWriteFile(t, owned, "owned stays hot")
			victim := filepath.Join(outside, "victim.txt")
			reviewWriteFile(t, victim, "outside stays put")
			bad := archiveFileMove{Source: victim, Relative: "files/transcripts/victim.jsonl"}
			switch corruption {
			case "escaping-relative":
				bad = archiveFileMove{Source: filepath.Join(dataDir, "transcripts", "exp73-bad.jsonl"), Relative: "../../../../../outside-write"}
				reviewWriteFile(t, bad.Source, "must not escape")
			case "symlink-parent":
				if err := os.Symlink(outside, filepath.Join(dataDir, "transcripts")); err != nil {
					t.Fatal(err)
				}
				bad.Source = filepath.Join(dataDir, "transcripts", "victim.txt")
				bad.Relative = "files/transcripts/victim.txt"
			}
			journal := archiveStageJournal{ArchiveID: 77, TaskID: "42", Moves: []archiveFileMove{
				{Source: filepath.Dir(owned), Relative: "files/tasks/42"}, bad,
			}}
			reviewWriteJournal(t, filepath.Join(root, "journal.json"), journal)
			if _, err := stageTaskArchiveFiles(dataDir, 77, "42", 73); err == nil {
				t.Error("malicious staging journal replay succeeded")
			}
			reviewRequireFile(t, owned, "owned stays hot")
			reviewRequireFile(t, victim, "outside stays put")
			if _, err := os.Lstat(filepath.Join(root, "payload", "files", "tasks", "42")); !os.IsNotExist(err) {
				t.Errorf("valid prefix was mutated before journal rejection: %v", err)
			}
		})
	}
}

func TestTaskRetentionReviewPackageIORejectsNestedSymlinks(t *testing.T) {
	t.Run("write-payload-parent", func(t *testing.T) {
		dataDir, outside := t.TempDir(), t.TempDir()
		payload := filepath.Join(taskArchiveRoot(dataDir), ".staging", "77", "payload")
		if err := os.MkdirAll(filepath.Dir(payload), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, payload); err != nil {
			t.Fatal(err)
		}
		reviewWriteFile(t, filepath.Join(outside, "protected.txt"), "outside payload")
		_, _, _, err := writeTaskArchivePackage(dataDir, taskArchivePath(dataDir, 77, "42"), payload, &pgdb.TaskArchiveSnapshot{FormatVersion: pgdb.TaskArchiveFormatVersion, TaskID: 42})
		if err == nil {
			t.Error("package writer followed nested symlinked payload")
		}
		if _, err := os.Lstat(filepath.Join(outside, "manifest.json")); !os.IsNotExist(err) {
			t.Errorf("package writer wrote manifest outside root: %v", err)
		}
		reviewRequireFile(t, filepath.Join(outside, "protected.txt"), "outside payload")
	})
	for _, nested := range []string{"traffic", "evidence", "tables"} {
		t.Run("replayed-payload/"+nested, func(t *testing.T) {
			dataDir, outside := t.TempDir(), t.TempDir()
			root := filepath.Join(taskArchiveRoot(dataDir), ".staging", "77")
			reviewWriteJournal(t, filepath.Join(root, "journal.json"), archiveStageJournal{ArchiveID: 77, TaskID: "42"})
			if err := os.MkdirAll(filepath.Join(root, "payload"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, filepath.Join(root, "payload", nested)); err != nil {
				t.Fatal(err)
			}
			if _, err := stageTaskArchiveFiles(dataDir, 77, "42", 73); err == nil {
				t.Errorf("replayed stage accepted symlinked %s output parent", nested)
			}
			entries, err := os.ReadDir(outside)
			if err != nil || len(entries) != 0 {
				t.Errorf("stage modified outside output parent: %v err=%v", entries, err)
			}
		})
	}
	t.Run("extract-destination-parent", func(t *testing.T) {
		dataDir, outside := t.TempDir(), t.TempDir()
		payload := filepath.Join(dataDir, "payload")
		reviewWriteFile(t, filepath.Join(payload, "files", "tasks", "42", "evidence.txt"), "archived evidence")
		archive := taskArchivePath(dataDir, 77, "42")
		_, _, checksum, err := writeTaskArchivePackage(dataDir, archive, payload, &pgdb.TaskArchiveSnapshot{FormatVersion: pgdb.TaskArchiveFormatVersion, TaskID: 42})
		if err != nil {
			t.Fatal(err)
		}
		destination := filepath.Join(taskArchiveRoot(dataDir), ".restore", "77-test")
		if err := os.MkdirAll(destination, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(destination, "files")); err != nil {
			t.Fatal(err)
		}
		if err := extractTaskArchivePackage(dataDir, archive, checksum, destination); err == nil {
			t.Error("package extractor followed nested symlinked destination")
		}
		if _, err := os.Lstat(filepath.Join(outside, "tasks")); !os.IsNotExist(err) {
			t.Errorf("package extractor wrote outside root: %v", err)
		}
	})
	t.Run("delete-package-parent", func(t *testing.T) {
		dataDir, outside := t.TempDir(), t.TempDir()
		parent := filepath.Join(taskArchiveRoot(dataDir), "nested")
		if err := os.MkdirAll(filepath.Dir(parent), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, parent); err != nil {
			t.Fatal(err)
		}
		victim := filepath.Join(outside, "protected.tar.zst")
		reviewWriteFile(t, victim, "outside package")
		if _, _, err := stageTaskArchivePackageDelete(dataDir, filepath.Join(parent, filepath.Base(victim)), 77); err == nil {
			t.Error("package deletion stage followed nested symlinked parent")
		}
		reviewRequireFile(t, victim, "outside package")
	})
}

func TestTaskRetentionReviewRecoveryRejectsMaliciousJournals(t *testing.T) {
	for _, kind := range []string{"archive", "restore"} {
		t.Run(kind, func(t *testing.T) {
			s := retentionTestServer(t)
			outside := t.TempDir()
			victim := filepath.Join(outside, "protected.txt")
			reviewWriteFile(t, victim, "outside protected")
			var err error
			if kind == "archive" {
				root := filepath.Join(taskArchiveRoot(s.m.dir), ".staging", "77")
				payload := filepath.Join(root, "payload", "files", "tasks", "42", "protected.txt")
				reviewWriteFile(t, payload, "staged protected")
				reviewWriteJournal(t, filepath.Join(root, "journal.json"), archiveStageJournal{
					ArchiveID: 77, TaskID: "42", Moves: []archiveFileMove{{Source: filepath.Join(outside, "new-outside-directory"), Relative: "files/tasks/42"}},
				})
				err = recoverTaskArchiveStages(s.m.dir, s.m.pg)
				reviewRequireFile(t, payload, "staged protected")
				if _, statErr := os.Lstat(filepath.Join(outside, "new-outside-directory")); !os.IsNotExist(statErr) {
					t.Errorf("recovery wrote outside root: %v", statErr)
				}
			} else {
				root := filepath.Join(taskArchiveRoot(s.m.dir), ".restore", "77-test")
				reviewWriteJournal(t, filepath.Join(root, "restore-journal.json"), archiveRestoreJournal{
					ArchiveID: 77, TaskID: 42, Moves: []restoredArchivePath{{Source: filepath.Join(root, "files", "tasks", "42"), Destination: outside}},
				})
				err = recoverTaskArchiveRestoreStages(s.m.dir, s.m.pg)
			}
			if err == nil {
				t.Error("malicious recovery journal accepted")
			}
			reviewRequireFile(t, victim, "outside protected")
		})
	}
}

func TestTaskRetentionReviewHotDetectionDoesNotStage(t *testing.T) {
	s := retentionTestServer(t)
	task := retentionServerTask(t, s, time.Now().UTC().Add(-31*24*time.Hour).Truncate(time.Microsecond))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s.ctx = ctx
	if _, err := s.m.pg.QueueTaskArchiveForRetention(ctx, mustTaskNumericID(t, task.ID), time.Now(), 7*24*time.Hour, 30*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := s.runOneTaskArchiveJob(); err != nil {
		t.Fatal(err)
	}
	job, err := s.m.pg.GetTaskArchiveByTask(mustTaskNumericID(t, task.ID))
	if err != nil || job == nil {
		t.Fatalf("archive=%+v err=%v", job, err)
	}
	workspace := filepath.Join(s.m.dir, "tasks", task.ID, "late.txt")
	reviewWriteFile(t, workspace, "late payload")
	// A nonmutating probe must not need its old .delete-staging destination.
	// An unwritable staging parent makes the old rename-based probe fail here.
	staging := filepath.Join(s.m.dir, ".delete-staging")
	if err := os.Mkdir(staging, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(staging, 0700) })
	retentionAgeConfirmedArchive(t, s, job)
	if _, err := s.m.pg.QueueTaskArchiveDeleteForRetention(ctx, job.ID, time.Now(), 30*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	err = s.runOneTaskArchiveJob()
	if err == nil || !strings.Contains(err.Error(), "unexpected hot task files") {
		t.Errorf("hot detection used a mutation path instead of a read-only check: %v", err)
	}
	reviewRequireFile(t, workspace, "late payload")
	if err := os.Chmod(staging, 0700); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(staging)
	if err != nil || len(entries) != 0 {
		t.Errorf("hot detection created staging entries: %v err=%v", entries, err)
	}
}

func TestTaskRetentionReviewInterruptedHotDetectionBlocksDelete(t *testing.T) {
	s := retentionTestServer(t)
	task := retentionServerTask(t, s, time.Now().UTC().Add(-31*24*time.Hour).Truncate(time.Microsecond))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s.ctx = ctx
	if _, err := s.m.pg.QueueTaskArchiveForRetention(ctx, mustTaskNumericID(t, task.ID), time.Now(), 7*24*time.Hour, 30*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := s.runOneTaskArchiveJob(); err != nil {
		t.Fatal(err)
	}
	job, err := s.m.pg.GetTaskArchiveByTask(mustTaskNumericID(t, task.ID))
	if err != nil || job == nil {
		t.Fatalf("archive=%+v err=%v", job, err)
	}
	// Reproduce the old probe's crash after its first rename: no journal exists
	// and the live workspace is absent. The next probe must still fail closed.
	orphan := filepath.Join(s.m.dir, ".delete-staging", "task-"+task.ID+"-interrupted", "0-"+task.ID, "late.txt")
	reviewWriteFile(t, orphan, "not in the archive")
	retentionAgeConfirmedArchive(t, s, job)
	if _, err := s.m.pg.QueueTaskArchiveDeleteForRetention(ctx, job.ID, time.Now(), 30*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := s.runOneTaskArchiveJob(); err == nil {
		t.Error("deletion ignored interrupted unjournalled hot detection")
	}
	retained, err := s.m.pg.GetTaskArchive(job.ID)
	if err != nil || retained == nil || retained.State != pgdb.DeleteFailed {
		t.Errorf("interrupted detection did not protect archive: %+v err=%v", retained, err)
	}
	reviewRequireFile(t, orphan, "not in the archive")
	if _, err := os.Lstat(job.ArchivePath); err != nil {
		t.Errorf("protected archive package changed: %v", err)
	}
	var stub int64
	if err := s.m.pg.QueryRow(`SELECT id FROM tasks WHERE id=$1`, mustTaskNumericID(t, task.ID)).Scan(&stub); err != nil {
		t.Errorf("protected cold-task row deleted: %v", err)
	}
}

func TestTaskRetentionReviewTrafficUnlinkFailureExcludesPurge(t *testing.T) {
	s := retentionTestServer(t)
	task := retentionServerTask(t, s, time.Now().UTC().Add(-31*24*time.Hour).Truncate(time.Microsecond))
	const host = "retention-unlink-fixture.example.test"
	if _, err := s.m.Assets().UpsertHTTPService(pgdb.UpsertHTTPServiceReq{URL: "https://" + host, TaskID: mustTaskNumericID(t, task.ID)}); err != nil {
		t.Fatal(err)
	}
	trafficDir := filepath.Join(s.m.dir, "traffic")
	tr, err := traffic.Open(trafficDir, "127.0.0.1:0") // never start a proxy
	if err != nil {
		t.Fatal(err)
	}
	s.m.traffic = tr
	legacy := filepath.Join(trafficDir, host)
	blocked := filepath.Join(legacy, "unlink-blocked")
	reviewWriteFile(t, filepath.Join(blocked, "request.http"), "captured fixture traffic")
	if err := os.Chmod(blocked, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = filepath.WalkDir(trafficDir, func(path string, entry os.DirEntry, err error) error {
			if err == nil && entry.IsDir() {
				_ = os.Chmod(path, 0700)
			}
			return nil
		})
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s.ctx = ctx
	if _, err := s.m.pg.QueueTaskArchiveForRetention(ctx, mustTaskNumericID(t, task.ID), time.Now(), 7*24*time.Hour, 30*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := s.runOneTaskArchiveJob(); err != nil {
		t.Fatal(err) // archive succeeds; failed hot cleanup must remain a warning
	}
	job, err := s.m.pg.GetTaskArchiveByTask(mustTaskNumericID(t, task.ID))
	if err != nil || job == nil || job.State != pgdb.ArchiveReady {
		t.Fatalf("archive was not restorable: %+v err=%v", job, err)
	}
	if job.ArchiveCleanupAt != nil || string(job.Warnings) == "[]" {
		t.Errorf("unlink failure incorrectly confirmed archive cleanup: %+v", job)
	}
	if job.ArchivedAt == nil {
		t.Fatal("archive missing success timestamp")
	}
	due := job.ArchivedAt.Add(31 * 24 * time.Hour)
	if _, err := s.m.pg.QueueTaskArchiveDeleteForRetention(ctx, job.ID, due, 30*24*time.Hour); err == nil {
		t.Error("retention deletion admitted archive with failed traffic unlink")
	}
	cfg, _ := taskRetentionConfigFromEnv(func(string) string { return "" })
	counts, err := s.runTaskRetentionCycle(ctx, cfg, due)
	if err != nil || counts.DeleteQueued != 0 {
		t.Errorf("traffic cleanup failure did not exclude purge: %+v err=%v", counts, err)
	}
	if _, err := os.Lstat(job.ArchivePath); err != nil {
		t.Errorf("restorable archive package removed: %v", err)
	}
}

func TestTaskRetentionReviewTwoManagerStartupProtectsLiveTrafficStage(t *testing.T) {
	if os.Getenv("ARTIFEX_RETENTION_STARTUP_CHILD") == "1" {
		// Exercise the real constructor in a separate process. No Server.New,
		// provider, task execution or proxy request is involved.
		manager, err := NewManager(os.Getenv("ARTIFEX_RETENTION_STARTUP_DIR"), "127.0.0.1:0")
		if err != nil {
			t.Logf("review-startup-blocked: %v", err)
			return
		}
		_ = manager.Close()
		t.Log("review-startup-succeeded")
		return
	}
	for _, activeSQLite := range []bool{false, true} {
		t.Run(fmt.Sprintf("active-sqlite=%v", activeSQLite), func(t *testing.T) {
			s := retentionTestServer(t) // first real NewManager on a fresh DB
			task := retentionServerTask(t, s, time.Now().UTC().Add(-8*24*time.Hour).Truncate(time.Microsecond))
			job, err := s.m.pg.QueueTaskArchiveForRetention(context.Background(), mustTaskNumericID(t, task.ID), time.Now(), 7*24*time.Hour, 30*24*time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			trafficDir := filepath.Join(s.m.dir, "traffic")
			tr, err := traffic.Open(trafficDir, "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			s.m.traffic = tr
			locked, err := s.m.pg.WithTaskArchiveWorkerLock(context.Background(), func() error {
				if _, err := s.m.pg.ClaimTaskArchiveJob(context.Background()); err != nil {
					return err
				}
				const host = "startup-stage-fixture.example.test"
				source := filepath.Join(trafficDir, host)
				var stageRoot string
				if activeSQLite {
					reviewWriteFile(t, filepath.Join(source, "protected.txt"), "live staged traffic")
					stage, err := tr.StageDeleteHostsExactForArchive([]string{host}, job.ID, mustTaskNumericID(t, task.ID))
					if err != nil {
						return err
					}
					defer stage.Rollback()
					entries, err := os.ReadDir(filepath.Join(trafficDir, "_delete_staging"))
					if err != nil || len(entries) != 1 {
						return fmt.Errorf("expected one actual live traffic stage: entries=%v err=%v", entries, err)
					}
					stageRoot = filepath.Join(trafficDir, "_delete_staging", entries[0].Name())
				} else {
					// A durable first rename while an archive worker owns its lock.
					// No SQLite writer prevents the second constructor from reaching
					// its real recovery routine on the unfixed implementation.
					stageRoot = filepath.Join(trafficDir, "_delete_staging", "hosts-review-live")
					reviewWriteJournal(t, filepath.Join(stageRoot, "journal.json"), map[string]any{
						"version": 1, "archive_id": job.ID, "task_id": mustTaskNumericID(t, task.ID), "hosts": []string{host},
						"moves": []map[string]string{{"source": source, "staged": filepath.Join(stageRoot, "0-"+host)}},
					})
					reviewWriteFile(t, filepath.Join(stageRoot, "0-"+host, "protected.txt"), "live staged traffic")
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestTaskRetentionReviewTwoManagerStartupProtectsLiveTrafficStage$", "-test.v")
				// The child uses the fixture DB. The parent suite lock belongs
				// to the disposable base DB; advisory locks are database-scoped.
				cmd.Env = append(os.Environ(), "ARTIFEX_RETENTION_STARTUP_CHILD=1", "ARTIFEX_RETENTION_STARTUP_DIR="+s.m.dir)
				output, err := cmd.CombinedOutput()
				if err != nil || !strings.Contains(string(output), "review-startup-blocked:") || !strings.Contains(string(output), "maintenance lock") {
					t.Errorf("real second manager was not refused before traffic recovery: err=%v output=%s", err, output)
				}
				reviewRequireFile(t, filepath.Join(stageRoot, "0-"+host, "protected.txt"), "live staged traffic")
				if _, err := os.Lstat(source); !os.IsNotExist(err) {
					t.Errorf("second-manager startup restored another worker's live stage: %v", err)
				}
				archive, err := s.m.pg.GetTaskArchive(job.ID)
				if err != nil || archive == nil || archive.State != pgdb.Archiving {
					t.Errorf("second-manager startup reset active archive: %+v err=%v", archive, err)
				}
				return nil
			})
			if err != nil || !locked {
				t.Fatalf("live fixture maintenance lock: acquired=%v err=%v", locked, err)
			}
		})
	}
}
