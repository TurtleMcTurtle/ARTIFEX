package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	pgdb "github.com/sebastian93921/artifex/db"
	"github.com/sebastian93921/artifex/traffic"
)

func remainingTrafficFixture(t *testing.T, s *Server) *traffic.Traffic {
	t.Helper()
	tr, err := traffic.Open(filepath.Join(s.m.dir, "traffic"), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s.m.traffic = tr // manager cleanup closes it; no proxy is started
	return tr
}

func remainingArchiveJob(t *testing.T, s *Server, task *Task) *pgdb.TaskArchive {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	s.ctx = ctx
	job, err := s.m.pg.QueueTaskArchiveForRetention(ctx, mustTaskNumericID(t, task.ID), time.Now(), 7*24*time.Hour, 30*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(taskArchiveRoot(s.m.dir), ".staging", strconv.FormatInt(job.ID, 10))
	reviewWriteJournal(t, filepath.Join(root, "journal.json"), archiveStageJournal{ArchiveID: job.ID, TaskID: task.ID})
	return job
}

func remainingRequireOutsideUnchanged(t *testing.T, outside string, want map[string]string) {
	t.Helper()
	got := map[string]string{}
	err := filepath.WalkDir(outside, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(outside, path)
		if err != nil {
			return err
		}
		got[rel] = string(raw)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Errorf("outside file count changed: got=%v want=%v", got, want)
	}
	for path, content := range want {
		if got[path] != content {
			t.Errorf("outside %s changed: got=%q want=%q", path, got[path], content)
		}
	}
}

func TestTaskRetentionRemainingTrafficManifestSymlink(t *testing.T) {
	s := retentionTestServer(t)
	task := retentionServerTask(t, s, time.Now().UTC().Add(-31*24*time.Hour))
	remainingTrafficFixture(t, s)
	const host = "manifest-fixture.example.test"
	if _, err := s.m.Assets().UpsertHTTPService(pgdb.UpsertHTTPServiceReq{URL: "https://" + host, TaskID: mustTaskNumericID(t, task.ID)}); err != nil {
		t.Fatal(err)
	}
	job := remainingArchiveJob(t, s, task)
	output := filepath.Join(taskArchiveRoot(s.m.dir), ".staging", strconv.FormatInt(job.ID, 10), "payload", "traffic")
	if err := os.MkdirAll(output, 0700); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	victim := filepath.Join(outside, "protected.json")
	reviewWriteFile(t, victim, "outside must not be truncated")
	if err := os.Symlink(victim, filepath.Join(output, "traffic.json")); err != nil {
		t.Fatal(err)
	}
	if err := s.runOneTaskArchiveJob(); err == nil {
		t.Error("archive export accepted a symlinked manifest file in a real output directory")
	}
	remainingRequireOutsideUnchanged(t, outside, map[string]string{"protected.json": "outside must not be truncated"})
	if live, err := s.m.pg.GetTask(mustTaskNumericID(t, task.ID)); err != nil || live == nil {
		t.Errorf("failed export compacted the task: task=%+v err=%v", live, err)
	}
}

func TestTaskRetentionRemainingNestedEvidenceOutputSymlink(t *testing.T) {
	s := retentionTestServer(t)
	task := retentionServerTask(t, s, time.Now().UTC().Add(-31*24*time.Hour))
	tr := remainingTrafficFixture(t, s)
	const id = "nested-evidence-fixture"
	if _, err := tr.DB().Exec(`INSERT INTO exchanges(id,ts,host,method,url_template,url,status,content_type,req_len,resp_len,path)
VALUES(?,1,'evidence-fixture.example.test','POST','/','https://evidence-fixture.example.test/',200,'text/plain',2,4,'')`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.DB().Exec(`INSERT INTO exchange_bodies(id,req_head,req_body,resp_head,resp_body) VALUES(?,'request',?,'response',?)`, id, []byte("{}"), []byte("body")); err != nil {
		t.Fatal(err)
	}
	persisted, err := s.m.pg.GetTask(mustTaskNumericID(t, task.ID))
	if err != nil || persisted == nil {
		t.Fatalf("fixture task=%+v err=%v", persisted, err)
	}
	if _, err := s.evidenceStore().Record(context.Background(), pgdb.RecordFindingInput{
		TaskID: mustTaskNumericID(t, task.ID), ExplorationID: persisted.ExplorationID,
		Worker: "fixture", VulnClass: "TEST", Name: "local evidence fixture", Severity: "low", Summary: "no target IO",
	}, []pgdb.TrafficRef{{TrafficID: id, Role: "proof"}}); err != nil {
		t.Fatal(err)
	}
	job := remainingArchiveJob(t, s, task)
	output := filepath.Join(taskArchiveRoot(s.m.dir), ".staging", strconv.FormatInt(job.ID, 10), "payload", "evidence")
	if err := os.MkdirAll(output, 0700); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	reviewWriteFile(t, filepath.Join(outside, "protected.txt"), "outside evidence output")
	if err := os.Symlink(outside, filepath.Join(output, "blobs")); err != nil {
		t.Fatal(err)
	}
	if err := s.runOneTaskArchiveJob(); err == nil {
		t.Error("archive export followed a nested evidence output-parent symlink")
	}
	remainingRequireOutsideUnchanged(t, outside, map[string]string{"protected.txt": "outside evidence output"})
}

func remainingSeedBlob(t *testing.T, tr *traffic.Traffic, dir, id, host string, body []byte, requestHash string) (string, string) {
	t.Helper()
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	path := filepath.Join(dir, "_blobs", "sha256", hash[:2], hash+".bin")
	reviewWriteFile(t, path, string(body))
	if _, err := tr.DB().Exec(`INSERT INTO exchanges(id,ts,host,method,url_template,url,status,content_type,req_len,resp_len,path)
VALUES(?,1,?,'GET','/',?,200,'text/plain',0,?,'')`, id, host, "https://"+host+"/", len(body)); err != nil {
		t.Fatal(err)
	}
	var request any
	if requestHash != "" {
		request = requestHash
	}
	if _, err := tr.DB().Exec(`INSERT INTO exchange_bodies(id,req_head,req_blob,resp_head,resp_blob) VALUES(?,'request',?,'response',?)`, id, request, hash); err != nil {
		t.Fatal(err)
	}
	for _, h := range []string{hash, requestHash} {
		if h != "" {
			if _, err := tr.DB().Exec(`INSERT INTO blob_refs(hash,exchange_id) VALUES(?,?)`, h, id); err != nil {
				t.Fatal(err)
			}
		}
	}
	return hash, path
}

func TestTaskRetentionRemainingBlobCleanupFailure(t *testing.T) {
	for _, failure := range []struct {
		name string
		mode os.FileMode
	}{{"unlink", 0500}, {"inspect", 0100}} {
		t.Run(failure.name, func(t *testing.T) {
			s := retentionTestServer(t)
			task := retentionServerTask(t, s, time.Now().UTC().Add(-31*24*time.Hour))
			tr := remainingTrafficFixture(t, s)
			const host = "blob-failure-fixture.example.test"
			if _, err := s.m.Assets().UpsertHTTPService(pgdb.UpsertHTTPServiceReq{URL: "https://" + host, TaskID: mustTaskNumericID(t, task.ID)}); err != nil {
				t.Fatal(err)
			}
			sharedBody := []byte("shared body must survive failed archive cleanup")
			sharedHash, shared := remainingSeedBlob(t, tr, filepath.Join(s.m.dir, "traffic"), "shared-blob-fixture", "unrelated-fixture.example.test", sharedBody, "")
			exclusiveBody := []byte("exclusive body must leave retention cleanup pending on failure")
			exclusiveHash, exclusive := remainingSeedBlob(t, tr, filepath.Join(s.m.dir, "traffic"), "exclusive-blob-fixture", host, exclusiveBody, sharedHash)
			if sharedHash[:2] == exclusiveHash[:2] {
				t.Fatal("fixture requires independent permission buckets")
			}
			bucket := filepath.Dir(exclusive)
			if failure.name == "inspect" {
				// This unreadable orphan bucket is not an export input. It sorts
				// before the exclusive bucket, so only the GC traversal fails.
				if exclusiveHash[:2] == "00" || sharedHash[:2] == "00" {
					t.Fatal("fixture requires the inspection bucket to be separate")
				}
				bucket = filepath.Join(s.m.dir, "traffic", "_blobs", "sha256", "00")
				reviewWriteFile(t, filepath.Join(bucket, "orphan.bin"), "uninspectable orphan")
			}
			if err := os.Chmod(bucket, failure.mode); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chmod(bucket, 0700) })
			job := remainingArchiveJob(t, s, task)
			if err := s.runOneTaskArchiveJob(); err != nil {
				t.Fatal(err) // a committed archive stays ready/restorable, with warning
			}
			retained, err := s.m.pg.GetTaskArchive(job.ID)
			if err != nil || retained == nil || retained.State != pgdb.ArchiveReady {
				t.Fatalf("archive not restorable: %+v err=%v", retained, err)
			}
			if retained.ArchiveCleanupAt != nil || !strings.Contains(string(retained.Warnings), "Reclaim traffic blobs") {
				t.Errorf("blob %s failure falsely confirmed hot cleanup: cleanup=%v warnings=%s", failure.name, retained.ArchiveCleanupAt, retained.Warnings)
			}
			reviewRequireFile(t, exclusive, string(exclusiveBody))
			reviewRequireFile(t, shared, string(sharedBody))
			var exclusiveRows, sharedRefs int
			if err := tr.DB().QueryRow(`SELECT count(*) FROM exchanges WHERE host=?`, host).Scan(&exclusiveRows); err != nil || exclusiveRows != 0 {
				t.Errorf("SQLite compaction not committed: count=%d err=%v", exclusiveRows, err)
			}
			if err := tr.DB().QueryRow(`SELECT count(*) FROM blob_refs WHERE hash=?`, sharedHash).Scan(&sharedRefs); err != nil || sharedRefs != 1 {
				t.Errorf("shared references not preserved: count=%d err=%v", sharedRefs, err)
			}
			if retained.ArchivedAt == nil {
				t.Fatal("committed archive missing its success timestamp")
			}
			due := retained.ArchivedAt.Add(31 * 24 * time.Hour)
			if _, err := s.m.pg.QueueTaskArchiveDeleteForRetention(s.ctx, job.ID, due, 30*24*time.Hour); err == nil {
				t.Error("retention deletion accepted failed blob cleanup")
			}
			cfg, err := taskRetentionConfigFromEnv(func(string) string { return "" })
			if err != nil {
				t.Fatal(err)
			}
			counts, err := s.runTaskRetentionCycle(s.ctx, cfg, due)
			if err != nil || counts.DeleteQueued != 0 {
				t.Errorf("failed blob cleanup admitted retention deletion: counts=%+v err=%v", counts, err)
			}
			if _, err := os.Stat(retained.ArchivePath); err != nil {
				t.Errorf("protected archive package removed: %v", err)
			}
			var stub int
			if err := s.m.pg.QueryRow(`SELECT count(*) FROM tasks WHERE id=$1`, job.TaskID).Scan(&stub); err != nil || stub != 1 {
				t.Errorf("protected cold task lost: count=%d err=%v", stub, err)
			}
		})
	}
}

func TestTaskRetentionIndependentDurationConfiguration(t *testing.T) {
	for _, env := range []map[string]string{
		{"ARTIFEX_TASK_RETENTION_DELETE_AFTER": "24h"},
		{"ARTIFEX_TASK_RETENTION_ARCHIVE_AFTER": "800h"},
		{"ARTIFEX_TASK_RETENTION_DELETE_AFTER": "168h"},
	} {
		if _, err := taskRetentionConfigFromEnv(func(key string) string { return env[key] }); err != nil {
			t.Errorf("independent positive completion/archive-residence durations rejected: config=%v err=%v", env, err)
		}
	}
}

func TestTaskRetentionArchivesBacklogWaitsArchiveResidence(t *testing.T) {
	s := retentionTestServer(t)
	cfg, err := taskRetentionConfigFromEnv(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	task := retentionServerTask(t, s, time.Now().UTC().Add(-120*24*time.Hour).Truncate(time.Microsecond))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s.ctx = ctx
	if counts, err := s.runTaskRetentionCycle(ctx, cfg, time.Now()); err != nil || counts.ArchiveQueued != 1 {
		t.Fatalf("old backlog not queued for archive: counts=%+v err=%v", counts, err)
	}
	if err := s.runOneTaskArchiveJob(); err != nil {
		t.Fatal(err)
	}
	job, err := s.m.pg.GetTaskArchiveByTask(mustTaskNumericID(t, task.ID))
	if err != nil || job == nil || job.ArchivedAt == nil || job.ArchiveCleanupAt == nil {
		t.Fatalf("backlog archive not confirmed: job=%+v err=%v", job, err)
	}
	for _, at := range []time.Time{time.Now().UTC(), job.ArchivedAt.Add(30*24*time.Hour - time.Microsecond)} {
		counts, err := s.runTaskRetentionCycle(ctx, cfg, at)
		if err != nil || counts.DeleteQueued != 0 {
			t.Errorf("backlog deleted before successful-archive residence expired: now=%v counts=%+v err=%v", at, counts, err)
		}
	}
	boundary := job.ArchivedAt.Add(30 * 24 * time.Hour)
	counts, err := s.runTaskRetentionCycle(ctx, cfg, boundary)
	if err != nil || counts.DeleteQueued != 1 {
		t.Fatalf("backlog archive not queued at exact residence boundary: counts=%+v err=%v", counts, err)
	}
	queued, err := s.m.pg.GetTaskArchive(job.ID)
	if err != nil || queued == nil || queued.RetentionDeleteAt == nil || !queued.RetentionDeleteAt.Equal(boundary) {
		t.Errorf("queued deletion deadline not successful-archive based: job=%+v err=%v", queued, err)
	}
	// No worker runs with the synthetic future clock, and no package is deleted.
	if _, err := os.Stat(job.ArchivePath); err != nil {
		t.Errorf("fresh confirmed package removed while only exercising queue clocks: %v", err)
	}
}

func TestTaskRetentionRemainingImportAncestry(t *testing.T) {
	for _, corruption := range []string{"source-ancestor", "hot-blob-parent"} {
		t.Run(corruption, func(t *testing.T) {
			s := retentionTestServer(t)
			tr := remainingTrafficFixture(t, s)
			outside := t.TempDir()
			reviewWriteFile(t, filepath.Join(outside, "protected.txt"), "outside import data")
			body := []byte("import fixture blob")
			sum := sha256.Sum256(body)
			hash := hex.EncodeToString(sum[:])
			manifest := traffic.ArchiveSnapshot{Version: 1, Blobs: []string{hash}, Exchanges: []traffic.ArchiveExchange{{
				ID: "outside-import-fixture", Host: "import-fixture.example.test", Method: "GET", URL: "https://import-fixture.example.test/", RespBlob: hash, RespLen: len(body),
			}}}
			raw, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			extracted := filepath.Join(taskArchiveRoot(s.m.dir), ".restore", "77-fixture")
			writeSource := extracted
			want := map[string]string{"protected.txt": "outside import data"}
			if corruption == "source-ancestor" {
				parent := filepath.Dir(extracted)
				if err := os.MkdirAll(filepath.Dir(parent), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, parent); err != nil {
					t.Fatal(err)
				}
				writeSource = filepath.Join(outside, "77-fixture")
				want[filepath.Join("77-fixture", "traffic", "traffic.json")] = string(raw)
				want[filepath.Join("77-fixture", "traffic", "blobs", hash+".bin")] = string(body)
			} else {
				bucket := filepath.Join(s.m.dir, "traffic", "_blobs", "sha256", hash[:2])
				if err := os.MkdirAll(filepath.Dir(bucket), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, bucket); err != nil {
					t.Fatal(err)
				}
			}
			reviewWriteFile(t, filepath.Join(writeSource, "traffic", "traffic.json"), string(raw))
			reviewWriteFile(t, filepath.Join(writeSource, "traffic", "blobs", hash+".bin"), string(body))
			snapshot := &pgdb.TaskArchiveSnapshot{FormatVersion: pgdb.TaskArchiveFormatVersion, TaskID: 42}
			if err := s.restoreTaskArchivePayload(&pgdb.TaskArchive{ID: 77, TaskID: 42}, snapshot, extracted); err == nil {
				t.Error("restore unexpectedly succeeded with corrupt import ancestry")
			}
			var imported int
			if err := tr.DB().QueryRow(`SELECT count(*) FROM exchanges WHERE id='outside-import-fixture'`).Scan(&imported); err != nil || imported != 0 {
				t.Errorf("restore imported traffic through corrupt ancestry before rejecting files: count=%d err=%v", imported, err)
			}
			remainingRequireOutsideUnchanged(t, outside, want)
		})
	}
}
