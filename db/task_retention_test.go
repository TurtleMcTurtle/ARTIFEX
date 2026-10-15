package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"
)

// Retention tests never resolve the application's configured database. An
// explicit loopback-only opt-in points at a disposable PostgreSQL server, and
// each test owns a fresh database on that server.
func retentionTestDB(t *testing.T) *DB {
	t.Helper()
	dsn := os.Getenv("ARTIFEX_RETENTION_TEST_DSN")
	if dsn == "" {
		t.Skip("ARTIFEX_RETENTION_TEST_DSN required (dedicated disposable loopback PostgreSQL)")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Hostname() != "127.0.0.1" {
		t.Fatal("retention tests require an explicit postgres URL on 127.0.0.1")
	}
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("retention_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(`CREATE DATABASE "` + name + `"`); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(`DROP DATABASE "` + name + `" WITH (FORCE)`); err != nil {
			t.Error(err)
		}
		admin.Close()
	})
	u.Path = "/" + name
	d, err := Open(u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	if err := d.EnsureLLMRecordsTable(); err != nil {
		t.Fatal(err)
	}
	if err := d.EnsureLLMUsageTable(); err != nil {
		t.Fatal(err)
	}
	return d
}

func retentionFixtureTask(t *testing.T, d *DB, status string, completed *time.Time) *Task {
	t.Helper()
	task, err := d.CreateTask("inert retention fixture", "no model or target IO", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	created := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := d.Exec(`UPDATE tasks SET status=$2,completed_at=$3,created_at=$4 WHERE id=$1`, task.ID, status, completed, created); err != nil {
		t.Fatal(err)
	}
	return task
}

func TestTaskArchiveRetentionPreservesTerminalCompletion(t *testing.T) {
	d := retentionTestDB(t)
	completed := time.Now().UTC().Add(-8 * 24 * time.Hour).Truncate(time.Microsecond)
	task := retentionFixtureTask(t, d, "done", &completed)
	job, err := d.QueueTaskArchive(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	var retained *time.Time
	if err := d.QueryRowContext(context.Background(), `SELECT task_completed_at FROM task_archives WHERE id=$1`, job.ID).Scan(&retained); err != nil {
		t.Fatalf("archive must retain trustworthy completion metadata: %v", err)
	}
	if retained == nil || !retained.Equal(completed) {
		t.Fatalf("completion=%v, want %v", retained, completed)
	}
}

func TestTaskRetentionRequiresConfirmedArchiveCleanup(t *testing.T) {
	d := retentionTestDB(t)
	completed := time.Now().UTC().Add(-31 * 24 * time.Hour).Truncate(time.Microsecond)
	task := retentionFixtureTask(t, d, "done", &completed)
	job, err := d.QueueTaskArchiveForRetention(context.Background(), task.ID, time.Now(), 7*24*time.Hour, 30*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.ClaimTaskArchiveJob(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot, err := d.SnapshotTaskArchive(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.CompleteTaskArchive(job.ID, snapshot, "managed-test-package", "test-checksum", 1, 1); err != nil {
		t.Fatal(err)
	}
	var cleanup *time.Time
	if err := d.QueryRow(`SELECT archive_cleanup_at FROM task_archives WHERE id=$1`, job.ID).Scan(&cleanup); err != nil {
		t.Fatal(err)
	}
	if cleanup != nil {
		t.Fatal("database commit alone must not confirm external cleanup")
	}
	if err := d.ConfirmTaskArchiveCleanup(job.ID); err != nil {
		t.Fatal(err)
	}
	job, err = d.GetTaskArchive(job.ID)
	if err != nil || job == nil || job.ArchiveCleanupAt == nil {
		t.Fatalf("cleanup not confirmed: %+v %v", job, err)
	}
}

func TestTaskRetentionCompactionRejectsChangedLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mutation string
	}{
		{"revived", `UPDATE tasks SET status='running',completed_at=NULL WHERE id=$1`},
		{"paused", `UPDATE tasks SET paused=true WHERE id=$1`},
		{"queued", `UPDATE tasks SET queued=true WHERE id=$1`},
		{"new completion", `UPDATE tasks SET completed_at=now() WHERE id=$1`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := retentionTestDB(t)
			now := time.Now().UTC().Truncate(time.Microsecond)
			completed := now.Add(-8 * 24 * time.Hour)
			task := retentionFixtureTask(t, d, "done", &completed)
			job, err := d.QueueTaskArchiveForRetention(context.Background(), task.ID, now, 7*24*time.Hour, 30*24*time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			claimed, err := d.ClaimTaskArchiveJob(context.Background())
			if err != nil || claimed == nil || claimed.ID != job.ID {
				t.Fatalf("claim=%v err=%v", claimed, err)
			}
			snapshot, err := d.SnapshotTaskArchive(task.ID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := d.Exec(tc.mutation, task.ID); err != nil {
				t.Fatal(err)
			}
			err = d.CompleteTaskArchive(job.ID, snapshot, "managed-test-package", "test-checksum", 1, 1)
			if !errors.Is(err, ErrTaskRetentionIneligible) {
				t.Fatalf("changed task compacted: %v", err)
			}
			persisted, err := d.GetTask(task.ID)
			if err != nil || persisted == nil || persisted.Description == "" {
				t.Fatalf("protected live task lost: task=%v err=%v", persisted, err)
			}
		})
	}
}

func retentionColdTask(t *testing.T, d *DB, completed time.Time) (*Task, *TaskArchive) {
	t.Helper()
	task := retentionFixtureTask(t, d, "done", &completed)
	job, err := d.QueueTaskArchiveForRetention(context.Background(), task.ID, time.Now(), 7*24*time.Hour, 30*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.ClaimTaskArchiveJob(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot, err := d.SnapshotTaskArchive(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.CompleteTaskArchive(job.ID, snapshot, "managed-test-package", "test-checksum", 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := d.ConfirmTaskArchiveCleanup(job.ID); err != nil {
		t.Fatal(err)
	}
	// Model a successfully archived historical task, not a just-drained backlog.
	archived := completed.Add(time.Hour)
	if _, err := d.Exec(`UPDATE task_archives SET archived_at=$2,archive_cleanup_at=$2,retention_delete_at=$3 WHERE id=$1`, job.ID, archived, archived.Add(30*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	job, err = d.GetTaskArchive(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	return task, job
}

func TestTaskRetentionDeleteBoundaryAndArchiveFailures(t *testing.T) {
	for _, tc := range []struct {
		name     string
		before   bool
		mutation string
		want     bool
	}{
		{name: "before boundary", before: true},
		{name: "exact boundary", want: true},
		{name: "archive failed", mutation: `UPDATE task_archives SET state='archive_failed' WHERE id=$1`},
		{name: "archive in progress", mutation: `UPDATE task_archives SET state='archiving' WHERE id=$1`},
		{name: "restore unresolved", mutation: `UPDATE task_archives SET state='restore_failed' WHERE id=$1`},
		{name: "cleanup unconfirmed", mutation: `UPDATE task_archives SET archive_cleanup_at=NULL WHERE id=$1`},
		{name: "cleanup warning", mutation: `UPDATE task_archives SET warnings='["cleanup failed"]' WHERE id=$1`},
		{name: "legacy missing completion", mutation: `UPDATE task_archives SET task_completed_at=NULL WHERE id=$1`},
		{name: "resumed task", mutation: `UPDATE tasks SET archived_at=NULL,deleted_at=NULL,paused=false,status='running',completed_at=NULL WHERE id=(SELECT task_id FROM task_archives WHERE id=$1)`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := retentionTestDB(t)
			now := time.Now().UTC().Truncate(time.Microsecond)
			completed := now.Add(-37 * 24 * time.Hour)
			task, job := retentionColdTask(t, d, completed)
			if _, err := d.Exec(`UPDATE task_archives SET archived_at=$2,archive_cleanup_at=$2,retention_delete_at=$3 WHERE id=$1`, job.ID, now.Add(-30*24*time.Hour), now); err != nil {
				t.Fatal(err)
			}
			if tc.mutation != "" {
				if _, err := d.Exec(tc.mutation, job.ID); err != nil {
					t.Fatal(err)
				}
			}
			if tc.before {
				now = now.Add(-time.Microsecond)
			}
			queued, err := d.QueueTaskArchiveDeleteForRetention(context.Background(), job.ID, now, 30*24*time.Hour)
			if tc.want {
				if err != nil || queued == nil || queued.State != DeleteQueued {
					t.Fatalf("delete not queued: %+v %v", queued, err)
				}
			} else if !errors.Is(err, ErrTaskRetentionIneligible) {
				t.Fatalf("protected archive queued: %+v %v", queued, err)
			}
			var count int
			if err := d.QueryRow(`SELECT count(*) FROM tasks WHERE id=$1`, task.ID).Scan(&count); err != nil || count != 1 {
				t.Fatalf("queue removed task: count=%d err=%v", count, err)
			}
		})
	}
}

func TestTaskRetentionDeleteRechecksStateAndPreservesTombstone(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("changed=%v", changed), func(t *testing.T) {
			d := retentionTestDB(t)
			completed := time.Now().UTC().Add(-31 * 24 * time.Hour).Truncate(time.Microsecond)
			task, job := retentionColdTask(t, d, completed)
			if _, err := d.Exec(`INSERT INTO task_submissions(owner,external_request_id,request_hash,task_id) VALUES('ARTIFEX','retained-request','same-hash',$1)`, task.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := d.Exec(`INSERT INTO settings(key,value) VALUES('task_language.'||$1::bigint::text,'en')`, task.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := d.QueueTaskArchiveDeleteForRetention(context.Background(), job.ID, time.Now(), 30*24*time.Hour); err != nil {
				t.Fatal(err)
			}
			claimed, err := d.ClaimTaskArchiveJob(context.Background())
			if err != nil || claimed == nil || claimed.State != Deleting {
				t.Fatalf("claim=%v err=%v", claimed, err)
			}
			if changed {
				if _, err := d.Exec(`UPDATE tasks SET archived_at=NULL,deleted_at=NULL,status='running',completed_at=NULL WHERE id=$1`, task.ID); err != nil {
					t.Fatal(err)
				}
			}
			err = d.DeleteTaskArchiveStub(job.ID)
			if changed {
				if !errors.Is(err, ErrTaskRetentionIneligible) {
					t.Fatalf("changed task deletion not protected: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var taskRef *int64
			if err := d.QueryRow(`SELECT task_id FROM task_submissions WHERE external_request_id='retained-request'`).Scan(&taskRef); err != nil || taskRef != nil {
				t.Fatalf("tombstone missing: %v %v", taskRef, err)
			}
			if _, err := d.CreateTaskWithOptions("inert retention fixture", "no model or target IO", TaskCreateOptions{SubmissionOwner: "ARTIFEX", ExternalRequestID: "retained-request", RequestHash: "same-hash"}); !errors.Is(err, ErrSubmissionConflict) {
				t.Fatalf("deleted request recreated task: %v", err)
			}
			for _, query := range []string{`SELECT count(*) FROM tasks WHERE id=$1`, `SELECT count(*) FROM task_archives WHERE task_id=$1`, `SELECT count(*) FROM settings WHERE key='task_language.'||$1::bigint::text`} {
				var n int
				if err := d.QueryRow(query, task.ID).Scan(&n); err != nil || n != 0 {
					t.Fatalf("task data retained: count=%d err=%v", n, err)
				}
			}
		})
	}
}

func TestTaskRetentionManualArchiveDoesNotInferPauseEligibility(t *testing.T) {
	d := retentionTestDB(t)
	completed := time.Now().UTC().Add(-31 * 24 * time.Hour).Truncate(time.Microsecond)
	task := retentionFixtureTask(t, d, "done", &completed)
	job, err := d.QueueTaskArchive(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.ClaimTaskArchiveJob(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := d.SetPaused(task.ID, true); err != nil {
		t.Fatal(err)
	}
	snapshot, err := d.SnapshotTaskArchive(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.CompleteTaskArchive(job.ID, snapshot, "managed-test-package", "test-checksum", 1, 1); err != nil {
		t.Fatal(err)
	}
	job, err = d.GetTaskArchive(job.ID)
	if err != nil || job == nil || job.TaskCompletedAt != nil {
		t.Fatalf("manual pause gained automatic purge metadata: %+v %v", job, err)
	}
}

func TestTaskRetentionDoesNotRequeueAnActiveDelete(t *testing.T) {
	d := retentionTestDB(t)
	_, job := retentionColdTask(t, d, time.Now().UTC().Add(-31*24*time.Hour).Truncate(time.Microsecond))
	if _, err := d.QueueTaskArchiveDeleteForRetention(context.Background(), job.ID, time.Now(), 30*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	claimed, err := d.ClaimTaskArchiveJob(context.Background())
	if err != nil || claimed == nil || claimed.State != Deleting {
		t.Fatalf("claim=%+v %v", claimed, err)
	}
	if _, err := d.QueueTaskArchiveDeleteForRetention(context.Background(), job.ID, time.Now(), 30*24*time.Hour); !errors.Is(err, ErrTaskRetentionIneligible) {
		t.Fatalf("active deletion requeued: %v", err)
	}
	claimed, err = d.GetTaskArchive(job.ID)
	if err != nil || claimed == nil || claimed.State != Deleting {
		t.Fatalf("active state changed: %+v %v", claimed, err)
	}
}

func TestTaskRetentionDistributedCycleLockAndArchiveRecoveryLock(t *testing.T) {
	d := retentionTestDB(t)
	for _, cycle := range []bool{false, true} {
		var run func(context.Context, func() error) (bool, error) = d.WithTaskArchiveWorkerLock
		if cycle {
			run = d.WithTaskRetentionCycleLock
		}
		entered := make(chan struct{})
		release := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			_, err := run(context.Background(), func() error { close(entered); <-release; return nil })
			done <- err
		}()
		<-entered
		locked, err := run(context.Background(), func() error { t.Error("concurrent process entered lock"); return nil })
		if err != nil || locked {
			t.Errorf("concurrent lock acquired=%v err=%v", locked, err)
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		locked, err = run(context.Background(), func() error { return nil })
		if err != nil || !locked {
			t.Fatalf("lock not released: acquired=%v err=%v", locked, err)
		}
	}
}

func TestTaskRetentionArchiveRejectsFutureCompletionEvenWithFutureCycleClock(t *testing.T) {
	d := retentionTestDB(t)
	completed := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	task := retentionFixtureTask(t, d, "done", &completed)
	_, err := d.QueueTaskArchiveForRetention(context.Background(), task.ID, completed.Add(8*24*time.Hour), 7*24*time.Hour, 30*24*time.Hour)
	if !errors.Is(err, ErrTaskRetentionIneligible) {
		t.Fatalf("future completion accepted: %v", err)
	}
}

func TestTaskRetentionArchiveBoundaryAndExcludedStates(t *testing.T) {
	d := retentionTestDB(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	archiveAfter, deleteAfter := 7*24*time.Hour, 30*24*time.Hour
	for _, tc := range []struct {
		name, status                            string
		age                                     time.Duration
		paused, queued, missing, invalidCreated bool
		want                                    bool
	}{
		{name: "before boundary", status: "done", age: archiveAfter - time.Microsecond},
		{name: "exact boundary", status: "done", age: archiveAfter, want: true},
		{name: "failed", status: "failed", age: archiveAfter + time.Microsecond, want: true},
		{name: "timeout", status: "timeout", age: archiveAfter, want: true},
		{name: "running", status: "running", age: deleteAfter},
		{name: "created", status: "created", age: deleteAfter},
		{name: "paused", status: "paused", age: deleteAfter, paused: true},
		{name: "terminal but paused", status: "done", age: deleteAfter, paused: true},
		{name: "terminal but queued", status: "done", age: deleteAfter, queued: true},
		{name: "no completion", status: "done", missing: true},
		{name: "future completion", status: "done", age: -time.Hour},
		{name: "completion predates creation", status: "done", age: deleteAfter, invalidCreated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			completed := now.Add(-tc.age)
			var stamp *time.Time = &completed
			if tc.missing {
				stamp = nil
			}
			task := retentionFixtureTask(t, d, tc.status, stamp)
			if _, err := d.Exec(`UPDATE tasks SET paused=$2,queued=$3 WHERE id=$1`, task.ID, tc.paused, tc.queued); err != nil {
				t.Fatal(err)
			}
			if tc.invalidCreated {
				if _, err := d.Exec(`UPDATE tasks SET created_at=$2 WHERE id=$1`, task.ID, now); err != nil {
					t.Fatal(err)
				}
			}
			job, err := d.QueueTaskArchiveForRetention(context.Background(), task.ID, now, archiveAfter, deleteAfter)
			if tc.want {
				if err != nil || job == nil {
					t.Fatalf("eligible archive: job=%v err=%v", job, err)
				}
				if job.RetentionArchiveAt == nil || !job.RetentionArchiveAt.Equal(completed.Add(archiveAfter)) || job.RetentionDeleteAt != nil || job.RetentionDeleteAfterNS == nil || *job.RetentionDeleteAfterNS != int64(deleteAfter) {
					t.Fatalf("archive must retain its completion deadline and residence duration without inventing an early deletion deadline: %+v", job)
				}
			} else {
				if !errors.Is(err, ErrTaskRetentionIneligible) {
					t.Fatalf("excluded task queued: job=%v err=%v", job, err)
				}
				if job, err := d.GetTaskArchiveByTask(task.ID); err != nil || job != nil {
					t.Fatalf("excluded task mutated: job=%v err=%v", job, err)
				}
			}
		})
	}
}
