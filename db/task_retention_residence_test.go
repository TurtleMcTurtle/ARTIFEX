package db

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTaskRetentionArchiveResidenceBacklog(t *testing.T) {
	d := retentionTestDB(t)
	completed := time.Now().UTC().Add(-120 * 24 * time.Hour).Truncate(time.Microsecond)
	task := retentionFixtureTask(t, d, "done", &completed)
	job, err := d.QueueTaskArchiveForRetention(context.Background(), task.ID, time.Now(), 7*24*time.Hour, 30*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if job.RetentionDeleteAt != nil {
		t.Errorf("archive queue prematurely set a deletion deadline before successful archival: %v", job.RetentionDeleteAt)
	}
	// Queue/start clocks are much older than successful archival. They must not
	// let a freshly written archive from an old backlog immediately be purged.
	if _, err := d.Exec(`UPDATE task_archives SET requested_at=$2,created_at=$2 WHERE id=$1`, job.ID, completed.Add(7*24*time.Hour)); err != nil {
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
	job, err = d.GetTaskArchive(job.ID)
	if err != nil || job == nil || job.ArchivedAt == nil {
		t.Fatalf("successful archive=%+v err=%v", job, err)
	}
	boundary := job.ArchivedAt.Add(30 * 24 * time.Hour)
	if job.RetentionDeleteAt == nil || !job.RetentionDeleteAt.Equal(boundary) {
		t.Errorf("delete deadline not based on successful ready stamp: archived=%v delete=%v want=%v", job.ArchivedAt, job.RetentionDeleteAt, boundary)
	}
	for _, now := range []time.Time{time.Now().UTC(), boundary.Add(-time.Microsecond)} {
		candidates, err := d.ListTaskRetentionCandidates(context.Background(), now, 7*24*time.Hour, 30*24*time.Hour, 100)
		if err != nil || len(candidates) != 0 {
			t.Errorf("fresh/backlogged archive prematurely discovered: now=%v candidates=%+v err=%v", now, candidates, err)
		}
		if _, err := d.QueueTaskArchiveDeleteForRetention(context.Background(), job.ID, now, 30*24*time.Hour); !errors.Is(err, ErrTaskRetentionIneligible) {
			t.Errorf("fresh/backlogged archive prematurely queued: now=%v err=%v", now, err)
		}
	}
	candidates, err := d.ListTaskRetentionCandidates(context.Background(), boundary, 7*24*time.Hour, 30*24*time.Hour, 100)
	if err != nil || len(candidates) != 1 || candidates[0].ArchiveID != job.ID {
		t.Errorf("exact archive-residence boundary not discovered: candidates=%+v err=%v", candidates, err)
	}
	queued, err := d.QueueTaskArchiveDeleteForRetention(context.Background(), job.ID, boundary, 30*24*time.Hour)
	if err != nil || queued == nil || queued.RetentionDeleteAt == nil || !queued.RetentionDeleteAt.Equal(boundary) {
		t.Fatalf("exact archive-residence boundary not queued correctly: job=%+v err=%v", queued, err)
	}
}

func TestTaskRetentionResidenceClockUsesReadyNotTransactionStart(t *testing.T) {
	d := retentionTestDB(t)
	completed := time.Now().UTC().Add(-8 * 24 * time.Hour).Truncate(time.Microsecond)
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
	// Observe a delayed real compaction transaction before its final ready
	// transition. PostgreSQL now() is the transaction START, not archive success.
	if _, err := d.Exec(`CREATE TABLE retention_clock_probe (started timestamptz, compacted timestamptz);
CREATE FUNCTION retention_clock_probe_fn() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM pg_sleep(0.05);
 INSERT INTO retention_clock_probe VALUES(transaction_timestamp(),clock_timestamp());
 RETURN NEW;
END; $$;
CREATE TRIGGER retention_clock_probe_trigger AFTER UPDATE ON explorations FOR EACH ROW EXECUTE FUNCTION retention_clock_probe_fn();`); err != nil {
		t.Fatal(err)
	}
	if err := d.CompleteTaskArchive(job.ID, snapshot, "managed-test-package", "test-checksum", 1, 1); err != nil {
		t.Fatal(err)
	}
	var started, compacted, ready time.Time
	if err := d.QueryRow(`SELECT p.started,p.compacted,a.archived_at FROM retention_clock_probe p CROSS JOIN task_archives a WHERE a.id=$1`, job.ID).Scan(&started, &compacted, &ready); err != nil {
		t.Fatal(err)
	}
	if !compacted.After(started) || ready.Before(compacted) {
		t.Errorf("archive-residence stamp is not the successful ready transition: started=%v compacted=%v ready=%v", started, compacted, ready)
	}
}

func TestTaskRetentionIndependentDurations(t *testing.T) {
	d := retentionTestDB(t)
	completed := time.Now().UTC().Add(-8 * 24 * time.Hour).Truncate(time.Microsecond)
	task := retentionFixtureTask(t, d, "done", &completed)
	job, err := d.QueueTaskArchiveForRetention(context.Background(), task.ID, time.Now(), 7*24*time.Hour, time.Hour)
	if err != nil || job == nil {
		t.Fatalf("independent positive archive/residence durations rejected: job=%+v err=%v", job, err)
	}
	if _, err := d.ListTaskRetentionCandidates(context.Background(), time.Now(), 7*24*time.Hour, time.Hour, 10); err != nil {
		t.Fatalf("candidate discovery rejected independent durations: %v", err)
	}
}
