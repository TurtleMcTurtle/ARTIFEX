package server

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	pgdb "github.com/sebastian93921/artifex/db"
)

func retentionTestServer(t *testing.T) *Server {
	t.Helper()
	dsn := os.Getenv("ARTIFEX_RETENTION_TEST_DSN")
	if dsn == "" {
		t.Skip("ARTIFEX_RETENTION_TEST_DSN required (dedicated disposable loopback PostgreSQL)")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" {
		t.Fatal("retention fixtures require dedicated loopback PostgreSQL")
	}
	t.Setenv("ARTIFEX_PG_DSN", dsn)
	s := keyTestServer(t) // creates and later drops a fresh database
	// Fixtures never construct New or start restored planners/models. A cancelled
	// runtime context makes accidental engine admission inert; each explicit
	// archive operation supplies its own bounded fixture-only context later.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.ctx = ctx
	s.archiveWake = make(chan struct{}, 1)
	s.taskAgents = map[string]*taskAgentBundle{}
	s.initSideQuestions()
	// Join the cancelled fixture checkpoint publisher before later explicit
	// worker probes swap the fixture context; otherwise that swap races its
	// goroutine's s.ctx read. No live background runtime remains in fixtures.
	<-s.side.done
	return s
}

func retentionServerTask(t *testing.T, s *Server, completed time.Time) *Task {
	t.Helper()
	task, err := s.m.CreateTask("inert retention fixture", "no model or target IO", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.m.pg.Exec(`UPDATE tasks SET created_at=$2,status='done',completed_at=$3 WHERE id=$1`, task.ID, completed.Add(-time.Hour), completed); err != nil {
		t.Fatal(err)
	}
	task.updateLifecycle(func(state *taskLifecycleState) { state.Status = "done" })
	return task
}

func TestTaskRetentionConfigurationDefaultsAndValidation(t *testing.T) {
	empty := func(string) string { return "" }
	cfg, err := taskRetentionConfigFromEnv(empty)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Enabled || cfg.ArchiveAfter != 7*24*time.Hour || cfg.DeleteAfter != 30*24*time.Hour || cfg.Interval != 24*time.Hour || cfg.BatchSize <= 0 || cfg.BatchSize > 100 || cfg.CycleTimeout <= 0 || cfg.CycleTimeout > 10*time.Minute {
		t.Fatalf("unsafe defaults: %+v", cfg)
	}
	for _, env := range []map[string]string{
		{"ARTIFEX_TASK_RETENTION_ENABLED": "invalid"},
		{"ARTIFEX_TASK_RETENTION_ARCHIVE_AFTER": "0h"},
		{"ARTIFEX_TASK_RETENTION_DELETE_AFTER": "0h"},
		{"ARTIFEX_TASK_RETENTION_DELETE_AFTER": "garbage"},
		{"ARTIFEX_TASK_RETENTION_INTERVAL": "0s"},
		{"ARTIFEX_TASK_RETENTION_BATCH_SIZE": "101"},
		{"ARTIFEX_TASK_RETENTION_BATCH_SIZE": "0"},
		{"ARTIFEX_TASK_RETENTION_CYCLE_TIMEOUT": "24h"},
	} {
		if _, err := taskRetentionConfigFromEnv(func(key string) string { return env[key] }); err == nil {
			t.Fatalf("invalid config accepted: %v", env)
		}
	}
}

func TestTaskRetentionCycleIsBoundedAndIdempotent(t *testing.T) {
	s := retentionTestServer(t)
	cfg, _ := taskRetentionConfigFromEnv(func(string) string { return "" })
	now := time.Now().UTC().Truncate(time.Microsecond)
	for i := 0; i < 3; i++ {
		retentionServerTask(t, s, now.Add(-8*24*time.Hour))
	}
	cfg.BatchSize = 2
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	first, err := s.runTaskRetentionCycle(ctx, cfg, now)
	if err != nil || first.ArchiveQueued != 2 || first.DeleteQueued != 0 || first.Candidates != 2 {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	second, err := s.runTaskRetentionCycle(ctx, cfg, now)
	if err != nil || second.ArchiveQueued != 1 {
		t.Fatalf("second=%+v err=%v", second, err)
	}
	third, err := s.runTaskRetentionCycle(ctx, cfg, now)
	if err != nil || third.ArchiveQueued != 0 || third.DeleteQueued != 0 {
		t.Fatalf("repeated=%+v err=%v", third, err)
	}
	var count int
	if err := s.m.pg.QueryRow(`SELECT count(*) FROM task_archives`).Scan(&count); err != nil || count != 3 {
		t.Fatalf("duplicate jobs=%d err=%v", count, err)
	}
}

func TestTaskRetentionArchivesThenDeletesThroughExistingWorker(t *testing.T) {
	s := retentionTestServer(t)
	cfg, _ := taskRetentionConfigFromEnv(func(string) string { return "" })
	completed := time.Now().UTC().Add(-31 * 24 * time.Hour).Truncate(time.Microsecond)
	task := retentionServerTask(t, s, completed)
	if _, err := s.m.pg.Exec(`INSERT INTO task_submissions(owner,external_request_id,request_hash,task_id) VALUES('ARTIFEX','housekeeping-request','hash',$1)`, task.ID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	first, err := s.runTaskRetentionCycle(ctx, cfg, time.Now())
	if err != nil || first.ArchiveQueued != 1 || first.DeleteQueued != 0 {
		t.Fatalf("unarchived task must archive first: %+v %v", first, err)
	}
	// Only this explicit archive worker operation gets an uncancelled context.
	// New/restoreTaskRuntimes/provider initialization is never invoked.
	s.ctx = ctx
	if err := s.runOneTaskArchiveJob(); err != nil {
		t.Fatal(err)
	}
	job, err := s.m.pg.GetTaskArchiveByTask(mustTaskNumericID(t, task.ID))
	if err != nil || job == nil || job.State != pgdb.ArchiveReady || job.ArchiveCleanupAt == nil {
		t.Fatalf("archive not safely complete: %+v err=%v", job, err)
	}
	retentionAgeConfirmedArchive(t, s, job)
	second, err := s.runTaskRetentionCycle(ctx, cfg, time.Now())
	if err != nil || second.DeleteQueued != 1 {
		t.Fatalf("confirmed cold archive not due for deletion: %+v %v", second, err)
	}
	if err := s.runOneTaskArchiveJob(); err != nil {
		t.Fatal(err)
	}
	if job, err := s.m.pg.GetTaskArchiveByTask(mustTaskNumericID(t, task.ID)); err != nil || job != nil {
		t.Fatalf("archive not deleted: %+v %v", job, err)
	}
	if _, err := os.Stat(job.ArchivePath); !os.IsNotExist(err) {
		t.Fatalf("package remains: %v", err)
	}
	var ref *int64
	if err := s.m.pg.QueryRow(`SELECT task_id FROM task_submissions WHERE external_request_id='housekeeping-request'`).Scan(&ref); err != nil || ref != nil {
		t.Fatalf("tombstone lost: ref=%v err=%v", ref, err)
	}
}

func TestTaskRetentionWorkerRejectsOutsideAndSymlinkedPaths(t *testing.T) {
	for _, symlinked := range []bool{false, true} {
		t.Run(strconv.FormatBool(symlinked), func(t *testing.T) {
			s := retentionTestServer(t)
			outside := t.TempDir()
			victim := filepath.Join(outside, "unmanaged.tar.zst")
			if err := os.WriteFile(victim, []byte("must survive"), 0600); err != nil {
				t.Fatal(err)
			}
			root := taskArchiveRoot(s.m.dir)
			candidate := victim
			if symlinked {
				if err := os.MkdirAll(filepath.Dir(root), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, root); err != nil {
					t.Fatal(err)
				}
				candidate = filepath.Join(root, "unmanaged.tar.zst")
			}
			if err := validateArchivePath(s.m.dir, candidate); err == nil {
				t.Fatal("unmanaged/symlinked path passed validation")
			}
			if err := s.deleteTaskArchive(&pgdb.TaskArchive{ID: 77, TaskID: 77, ArchivePath: candidate}); err == nil {
				t.Fatal("unmanaged deletion unexpectedly succeeded")
			}
			bytes, err := os.ReadFile(victim)
			if err != nil || string(bytes) != "must survive" {
				t.Fatalf("unmanaged file touched: %q %v", bytes, err)
			}
		})
	}
}

func TestTaskRetentionDeletionHonorsEngineInflightBarrier(t *testing.T) {
	s := retentionTestServer(t)
	completed := time.Now().UTC().Add(-31 * 24 * time.Hour).Truncate(time.Microsecond)
	task := retentionServerTask(t, s, completed)
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
		t.Fatalf("job=%v err=%v", job, err)
	}
	retentionAgeConfirmedArchive(t, s, job)
	if _, err := s.m.pg.QueueTaskArchiveDeleteForRetention(ctx, job.ID, time.Now(), 30*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if !s.engine.beginTaskOperation(task.ID) {
		t.Fatal("start inflight fixture")
	}
	t.Cleanup(func() { s.engine.decInflight(task.ID) })
	blocked, cancelBlocked := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelBlocked()
	s.ctx = blocked
	if err := s.runOneTaskArchiveJob(); err == nil {
		t.Fatal("cold deletion bypassed an inflight engine operation")
	}
	retained, err := s.m.pg.GetTaskArchive(job.ID)
	if err != nil || retained == nil || retained.State != pgdb.DeleteFailed {
		t.Fatalf("protected job lost: %+v %v", retained, err)
	}
	if _, err := os.Stat(job.ArchivePath); err != nil {
		t.Fatalf("protected archive file removed: %v", err)
	}
}

func TestTaskRetentionConcurrentCyclesQueueOnce(t *testing.T) {
	s := retentionTestServer(t)
	cfg, _ := taskRetentionConfigFromEnv(func(string) string { return "" })
	retentionServerTask(t, s, time.Now().UTC().Add(-8*24*time.Hour).Truncate(time.Microsecond))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var wg sync.WaitGroup
	counts := make(chan taskRetentionCounts, 8)
	errs := make(chan error, 8)
	start := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			n, err := s.runTaskRetentionCycle(ctx, cfg, time.Now())
			counts <- n
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(counts)
	close(errs)
	queued := 0
	for n := range counts {
		queued += n.ArchiveQueued
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if queued != 1 {
		t.Fatalf("concurrent cycles claimed %d queues, want 1", queued)
	}
}

func TestTaskRetentionSchedulerDisabledAndDailyTick(t *testing.T) {
	s := retentionTestServer(t)
	cfg, _ := taskRetentionConfigFromEnv(func(string) string { return "" })
	task := retentionServerTask(t, s, time.Now().UTC().Add(-8*24*time.Hour).Truncate(time.Microsecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ticks := make(chan time.Time)
	done := make(chan struct{})
	go func() { defer close(done); s.runTaskRetentionScheduler(ctx, cfg, ticks) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("disabled scheduler started")
	}
	if job, err := s.m.pg.GetTaskArchiveByTask(mustTaskNumericID(t, task.ID)); err != nil || job != nil {
		t.Fatal("disabled scheduler queued work")
	}
	cfg.Enabled = true
	done = make(chan struct{})
	go func() { defer close(done); s.runTaskRetentionScheduler(ctx, cfg, ticks) }()
	if job, err := s.m.pg.GetTaskArchiveByTask(mustTaskNumericID(t, task.ID)); err != nil || job != nil {
		t.Fatal("scheduler swept tasks at startup before daily tick")
	}
	ticks <- time.Now().UTC()
	select {
	case <-s.archiveWake:
	case <-time.After(5 * time.Second):
		t.Fatal("daily tick did not queue archive")
	}
	if job, err := s.m.pg.GetTaskArchiveByTask(mustTaskNumericID(t, task.ID)); err != nil || job == nil {
		t.Fatalf("tick did not queue: %v %v", job, err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not stop on cancellation")
	}
}

func TestTaskRetentionNativeSchedulerStartup(t *testing.T) {
	s := retentionTestServer(t)
	task := retentionServerTask(t, s, time.Now().UTC().Add(-8*24*time.Hour).Truncate(time.Microsecond))
	t.Setenv("ARTIFEX_TASK_RETENTION_ENABLED", "true")
	t.Setenv("ARTIFEX_TASK_RETENTION_INTERVAL", "20ms")
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.startTaskRetentionScheduler()
	select {
	case <-s.archiveWake:
	case <-time.After(5 * time.Second):
		cancel()
		s.archiveWG.Wait()
		t.Fatal("native scheduler startup did not tick")
	}
	cancel()
	s.archiveWG.Wait()
	if job, err := s.m.pg.GetTaskArchiveByTask(mustTaskNumericID(t, task.ID)); err != nil || job == nil {
		t.Fatalf("native scheduler did not queue: %+v %v", job, err)
	}
}

func TestTaskRetentionWorkerFailureRetriesArchiveNeverDeletesLiveTask(t *testing.T) {
	s := retentionTestServer(t)
	cfg, _ := taskRetentionConfigFromEnv(func(string) string { return "" })
	task := retentionServerTask(t, s, time.Now().UTC().Add(-31*24*time.Hour).Truncate(time.Microsecond))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s.ctx = ctx
	if _, err := s.runTaskRetentionCycle(ctx, cfg, time.Now()); err != nil {
		t.Fatal(err)
	}
	// Force package preparation to fail without using a target/model or changing
	// the safe purge pipeline: the managed archives parent is an ordinary file.
	if err := os.WriteFile(filepath.Join(s.m.dir, "archives"), []byte("fixture blocker"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.runOneTaskArchiveJob(); err == nil {
		t.Fatal("archive failure fixture unexpectedly succeeded")
	}
	job, err := s.m.pg.GetTaskArchiveByTask(mustTaskNumericID(t, task.ID))
	if err != nil || job == nil || job.State != pgdb.ArchiveFailed {
		t.Fatalf("failure not durable: %+v %v", job, err)
	}
	counts, err := s.runTaskRetentionCycle(ctx, cfg, time.Now())
	if err != nil || counts.ArchiveQueued != 1 || counts.DeleteQueued != 0 {
		t.Fatalf("archive failure fell through to deletion: %+v %v", counts, err)
	}
	live, err := s.m.pg.GetTask(mustTaskNumericID(t, task.ID))
	if err != nil || live == nil {
		t.Fatalf("live task removed after archive failure: %+v %v", live, err)
	}
}

func TestTaskRetentionCycleCancelsBlockedTaskRow(t *testing.T) {
	s := retentionTestServer(t)
	cfg, _ := taskRetentionConfigFromEnv(func(string) string { return "" })
	task := retentionServerTask(t, s, time.Now().UTC().Add(-31*24*time.Hour).Truncate(time.Microsecond))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s.ctx = ctx
	if _, err := s.runTaskRetentionCycle(ctx, cfg, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := s.runOneTaskArchiveJob(); err != nil {
		t.Fatal(err)
	}
	completedArchive, err := s.m.pg.GetTaskArchiveByTask(mustTaskNumericID(t, task.ID))
	if err != nil || completedArchive == nil {
		t.Fatalf("completed fixture archive=%+v err=%v", completedArchive, err)
	}
	retentionAgeConfirmedArchive(t, s, completedArchive)
	tx, err := s.m.pg.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`SELECT id FROM tasks WHERE id=$1 FOR UPDATE`, mustTaskNumericID(t, task.ID)); err != nil {
		t.Fatal(err)
	}
	cfg.CycleTimeout = 300 * time.Millisecond
	type result struct {
		counts taskRetentionCounts
		err    error
	}
	done := make(chan result, 1)
	go func() {
		counts, err := s.runTaskRetentionCycle(ctx, cfg, time.Now())
		done <- result{counts, err}
	}()
	// Confirm the exact cold-task FOR UPDATE is blocked in PostgreSQL before
	// judging the deadline. Fast metadata rejection is not a cancellation test.
	waitUntil := time.Now().Add(250 * time.Millisecond)
	for {
		var blocked bool
		if err := s.m.pg.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_stat_activity
WHERE datname=current_database() AND wait_event_type='Lock'
AND query LIKE 'SELECT status,created_at,completed_at,archived_at,deleted_at,queued FROM tasks%')`).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		if time.Now().After(waitUntil) {
			t.Fatal("cycle never reached the blocked cold-task row")
		}
		time.Sleep(5 * time.Millisecond)
	}
	select {
	case got := <-done:
		if got.counts.DeleteQueued != 0 || got.err == nil && got.counts.Errors != 1 {
			t.Fatalf("deadline did not report its blocked enqueue: %+v err=%v", got.counts, got.err)
		}
	case <-time.After(time.Second):
		_ = tx.Rollback()
		<-done
		t.Fatal("bounded cycle did not cancel a genuinely blocked task-row recheck")
	}
	// Keep the row blocker held: the cycle must have released the advisory
	// session independently of that row becoming available.
	pool, err := sql.Open("pgx", os.Getenv("ARTIFEX_PG_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	second := &pgdb.DB{DB: pool} // no schema migration while the blocker is held
	lockCtx, lockCancel := context.WithTimeout(context.Background(), time.Second)
	defer lockCancel()
	locked, err := second.WithTaskRetentionCycleLock(lockCtx, func() error { return nil })
	if err != nil || !locked {
		t.Fatalf("deadline left distributed cycle lock held: acquired=%v err=%v", locked, err)
	}
	job, err := s.m.pg.GetTaskArchiveByTask(mustTaskNumericID(t, task.ID))
	if err != nil || job == nil || job.State != pgdb.ArchiveReady {
		t.Fatalf("cancelled cold enqueue mutated state: %+v err=%v", job, err)
	}
}

func TestTaskRetentionBoundedBatchDoesNotStarveBehindDependencies(t *testing.T) {
	s := retentionTestServer(t)
	cfg, _ := taskRetentionConfigFromEnv(func(string) string { return "" })
	cfg.BatchSize = 1
	now := time.Now().UTC().Truncate(time.Microsecond)
	parent := retentionServerTask(t, s, now.Add(-31*24*time.Hour))
	child := retentionServerTask(t, s, now.Add(-8*24*time.Hour))
	if _, err := s.m.pg.Exec(`INSERT INTO task_relations(task_id,source_task_id) VALUES($1,$2)`, child.ID, parent.ID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	counts, err := s.runTaskRetentionCycle(ctx, cfg, now)
	if err != nil || counts.ArchiveQueued != 1 {
		t.Fatalf("blocked oldest task starved batch: %+v %v", counts, err)
	}
	job, err := s.m.pg.GetTaskArchiveByTask(mustTaskNumericID(t, child.ID))
	if err != nil || job == nil {
		t.Fatalf("dependent not queued first: %+v %v", job, err)
	}
}

func TestTaskRetentionArchiveDoesNotMoveOutsideManagedTaskDirectory(t *testing.T) {
	s := retentionTestServer(t)
	completed := time.Now().UTC().Add(-31 * 24 * time.Hour).Truncate(time.Microsecond)
	task := retentionServerTask(t, s, completed)
	outside := t.TempDir()
	owned := filepath.Join(outside, task.ID)
	if err := os.MkdirAll(owned, 0700); err != nil {
		t.Fatal(err)
	}
	victim := filepath.Join(owned, "must-survive.txt")
	if err := os.WriteFile(victim, []byte("outside evidence"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(s.m.dir, "tasks")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(s.m.dir, "tasks")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s.ctx = ctx
	if _, err := s.m.pg.QueueTaskArchiveForRetention(ctx, mustTaskNumericID(t, task.ID), time.Now(), 7*24*time.Hour, 30*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := s.runOneTaskArchiveJob(); err == nil {
		t.Fatal("archive followed symlinked unmanaged task directory")
	}
	bytes, err := os.ReadFile(victim)
	if err != nil || string(bytes) != "outside evidence" {
		t.Fatalf("outside task files moved or deleted: %q %v", bytes, err)
	}
}

func TestTaskRetentionStartupRecoveryCannotDeleteOutsideManagedRoot(t *testing.T) {
	s := retentionTestServer(t)
	outside := t.TempDir()
	victim := filepath.Join(outside, "unmanaged.tar.zst.deleting-77")
	if err := os.WriteFile(victim, []byte("must survive recovery"), 0600); err != nil {
		t.Fatal(err)
	}
	root := taskArchiveRoot(s.m.dir)
	if err := os.MkdirAll(filepath.Dir(root), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, root); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.startTaskArchiveWorker()
	cancel()
	s.archiveWG.Wait()
	bytes, err := os.ReadFile(victim)
	if err != nil || string(bytes) != "must survive recovery" {
		t.Fatalf("startup recovery deleted unmanaged file: %q %v", bytes, err)
	}
}

func TestTaskRetentionDeletionRefusesNewHotTaskPayload(t *testing.T) {
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
		t.Fatalf("archive=%v %v", job, err)
	}
	retentionAgeConfirmedArchive(t, s, job)
	if _, err := s.m.pg.QueueTaskArchiveDeleteForRetention(ctx, job.ID, time.Now(), 30*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	// An unexpected hot workspace after archival means a writer bypassed the
	// archive barrier; full deletion must not pretend this payload is covered.
	if err := os.MkdirAll(filepath.Join(s.m.dir, "tasks", task.ID), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.m.dir, "tasks", task.ID, "late.txt"), []byte("late payload"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.runOneTaskArchiveJob(); err == nil {
		t.Fatal("deletion accepted unexpected hot payload outside its confirmed archive")
	}
	job, err = s.m.pg.GetTaskArchive(job.ID)
	if err != nil || job == nil || job.State != pgdb.DeleteFailed {
		t.Fatalf("unsafe deletion did not stay recoverable: %+v %v", job, err)
	}
}

// retentionAgeConfirmedArchive models elapsed archive residence on fixture-only
// rows. It never confirms missing cleanup or edits an application database.
func retentionAgeConfirmedArchive(t *testing.T, s *Server, job *pgdb.TaskArchive) {
	t.Helper()
	if os.Getenv("ARTIFEX_RETENTION_TEST_DSN") == "" || job.ArchiveCleanupAt == nil {
		t.Fatal("only confirmed disposable archives may be aged")
	}
	archived := time.Now().UTC().Add(-31 * 24 * time.Hour).Truncate(time.Microsecond)
	completed := archived.Add(-8 * 24 * time.Hour)
	if _, err := s.m.pg.Exec(`UPDATE tasks SET created_at=$2,completed_at=$3,archived_at=$4,deleted_at=$4 WHERE id=$1`, job.TaskID, completed.Add(-time.Hour), completed, archived); err != nil {
		t.Fatal(err)
	}
	if _, err := s.m.pg.Exec(`UPDATE task_archives SET task_completed_at=$2,archived_at=$3,archive_cleanup_at=$3,retention_archive_at=$4,retention_delete_at=$5 WHERE id=$1`, job.ID, completed, archived, completed.Add(7*24*time.Hour), archived.Add(30*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
}

func mustTaskNumericID(t *testing.T, id string) int64 {
	t.Helper()
	value, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
