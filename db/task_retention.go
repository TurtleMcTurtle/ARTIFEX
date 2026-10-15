package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"time"
)

const (
	taskRetentionCycleLock int64 = 7337741010
	taskArchiveWorkerLock  int64 = 7337741011
)

// Pin a session advisory lock to a dedicated connection. A lock-holder is not
// put back in the pool: every unlock uses that same session, even on cancellation.
func (d *DB) withTaskMaintenanceLock(ctx context.Context, key int64, action func() error) (locked bool, err error) {
	conn, err := d.Conn(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&locked); err != nil {
		return false, err
	}
	if !locked {
		return false, nil
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var released bool
		if unlockErr := conn.QueryRowContext(unlockCtx, `SELECT pg_advisory_unlock($1)`, key).Scan(&released); unlockErr != nil {
			err = errors.Join(err, unlockErr)
			// Destroy rather than return a potentially locked session to the pool.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
	}()
	return true, action()
}

// WithTaskRetentionCycleLock keeps distributed discovery/counting bounded.
func (d *DB) WithTaskRetentionCycleLock(ctx context.Context, action func() error) (bool, error) {
	return d.withTaskMaintenanceLock(ctx, taskRetentionCycleLock, action)
}

// WithTaskArchiveWorkerLock prevents one server's startup recovery from resetting
// another server's in-flight archive/delete state or rolling its staged files
// back. Recovery and the FIFO worker share this distributed lock.
func (d *DB) WithTaskArchiveWorkerLock(ctx context.Context, action func() error) (bool, error) {
	return d.withTaskMaintenanceLock(ctx, taskArchiveWorkerLock, action)
}

var ErrTaskRetentionIneligible = errors.New("task is not eligible for automatic retention")

type taskRetentionRequest struct {
	now                       time.Time
	archiveAfter, deleteAfter time.Duration
}

func trustworthyTaskCompletion(status string, paused, queued bool, created time.Time, completed *time.Time, now time.Time) bool {
	return IsTerminal(status) && !paused && !queued && completed != nil &&
		!completed.IsZero() && !completed.Before(created) && !completed.After(now)
}

// QueueTaskArchiveForRetention uses the same dependency-checked durable queue as
// manual archives, but rechecks the stricter retention predicate under its task
// row lock. Deadlines travel with the job so a delayed worker never trusts a new
// or missing completion timestamp.
func (d *DB) QueueTaskArchiveForRetention(ctx context.Context, taskID int64, now time.Time, archiveAfter, deleteAfter time.Duration) (*TaskArchive, error) {
	if now.IsZero() || archiveAfter <= 0 || deleteAfter <= 0 {
		return nil, ErrTaskRetentionIneligible
	}
	return d.queueTaskArchive(ctx, taskID, &taskRetentionRequest{now: now, archiveAfter: archiveAfter, deleteAfter: deleteAfter})
}

// QueueTaskArchiveDeleteForRetention only admits successfully archived cold
// tasks. Archive failures, interrupted restores and unconfirmed external cleanup
// never fall through to a direct live-task purge.
func (d *DB) QueueTaskArchiveDeleteForRetention(ctx context.Context, id int64, now time.Time, deleteAfter time.Duration) (*TaskArchive, error) {
	if now.IsZero() || deleteAfter <= 0 {
		return nil, ErrTaskRetentionIneligible
	}
	return d.queueTaskArchiveDelete(ctx, id, &taskRetentionRequest{now: now, deleteAfter: deleteAfter})
}

func validateColdTaskRetention(ctx context.Context, tx *sql.Tx, item *TaskArchive, now time.Time, deleteAfter time.Duration) error {
	if item == nil || (item.State != ArchiveReady && item.State != DeleteFailed && item.State != Deleting) ||
		item.ArchivedAt == nil || item.ArchiveCleanupAt == nil || item.TaskCompletedAt == nil ||
		item.ArchivePath == "" || item.SHA256 == "" || item.CompressedSize <= 0 ||
		string(item.Warnings) != "[]" || !IsTerminal(item.OriginalStatus) ||
		item.ArchivedAt.IsZero() || item.ArchivedAt.After(now) ||
		item.ArchiveCleanupAt.Before(*item.ArchivedAt) || item.ArchiveCleanupAt.After(now) ||
		now.Before(item.ArchivedAt.Add(deleteAfter)) {
		return ErrTaskRetentionIneligible
	}
	var status string
	var created time.Time
	var completed, archived, deleted *time.Time
	var queued bool
	if err := tx.QueryRowContext(ctx, `SELECT status,created_at,completed_at,archived_at,deleted_at,queued FROM tasks WHERE id=$1 FOR UPDATE`, item.TaskID).
		Scan(&status, &created, &completed, &archived, &deleted, &queued); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrTaskRetentionIneligible
		}
		return err
	}
	// Compaction intentionally sets paused=true on the cold ID stub; eligibility
	// comes from the pre-archive completion metadata, not that synthetic pause.
	if archived == nil || deleted == nil || !trustworthyTaskCompletion(status, false, queued, created, completed, now) ||
		!completed.Equal(*item.TaskCompletedAt) || item.ArchivedAt.Before(*completed) {
		return ErrTaskRetentionIneligible
	}
	return nil
}

// TaskRetentionCandidate contains identities only; no target, finding, model
// configuration or other task payload needs to enter scheduler logs.
type TaskRetentionCandidate struct {
	TaskID, ArchiveID int64
}

// ListTaskRetentionCandidates returns at most limit due transitions (deletions
// first). Invalid completion data is never repaired by inference. The queue
// methods recheck every decision under row locks before mutating anything.
func (d *DB) ListTaskRetentionCandidates(ctx context.Context, now time.Time, archiveAfter, deleteAfter time.Duration, limit int) ([]TaskRetentionCandidate, error) {
	if now.IsZero() || archiveAfter <= 0 || deleteAfter <= 0 || limit < 1 || limit > 100 {
		return nil, ErrTaskRetentionIneligible
	}
	rows, err := d.QueryContext(ctx, `SELECT task_id,archive_id FROM (
 SELECT t.id task_id,a.id archive_id,0 priority,a.archived_at completed
 FROM task_archives a JOIN tasks t ON t.id=a.task_id
 WHERE a.state IN ('ready','delete_failed') AND a.archived_at IS NOT NULL
 AND a.archive_cleanup_at IS NOT NULL AND a.warnings='[]'::jsonb
 AND a.archive_path<>'' AND a.sha256<>'' AND a.compressed_size>0
 AND a.original_status IN ('done','failed','timeout')
 AND a.archived_at <= $2 AND a.archived_at >= a.task_completed_at
 AND a.archive_cleanup_at >= a.archived_at AND a.archive_cleanup_at <= $4
 AND a.task_completed_at IS NOT NULL AND a.task_completed_at <= $4
 AND a.task_completed_at >= t.created_at AND a.task_completed_at = t.completed_at
 AND t.status IN ('done','failed','timeout') AND NOT t.queued
 AND t.archived_at IS NOT NULL AND t.deleted_at IS NOT NULL
 AND NOT EXISTS (SELECT 1 FROM task_archives dependent
   WHERE dependent.id<>a.id AND a.task_id=ANY(dependent.source_task_ids)
   AND dependent.state NOT IN ('delete_queued','deleting'))
 UNION ALL
 SELECT t.id,0::bigint,1,t.completed_at
 FROM tasks t LEFT JOIN task_archives a ON a.task_id=t.id
 WHERE t.deleted_at IS NULL AND t.archived_at IS NULL
 AND t.status IN ('done','failed','timeout') AND NOT t.paused AND NOT t.queued
 AND t.completed_at IS NOT NULL AND t.completed_at >= t.created_at AND t.completed_at <= $1
 AND (a.id IS NULL OR (a.state='archive_failed' AND a.retention_archive_at IS NOT NULL))
 AND NOT EXISTS (SELECT 1 FROM task_relations relation
   JOIN tasks child ON child.id=relation.task_id AND child.deleted_at IS NULL
   LEFT JOIN task_archives pending ON pending.task_id=child.id
   WHERE relation.source_task_id=t.id
   AND (pending.id IS NULL OR pending.state NOT IN ('archive_queued','archiving')))
) due ORDER BY priority,completed,task_id LIMIT $3`, now.Add(-archiveAfter), now.Add(-deleteAfter), limit, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TaskRetentionCandidate
	for rows.Next() {
		var item TaskRetentionCandidate
		if err := rows.Scan(&item.TaskID, &item.ArchiveID); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// ConfirmTaskArchiveCleanup is called only after all external archive stages
// commit successfully. A crash or cleanup warning leaves automatic deletion
// fail-closed, even if the package is otherwise restorable.
func (d *DB) ConfirmTaskArchiveCleanup(id int64) error {
	res, err := d.Exec(`UPDATE task_archives SET archive_cleanup_at=COALESCE(archive_cleanup_at,now())
WHERE id=$1 AND state='ready' AND archived_at IS NOT NULL AND warnings='[]'::jsonb`, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrTaskArchiveState
	}
	return nil
}
