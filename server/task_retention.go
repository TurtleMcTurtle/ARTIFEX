package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	pgdb "github.com/sebastian93921/artifex/db"
)

// Native backend retention is opt-in. No cron enrollment, destructive startup
// sweep, or deployment/configuration writes are performed by this feature.
type taskRetentionConfig struct {
	Enabled                                           bool
	ArchiveAfter, DeleteAfter, Interval, CycleTimeout time.Duration
	BatchSize                                         int
}

func taskRetentionConfigFromEnv(getenv func(string) string) (taskRetentionConfig, error) {
	cfg := taskRetentionConfig{ArchiveAfter: 7 * 24 * time.Hour, DeleteAfter: 30 * 24 * time.Hour,
		Interval: 24 * time.Hour, CycleTimeout: 2 * time.Minute, BatchSize: 50}
	if raw := strings.TrimSpace(getenv("ARTIFEX_TASK_RETENTION_ENABLED")); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			return cfg, fmt.Errorf("ARTIFEX_TASK_RETENTION_ENABLED must be a boolean")
		}
		cfg.Enabled = v
	}
	for _, field := range []struct {
		name   string
		target *time.Duration
	}{
		{"ARCHIVE_AFTER", &cfg.ArchiveAfter}, {"DELETE_AFTER", &cfg.DeleteAfter},
		{"INTERVAL", &cfg.Interval}, {"CYCLE_TIMEOUT", &cfg.CycleTimeout},
	} {
		if raw := strings.TrimSpace(getenv("ARTIFEX_TASK_RETENTION_" + field.name)); raw != "" {
			v, err := time.ParseDuration(raw)
			if err != nil || v <= 0 {
				return cfg, fmt.Errorf("ARTIFEX_TASK_RETENTION_%s must be a positive Go duration (e.g. 168h)", field.name)
			}
			*field.target = v
		}
	}
	if raw := strings.TrimSpace(getenv("ARTIFEX_TASK_RETENTION_BATCH_SIZE")); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil {
			return cfg, fmt.Errorf("ARTIFEX_TASK_RETENTION_BATCH_SIZE must be an integer")
		}
		cfg.BatchSize = v
	}
	// The archive-age and archive-residence durations are independent clocks.
	if cfg.BatchSize < 1 || cfg.BatchSize > 100 {
		return cfg, fmt.Errorf("task retention batch size must be 1..100")
	}
	if cfg.CycleTimeout > 10*time.Minute {
		return cfg, fmt.Errorf("task retention cycle timeout must not exceed 10m")
	}
	return cfg, nil
}

type taskRetentionCounts struct {
	Candidates, ArchiveQueued, DeleteQueued, Skipped, Errors int
	LockSkipped                                              bool
}

// hasTaskRetentionHotPayload is a read-only, confined probe. It also protects
// unjournalled remnants left by the previous rename-based probe: absence from
// the live workspace is not evidence that those files were archived.
func hasTaskRetentionHotPayload(dataDir, taskID string, explorationID int64) (bool, error) {
	f, err := openTaskArchiveFS(dataDir)
	if err != nil {
		return false, err
	}
	defer f.Close()
	workspace := filepath.Join(f.dir, "tasks", taskID)
	if _, err := f.Lstat(workspace); err == nil {
		return true, nil
	} else if !os.IsNotExist(err) {
		return false, err
	}
	transcripts := filepath.Join(f.dir, "transcripts")
	entries, err := f.ReadDir(transcripts)
	if err != nil && !os.IsNotExist(err) {
		return false, err
	}
	prefix := fmt.Sprintf("exp%d-", explorationID)
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), prefix) && (entry.IsDir() || strings.HasSuffix(entry.Name(), ".jsonl") || entry.Type()&os.ModeSymlink != 0) {
			return true, nil
		}
	}
	entries, err = f.ReadDir(filepath.Join(f.dir, ".delete-staging"))
	if err != nil && !os.IsNotExist(err) {
		return false, err
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "task-"+taskID+"-") {
			return true, nil
		}
	}
	return false, nil
}

func (s *Server) runTaskRetentionCycle(ctx context.Context, cfg taskRetentionConfig, now time.Time) (taskRetentionCounts, error) {
	var counts taskRetentionCounts
	if s == nil || s.m == nil || s.m.pg == nil {
		return counts, fmt.Errorf("task retention database unavailable")
	}
	cycleCtx, cancel := context.WithTimeout(ctx, cfg.CycleTimeout)
	defer cancel()
	locked, err := s.m.pg.WithTaskRetentionCycleLock(cycleCtx, func() error {
		var runErr error
		counts, runErr = s.queueTaskRetentionBatch(cycleCtx, cfg, now)
		return runErr
	})
	counts.LockSkipped = !locked && err == nil
	return counts, err
}

func (s *Server) queueTaskRetentionBatch(cycleCtx context.Context, cfg taskRetentionConfig, now time.Time) (taskRetentionCounts, error) {
	var counts taskRetentionCounts
	items, err := s.m.pg.ListTaskRetentionCandidates(cycleCtx, now, cfg.ArchiveAfter, cfg.DeleteAfter, cfg.BatchSize)
	if err != nil {
		return counts, err
	}
	counts.Candidates = len(items)
	for _, item := range items {
		if err := cycleCtx.Err(); err != nil {
			return counts, err
		}
		if item.ArchiveID == 0 {
			_, err = s.m.pg.QueueTaskArchiveForRetention(cycleCtx, item.TaskID, now, cfg.ArchiveAfter, cfg.DeleteAfter)
			if err == nil {
				counts.ArchiveQueued++
			}
		} else {
			_, err = s.m.pg.QueueTaskArchiveDeleteForRetention(cycleCtx, item.ArchiveID, now, cfg.DeleteAfter)
			if err == nil {
				counts.DeleteQueued++
			}
		}
		if err != nil {
			if errors.Is(err, pgdb.ErrTaskRetentionIneligible) || errors.Is(err, pgdb.ErrTaskArchiveState) ||
				errors.Is(err, pgdb.ErrTaskArchiveNotFound) || errors.Is(err, pgdb.ErrTaskArchiveDependent) || errors.Is(err, pgdb.ErrTaskArchiveDeleteBlocked) {
				counts.Skipped++
			} else {
				counts.Errors++
				log.Printf("[task-retention] task=%d archive=%d enqueue failed; retry next cycle: %v", item.TaskID, item.ArchiveID, err)
			}
		}
	}
	if counts.ArchiveQueued+counts.DeleteQueued > 0 {
		s.notifyTaskArchiveWorker()
	}
	return counts, nil
}

func (s *Server) startTaskRetentionScheduler() {
	if s == nil || s.m == nil || s.m.pg == nil || s.ctx == nil || s.ctx.Err() != nil {
		return
	}
	cfg, _, revision, err := s.loadTaskRetentionConfig(s.ctx)
	if err != nil {
		log.Printf("[task-retention] disabled: invalid configuration: %v", err)
		cfg = taskRetentionConfig{Interval: time.Hour}
		revision = ""
	}
	s.retentionMu.Lock()
	if s.retentionStarted {
		s.retentionMu.Unlock()
		return
	}
	s.retentionStarted = true
	s.retentionWake = make(chan struct{}, 1)
	s.retentionRevision = revision
	if cfg.Enabled {
		s.retentionNextCheck = time.Now().Add(cfg.Interval)
	}
	wake := s.retentionWake
	s.retentionMu.Unlock()
	s.archiveWG.Add(1)
	go func() {
		defer s.archiveWG.Done()
		s.runLiveTaskRetentionScheduler(s.ctx, cfg, revision, wake)
	}()
}

func (s *Server) notifyTaskRetentionSettings() {
	s.retentionMu.Lock()
	wake := s.retentionWake
	s.retentionMu.Unlock()
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (s *Server) retentionSchedule(cfg taskRetentionConfig, revision string) time.Time {
	next := time.Time{}
	if cfg.Enabled {
		next = time.Now().Add(cfg.Interval)
	}
	s.retentionMu.Lock()
	s.retentionRevision, s.retentionNextCheck = revision, next
	s.retentionMu.Unlock()
	return next
}

func (s *Server) runLiveTaskRetentionScheduler(ctx context.Context, cfg taskRetentionConfig, revision string, wake <-chan struct{}) {
	poll := time.NewTicker(time.Second)
	defer poll.Stop()
	timer := time.NewTimer(cfg.Interval)
	defer timer.Stop()
	if !cfg.Enabled {
		timer.Stop()
	}
	defer func() {
		s.retentionMu.Lock()
		s.retentionStarted = false
		s.retentionNextCheck = time.Time{}
		s.retentionMu.Unlock()
	}()
	reset := func(next taskRetentionConfig, rev string) {
		cfg, revision = next, rev
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		if !s.retentionSchedule(cfg, revision).IsZero() {
			timer.Reset(cfg.Interval)
		}
	}
	reload := func() {
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		next, _, rev, err := s.loadTaskRetentionConfig(readCtx)
		if err != nil {
			reset(taskRetentionConfig{}, "")
			return
		}
		if rev != revision {
			reset(next, rev)
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-wake:
			reload()
		case <-poll.C:
			reload()
		case now := <-timer.C:
			counts, err := s.runConfiguredTaskRetentionCycle(ctx, cfg, revision, now.UTC())
			log.Printf("[task-retention] archive_queued=%d delete_queued=%d skipped=%d errors=%d cycle_error=%v", counts.ArchiveQueued, counts.DeleteQueued, counts.Skipped, counts.Errors, err)
			reload()
			if cfg.Enabled {
				s.retentionSchedule(cfg, revision)
				timer.Reset(cfg.Interval)
			}
		}
	}
}

func (s *Server) runConfiguredTaskRetentionCycle(ctx context.Context, cfg taskRetentionConfig, revision string, now time.Time) (taskRetentionCounts, error) {
	var counts taskRetentionCounts
	cycleCtx, cancel := context.WithTimeout(ctx, cfg.CycleTimeout)
	defer cancel()
	locked, err := s.m.pg.WithTaskRetentionCycleLock(cycleCtx, func() error {
		current, _, rev, err := s.loadTaskRetentionConfig(cycleCtx)
		if err != nil || !current.Enabled || rev != revision {
			return err
		}
		counts, err = s.queueTaskRetentionBatch(cycleCtx, current, now)
		return err
	})
	counts.LockSkipped = !locked && err == nil
	return counts, err
}

func (s *Server) runTaskRetentionScheduler(ctx context.Context, cfg taskRetentionConfig, ticks <-chan time.Time) {
	if !cfg.Enabled {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case now, ok := <-ticks:
			if !ok || ctx.Err() != nil {
				return
			}
			counts, err := s.runTaskRetentionCycle(ctx, cfg, now.UTC())
			log.Printf("[task-retention] candidates=%d archive_queued=%d delete_queued=%d skipped=%d errors=%d lock_skipped=%v cycle_error=%v", counts.Candidates, counts.ArchiveQueued, counts.DeleteQueued, counts.Skipped, counts.Errors, counts.LockSkipped, err)
		}
	}
}
