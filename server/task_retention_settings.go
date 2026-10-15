package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const taskRetentionSettingKey = "task_retention"

var errRetentionSettingsChanged = errors.New("retention settings changed; reload before saving")

type taskRetentionPolicy struct {
	Enabled                bool `json:"enabled"`
	ArchiveAfterDays       int  `json:"archive_after_days"`
	DeleteAfterArchiveDays int  `json:"delete_after_archive_days"`
	CheckIntervalHours     int  `json:"check_interval_hours"`
	BatchSize              int  `json:"batch_size"`
	CycleTimeoutSeconds    int  `json:"cycle_timeout_seconds"`
}

type storedTaskRetentionPolicy struct {
	taskRetentionPolicy
	Generation string `json:"generation,omitempty"`
}

func (p taskRetentionPolicy) config() (taskRetentionConfig, error) {
	if p.ArchiveAfterDays < 1 || p.ArchiveAfterDays > 3650 || p.DeleteAfterArchiveDays < 1 || p.DeleteAfterArchiveDays > 3650 || p.CheckIntervalHours < 1 || p.CheckIntervalHours > 168 || p.BatchSize < 1 || p.BatchSize > 100 || p.CycleTimeoutSeconds < 1 || p.CycleTimeoutSeconds > 600 {
		return taskRetentionConfig{}, fmt.Errorf("archive/delete days must be 1..3650, interval hours 1..168, batch 1..100, timeout seconds 1..600")
	}
	return taskRetentionConfig{Enabled: p.Enabled, ArchiveAfter: time.Duration(p.ArchiveAfterDays) * 24 * time.Hour, DeleteAfter: time.Duration(p.DeleteAfterArchiveDays) * 24 * time.Hour, Interval: time.Duration(p.CheckIntervalHours) * time.Hour, BatchSize: p.BatchSize, CycleTimeout: time.Duration(p.CycleTimeoutSeconds) * time.Second}, nil
}

func retentionSettingsRevision(source, raw string) string {
	hash := sha256.Sum256([]byte(source + ":" + raw))
	return hex.EncodeToString(hash[:])
}

func (s *Server) loadTaskRetentionConfig(ctx context.Context) (taskRetentionConfig, string, string, error) {
	if s == nil || s.m == nil || s.m.pg == nil {
		return taskRetentionConfig{}, "", "", errors.New("retention storage unavailable")
	}
	var raw string
	err := s.m.pg.QueryRowContext(ctx, `SELECT value FROM settings WHERE key=$1`, taskRetentionSettingKey).Scan(&raw)
	if err == sql.ErrNoRows {
		cfg, err := taskRetentionConfigFromEnv(os.Getenv)
		encoded, _ := json.Marshal(cfg)
		return cfg, "environment", retentionSettingsRevision("environment", string(encoded)), err
	}
	if err != nil {
		return taskRetentionConfig{}, "", "", err
	}
	var stored storedTaskRetentionPolicy
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&stored); err != nil || decoder.Decode(new(any)) != io.EOF {
		return taskRetentionConfig{}, "saved", "", errors.New("invalid saved retention settings")
	}
	cfg, err := stored.taskRetentionPolicy.config()
	revision := retentionSettingsRevision("saved", raw)
	if stored.Generation != "" {
		if generation, decodeErr := hex.DecodeString(stored.Generation); decodeErr != nil || len(generation) != 32 {
			return taskRetentionConfig{}, "saved", "", errors.New("invalid saved retention generation")
		}
		revision = stored.Generation
	}
	return cfg, "saved", revision, err
}

func (s *Server) retentionSettingsResponse(cfg taskRetentionConfig, source, revision string) map[string]any {
	s.retentionMu.Lock()
	available := s.retentionStarted && s.ctx != nil && s.ctx.Err() == nil
	var nextCheck any
	if available && cfg.Enabled && s.retentionRevision == revision && !s.retentionNextCheck.IsZero() {
		nextCheck = s.retentionNextCheck.Unix()
	}
	s.retentionMu.Unlock()
	return map[string]any{
		"enabled": cfg.Enabled, "archive_after_days": cfg.ArchiveAfter.Hours() / 24,
		"delete_after_archive_days": cfg.DeleteAfter.Hours() / 24, "check_interval_hours": cfg.Interval.Hours(),
		"batch_size": cfg.BatchSize, "cycle_timeout_seconds": cfg.CycleTimeout.Seconds(),
		"source": source, "revision": revision, "scheduler_available": available,
		"scheduler_running": available && cfg.Enabled, "next_check_unix": nextCheck,
	}
}

func (s *Server) retentionSettingsAdmin(w http.ResponseWriter, r *http.Request) bool {
	if apiKeyPrincipal(r) != nil || r.Header.Get("X-API-Key") != "" {
		writeErr(w, http.StatusForbidden, "Task API keys cannot manage retention settings")
		return false
	}
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
		writeErr(w, http.StatusUnauthorized, "Administrator bearer authentication required")
		return false
	}
	if _, ok := jwtOwner(token, s.jwtKey); !ok {
		writeErr(w, http.StatusUnauthorized, "Administrator authentication required")
		return false
	}
	return true
}

func (s *Server) getTaskRetentionSettings(w http.ResponseWriter, r *http.Request) {
	if !s.retentionSettingsAdmin(w, r) {
		return
	}
	cfg, source, revision, err := s.loadTaskRetentionConfig(r.Context())
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "Retention settings unavailable")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, s.retentionSettingsResponse(cfg, source, revision))
}

func (s *Server) putTaskRetentionSettings(w http.ResponseWriter, r *http.Request) {
	if !s.retentionSettingsAdmin(w, r) {
		return
	}
	var req struct {
		Enabled                *bool  `json:"enabled"`
		ArchiveAfterDays       *int   `json:"archive_after_days"`
		DeleteAfterArchiveDays *int   `json:"delete_after_archive_days"`
		CheckIntervalHours     *int   `json:"check_interval_hours"`
		BatchSize              *int   `json:"batch_size"`
		CycleTimeoutSeconds    *int   `json:"cycle_timeout_seconds"`
		Revision               string `json:"revision"`
		ConfirmEnable          bool   `json:"confirm_enable"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil || decoder.Decode(new(any)) != io.EOF || req.Enabled == nil || req.ArchiveAfterDays == nil || req.DeleteAfterArchiveDays == nil || req.CheckIntervalHours == nil || req.BatchSize == nil || req.CycleTimeoutSeconds == nil || req.Revision == "" {
		writeErr(w, http.StatusBadRequest, "All retention settings and their current revision are required")
		return
	}
	policy := taskRetentionPolicy{*req.Enabled, *req.ArchiveAfterDays, *req.DeleteAfterArchiveDays, *req.CheckIntervalHours, *req.BatchSize, *req.CycleTimeoutSeconds}
	cfg, err := policy.config()
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if cfg.Enabled && !req.ConfirmEnable {
		writeErr(w, http.StatusBadRequest, "Confirm automatic archival and permanent deletion before saving enabled settings")
		return
	}
	var generation string
	locked, err := s.m.pg.WithTaskRetentionCycleLock(r.Context(), func() error {
		_, _, revision, err := s.loadTaskRetentionConfig(r.Context())
		if err != nil {
			return err
		}
		if req.Revision != revision {
			return errRetentionSettingsChanged
		}
		var nonce [32]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return err
		}
		generation = hex.EncodeToString(nonce[:])
		if generation == revision {
			return errors.New("retention generation did not advance")
		}
		encoded, err := json.Marshal(storedTaskRetentionPolicy{taskRetentionPolicy: policy, Generation: generation})
		if err != nil {
			return err
		}
		_, err = s.m.pg.ExecContext(r.Context(), `INSERT INTO settings(key,value) VALUES($1,$2) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,updated_at=now()`, taskRetentionSettingKey, string(encoded))
		return err
	})
	if errors.Is(err, errRetentionSettingsChanged) || err == nil && !locked {
		writeErr(w, http.StatusConflict, "Retention settings changed or a cycle is active; reload before saving")
		return
	}
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "Retention settings could not be saved; reload before retrying")
		return
	}
	s.notifyTaskRetentionSettings()
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, s.retentionSettingsResponse(cfg, "saved", generation))
}
