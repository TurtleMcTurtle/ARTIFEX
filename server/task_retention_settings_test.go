package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func retentionSettingsFixture(t *testing.T) *Server {
	t.Helper()
	for _, key := range []string{"ENABLED", "ARCHIVE_AFTER", "DELETE_AFTER", "INTERVAL", "BATCH_SIZE", "CYCLE_TIMEOUT"} {
		t.Setenv("ARTIFEX_TASK_RETENTION_"+key, "")
	}
	s := keyTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.ctx = ctx
	return s
}

func TestTaskRetentionSettingsDefaultsAreReadOnly(t *testing.T) {
	s := retentionSettingsFixture(t)
	token, err := signJWT(s.jwtKey)
	if err != nil {
		t.Fatal(err)
	}
	w := keyRequest(t, s.Handler(), http.MethodGet, "/api/settings/task-retention", token, "")
	if w.Code != http.StatusOK {
		t.Fatalf("settings GET=%d %s", w.Code, w.Body.String())
	}
	var settings map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &settings); err != nil {
		t.Fatal(err)
	}
	if settings["enabled"] != false || settings["archive_after_days"] != float64(7) || settings["delete_after_archive_days"] != float64(30) || settings["check_interval_hours"] != float64(24) {
		t.Fatalf("unexpected defaults: %v", settings)
	}
	var stored, archives int
	if err := s.m.pg.QueryRow(`SELECT count(*) FROM settings WHERE key='task_retention'`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if err := s.m.pg.QueryRow(`SELECT count(*) FROM task_archives`).Scan(&archives); err != nil {
		t.Fatal(err)
	}
	if stored != 0 || archives != 0 {
		t.Fatalf("opening settings mutated storage: settings=%d archives=%d", stored, archives)
	}
}

func TestTaskRetentionSettingsSavePersistsWithoutScheduling(t *testing.T) {
	s := retentionSettingsFixture(t)
	token, _ := signJWT(s.jwtKey)
	before := keyRequest(t, s.Handler(), http.MethodGet, "/api/settings/task-retention", token, "")
	var settings map[string]any
	if err := json.Unmarshal(before.Body.Bytes(), &settings); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{"enabled": false, "archive_after_days": 9, "delete_after_archive_days": 40,
		"check_interval_hours": 24, "batch_size": 50, "cycle_timeout_seconds": 120, "revision": settings["revision"]})
	w := keyRequest(t, s.Handler(), http.MethodPut, "/api/settings/task-retention", token, string(payload))
	if w.Code != http.StatusOK {
		t.Fatalf("settings PUT=%d %s", w.Code, w.Body.String())
	}
	s2 := &Server{m: s.m, engine: s.engine, ctx: s.ctx, jwtKey: s.jwtKey}
	w = keyRequest(t, s2.Handler(), http.MethodGet, "/api/settings/task-retention", token, "")
	if err := json.Unmarshal(w.Body.Bytes(), &settings); err != nil {
		t.Fatal(err)
	}
	if settings["archive_after_days"] != float64(9) || settings["delete_after_archive_days"] != float64(40) || settings["source"] != "saved" {
		t.Fatalf("saved settings did not survive a new server: %v", settings)
	}
	var jobs int
	if err := s.m.pg.QueryRow(`SELECT count(*) FROM task_archives`).Scan(&jobs); err != nil || jobs != 0 {
		t.Fatalf("save triggered a sweep: jobs=%d err=%v", jobs, err)
	}
}

func retentionSettingsRead(t *testing.T, s *Server) map[string]any {
	t.Helper()
	token, _ := signJWT(s.jwtKey)
	w := keyRequest(t, s.Handler(), http.MethodGet, "/api/settings/task-retention", token, "")
	if w.Code != http.StatusOK {
		t.Fatalf("settings GET=%d %s", w.Code, w.Body.String())
	}
	var settings map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &settings); err != nil {
		t.Fatal(err)
	}
	return settings
}

func retentionSettingsWrite(t *testing.T, s *Server, settings map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	fields := []string{"enabled", "archive_after_days", "delete_after_archive_days", "check_interval_hours", "batch_size", "cycle_timeout_seconds", "revision", "confirm_enable"}
	payload := map[string]any{}
	for _, field := range fields {
		if value, ok := settings[field]; ok {
			payload[field] = value
		}
	}
	encoded, _ := json.Marshal(payload)
	token, _ := signJWT(s.jwtKey)
	return keyRequest(t, s.Handler(), http.MethodPut, "/api/settings/task-retention", token, string(encoded))
}

func TestTaskRetentionSettingsRequireConfirmationAndRejectStaleRevision(t *testing.T) {
	s := retentionSettingsFixture(t)
	settings := retentionSettingsRead(t, s)
	settings["enabled"] = true
	if w := retentionSettingsWrite(t, s, settings); w.Code != http.StatusBadRequest {
		t.Fatalf("enabling without confirmation=%d", w.Code)
	}
	settings["confirm_enable"] = true
	if w := retentionSettingsWrite(t, s, settings); w.Code != http.StatusOK {
		t.Fatalf("confirmed enable=%d %s", w.Code, w.Body.String())
	}
	settings["enabled"] = false
	if w := retentionSettingsWrite(t, s, settings); w.Code != http.StatusConflict {
		t.Fatalf("stale settings overwrite=%d", w.Code)
	}
	fresh := retentionSettingsRead(t, s)
	if fresh["enabled"] != true {
		t.Fatal("stale write disabled retention")
	}
	var jobs int
	if err := s.m.pg.QueryRow(`SELECT count(*) FROM task_archives`).Scan(&jobs); err != nil || jobs != 0 {
		t.Fatalf("enable triggered immediate cleanup: %d %v", jobs, err)
	}
}

func TestTaskRetentionSettingsRejectInvalidValuesAndKeepSavedPolicy(t *testing.T) {
	s := retentionSettingsFixture(t)
	base := retentionSettingsRead(t, s)
	for _, change := range []struct {
		field string
		value any
	}{
		{"archive_after_days", 0}, {"archive_after_days", 3651}, {"archive_after_days", 1.5},
		{"delete_after_archive_days", -1}, {"check_interval_hours", 0}, {"check_interval_hours", 169},
		{"batch_size", 101}, {"cycle_timeout_seconds", 601}, {"enabled", nil},
	} {
		t.Run(change.field+"-"+strings.ReplaceAll(strings.TrimSpace(string(retentionSettingsJSON(change.value))), " ", ""), func(t *testing.T) {
			candidate := map[string]any{}
			for key, value := range base {
				candidate[key] = value
			}
			candidate[change.field] = change.value
			if w := retentionSettingsWrite(t, s, candidate); w.Code != http.StatusBadRequest {
				t.Fatalf("invalid settings accepted: %d %s", w.Code, w.Body.String())
			}
		})
	}
	var stored int
	if err := s.m.pg.QueryRow(`SELECT count(*) FROM settings WHERE key='task_retention'`).Scan(&stored); err != nil || stored != 0 {
		t.Fatalf("invalid settings wrote storage: %d %v", stored, err)
	}
}

func retentionSettingsJSON(value any) []byte { encoded, _ := json.Marshal(value); return encoded }

func TestTaskRetentionSettingsRejectTaskKeysAndCookieOnlyWrites(t *testing.T) {
	s := retentionSettingsFixture(t)
	token, _ := signJWT(s.jwtKey)
	w := keyRequest(t, s.Handler(), http.MethodPost, "/api/api-keys", token, `{"scopes":["tasks:read","tasks:write"]}`)
	var key struct{ Key string }
	if err := json.Unmarshal(w.Body.Bytes(), &key); err != nil || key.Key == "" {
		t.Fatal("fixture key unavailable")
	}
	for _, method := range []string{http.MethodGet, http.MethodPut} {
		w = keyRequest(t, s.Handler(), method, "/api/settings/task-retention", key.Key, `{}`)
		if w.Code != http.StatusForbidden {
			t.Fatalf("task key reached settings: %d", w.Code)
		}
	}
	r := httptest.NewRequest(http.MethodPut, "/api/settings/task-retention", strings.NewReader(`{}`))
	r.AddCookie(&http.Cookie{Name: "artifex_token", Value: token})
	w = httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("cookie-only settings write accepted: %d", w.Code)
	}
}

func TestTaskRetentionSettingsStoredPolicyDrivesDailyScheduler(t *testing.T) {
	s := retentionSettingsFixture(t)
	task := retentionServerTask(t, s, time.Now().Add(-8*24*time.Hour))
	settings := retentionSettingsRead(t, s)
	settings["enabled"], settings["confirm_enable"] = true, true
	if w := retentionSettingsWrite(t, s, settings); w.Code != http.StatusOK {
		t.Fatalf("save=%d %s", w.Code, w.Body.String())
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.archiveWake = make(chan struct{}, 1)
	s.startTaskRetentionScheduler()
	defer func() { cancel(); s.archiveWG.Wait() }()
	if job, _ := s.m.pg.GetTaskArchiveByTask(mustTaskNumericID(t, task.ID)); job != nil {
		t.Fatal("startup ran immediate cleanup")
	}
	fresh := retentionSettingsRead(t, s)
	if fresh["scheduler_running"] != true || fresh["next_check_unix"] == nil {
		t.Fatalf("saved enabled config not applied: %v", fresh)
	}
	fresh["enabled"] = false
	if w := retentionSettingsWrite(t, s, fresh); w.Code != http.StatusOK {
		t.Fatalf("disable=%d %s", w.Code, w.Body.String())
	}
	fresh = retentionSettingsRead(t, s)
	if fresh["scheduler_running"] != false || fresh["next_check_unix"] != nil {
		t.Fatalf("disable not applied live: %v", fresh)
	}
}

func TestTaskRetentionSettingsEnableAndUpdateWithoutRestart(t *testing.T) {
	s := retentionSettingsFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.startTaskRetentionScheduler()
	defer func() { cancel(); s.archiveWG.Wait() }()
	s.retentionMu.Lock()
	wake := s.retentionWake
	s.retentionMu.Unlock()
	s.startTaskRetentionScheduler()
	if s.retentionWake != wake {
		t.Fatal("started a second scheduler")
	}
	settings := retentionSettingsRead(t, s)
	settings["enabled"], settings["confirm_enable"] = true, true
	if w := retentionSettingsWrite(t, s, settings); w.Code != http.StatusOK {
		t.Fatalf("enable=%d %s", w.Code, w.Body.String())
	}
	var next int64
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		settings = retentionSettingsRead(t, s)
		if n, ok := settings["next_check_unix"].(float64); ok {
			next = int64(n)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if next < time.Now().Add(23*time.Hour).Unix() {
		t.Fatalf("enable swept early or failed to schedule: %v", settings)
	}
	settings["check_interval_hours"], settings["confirm_enable"] = 48, true
	if w := retentionSettingsWrite(t, s, settings); w.Code != http.StatusOK {
		t.Fatalf("update=%d %s", w.Code, w.Body.String())
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		settings = retentionSettingsRead(t, s)
		if n, ok := settings["next_check_unix"].(float64); ok && int64(n) >= time.Now().Add(47*time.Hour).Unix() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("new interval not applied without restart: %v", settings)
}

func TestTaskRetentionSettingsFailClosedOnBadStorageAndFailedSave(t *testing.T) {
	s := retentionSettingsFixture(t)
	before := retentionSettingsRead(t, s)
	if _, err := s.m.pg.Exec(`INSERT INTO settings(key,value) VALUES('task_retention','{"enabled":true}')`); err != nil {
		t.Fatal(err)
	}
	token, _ := signJWT(s.jwtKey)
	if w := keyRequest(t, s.Handler(), http.MethodGet, "/api/settings/task-retention", token, ""); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("corrupt stored config returned %d", w.Code)
	}
	if _, err := s.m.pg.Exec(`DELETE FROM settings WHERE key='task_retention'; ALTER TABLE settings ADD CONSTRAINT retention_fixture_reject CHECK(key<>'task_retention')`); err != nil {
		t.Fatal(err)
	}
	if w := retentionSettingsWrite(t, s, before); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("save failure returned %d", w.Code)
	}
	if got := retentionSettingsRead(t, s); got["source"] != "environment" || got["enabled"] != false {
		t.Fatalf("failed save changed effective config: %v", got)
	}
}

func TestTaskRetentionSettingsStaleCycleCannotOverrideDisable(t *testing.T) {
	s := retentionSettingsFixture(t)
	settings := retentionSettingsRead(t, s)
	settings["enabled"], settings["confirm_enable"] = true, true
	if w := retentionSettingsWrite(t, s, settings); w.Code != http.StatusOK {
		t.Fatalf("enable=%d", w.Code)
	}
	cfg, _, revision, err := s.loadTaskRetentionConfig(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	settings = retentionSettingsRead(t, s)
	settings["enabled"] = false
	if w := retentionSettingsWrite(t, s, settings); w.Code != http.StatusOK {
		t.Fatalf("disable=%d", w.Code)
	}
	retentionServerTask(t, s, time.Now().Add(-8*24*time.Hour))
	counts, err := s.runConfiguredTaskRetentionCycle(context.Background(), cfg, revision, time.Now())
	if err != nil || counts.ArchiveQueued != 0 {
		t.Fatalf("stale cycle ignored current disabled policy: %+v %v", counts, err)
	}
}

func TestTaskRetentionSettingsRecoverAfterInvalidStartupPolicyWithoutRestart(t *testing.T) {
	s := retentionSettingsFixture(t)
	if _, err := s.m.pg.Exec(`INSERT INTO settings(key,value) VALUES('task_retention','invalid')`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.startTaskRetentionScheduler()
	defer func() { cancel(); s.archiveWG.Wait() }()
	if _, err := s.m.pg.Exec(`DELETE FROM settings WHERE key='task_retention'`); err != nil {
		t.Fatal(err)
	}
	settings := retentionSettingsRead(t, s)
	settings["enabled"], settings["confirm_enable"] = true, true
	if w := retentionSettingsWrite(t, s, settings); w.Code != http.StatusOK {
		t.Fatalf("save=%d", w.Code)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		settings = retentionSettingsRead(t, s)
		if settings["scheduler_running"] == true && settings["next_check_unix"] != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("repaired settings require a restart: %v", settings)
}

func TestTaskRetentionSettingsDisableReenableInvalidatesDueCycle(t *testing.T) {
	s := retentionSettingsFixture(t)
	settings := retentionSettingsRead(t, s)
	settings["enabled"], settings["confirm_enable"] = true, true
	if w := retentionSettingsWrite(t, s, settings); w.Code != http.StatusOK {
		t.Fatalf("first enable=%d %s", w.Code, w.Body.String())
	}
	cfg, _, oldRevision, err := s.loadTaskRetentionConfig(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	settings = retentionSettingsRead(t, s)
	settings["enabled"] = false
	if w := retentionSettingsWrite(t, s, settings); w.Code != http.StatusOK {
		t.Fatalf("disable=%d %s", w.Code, w.Body.String())
	}
	settings = retentionSettingsRead(t, s)
	settings["enabled"], settings["confirm_enable"] = true, true
	if w := retentionSettingsWrite(t, s, settings); w.Code != http.StatusOK {
		t.Fatalf("second enable=%d %s", w.Code, w.Body.String())
	}
	retentionServerTask(t, s, time.Now().Add(-8*24*time.Hour))
	counts, err := s.runConfiguredTaskRetentionCycle(context.Background(), cfg, oldRevision, time.Now())
	if err != nil || counts.ArchiveQueued != 0 || counts.DeleteQueued != 0 {
		t.Fatalf("old due tick survived disable/re-enable: %+v %v", counts, err)
	}
	fresh := retentionSettingsRead(t, s)
	if fresh["revision"] == oldRevision {
		t.Fatal("identical re-enabled policy reused its old generation")
	}
	var jobs int
	if err := s.m.pg.QueryRow(`SELECT count(*) FROM task_archives`).Scan(&jobs); err != nil || jobs != 0 {
		t.Fatalf("re-enable allowed immediate cleanup: jobs=%d err=%v", jobs, err)
	}
}

func TestTaskRetentionSettingsEverySaveAdvancesPersistedGeneration(t *testing.T) {
	s := retentionSettingsFixture(t)
	settings := retentionSettingsRead(t, s)
	if w := retentionSettingsWrite(t, s, settings); w.Code != http.StatusOK {
		t.Fatalf("first save=%d %s", w.Code, w.Body.String())
	}
	settings = retentionSettingsRead(t, s)
	firstRevision := settings["revision"]
	if w := retentionSettingsWrite(t, s, settings); w.Code != http.StatusOK {
		t.Fatalf("identical save=%d %s", w.Code, w.Body.String())
	}
	second := retentionSettingsRead(t, s)
	if second["revision"] == firstRevision {
		t.Fatal("successful identical save did not advance the generation")
	}
	if w := retentionSettingsWrite(t, s, settings); w.Code != http.StatusConflict {
		t.Fatalf("first generation can still overwrite the second: %d", w.Code)
	}
	s2 := &Server{m: s.m, engine: s.engine, ctx: s.ctx, jwtKey: s.jwtKey}
	if reloaded := retentionSettingsRead(t, s2); reloaded["revision"] != second["revision"] {
		t.Fatal("generation did not survive a new server instance")
	}
}

func TestTaskRetentionSettingsLegacyPolicyReceivesGenerationOnSave(t *testing.T) {
	s := retentionSettingsFixture(t)
	raw := `{"enabled":false,"archive_after_days":7,"delete_after_archive_days":30,"check_interval_hours":24,"batch_size":50,"cycle_timeout_seconds":120}`
	if _, err := s.m.pg.Exec(`INSERT INTO settings(key,value) VALUES($1,$2)`, taskRetentionSettingKey, raw); err != nil {
		t.Fatal(err)
	}
	settings := retentionSettingsRead(t, s)
	if settings["revision"] != retentionSettingsRevision("saved", raw) {
		t.Fatal("legacy saved policy was not readable")
	}
	if w := retentionSettingsWrite(t, s, settings); w.Code != http.StatusOK {
		t.Fatalf("legacy save=%d %s", w.Code, w.Body.String())
	}
	var persisted string
	if err := s.m.pg.QueryRow(`SELECT value FROM settings WHERE key=$1`, taskRetentionSettingKey).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	var stored storedTaskRetentionPolicy
	if err := json.Unmarshal([]byte(persisted), &stored); err != nil || len(stored.Generation) != 64 {
		t.Fatalf("new generation was not persisted: %v", err)
	}
	fresh := retentionSettingsRead(t, s)
	if fresh["revision"] != stored.Generation || fresh["revision"] == settings["revision"] {
		t.Fatal("legacy write failed to advance the generation")
	}
}

func TestTaskRetentionSettingsInvalidGenerationFailsClosed(t *testing.T) {
	s := retentionSettingsFixture(t)
	raw := `{"enabled":true,"archive_after_days":7,"delete_after_archive_days":30,"check_interval_hours":24,"batch_size":50,"cycle_timeout_seconds":120,"generation":"bad-generation"}`
	if _, err := s.m.pg.Exec(`INSERT INTO settings(key,value) VALUES($1,$2)`, taskRetentionSettingKey, raw); err != nil {
		t.Fatal(err)
	}
	token, _ := signJWT(s.jwtKey)
	if w := keyRequest(t, s.Handler(), http.MethodGet, "/api/settings/task-retention", token, ""); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("invalid persisted generation accepted: %d", w.Code)
	}
}

func TestTaskRetentionSettingsReenableResetsLiveDeadline(t *testing.T) {
	s := retentionSettingsFixture(t)
	settings := retentionSettingsRead(t, s)
	settings["enabled"], settings["confirm_enable"] = true, true
	if w := retentionSettingsWrite(t, s, settings); w.Code != http.StatusOK {
		t.Fatalf("enable=%d %s", w.Code, w.Body.String())
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.startTaskRetentionScheduler()
	defer func() { cancel(); s.archiveWG.Wait() }()
	s.retentionMu.Lock()
	s.retentionNextCheck = time.Now().Add(-time.Second)
	s.retentionMu.Unlock()
	settings = retentionSettingsRead(t, s)
	oldRevision := settings["revision"]
	settings["enabled"] = false
	if w := retentionSettingsWrite(t, s, settings); w.Code != http.StatusOK {
		t.Fatalf("disable=%d %s", w.Code, w.Body.String())
	}
	settings = retentionSettingsRead(t, s)
	settings["enabled"], settings["confirm_enable"] = true, true
	if w := retentionSettingsWrite(t, s, settings); w.Code != http.StatusOK {
		t.Fatalf("re-enable=%d %s", w.Code, w.Body.String())
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		settings = retentionSettingsRead(t, s)
		if next, ok := settings["next_check_unix"].(float64); ok && settings["revision"] != oldRevision && int64(next) >= time.Now().Add(23*time.Hour).Unix() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("re-enable reused the old live deadline: %v", settings)
}
