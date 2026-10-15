package server

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/sebastian93921/artifex/db"
)

type reportCounterSnapshot struct {
	Tokens       TokenTotalDTO
	GoalsTotal   int
	GoalsMet     int
	InFlight     int
	LastActivity int64
	Findings     FindingSeverityDTO
}

func reportCounters(dto TaskDTO) reportCounterSnapshot {
	return reportCounterSnapshot{
		Tokens: dto.Tokens, GoalsTotal: dto.GoalsTotal, GoalsMet: dto.GoalsMet,
		InFlight: dto.InFlight, LastActivity: dto.LastActivity, Findings: dto.Findings,
	}
}

// The existing integration fixture creates a fresh database on loopback
// PostgreSQL and cancels the server runtime before any task is created. Tasks
// below are persisted through the manager only: no seed, admission or LLM calls.
func persistedReportCounterFixture(t *testing.T) (*Server, string, *Task, reportCounterSnapshot) {
	t.Helper()
	s, key := integrationFixture(t)
	if s.ctx.Err() == nil {
		t.Fatal("counter fixture requires a cancelled runtime")
	}
	// Task and exploration IDs are separate sequences. Offset the latter so
	// the regression detects hydration keyed by task ID instead of ExpID.
	if _, err := s.m.pg.CreateExploration("unrelated exploration fixture", "no network targets"); err != nil {
		t.Fatal(err)
	}
	task, err := s.m.CreateTask("report counter fixture", "no network targets", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	other, err := s.m.CreateTask("unrelated counter fixture", "no network targets", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	resultAt := time.Unix(1700000000, 0).UTC()
	latestAt := resultAt.Add(2 * time.Second)
	if _, err := s.m.pg.Exec(`
INSERT INTO activity(exploration_id, worker, kind, input_tokens, output_tokens,
                     cache_read_tokens, cache_write_tokens, created_at)
VALUES ($1,'mainagent','result',11,7,3,2,$2),
       ($1,'planner','result',17,13,5,4,$2),
       ($1,'work#1','tool_result',999,999,999,999,$3),
       ($4,'mainagent','result',777,777,777,777,$3)`,
		task.ExpID, resultAt, latestAt, other.ExpID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.m.pg.Exec(`
INSERT INTO exploration_nodes(exploration_id, kind, payload, state)
VALUES ($1,'goal','{}','met'), ($1,'goal','{}','open'),
       ($1,'intent','{}','running'), ($1,'intent','{}','running'),
       ($1,'intent','{}','open'), ($1,'intent','{}','blocked'),
       ($2,'goal','{}','met'), ($2,'intent','{}','running')`, task.ExpID, other.ExpID); err != nil {
		t.Fatal(err)
	}

	// Raw call metering and debug records describe the same completed runs.
	// They must not be added to the canonical result-row totals a second time.
	for _, usage := range []db.LLMUsage{
		{TaskID: task.ID, ExplorationID: task.ExpID, Worker: "mainagent", Model: "fixture-only", InputTokens: 11, OutputTokens: 7, CacheRead: 3, CacheWrite: 2, Status: "ok"},
		{TaskID: task.ID, ExplorationID: task.ExpID, Worker: "planner", Model: "fixture-only", InputTokens: 17, OutputTokens: 13, CacheRead: 5, CacheWrite: 4, Status: "ok"},
	} {
		if err := s.m.pg.InsertLLMUsage(&usage); err != nil {
			t.Fatal(err)
		}
		if err := s.m.pg.InsertLLMRecord(&db.LLMRecord{
			TaskID: usage.TaskID, Worker: usage.Worker, Model: usage.Model,
			InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens,
			CacheRead: usage.CacheRead, CacheWrite: usage.CacheWrite, Status: usage.Status,
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, severity := range []string{"critical", "high", "high", "medium", "low", "info"} {
		if _, err := s.m.pg.AddFinding(mustInt(task.ID), 0, "test", "counter fixture", severity, "fixture summary", "fixture evidence", "test", nil); err != nil {
			t.Fatal(err)
		}
	}
	want := reportCounterSnapshot{
		Tokens:     TokenTotalDTO{InputTokens: 28, OutputTokens: 20, CacheReadTokens: 8, CacheWriteTokens: 6},
		GoalsTotal: 2, GoalsMet: 1, InFlight: 2, LastActivity: latestAt.Unix(),
		Findings: FindingSeverityDTO{Critical: 1, High: 2, Medium: 1, Low: 1},
	}
	return s, key, task, want
}

func listedReportCounters(t *testing.T, h http.Handler, key, id string) reportCounterSnapshot {
	t.Helper()
	w := keyRequest(t, h, http.MethodGet, "/api/tasks", key, "")
	if w.Code != http.StatusOK {
		t.Fatalf("list HTTP=%d body=%s", w.Code, w.Body.String())
	}
	var list struct {
		Tasks []TaskDTO `json:"tasks"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	for _, task := range list.Tasks {
		if task.ID == id {
			return reportCounters(task)
		}
	}
	t.Fatalf("task %s missing from list", id)
	return reportCounterSnapshot{}
}

func TestTaskReportCountersMatchPersistedList(t *testing.T) {
	s, key, task, want := persistedReportCounterFixture(t)
	h := s.Handler()
	listed := listedReportCounters(t, h, key, task.ID)
	if listed != want {
		t.Fatalf("canonical list counters=%+v, want persisted fixture %+v", listed, want)
	}
	w := keyRequest(t, h, http.MethodGet, "/api/tasks/"+task.ID+"/report", key, "")
	if w.Code != http.StatusOK {
		t.Fatalf("report HTTP=%d body=%s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("report Cache-Control=%q, want no-store", got)
	}
	var report struct {
		SchemaVersion      string         `json:"schema_version"`
		Task               TaskDTO        `json:"task"`
		Outcome            string         `json:"outcome"`
		ScopeEnforcement   string         `json:"scope_enforcement"`
		NetworkScope       bool           `json:"network_scope_enforced"`
		FindingsBySeverity map[string]int `json:"findings_by_severity"`
		Limitations        []string       `json:"limitations"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Task.ID != task.ID {
		t.Fatalf("report task=%s, want %s", report.Task.ID, task.ID)
	}
	if got := reportCounters(report.Task); got != listed {
		t.Fatalf("report counters=%+v, want canonical list %+v", got, listed)
	}
	if report.SchemaVersion != "1" || report.Outcome != "in_progress" ||
		report.ScopeEnforcement != "asset_gate_only" || report.NetworkScope || len(report.Limitations) == 0 {
		t.Fatalf("report safety/lifecycle contract changed: %+v", report)
	}
	if report.FindingsBySeverity["high"] != 2 || report.FindingsBySeverity["info"] != 1 {
		t.Fatalf("report severity counts changed: %v", report.FindingsBySeverity)
	}
}

func TestTaskDetailCountersMatchPersistedList(t *testing.T) {
	s, key, task, want := persistedReportCounterFixture(t)
	h := s.Handler()
	if listed := listedReportCounters(t, h, key, task.ID); listed != want {
		t.Fatalf("list counters=%+v, want %+v", listed, want)
	}
	w := keyRequest(t, h, http.MethodGet, "/api/tasks/"+task.ID, key, "")
	if w.Code != http.StatusOK {
		t.Fatalf("detail HTTP=%d body=%s", w.Code, w.Body.String())
	}
	var detail TaskDTO
	if err := json.Unmarshal(w.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if got := reportCounters(detail); got != want {
		t.Fatalf("detail counters=%+v, want canonical list %+v", got, want)
	}
}

func TestTaskCompletedCountersSurviveRestart(t *testing.T) {
	s, key, task, want := persistedReportCounterFixture(t)
	completedAt := time.Unix(want.LastActivity+60, 0).UTC()
	if _, err := s.m.pg.Exec(`UPDATE exploration_nodes SET state='done' WHERE exploration_id=$1 AND kind='intent' AND state='running'`, task.ExpID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.m.pg.Exec(`UPDATE tasks SET status='done',completed_at=$2 WHERE id=$1`, task.ID, completedAt); err != nil {
		t.Fatal(err)
	}
	want.InFlight = 0
	task.updateLifecycle(func(state *taskLifecycleState) {
		state.Status = "done"
		state.CompletedAt = completedAt.Unix()
	})

	// Reconstruct the task handles and engine from persisted state, without
	// starting workers or opening a second database. This is the read boundary
	// used on restart; counters must not depend on engine memory.
	s.m.mu.Lock()
	s.m.tasks = map[string]*Task{}
	s.m.active = ""
	s.m.mu.Unlock()
	if loaded := s.m.LoadExisting(); len(loaded) != 2 {
		t.Fatalf("reloaded %d tasks, want 2", len(loaded))
	}
	s.engine = NewEngine(s.m)
	h := s.Handler()
	if listed := listedReportCounters(t, h, key, task.ID); listed != want {
		t.Fatalf("restarted list counters=%+v, want %+v", listed, want)
	}
	for _, endpoint := range []struct{ name, path string }{
		{"detail", "/api/tasks/" + task.ID},
		{"report", "/api/tasks/" + task.ID + "/report"},
	} {
		t.Run(endpoint.name, func(t *testing.T) {
			w := keyRequest(t, h, http.MethodGet, endpoint.path, key, "")
			if w.Code != http.StatusOK {
				t.Fatalf("HTTP=%d body=%s", w.Code, w.Body.String())
			}
			var detail TaskDTO
			if endpoint.name == "report" {
				var report struct {
					Task    TaskDTO `json:"task"`
					Outcome string  `json:"outcome"`
				}
				if err := json.Unmarshal(w.Body.Bytes(), &report); err != nil {
					t.Fatal(err)
				}
				if report.Outcome != "completed_unverified" {
					t.Fatalf("completed outcome=%q, want completed_unverified", report.Outcome)
				}
				detail = report.Task
			} else if err := json.Unmarshal(w.Body.Bytes(), &detail); err != nil {
				t.Fatal(err)
			}
			if detail.Status != "done" || detail.CompletedUnix != completedAt.Unix() {
				t.Fatalf("completed lifecycle changed: status=%q completed=%d", detail.Status, detail.CompletedUnix)
			}
			if got := reportCounters(detail); got != want {
				t.Fatalf("restarted counters=%+v, want %+v", got, want)
			}
		})
	}
}

func TestTaskCountersUseLatestHeartbeat(t *testing.T) {
	s, key, task, persisted := persistedReportCounterFixture(t)
	h := s.Handler()
	for _, tc := range []struct {
		name string
		live int64
		want int64
	}{
		{"older_live", persisted.LastActivity - 1, persisted.LastActivity},
		{"newer_live", persisted.LastActivity + 60, persisted.LastActivity + 60},
		{"no_live", 0, persisted.LastActivity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s.engine.lastAct.Store(task.ID, tc.live)
			want := persisted
			want.LastActivity = tc.want
			if listed := listedReportCounters(t, h, key, task.ID); listed != want {
				t.Fatalf("heartbeat list counters=%+v, want %+v", listed, want)
			}
			for _, path := range []string{"/api/tasks/" + task.ID, "/api/tasks/" + task.ID + "/report"} {
				w := keyRequest(t, h, http.MethodGet, path, key, "")
				if w.Code != http.StatusOK {
					t.Fatalf("HTTP=%d body=%s", w.Code, w.Body.String())
				}
				var detail TaskDTO
				if path == "/api/tasks/"+task.ID+"/report" {
					var report struct {
						Task TaskDTO `json:"task"`
					}
					if err := json.Unmarshal(w.Body.Bytes(), &report); err != nil {
						t.Fatal(err)
					}
					detail = report.Task
				} else if err := json.Unmarshal(w.Body.Bytes(), &detail); err != nil {
					t.Fatal(err)
				}
				if got := reportCounters(detail); got != want {
					t.Fatalf("heartbeat %s counters=%+v, want %+v", path, got, want)
				}
			}
		})
	}
}

func TestTaskCountersAllowPersistedZeros(t *testing.T) {
	s, key := integrationFixture(t)
	task, err := s.m.CreateTask("empty counter fixture", "no network targets", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	h := s.Handler()
	want := reportCounterSnapshot{}
	if listed := listedReportCounters(t, h, key, task.ID); listed != want {
		t.Fatalf("empty counters=%+v, want actual zeros", listed)
	}
	for _, path := range []string{"/api/tasks/" + task.ID, "/api/tasks/" + task.ID + "/report"} {
		w := keyRequest(t, h, http.MethodGet, path, key, "")
		if w.Code != http.StatusOK {
			t.Fatalf("empty task HTTP=%d body=%s", w.Code, w.Body.String())
		}
		var detail TaskDTO
		if path == "/api/tasks/"+task.ID+"/report" {
			var report struct {
				Task TaskDTO `json:"task"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &report); err != nil {
				t.Fatal(err)
			}
			detail = report.Task
		} else if err := json.Unmarshal(w.Body.Bytes(), &detail); err != nil {
			t.Fatal(err)
		}
		if detail.ID != task.ID || reportCounters(detail) != want {
			t.Fatalf("empty task counters changed: id=%s counters=%+v", detail.ID, reportCounters(detail))
		}
	}
}

func TestTaskCountersFailClosedOnMissingMetrics(t *testing.T) {
	s, _, task, _ := persistedReportCounterFixture(t)
	// A manager snapshot can outlive its persisted task during archive/delete.
	// Missing aggregates must not be mistaken for a present task with no work.
	if _, err := s.m.pg.Exec(`UPDATE tasks SET deleted_at=now() WHERE id=$1`, task.ID); err != nil {
		t.Fatal(err)
	}
	jwt, err := signJWT(s.jwtKey)
	if err != nil {
		t.Fatal(err)
	}
	h := s.Handler()
	for _, endpoint := range []struct{ name, path string }{
		{"list", "/api/tasks"},
		{"detail", "/api/tasks/" + task.ID},
		{"report", "/api/tasks/" + task.ID + "/report"},
	} {
		t.Run(endpoint.name, func(t *testing.T) {
			w := keyRequest(t, h, http.MethodGet, endpoint.path, jwt, "")
			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("missing metrics: HTTP=%d body=%s, want 503 without counters", w.Code, w.Body.String())
			}
		})
	}
}

func TestTaskCountersFailClosedOnStorageError(t *testing.T) {
	s, key, task, _ := persistedReportCounterFixture(t)
	// Break only the aggregate query in this test's disposable database. Auth,
	// report metadata and findings remain readable, so this tests the metrics
	// boundary rather than an earlier authorization/storage failure.
	if _, err := s.m.pg.Exec(`ALTER TABLE activity RENAME COLUMN input_tokens TO unavailable_input_tokens`); err != nil {
		t.Fatal(err)
	}
	h := s.Handler()
	for _, endpoint := range []struct{ name, path string }{
		{"list", "/api/tasks"},
		{"detail", "/api/tasks/" + task.ID},
		{"report", "/api/tasks/" + task.ID + "/report"},
	} {
		t.Run(endpoint.name, func(t *testing.T) {
			w := keyRequest(t, h, http.MethodGet, endpoint.path, key, "")
			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("metrics unavailable: HTTP=%d body=%s, want 503 without counters", w.Code, w.Body.String())
			}
			var failure map[string]json.RawMessage
			if err := json.Unmarshal(w.Body.Bytes(), &failure); err != nil {
				t.Fatal(err)
			}
			if len(failure["error"]) == 0 {
				t.Fatalf("failure lacks explicit error: %s", w.Body.String())
			}
			for _, field := range []string{"task", "tasks", "tokens", "goals_total", "goals_met", "in_flight", "last_activity_unix", "findings"} {
				if _, ok := failure[field]; ok {
					t.Fatalf("unavailable metrics returned as %s: %s", field, w.Body.String())
				}
			}
		})
	}
}
