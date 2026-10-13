package server

import (
	"encoding/json"
	"github.com/sebastian93921/artifex/db"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func mustInt(s string) int64 { i, _ := strconv.ParseInt(s, 10, 64); return i }

func TestIntegrationScopedReadsAndCancellation(t *testing.T) {
	s, k := integrationFixture(t)
	h := s.Handler()
	own, _ := s.m.CreateTask("own", "inert", nil, 0, 600)
	other, _ := s.m.CreateTask("foreign", "inert", nil, 0, 600)
	fid, err := s.m.pg.AddFinding(mustInt(own.ID), 0, "test", "fixture", "high", "own summary", "fixture evidence", "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	foreignFID, _ := s.m.pg.AddFinding(mustInt(other.ID), 0, "test", "foreign finding", "low", "foreign summary", "secret foreign evidence", "test", nil)
	s.m.pg.Exec(`UPDATE tasks SET owner='other' WHERE id=$1`, other.ID)
	for _, path := range []string{"/api/tasks/" + other.ID + "/findings", "/api/tasks/" + other.ID + "/report", "/api/tasks/" + own.ID + "/findings/" + strconv.FormatInt(foreignFID, 10)} {
		w := keyRequest(t, h, "GET", path, k, "")
		if w.Code != 404 || strings.Contains(w.Body.String(), "foreign evidence") {
			t.Fatalf("isolation %s %d", path, w.Code)
		}
	}
	w := keyRequest(t, h, "GET", "/api/tasks/"+own.ID+"/findings?task="+other.ID, k, "")
	if w.Code != 200 || strings.Contains(w.Body.String(), "foreign summary") || !strings.Contains(w.Body.String(), "own summary") {
		t.Fatalf("scoped list %d %s", w.Code, w.Body.String())
	}
	w = keyRequest(t, h, "GET", "/api/tasks/"+own.ID+"/findings/"+strconv.FormatInt(fid, 10), k, "")
	if w.Code != 200 {
		t.Fatalf("detail %d", w.Code)
	}
	for i := 0; i < 2; i++ {
		w = keyRequest(t, h, "POST", "/api/tasks/"+own.ID+"/cancel", k, "{}")
		if w.Code != 200 {
			t.Fatalf("cancel %d %s", w.Code, w.Body.String())
		}
	}
	var paused, queued, cancelled bool
	s.m.pg.QueryRow(`SELECT paused,queued,cancel_requested_at IS NOT NULL FROM tasks WHERE id=$1`, own.ID).Scan(&paused, &queued, &cancelled)
	if !paused || queued || !cancelled {
		t.Fatalf("durable cancel %v %v %v", paused, queued, cancelled)
	}
	// Rebuild handles from persisted rows, without launching runtime work.
	s.m.mu.Lock()
	delete(s.m.tasks, own.ID)
	s.m.mu.Unlock()
	s.m.LoadExisting()
	own, _ = s.m.Task(own.ID)
	if _, err := s.admitTask(own, "bootstrap"); err == nil {
		t.Fatal("cancelled task was readmitted")
	}
	w = keyRequest(t, h, "GET", "/api/tasks/"+own.ID+"/report", k, "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"outcome":"cancelled"`) || !strings.Contains(w.Body.String(), `"network_scope_enforced":false`) {
		t.Fatalf("report %s", w.Body.String())
	}
	for _, path := range []string{"/api/api-keys", "/api/llm/profiles", "/api/intercept/rules", "/api/exploration/findings"} {
		w = keyRequest(t, h, "GET", path, k, "")
		if w.Code != 403 {
			t.Fatalf("admin overreach %s %d", path, w.Code)
		}
	}
}

func TestIntegrationHeaderReplayAndValidation(t *testing.T) {
	s, k := integrationFixture(t)
	h := s.Handler()
	body := `{"description":"inert","scope":{"targets":[{"kind":"hostname","value":"example.invalid"}]}}`
	submit := func(body, header string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/api/tasks", strings.NewReader(body))
		r.Header.Set("X-API-Key", k)
		r.Header.Set("Idempotency-Key", header)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	w := submit(body, "fixture:header")
	if w.Code != 201 {
		t.Fatalf("header %d %s", w.Code, w.Body.String())
	}
	var task struct{ ID string }
	json.Unmarshal(w.Body.Bytes(), &task)
	rules, err := s.m.Assets().ListTaskInterceptRules(mustInt(task.ID))
	if err != nil || len(rules) != 1 || rules[0].Kind != "exact_domain" || !rules[0].Enabled {
		t.Fatalf("atomic rules %v %v", rules, err)
	}
	s.m.mu.Lock()
	delete(s.m.tasks, task.ID)
	s.m.mu.Unlock()
	s.m.LoadExisting()
	w = submit(body, "fixture:header")
	if w.Code != 200 || w.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("replay %d", w.Code)
	}
	for _, bad := range []string{`{"scope":{"targets":[]}}`, `{"scope":{"targets":[{"kind":"hostname","value":"*.example.invalid"}]}}`, `{"scope":{"targets":[{"kind":"url","value":"https://user:password@example.invalid"}]}}`, `{"timeout_seconds":-1}`, `{"` + strings.Repeat("SECRET", 5000) + `":1}`, body + ` {}`} {
		w = submit(bad, "")
		if w.Code != 400 || len(w.Body.Bytes()) > 256 || strings.Contains(w.Body.String(), "SECRET") || strings.Contains(w.Body.String(), "password") {
			t.Fatalf("unsafe error %d %s", w.Code, w.Body.String())
		}
	}
	w = submit(strings.Replace(body, `"description"`, `"external_request_id":"different","description"`, 1), "fixture:header")
	if w.Code != 400 {
		t.Fatalf("ID mismatch %d", w.Code)
	}
	s.m.pg.Exec(`UPDATE tasks SET deleted_at=now() WHERE id=$1`, task.ID)
	w = submit(body, "fixture:header")
	if w.Code != 409 {
		t.Fatalf("deleted replay %d", w.Code)
	}
}

func TestIntegrationSubmissionRollback(t *testing.T) {
	s, _ := integrationFixture(t)
	_, err := s.m.pg.CreateTaskWithOptions("fixture", "inert", db.TaskCreateOptions{SubmissionOwner: "ARTIFEX", ExternalRequestID: "rollback", RequestHash: "hash", InterceptRules: []db.TaskInterceptRuleInput{{Kind: "invalid", Action: "allow", Enabled: true, Pattern: "example.invalid"}}})
	if err == nil {
		t.Fatal("expected rule constraint failure")
	}
	var tasks, submissions, explorations int
	s.m.pg.QueryRow(`SELECT count(*) FROM tasks`).Scan(&tasks)
	s.m.pg.QueryRow(`SELECT count(*) FROM task_submissions`).Scan(&submissions)
	s.m.pg.QueryRow(`SELECT count(*) FROM explorations`).Scan(&explorations)
	if tasks != 0 || submissions != 0 || explorations != 0 {
		t.Fatalf("orphan rows %d %d %d", tasks, submissions, explorations)
	}
}
