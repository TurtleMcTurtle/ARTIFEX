package server

import (
	"encoding/json"
	"errors"
	"github.com/sebastian93921/artifex/agent"
	"github.com/sebastian93921/artifex/db"
	"github.com/sebastian93921/artifex/locale"
	"github.com/sebastian93921/artifex/sidequestion"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

type integrationTarget struct {
	Kind  string `json:"kind"`
	Value string `json:"value"`
}
type integrationScope struct {
	Targets []integrationTarget `json:"targets"`
}
type integrationRequest struct {
	Name                 string            `json:"name"`
	Description          string            `json:"description"`
	Goal                 string            `json:"goal"`
	ExternalRequestID    string            `json:"external_request_id,omitempty"`
	Scope                *integrationScope `json:"scope,omitempty"`
	TimeoutSeconds       int               `json:"timeout_seconds"`
	PlanHeartbeatSeconds int               `json:"plan_heartbeat_seconds"`
	CoverageEnabled      *bool             `json:"coverage_enabled,omitempty"`
	SeedFirstIntent      *bool             `json:"seed_first_intent,omitempty"`
}

var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
var hostnamePattern = regexp.MustCompile(`^[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+$`)

func decodeIntegration(w http.ResponseWriter, r *http.Request, out any) bool {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	var raw json.RawMessage
	if d.Decode(&raw) != nil || d.Decode(new(any)) != io.EOF || len(raw) == 0 || raw[0] != '{' {
		writeErr(w, 400, "Invalid integration request")
		return false
	}
	object := json.NewDecoder(strings.NewReader(string(raw)))
	object.DisallowUnknownFields()
	if object.Decode(out) != nil {
		writeErr(w, 400, "Invalid integration request")
		return false
	}
	return true
}
func exactScopeRules(scope *integrationScope) ([]db.TaskInterceptRuleInput, error) {
	if scope == nil {
		return nil, nil
	}
	if len(scope.Targets) == 0 || len(scope.Targets) > 32 {
		return nil, errors.New("invalid scope")
	}
	seen := map[string]bool{}
	rules := make([]db.TaskInterceptRuleInput, 0, len(scope.Targets))
	for _, target := range scope.Targets {
		v := target.Value
		if target.Kind == "ip" && net.ParseIP(v) != nil && net.ParseIP(v).To4() == nil {
			return nil, errors.New("IPv6 seeding is not supported")
		}
		kind := ""
		if v != strings.TrimSpace(v) || len(v) > 2048 {
			return nil, errors.New("invalid target")
		}
		switch target.Kind {
		case "hostname":
			if len(v) > 253 || !hostnamePattern.MatchString(v) || net.ParseIP(v) != nil {
				return nil, errors.New("invalid hostname")
			}
			kind = "exact_domain"
		case "ip":
			if net.ParseIP(v) == nil {
				return nil, errors.New("invalid ip")
			}
			kind = "exact_ip"
		case "url":
			u, err := url.Parse(v)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.Fragment != "" || u.RawQuery != "" {
				return nil, errors.New("invalid url")
			}
			if net.ParseIP(u.Hostname()) == nil && !hostnamePattern.MatchString(u.Hostname()) {
				return nil, errors.New("invalid url host")
			}
			if u.Port() != "" {
				p, err := strconv.Atoi(u.Port())
				if err != nil || p < 1 || p > 65535 {
					return nil, errors.New("invalid port")
				}
			}
			kind = "exact_url"
		default:
			return nil, errors.New("invalid kind")
		}
		key := target.Kind + ":" + strings.ToLower(v)
		if seen[key] {
			return nil, errors.New("duplicate target")
		}
		seen[key] = true
		rules = append(rules, db.TaskInterceptRuleInput{Enabled: true, Action: "allow", Kind: kind, Pattern: v, Note: "Integration exact target scope"})
	}
	return rules, nil
}

func (s *Server) createIntegrationTask(w http.ResponseWriter, r *http.Request) {
	var req integrationRequest
	if !decodeIntegration(w, r, &req) {
		return
	}
	header := r.Header.Get("Idempotency-Key")
	if header != "" && req.ExternalRequestID != "" && header != req.ExternalRequestID {
		writeErr(w, 400, "Request IDs disagree")
		return
	}
	if header != "" {
		req.ExternalRequestID = header
	}
	if (req.ExternalRequestID != "" && !requestIDPattern.MatchString(req.ExternalRequestID)) || len(req.Name) > 200 || len(req.Description) > 16384 || len(req.Goal) > 16384 || req.TimeoutSeconds < 0 || req.TimeoutSeconds > 86400 || req.PlanHeartbeatSeconds < 0 || req.PlanHeartbeatSeconds > 86400 {
		writeErr(w, 400, "Invalid integration request")
		return
	}
	rules, err := exactScopeRules(req.Scope)
	if err != nil {
		writeErr(w, 400, "Invalid exact target scope")
		return
	}
	// Preserve legacy unscoped creation, but do not accept an idempotent integration without scope.
	if req.ExternalRequestID != "" && req.Scope == nil {
		writeErr(w, 400, "Exact target scope is required for external submissions")
		return
	}
	payload, _ := json.Marshal(req)
	hash := apiKeyHash(string(payload))
	scopeJSON, _ := json.Marshal(req.Scope)
	description := req.Description
	if strings.TrimSpace(description) == "" {
		description = "Untitled task"
	}
	goal := req.Goal
	if req.Scope != nil {
		goal += "\nAuthorized exact targets: " + string(scopeJSON) + ". Do not expand scope. Asset allowlisting is not a network sandbox."
	}
	opts := db.TaskCreateOptions{Language: locale.FromRequest(r), Name: req.Name, TimeoutSeconds: req.TimeoutSeconds, PlanHeartbeatSeconds: req.PlanHeartbeatSeconds, CoverageEnabled: req.CoverageEnabled, InterceptRules: rules, SubmissionOwner: apiKeyPrincipal(r).Owner, ExternalRequestID: req.ExternalRequestID, RequestHash: hash, IntegrationScope: scopeJSON}
	// Keep registry publication serialized with replay lookup in this process.
	s.m.taskStateMu.Lock()
	pt, err := s.m.pg.CreateTaskWithOptions(description, goal, opts)
	var t *Task
	if err == nil {
		id := strconv.FormatInt(pt.ID, 10)
		if pt.Replay {
			t, _ = s.m.Task(id)
		} else {
			t = taskFromPG(pt, s.m.pg.Exploration(pt.ExplorationID), s.m.interceptor)
			s.m.mu.Lock()
			s.m.tasks[id] = t
			s.m.active = id
			s.m.mu.Unlock()
		}
	}
	s.m.taskStateMu.Unlock()
	if errors.Is(err, db.ErrSubmissionConflict) {
		writeErr(w, 409, "Request ID already used with different content or unavailable task")
		return
	}
	if err != nil || t == nil {
		writeErr(w, 503, "Task submission unavailable")
		return
	}
	if pt.Replay {
		w.Header().Set("Idempotency-Replayed", "true")
		writeJSON(w, 200, taskDTO(t, s.resolvedTaskStatus(t)))
		return
	}
	seed := description + " " + goal
	if req.Scope != nil {
		seed = ""
		for _, target := range req.Scope.Targets {
			s.seed(t, target.Value)
		}
	}
	if req.Scope == nil {
		s.launchTask(t, seed, req.SeedFirstIntent != nil && *req.SeedFirstIntent)
	} else {
		if req.SeedFirstIntent != nil && *req.SeedFirstIntent {
			s.seedFirstIntent(t)
		}
		_, _ = s.admitTask(t, "bootstrap")
	}
	writeJSON(w, 201, taskDTO(t, s.resolvedTaskStatus(t)))
}

func integrationPage(r *http.Request) (int, int, bool) {
	page, size := 1, 50
	var err error
	if v := r.URL.Query().Get("page"); v != "" {
		page, err = strconv.Atoi(v)
		if err != nil {
			return 0, 0, false
		}
	}
	if v := r.URL.Query().Get("page_size"); v != "" {
		size, err = strconv.Atoi(v)
		if err != nil {
			return 0, 0, false
		}
	}
	return page, size, page > 0 && page <= 1000000 && size > 0 && size <= 100
}
func (s *Server) integrationFindings(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 || strconv.FormatInt(id, 10) != r.PathValue("id") {
		writeErr(w, 404, "Task not found")
		return
	}
	if _, ok := s.m.Task(r.PathValue("id")); !ok {
		writeErr(w, 404, "Task not found")
		return
	}
	page, size, ok := integrationPage(r)
	if !ok {
		writeErr(w, 400, "Invalid pagination")
		return
	}
	findings, total, err := s.m.pg.ListFindingsPage(db.FindingFilter{TaskID: r.PathValue("id")}, page, size)
	if err != nil {
		writeErr(w, 503, "Finding storage unavailable")
		return
	}
	out := make([]FindingDTO, 0, len(findings))
	for _, f := range findings {
		out = append(out, findingFromDB(f, nil))
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, 200, map[string]any{"findings": out, "total": total, "page": page, "page_size": size, "has_more": page < (total+size-1)/size})
}
func (s *Server) integrationFinding(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("fid"), 10, 64)
	if err != nil || id <= 0 {
		writeErr(w, 404, "Finding not found")
		return
	}
	f, err := s.m.pg.GetFinding(id)
	if err != nil {
		writeErr(w, 503, "Finding storage unavailable")
		return
	}
	if f == nil || f.TaskID == nil || strconv.FormatInt(*f.TaskID, 10) != r.PathValue("id") {
		writeErr(w, 404, "Finding not found")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, 200, findingFromDB(f, nil))
}
func (s *Server) integrationReport(w http.ResponseWriter, r *http.Request) {
	t, ok := s.m.Task(r.PathValue("id"))
	if !ok {
		writeErr(w, 404, "Task not found")
		return
	}
	var scope json.RawMessage
	var external string
	var cancelled bool
	err := s.m.pg.QueryRowContext(r.Context(), `SELECT integration_scope,COALESCE((SELECT external_request_id FROM task_submissions WHERE task_id=tasks.id),''),cancel_requested_at IS NOT NULL FROM tasks WHERE id=$1`, t.ID).Scan(&scope, &external, &cancelled)
	if err != nil {
		writeErr(w, 503, "Report storage unavailable")
		return
	}
	counts := map[string]int{}
	rows, err := s.m.pg.QueryContext(r.Context(), `SELECT severity,count(*) FROM findings WHERE task_id=$1 GROUP BY severity`, t.ID)
	if err != nil {
		writeErr(w, 503, "Report storage unavailable")
		return
	}
	for rows.Next() {
		var severity string
		var n int
		if err = rows.Scan(&severity, &n); err != nil {
			break
		}
		counts[severity] = n
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		writeErr(w, 503, "Report storage unavailable")
		return
	}
	status := s.resolvedTaskStatus(t)
	outcome := "in_progress"
	if status == "done" {
		outcome = "completed_unverified"
	} else if status == "failed" || status == "timeout" {
		outcome = status
	}
	if cancelled {
		outcome = "cancelled"
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, 200, map[string]any{"schema_version": "1", "task": taskDTO(t, status), "external_request_id": external, "scope": scope, "scope_enforcement": "asset_gate_only", "network_scope_enforced": false, "outcome": outcome, "cancel_requested": cancelled, "findings_by_severity": counts, "findings_url": "/api/tasks/" + t.ID + "/findings", "limitations": []string{"No network-egress sandbox; arbitrary tools and redirects are not constrained by asset allowlists.", "Completion and finding counts do not establish executed coverage, verified detection, or a clean assessment."}})
}
func (s *Server) integrationCancel(w http.ResponseWriter, r *http.Request) {
	var req struct{}
	if r.ContentLength != 0 && !decodeIntegration(w, r, &req) {
		return
	}
	t, ok := s.m.Task(r.PathValue("id"))
	if !ok {
		writeErr(w, 404, "Task not found")
		return
	}
	s.concMu.Lock()
	defer s.concMu.Unlock()
	if s.engine.IsDeleting(t.ID) {
		writeErr(w, 409, "Task unavailable for cancellation")
		return
	}
	// Durable pause/dequeue first; it is the restart-safe no-admission barrier.
	if !db.IsTerminal(t.lifecycleSnapshot().Status) {
		s.m.taskStateMu.Lock()
		result, err := s.m.pg.ExecContext(r.Context(), `UPDATE tasks SET paused=true,queued=false,queued_at=NULL,cancel_requested_at=COALESCE(cancel_requested_at,now()) WHERE id=$1 AND deleted_at IS NULL AND status NOT IN ('done','failed','timeout')`, t.ID)
		if err == nil {
			n, _ := result.RowsAffected()
			if n > 0 {
				t.updateLifecycle(func(state *taskLifecycleState) { state.Paused = true; state.Queued = false; state.QueuedAt = 0 })
			}
		}
		s.m.taskStateMu.Unlock()
		if err != nil {
			writeErr(w, 503, "Cancellation storage unavailable")
			return
		}
		s.engine.Pause(t.ID, agent.AbortPausedByUser)
		s.cancelTaskChat(t.ID, agent.AbortChatPausedWithTask)
		s.cancelSideWhere(func(p sidequestion.Parent) bool { return strconv.FormatInt(p.TaskID, 10) == t.ID })
	}
	writeJSON(w, 200, map[string]any{"id": t.ID, "status": s.resolvedTaskStatus(t), "cancel_requested": !db.IsTerminal(t.lifecycleSnapshot().Status), "semantics": "durable_pause", "drained": false})
}
