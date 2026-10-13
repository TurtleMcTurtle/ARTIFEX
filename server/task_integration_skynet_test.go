package server

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
)

// Opt-in cross-repository test: the real .NET adapter uses TLS HTTP against the
// real ARTIFEX handlers and dedicated PostgreSQL, with runtime already cancelled.
func TestIntegrationSkynetTransport(t *testing.T) {
	repo := os.Getenv("SKYNET_CONTRACT_REPO")
	if repo == "" {
		t.Skip("SKYNET_CONTRACT_REPO required")
	}
	s, key := integrationFixture(t)
	handler := s.Handler()
	var once sync.Once
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, r)
		if r.Method == "POST" && r.URL.Path == "/api/tasks" && recorder.Code == 201 {
			var task struct{ ID string }
			if err := json.Unmarshal(recorder.Body.Bytes(), &task); err != nil {
				t.Error(err)
			}
			once.Do(func() {
				for i := 0; i < 101; i++ {
					fid, err := s.m.pg.AddFinding(mustInt(task.ID), 0, "test", "fixture", "high", "fixture summary", "fixture evidence", "test", nil)
					if err != nil {
						t.Error(err)
						return
					}
					if _, err := s.m.pg.Exec(`UPDATE findings SET report='fixture Markdown' WHERE id=$1`, fid); err != nil {
						t.Error(err)
						return
					}
				}
			})
		}
		if r.Method == "GET" && len(r.URL.Path) > 7 && r.URL.Path[len(r.URL.Path)-7:] == "/status" && recorder.Code == 200 {
			t.Logf("observed task status: %s", recorder.Body.String())
		}
		if r.Method == "POST" && r.URL.Path == "/api/tasks" {
			t.Logf("observed create HTTP=%d replay=%q", recorder.Code, recorder.Header().Get("Idempotency-Replayed"))
		}
		for k, vs := range recorder.Header() {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(recorder.Code)
		w.Write(recorder.Body.Bytes())
	}))
	defer ts.Close()
	cmd := exec.Command("dotnet", "test", filepath.Join(repo, "test-proj/artifex-tests.csproj"), "--no-restore", "--filter", "RealArtifexTransportContract", "--verbosity", "quiet")
	cmd.Dir = repo
	cmd.Env = append(os.Environ(), "ARTIFEX_CONTRACT_URL="+ts.URL, "ARTIFEX_CONTRACT_KEY="+key, "ARTIFEX_CONTRACT_CERT="+base64.StdEncoding.EncodeToString(ts.Certificate().Raw))
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("real Skynet transport: %v\n%s", err, out)
	}
	t.Logf("real Skynet transport passed:\n%s", out)
	var tasks, mappings int
	if err := s.m.pg.QueryRow(`SELECT count(*) FROM tasks`).Scan(&tasks); err != nil {
		t.Fatal(err)
	}
	if err := s.m.pg.QueryRow(`SELECT count(*) FROM task_submissions`).Scan(&mappings); err != nil {
		t.Fatal(err)
	}
	if tasks != 1 || mappings != 1 {
		t.Fatalf("tasks=%d mappings=%d", tasks, mappings)
	}
}
