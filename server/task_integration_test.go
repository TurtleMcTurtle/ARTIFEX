package server

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func integrationFixture(t *testing.T) (*Server, string) {
	s := keyTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.ctx = ctx
	jwt, _ := signJWT(s.jwtKey)
	w := keyRequest(t, s.Handler(), "POST", "/api/api-keys", jwt, `{"scopes":["tasks:read","tasks:write"]}`)
	var k struct{ Key string }
	json.Unmarshal(w.Body.Bytes(), &k)
	if k.Key == "" {
		t.Fatal("mint failed")
	}
	return s, k.Key
}

func TestIntegrationIdempotentSubmission(t *testing.T) {
	s, k := integrationFixture(t)
	h := s.Handler()
	body := `{"external_request_id":"skynet-fixture-1","name":"fixture","description":"inert","goal":"no network","scope":{"targets":[{"kind":"hostname","value":"example.invalid"}]}}`
	var wg sync.WaitGroup
	results := make(chan *httptest.ResponseRecorder, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); results <- keyRequest(t, h, "POST", "/api/tasks", k, body) }()
	}
	wg.Wait()
	close(results)
	var id string
	created := 0
	for w := range results {
		if w.Code != 200 && w.Code != 201 {
			t.Fatalf("submit %d %s", w.Code, w.Body.String())
		}
		if w.Code == 201 {
			created++
		}
		var task struct{ ID string }
		json.Unmarshal(w.Body.Bytes(), &task)
		if id != "" && id != task.ID {
			t.Fatal("duplicate task")
		}
		id = task.ID
	}
	if created != 1 {
		t.Fatalf("created=%d", created)
	}
	w := keyRequest(t, h, "POST", "/api/tasks", k, strings.Replace(body, "inert", "changed", 1))
	if w.Code != 409 {
		t.Fatalf("conflict %d", w.Code)
	}
	var count int
	s.m.pg.QueryRow(`SELECT count(*) FROM tasks`).Scan(&count)
	if count != 1 {
		t.Fatalf("tasks %d", count)
	}
	w = keyRequest(t, h, "GET", "/api/tasks/"+id+"/report", k, "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "example.invalid") {
		t.Fatalf("report %d %s", w.Code, w.Body.String())
	}
}
