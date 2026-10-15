package server

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTaskRetentionSettingsBrowserFixture(t *testing.T) {
	checkpoint := os.Getenv("ARTIFEX_RETENTION_BROWSER_CHECKPOINT")
	if checkpoint == "" {
		t.Skip("isolated browser fixture is opt-in")
	}
	root := os.Getenv("ARTIFEX_RETENTION_BROWSER_WEB_ROOT")
	if !filepath.IsAbs(root) {
		t.Fatal("absolute static fixture root required")
	}
	s := retentionSettingsFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.archiveWake = make(chan struct{}, 1)
	s.startTaskRetentionScheduler()
	defer func() { cancel(); s.archiveWG.Wait() }()
	token, err := signJWT(s.jwtKey)
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan struct{}, 1)
	api := s.Handler()
	static := http.FileServer(http.Dir(root))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /__fixture/start", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "artifex_token", Value: token, Path: "/", SameSite: http.SameSiteStrictMode})
		encoded, _ := json.Marshal(token)
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<script>localStorage.setItem("artifex_token",` + string(encoded) + `);location.replace("/function/tasks/")</script>`))
	})
	mux.HandleFunc("POST /__fixture/finish", func(w http.ResponseWriter, r *http.Request) {
		select {
		case finished <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.Handle("/api/", api)
	mux.Handle("/", static)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = server.Serve(listener) }()
	defer server.Close()
	url := "http://" + listener.Addr().String()
	encoded, _ := json.Marshal(map[string]any{"url": url, "fixture_only": true})
	if err := os.WriteFile(checkpoint, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	case <-time.After(5 * time.Minute):
		t.Fatal("browser fixture timed out")
	}
	var tasks, jobs int
	if err := s.m.pg.QueryRow(`SELECT count(*) FROM tasks`).Scan(&tasks); err != nil {
		t.Fatal(err)
	}
	if err := s.m.pg.QueryRow(`SELECT count(*) FROM task_archives`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if tasks != 0 || jobs != 0 {
		t.Fatalf("browser checks created task or cleanup work: %d/%d", tasks, jobs)
	}
}
