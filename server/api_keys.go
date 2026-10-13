package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/sebastian93921/artifex/db"
)

const apiKeyPrefix = "artifex_key_"

type apiPrincipalContextKey struct{}

func apiKeyPrincipal(r *http.Request) *db.APIKey {
	k, _ := r.Context().Value(apiPrincipalContextKey{}).(*db.APIKey)
	return k
}

func apiKeyHash(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func jwtOwner(token string, key []byte) (string, bool) {
	claims := new(jwt.RegisteredClaims)
	parsed, err := jwt.ParseWithClaims(token, claims, func(*jwt.Token) (any, error) { return key, nil }, jwt.WithValidMethods([]string{"HS256"}), jwt.WithExpirationRequired())
	return claims.Subject, err == nil && parsed.Valid && claims.Subject == "ARTIFEX"
}

func (s *Server) apiKeyManagementOwner(w http.ResponseWriter, r *http.Request) (string, bool) {
	if apiKeyPrincipal(r) != nil || r.Header.Get("X-API-Key") != "" {
		writeErr(w, 403, "API keys cannot manage keys")
		return "", false
	}
	owner, ok := jwtOwner(extractToken(r), s.jwtKey)
	if !ok {
		writeErr(w, 401, "Unauthorized")
	}
	return owner, ok
}

func (s *Server) createAPIKey(w http.ResponseWriter, r *http.Request) {
	owner, ok := s.apiKeyManagementOwner(w, r)
	if !ok {
		return
	}
	var req struct {
		Name      string     `json:"name"`
		Scopes    []string   `json:"scopes"`
		ExpiresAt *time.Time `json:"expires_at"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil || len(req.Name) > 200 || len(req.Scopes) == 0 || len(req.Scopes) > 2 {
		writeErr(w, 400, "Invalid API key request")
		return
	}
	seen := map[string]bool{}
	for _, scope := range req.Scopes {
		if (scope != "tasks:read" && scope != "tasks:write") || seen[scope] {
			writeErr(w, 400, "Invalid or duplicate scope")
			return
		}
		seen[scope] = true
	}
	if req.ExpiresAt != nil && !req.ExpiresAt.After(time.Now()) {
		writeErr(w, 400, "Expiry must be in the future")
		return
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		writeErr(w, 500, "Key generation failed")
		return
	}
	raw := apiKeyPrefix + hex.EncodeToString(buf)
	k, err := s.m.pg.CreateAPIKey(r.Context(), apiKeyHash(raw), owner, req.Name, req.Scopes, req.ExpiresAt)
	if err != nil {
		writeErr(w, 503, "Key storage unavailable")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, 201, struct {
		*db.APIKey
		Key string `json:"key"`
	}{k, raw})
}

func (s *Server) listAPIKeys(w http.ResponseWriter, r *http.Request) {
	owner, ok := s.apiKeyManagementOwner(w, r)
	if !ok {
		return
	}
	keys, err := s.m.pg.ListAPIKeys(r.Context(), owner)
	if err != nil {
		writeErr(w, 503, "Key storage unavailable")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, 200, map[string]any{"keys": keys})
}

func (s *Server) revokeAPIKey(w http.ResponseWriter, r *http.Request) {
	owner, ok := s.apiKeyManagementOwner(w, r)
	if !ok {
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeErr(w, 404, "Key not found")
		return
	}
	found, err := s.m.pg.RevokeAPIKey(r.Context(), id, owner)
	if err != nil {
		writeErr(w, 503, "Key storage unavailable")
		return
	}
	if !found {
		writeErr(w, 404, "Key not found")
		return
	}
	writeJSON(w, 200, map[string]bool{"revoked": true})
}

func (s *Server) authenticateKeyRequest(w http.ResponseWriter, r *http.Request, h http.Handler, raw string) {
	if !strings.HasPrefix(raw, apiKeyPrefix) || len(raw) != len(apiKeyPrefix)+64 {
		writeErr(w, 401, "Invalid API key")
		return
	}
	if s.m == nil || s.m.pg == nil {
		writeErr(w, 503, "Key storage unavailable")
		return
	}
	k, err := s.m.pg.AuthenticateAPIKey(r.Context(), apiKeyHash(raw))
	if err != nil {
		writeErr(w, 503, "Key storage unavailable")
		return
	}
	if k == nil || k.Owner != "ARTIFEX" {
		writeErr(w, 401, "Invalid or expired API key")
		return
	}
	scope := apiKeyRouteScope(r.Method, r.URL.Path)
	if scope == "" {
		writeErr(w, 403, "API key is not permitted on this endpoint")
		return
	}
	permitted := false
	for _, v := range k.Scopes {
		if v == scope {
			permitted = true
		}
	}
	if !permitted {
		writeErr(w, 403, "API key scope is insufficient")
		return
	}
	if r.URL.Path != "/api/tasks" {
		id := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/tasks/"), "/")[0]
		owned, err := s.m.pg.TaskOwnedBy(r.Context(), id, k.Owner)
		if err != nil {
			writeErr(w, 503, "Task storage unavailable")
			return
		}
		if !owned {
			writeErr(w, 404, "Task not found")
			return
		}
	}
	h.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), apiPrincipalContextKey{}, k)))
}

func apiKeyRouteScope(method, path string) string {
	if path == "/api/tasks" {
		if method == "GET" {
			return "tasks:read"
		}
		if method == "POST" {
			return "tasks:write"
		}
		return ""
	}
	if !strings.HasPrefix(path, "/api/tasks/") {
		return ""
	}
	parts := strings.Split(strings.TrimPrefix(path, "/api/tasks/"), "/")

	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || id <= 0 || strconv.FormatInt(id, 10) != parts[0] {
		return ""
	}
	if method == "POST" && len(parts) == 2 && parts[1] == "cancel" {
		return "tasks:write"
	}
	if method != "GET" {
		return ""
	}
	if len(parts) == 1 || (len(parts) == 2 && (parts[1] == "status" || parts[1] == "report" || parts[1] == "findings")) {
		return "tasks:read"
	}
	if len(parts) == 3 && parts[1] == "findings" {
		fid, err := strconv.ParseInt(parts[2], 10, 64)
		if err == nil && fid > 0 && strconv.FormatInt(fid, 10) == parts[2] {
			return "tasks:read"
		}
	}
	return ""
}

func (s *Server) taskStatusAPI(w http.ResponseWriter, r *http.Request) {
	t, ok := s.m.Task(r.PathValue("id"))
	if !ok {
		writeErr(w, 404, "Task not found")
		return
	}
	dto := taskDTO(t, s.resolvedTaskStatus(t))
	writeJSON(w, 200, map[string]any{"id": dto.ID, "status": dto.Status, "paused": dto.Paused, "queued": dto.Queued})
}
