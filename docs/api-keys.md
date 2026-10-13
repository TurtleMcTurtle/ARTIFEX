# Task integration API keys

**Integration extensions:** see [Skynet integration contract](skynet-integration.md)
for idempotent submissions, exact target metadata/asset allow rules, scoped
findings/report reads and cancellation. The endpoint table below describes the
original baseline; the extension contract supersedes its denied findings/control
statement only for the explicitly named task-scoped endpoints. No admin access
is granted and network-egress scope enforcement is not provided.

ARTIFEX currently has one administrator identity: the fixed `ARTIFEX` login and
JWT subject. There is no user registry or multiuser login. Keys are bound to this
verified JWT subject, not to a caller-supplied owner. This feature does **not**
create full multiuser management. Existing and newly created tasks belong to
`ARTIFEX` by default; keys with `tasks:read` can read all tasks belonging to that
owner, not just tasks created using the same key. JWT administrator behavior is
unchanged. Task ownership is persisted in `tasks.owner` and checked for key reads.

Keys contain 32 cryptographically random bytes and use the `artifex_key_` prefix.
Only a SHA-256 digest is stored. The secret is returned once, at creation; list
responses contain metadata only. Revocation and expiry are checked against the
PostgreSQL database on each request, with no authentication cache. Database
errors deny access (503), invalid/expired/revoked keys return 401, scope/endpoint
denials return 403, and unowned/missing task IDs return 404.

## Key management (JWT only)

- `POST /api/api-keys`: `{name, scopes, expires_at?}`; returns 201 with key metadata
  (`id`, `owner`, `name`, `scopes`, `created_at`, `expires_at?`) plus one-time `key`.
- `GET /api/api-keys`: returns `{keys:[...]}`, including revoked keys.
- `DELETE /api/api-keys/{id}`: idempotently revokes an owned key; returns
  `{revoked:true}` (404 if absent/not owned).

Scopes must be a nonempty, duplicate-free list containing only `tasks:read` and/or
`tasks:write`. Optional `expires_at` is a future RFC3339 timestamp. Unknown fields,
including `owner`, are rejected. Management requires an unexpired HS256 JWT with
subject `ARTIFEX`; API keys cannot list, mint, or revoke keys.

```sh
BASE=http://127.0.0.1:8787
# JWT_TOKEN is obtained through the existing ARTIFEX login, not an API key.
curl -sS "$BASE/api/api-keys" \
  -H "Authorization: Bearer $JWT_TOKEN" -H 'Content-Type: application/json' \
  -d '{"name":"future-skynet","scopes":["tasks:read","tasks:write"],"expires_at":"2030-01-01T00:00:00Z"}'

curl -sS "$BASE/api/api-keys" -H "Authorization: Bearer $JWT_TOKEN"
curl -sS -X DELETE "$BASE/api/api-keys/$KEY_ID" \
  -H "Authorization: Bearer $JWT_TOKEN"
```

## Task API (strict allowlist)

| Method/path | Scope | Response |
| --- | --- | --- |
| POST `/api/tasks` | tasks:write | 201, existing task DTO |
| GET `/api/tasks` | tasks:read | `{tasks:[task DTOs],active:"id or empty"}` |
| GET `/api/tasks/{id}` | tasks:read | existing task DTO, including status |
| GET `/api/tasks/{id}/status` | tasks:read | `{id,status,paused,queued}` |

Use **either** `Authorization: Bearer $API_KEY` **or** `X-API-Key: $API_KEY`.
Simultaneous credentials are rejected. API keys in `?token=` or the
`artifex_token` cookie are not accepted; those legacy transports remain JWT-only.
All other protected endpoints are denied to keys, including task updates,
deletes, control, scope, coverage, assets, findings, LLM configuration and admin
routes. Status uses the existing resolved lifecycle logic; clients should not
assume that `created` means running.

```sh
curl -sS "$BASE/api/tasks" -H "X-API-Key: $API_KEY"
curl -sS "$BASE/api/tasks/$TASK_ID" -H "Authorization: Bearer $API_KEY"
curl -sS "$BASE/api/tasks/$TASK_ID/status" -H "X-API-Key: $API_KEY"

# WARNING: creation immediately launches the existing planner/worker pipeline.
# Only submit authorized targets after reviewing scope and operational guardrails.
curl -sS "$BASE/api/tasks" \
  -H "Authorization: Bearer $API_KEY" -H 'Content-Type: application/json' \
  -d '{"name":"authorized assessment","description":"Authorized target and boundaries","goal":"Approved assessment objective","timeout_seconds":3600}'
```

Key-based creation reuses the existing task handler, but accepts only `name`,
`description`, `goal`, `timeout_seconds`, `plan_heartbeat_seconds`,
`coverage_enabled`, and `seed_first_intent`. Owner selection, inherited source
 tasks, company associations, categories, LLM profile selection and interception
rule overrides are rejected. Tasks use the existing global LLM configuration.

## Local verification

Use a dedicated local PostgreSQL instance, **never the running application's
 database**, because the existing suite creates/deletes fixtures and starts
background runtimes. API key tests create/drop a fresh database per test and use
inert tasks; their HTTP create test has a canceled runtime context and no provider.
A curl test uses a real isolated loopback HTTP server and PostgreSQL.

```sh
ARTIFEX_PG_DSN='postgres://artifex_test@127.0.0.1:55439/artifex_test?sslmode=disable' \
  go test ./server -run TestAPIKey -count=1 -v
# Requires local PostgreSQL credentials with CREATE DATABASE permission.
go build ./...
```

No Skynet calls, remote deployment, user provisioning, per-key task isolation,
key rotation automation, last-used audit tracking, or integration UI are included.
Use HTTPS outside loopback, protect keys as passwords, and revoke unused keys.
