# Skynet / ARTIFEX integration contract (schema version 1)

These extensions use existing owner-bound API keys: Bearer or X-API-Key,
`tasks:read` / `tasks:write`. Keys still cannot access admin, profiles, raw LLM
records, global findings, task mutation/control, scope editing, or key management.
All keys for the fixed ARTIFEX identity share its task ownership; this is **not
per-key tenancy**. Unowned task/finding reads are 404.

## Submission

`POST /api/tasks` (`tasks:write`):

```json
{
  "external_request_id": "skynet:assessment-123",
  "name": "assessment-123",
  "description": "Approved staging assessment",
  "goal": "Minimal non-destructive verification",
  "timeout_seconds": 3600,
  "scope": {"targets": [{"kind": "hostname", "value": "staging.example.invalid"}]}
}
```

Alternatively use `Idempotency-Key: skynet:assessment-123`; both values must agree
when both are present. ID grammar: `[A-Za-z0-9][A-Za-z0-9._:-]{0,127}`. IDs are
owner-scoped, persistent, and have no expiry. Same decoded request returns 200
with the existing task DTO and `Idempotency-Replayed: true`; new creation returns
201 with task DTO (`id` is a string). Replays never launch another run. Changed
content or a deleted/unowned task returns 409. JSON key order/whitespace does not
matter; target array order and explicit optional boolean values do matter.

A PostgreSQL transaction/advisory lock commits the mapping, exploration, task,
scope JSON, and enabled allow rules together. Deleted mappings are tombstones
(task_id nullable), not permission to run again. The app remains a single-process
executor: a second process without the task handle returns 503, not a duplicate.
Creation and execution admission are separate; a 201 does not prove running
workers. Crash between commit and launch is not exactly-once execution; existing
restart loading/admission must be staging-tested.

External submissions require scope; legacy requests without an external ID
retain optional scope. Accepted fields only: `name`, `description`, `goal`,
`external_request_id`, `scope`, `timeout_seconds`, `plan_heartbeat_seconds`,
`coverage_enabled`, `seed_first_intent`. Body max 64 KiB; name max 200 bytes;
description/goal max 16384 bytes; timeout/heartbeat 0..86400 seconds (0 timeout
means unlimited; heartbeat values below 600 become 600). Skynet should always
send a positive timeout and keep concurrency/cost limits outside this API.

### Scope limitation — rollout blocker

1..32 unique targets, kinds `hostname`, `ip` (IPv4), `url` (HTTP/S without
credentials/query/fragment). No wildcards, CIDRs, implicit root-domain expansion,
or surrounding whitespace. Values are persisted unchanged. Targets generate
exact_domain/exact_ip/exact_url task allow rules atomically; global block rules
still win. Seeding uses only these values, never arbitrary prose.

**This is asset-gate enforcement, NOT network-egress enforcement.** Existing
`insert_assets`/`add_intent` gates evaluate candidate sets: aliases/root domains
can affect a match; empty linked-asset lists bypass checks, and insert_assets rule
read failures currently fail open. Arbitrary shell tools, redirects, DNS/IP
resolution and externally spawned tools are not comprehensively constrained.
The report always says `network_scope_enforced:false`. Do not treat this as a
safe replacement for a strict target sandbox or proceed to production cutover.
URL rules compare literal URLs; seeding still creates the service origin.

## Reads

- `GET /api/tasks/{id}/status`: existing `{id,status,paused,queued}`.
- `GET /api/tasks/{id}/findings?page=1&page_size=50` (`tasks:read`):
  `{findings:[FindingDTO],total,page,page_size,has_more}`. page 1..1000000,
  page_size 1..100. Task filter is bound to the path; query `task` cannot override.
  Stable newest timestamp/id ordering. No inherited/global findings.
- `GET /api/tasks/{id}/findings/{fid}`: one FindingDTO, including saved Markdown
  `report` when present. Validates finding.task_id as well as task owner.
  DTO includes string `id`, `finding_id`, `task_id`, `vulnclass`, `severity`,
  triage `status`, `summary`, `evidence`, `ts`, `evidence_version`,
  `report_evidence_version`, `report_stale`, `traffic_count`. Does not export raw
  traffic, unrelated assets, or model request/response logs. Finding evidence and
  Markdown are sensitive untrusted stored content; consumers must escape it.
- `GET /api/tasks/{id}/report`: JSON summary, not a generated pentest narrative:
  `{schema_version:"1",task:TaskDTO,external_request_id,scope,
  scope_enforcement:"asset_gate_only",network_scope_enforced:false,outcome,
  cancel_requested,findings_by_severity,findings_url,limitations:[...]}`.
  Outcome: in_progress / completed_unverified / failed / timeout / cancelled.
  Counts include all triage states; no fabricated score, tested-category claims,
  clean/pass verdict, or benchmark detection rate. Fetch all finding pages and
  finding details for an evidence-backed report.

## Cancellation

`POST /api/tasks/{id}/cancel` (`tasks:write`), empty body or `{}`:
`{id,status,cancel_requested,semantics:"durable_pause",drained:false}` (200).
Active/queued tasks are persistently paused/dequeued with cancel_requested_at
in one update, current planner/worker/main/side runs receive cancellation, and
subsequent admissions/resumes are rejected, including after handle reload.
Repeated cancel is idempotent. Terminal tasks are unchanged. This API does not
invent a new lifecycle enum or claim OS subprocess drain; treat report.outcome
cancelled separately from the existing paused status. No API-key resume exists.
Task history/evidence is retained. Full process-drain semantics remain unverified.

Errors are short static `{error:"..."}`; no reflected field names, SQL/provider
errors, credentials or request content. 400 invalid input; 401 key invalid;
403 endpoint/scope forbidden; 404 missing/unowned; 409 conflict; 503 storage or
handle unavailable. Report/findings responses use Cache-Control: no-store.

## Local verification / staging gate

Dedicated PostgreSQL fixture container on 127.0.0.1:55439, never application DB:

```sh
ARTIFEX_PG_DSN='postgres://artifex_test@127.0.0.1:55439/artifex_test?sslmode=disable' \
 go test -race ./server -run 'TestIntegration|TestAPIKey' -count=1
go build ./...
```

Tests cancel runtime context; no live scans. Cover concurrent HTTP and direct DB
submissions, atomic rollback, replay/conflict/tombstone/owner checks, handle
reload, scoped finding access, durable cancellation/admission rejection,
malformed/oversized input and no-admin allowlist. Parent must deploy to isolated
staging and coordinate actual Skynet submit/poll/report/cancel tests before any
production cutover. No child deployment is performed.
