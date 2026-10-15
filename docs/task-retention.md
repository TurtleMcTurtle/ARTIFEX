# Automatic task retention (opt-in)

The native backend scheduler implements **archive 7 days after terminal completion;
permanently delete the task and its archive 30 days after successful archival**.
The normal earliest timeline is about 37 days after completion; backlog, failed
archives, dependencies and daily scheduling can make it longer, never shorter.
It is disabled by default. This feature does not enroll a Hermes/OS cron job, change
Compose/environment files, deploy a binary, or run a live database purge.

**Local verification is not rollout approval.** The five blocking retention
review findings have dedicated local regressions; independent re-review must
still pass before a new image is prepared or deployed. Earlier test passes or
previously prepared images do not establish that this repaired source is live.

## Lifecycle and safety

- Only backend terminal statuses `done` (the API's completed outcome), `failed`,
  and `timeout` qualify. `created`, `running`, `paused`, queued tasks, unresolved
  archive/restore operations and missing completion timestamps do not qualify.
- Archive eligibility age is `tasks.completed_at`, never creation, `updated_at`,
  deadline, archive request time or archive creation time. It must be non-null,
  not in the future, and not earlier than task creation. Reopening the task clears
  its completion timestamp through the existing lifecycle API.
- At `completed_at + 168h`, enqueue the existing durable archive job. The worker
  uses its existing engine/chat/side-question drain barrier, reversible file and
  exclusive-traffic staging, checksummed package and PostgreSQL compaction.
  Retention eligibility is rechecked under the task/archive row locks at commit.
- Compaction **does not delete the task registry row**: it leaves task/exploration
  ID stubs, marks the task archived/deleted for live lookups, removes hot findings,
  graph rows, history and exclusive assets, and stores the payload in
  `data/archives/tasks/task-<task_id>-<archive_id>.tar.zst`.
- The compact archive record retains `task_completed_at`. Automatic jobs also
  retain the completion-based `retention_archive_at` and independent archive
  residence duration `retention_delete_after_ns`. A queued or failed archive has
  no deletion deadline. Only the successful ready transition sets `archived_at`
  and `retention_delete_at = archived_at + residence duration`; its timestamp is
  captured at the end of compaction, not PostgreSQL's transaction-start `now()`.
- At `task_archives.archived_at + 720h`, only a **confirmed successful cold archive** can enter
  the existing durable deletion pipeline. A ready state alone is insufficient:
  package metadata, unchanged terminal completion, clean warnings and explicit
  `archive_cleanup_at` confirmation are required. Archive failure, interrupted
  restore or incomplete hot-file/traffic cleanup prevents automatic deletion.
  Archive traffic commit awaits the actual staged-tree unlink result after
  releasing the recorder's write lock. An unlink failure is returned to the
  archive caller: the package stays restorable, warnings are recorded, and the
  cleanup timestamp stays unset. Ordinary non-archive traffic deletion retains
  its existing background staged-directory cleanup behavior. Traffic blob GC now
  returns traversal/reference-inspection and unreferenced-blob unlink errors to
  manual callers too: the index deletion may already be committed even when the
  API returns an error. It is not rolled back by a cleanup error. Shared referenced
  blobs are preserved; empty-bucket removal and index vacuum remain best-effort.
  An overdue live task is archived first and then receives a full 30 days of
  archive residence; it is never directly purged. Cleanup confirmation remains a
  separate required gate and does not substitute queue/start/completion time for
  the successful archive timestamp.
- Cold deletion stages the package, transactionally rechecks lifecycle and
  inheritance, deletes task/exploration/archive rows and task-language metadata,
  then removes the staged package. Engine/inflight barriers also apply to cold
  deletion. Unexpected hot task files appearing after archival block automatic
  deletion for review rather than silently leaving unarchived payload behind.
  Detection uses confined, read-only workspace/transcript checks, not temporary
  renames. Matching `.delete-staging/task-<task_id>-*` remnants from an interrupted
  old probe also block deletion; they require operator reconciliation and are not
  silently discarded. Inspection errors fail closed.
  Referenced/shared global assets and traffic are not bulk-purged.
- **External-request tombstones are intentionally retained.**
  `task_submissions(owner, external_request_id, request_hash)` survives with
  `task_id = NULL` through its existing foreign key. Replaying a deleted request
  returns a conflict instead of launching a replacement scan. Full task deletion
  is not deletion of this anti-replay ledger, shared evidence, operator backups,
  or logs already exported outside ARTIFEX. Unreferenced content-addressed traffic
  evidence continues through the existing hourly evidence GC with its 24-hour
  grace period; it is not aggressively unlinked by the retention scheduler.
- Legacy archives are not backfilled with inferred finish times or cleanup
  success. Archives without these trustworthy metadata remain protected and need
  explicit operator review. Do not populate these columns from `updated_at` or
  merely set the cleanup marker to make a purge proceed.
- Archive staging/recovery validates `.staging` and `.restore` parents and every
  journal move before applying or reversing any move. Journal identities and
  workspace/transcript source-destination mappings must match their managed
  locations. Managed ancestors cannot be symlinks. Staging, package IO, recovery,
  recursive removal, traffic export/import and evidence export/install use pinned
  `os.Root` descriptors derived from the managed root, including all nested body
  reads, writes, manifest files and existing-body verification. Operations remain confined
  even if an ancestor changes after validation. This is not a mount, hard-link,
  tenant-isolation or distributed-execution boundary; the configured data root
  and its host-side ancestors remain operator-controlled.

## Configuration

Open **Current tasks → Retention settings**, next to Concurrency settings. The dialog shows the effective policy and saves it in ARTIFEX's existing `settings` table. Saved values override the startup environment and survive restart; no retention schema migration is needed for this UI.

The dialog uses whole days for completion age and archive residence, and hours for the check interval. Defaults are 7 days, 30 days and 24 hours. Advanced limits default to 50 transitions and 120 seconds. Enabling requires a separate confirmation explaining permanent deletion, Skynet's live-report limitation and preservation of required snapshots. Opening the dialog, cancelling it or saving does not perform an immediate cleanup.

The administrator-only `GET`/`PUT /api/settings/task-retention` API requires the ARTIFEX administrator Bearer JWT; task API keys and cookie-only requests are rejected. Every save supplies all fields and the revision from the last GET. Stale edits or an active cleanup cycle return 409, so reload rather than blindly retry. `confirm_enable=true` is required for enabled saves. Values are validated before an atomic single-row write. A failed write leaves the previous policy intact; an error after commit or a lost response can still mean the policy was saved, so reload before retrying.

Every successful save persists a fresh generation with the policy, even when its values are unchanged. The generation survives restart and prevents a quick disable/re-enable from reusing an old due timer. Legacy generation-free saved values remain readable and receive a generation on their next successful save.

The live scheduler reads saved changes without restarting the container, resets its next check after every successful save, and rechecks the current generation under the distributed cycle lock before enqueueing work. Disabling prevents new cycles, not jobs already queued. If archive recovery could not start the worker, the dialog reports that scheduling is unavailable rather than claiming cleanup is running.

When no saved policy exists, the **backend process environment** supplies fallback values:

| Variable | Default | Validation |
| --- | --- | --- |
| `ARTIFEX_TASK_RETENTION_ENABLED` | `false` | Boolean |
| `ARTIFEX_TASK_RETENTION_ARCHIVE_AFTER` | `168h` | Positive Go duration |
| `ARTIFEX_TASK_RETENTION_DELETE_AFTER` | `720h` | Positive Go duration of residence after successful archival |
| `ARTIFEX_TASK_RETENTION_INTERVAL` | `24h` | Positive Go duration |
| `ARTIFEX_TASK_RETENTION_BATCH_SIZE` | `50` | Integer 1–100, total transitions per cycle |
| `ARTIFEX_TASK_RETENTION_CYCLE_TIMEOUT` | `2m` | Positive Go duration, at most `10m` |

Go durations do not accept `7d` / `30d`; use `168h` / `720h`. Invalid settings
fail closed and log a disabled-scheduler message. UI saves require days 1–3650,
interval hours 1–168, batch 1–100 and timeout seconds 1–600. Archive age and archive residence are independent
durations, so neither is required to be larger than the other.

The first cycle is **one interval after successful backend/archive recovery**,
not an immediate startup sweep. Defaults mean an elapsed-24-hour timer rather
than midnight in a particular timezone. Stopping/restarting resets that timer.
Each cycle queues at most the batch limit; the archive worker executes jobs
asynchronously, not inside the bounded scheduler cycle. More than one batch,
worker backlog, dependencies and failures can delay the actual transitions.
Cold-task `FOR UPDATE` rechecks use the cycle context, including while blocked
by another transaction. Cancellation releases the distributed cycle lock without
waiting for that row blocker to finish. The enqueue timeout does not bound archive
compression, filesystem unlink time or traffic recovery execution.
An overdue task archived during a cycle becomes deletable on a subsequent cycle
only after 30 days from successful archival and confirmed cleanup, irrespective
of how long it waited in the queue or how old its terminal completion already is.

Cycles log candidates, archive/deletion enqueues, skipped transitions and errors.
Automatic archive failures and deletion failures are retried on later cycles,
not in an unbounded busy loop. Failed manual archive and unresolved restore jobs
are not automatically replayed. A warning or crash before cleanup confirmation
stays protected for operator review. Package unlink errors after metadata deletion
leave the existing `.deleting-<id>` recovery file; startup recovery retries its
cleanup. Enqueue counts are not proof that compression or deletion completed.

PostgreSQL session advisory locks serialize retention discovery across processes
and serialize traffic initialization/recovery in `NewManager` with archive worker
staging/recovery. The recording proxy is not started before that critical section
completes. A busy maintenance lock refuses traffic-enabled manager startup before
opening/recovering traffic; retry once the owning worker is idle. Row locks and
unique task archive IDs keep repeated transitions idempotent.
All cooperating backends must run this code and use the **same managed data
storage**; a database lock does not synchronize unrelated local filesystems.
If the later server archive recovery cannot acquire its worker lock or complete,
that process disables archive/retention work rather than touching another
process's staging. Retry startup once the existing worker is idle. Runtime execution drain remains
local to the owning backend; do not treat this as a new distributed scan engine.

## Skynet/report caveat

**Archival changes backend availability.** Live task/report/findings APIs do not
serve cold packages: after archival a live lookup may return 404, and hot findings
have moved out of live tables. Skynet is **not** assumed to have persisted a complete
report or evidence snapshot. Before enabling retention, verify that the required
consumer has saved a complete, provenance-bearing report/evidence snapshot, or
accept that its live report fetch will become unavailable after day 7. This change
makes no Skynet code or preservation changes. Restoring before permanent deletion
can recover an archive; deletion after its 30-day archive residence removes that
recovery option (normally no earlier than about day 37 after task completion).

## Reviewed runtime enablement (not executed by implementation)

1. Review the local diff and isolated test results, including schema additions,
   age predicates, shared-data rules and tombstone preservation.
2. Approve retention semantics and verify report/evidence snapshots independently.
3. Approve deployment and back up the DB **and** managed data directory under the
   operator's backup policy. Review protected legacy/failed jobs separately.
4. Deploy the reviewed backend with retention **still disabled**; verify its exact
   source/image and additive schema. Do not equate process health with the new
   code having been loaded.
5. After explicit enablement approval, set `ARTIFEX_TASK_RETENTION_ENABLED=true`
   in the backend's approved runtime configuration, keeping `168h`, `720h`, `24h`
   and a conservative bounded batch. Do not shorten production intervals to
   verify behavior. Restart only within an approved maintenance window.
6. Observe the enabled startup log and first daily cycle. Read back exact archive
   state/cleanup markers before claiming archival, and exact task/archive absence
   plus retained submission tombstones before claiming permanent deletion.
7. To stop new automatic decisions, disable retention and restart under approval.
   Durable jobs already queued remain queued for the existing archive worker;
   disabling the scheduler does not cancel an in-flight/queued deletion. Review
   and reconcile those jobs explicitly before relying on a rollback.

## Isolated verification

Retention tests require `ARTIFEX_RETENTION_TEST_DSN` pointing at a **dedicated,
disposable loopback PostgreSQL server**, not the application DB. Each test creates
and drops a fresh database. Existing package `TestMain` also resolves
`ARTIFEX_PG_DSN`; point it at that same disposable server to avoid app-DB locks.
Use a scratch `TMPDIR` so fixture files never enter application storage.

```sh
ARTIFEX_PG_DSN='postgres://housekeeping_test@127.0.0.1:55437/housekeeping_test?sslmode=disable' \
ARTIFEX_RETENTION_TEST_DSN='postgres://housekeeping_test@127.0.0.1:55437/housekeeping_test?sslmode=disable' \
go test -mod=readonly -race ./db ./server -run '^TestTask(ArchiveRetention|Retention)' -count=1 -v
```

DB-dependent retention fixture helpers skip without the opt-in DSN. That does
**not** disable the package's `TestMain`, which still resolves its ordinary DSN;
an unset environment is not proof of application-DB isolation. Always set both
DSNs to the dedicated disposable server for focused or race runs.

Retention fixtures never call `server.New` (provider/restored-task startup).
They exercise real `NewManager` construction against fresh databases; the
two-manager startup regression runs a second constructor in a separate process
while the first owns a live archive/traffic stage. Engine contexts are cancelled;
explicit archive worker probes receive only a bounded fixture context. Tests
cover nested symlink parents, malicious replay journals before any move, a real
blocked PostgreSQL row and released cycle lock, nonmutating hot checks and old
crash remnants, injected unlink failure excluding purge, and actual manager
startup protecting another worker's stage. These are focused local checks, not
a full provider/runtime suite, deployed verification, or report-preservation proof.
