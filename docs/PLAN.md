---
PLAN: "feat: outbox — durable, ordered queue of pending mutations with retry, rejection and coalescing"
TAG: v0.1.0
EXECUTOR: jules
REVIEWER: none
STATUS: review
SESSION: 14387799303993764422
PR: https://github.com/webtyp/outbox/pull/1
---

> This plan is dispatched via the CodeJob workflow. See skill: agents-workflow.

# Plan — `outbox`: what was done offline and still has to reach the server

## 0. Context (read first)

This repository is new and empty (created with `gonew`; module `webtyp.com/outbox`).

An offline-first wave (master plan, in Spanish:
<https://github.com/veltylabs/mjosefa-cms/blob/main/docs/OFFLINE_FIRST_MASTER_PLAN.md>) lets a
clinic keep booking appointments and writing medical records while the server is unreachable.
Each browser applies every change to its **local** database at once, and records the change as a
**mutation** (an operation name + its encoded arguments) in this outbox. When the server is
reachable, the outbox hands the mutations, **in order**, to a sender supplied by the caller and
records the server's verdict.

Like the "Outbox" folder of an email client: what you wrote and has not been sent yet.

The outbox knows nothing about HTTP, MCP, JSON or browsers: the payload is opaque bytes and the
transport is an injected `Sender`. It runs in the browser (TinyGo WASM, over `webtyp/indexdb`)
and in tests (over `webtyp.com/storage/mem`), always through `*orm.DB`.

## Design gate

### 1. Prior art
- **Transactional outbox pattern** (microservices; Debezium outbox router, MassTransit,
  NServiceBus): changes are written to a local outbox table and delivered asynchronously, at
  least once, with idempotency on the receiving side. Our name and guarantee.
- **Replicache / Zero pending mutations**: the client keeps a list of named mutations with a
  monotonically increasing mutation id, pushes them in order, and drops them when the server
  confirms. Our ordering and "named operation + args" model.
- **Workbox Background Sync (`workbox-background-sync`)**: failed requests are queued in
  IndexedDB and replayed when connectivity returns, with retry. Rejected as the mechanism (it
  replays raw HTTP requests, so it cannot coalesce, report a typed rejection, or run outside a
  service worker), kept as evidence that the browser-side queue belongs in IndexedDB.
- **Email clients' Outbox**: the user-facing metaphor and the name.

### 2. Novice-name test
`outbox.New(db, ids, now)`, `box.Enqueue(outbox.Entry{Op: "patient_directory.upsert_patient", Payload: b})`,
`box.Deliver(sender)`, `box.Pending()`, `box.Rejected()`, `box.Dismiss(id)`,
`box.Redeliver(since)`, `box.Prune(before)`. A sender answers `outbox.Delivered`,
`outbox.Rejected` or `outbox.Retry`.

### 3. Complexity ledger
```
Concepts the developer must learn   +4 (Outbox, Entry, Sender/Result, Mutation)
Files they must touch to do X        +0 (used by dbsync, not by modules)
Lines at the call site               n/a (new capability)
Ways to do the same thing            0
```

### 4. Where it belongs
A queue with delivery semantics is one concern, reusable beyond sync (any "send when online":
notifications, uploads). The sync engine (`webtyp/dbsync`) composes it with a transport and a
change log; this library must not import either.

### 5. What this deletes
Nothing: genuinely new capability.

## 1. Model (`model.go`, generated with `ormc`)

```go
var MutationModel = model.Definition{
    Name: "outbox_mutation",
    Fields: model.Fields{
        {Name: "id", Type: model.Text(), DB: &model.FieldDB{PK: true}}, // minted by the injected IDGenerator
        {Name: "seq", Type: model.Int(), NotNull: true},                // delivery order
        {Name: "op", Type: model.Text(), NotNull: true},                // qualified operation name
        {Name: "payload", Type: model.Blob()},                          // opaque encoded args
        {Name: "coalesce", Type: model.Text()},                         // optional merge key
        {Name: "state", Type: model.Text(), NotNull: true},             // see State
        {Name: "attempts", Type: model.Int(), NotNull: true},
        {Name: "next_attempt_at", Type: model.Int(), NotNull: true},    // unix ns; 0 = now
        {Name: "delivered_at", Type: model.Int(), NotNull: true},       // unix ns; 0 = not delivered
        {Name: "reason", Type: model.Text()},                           // rejection reason or last retry error
        {Name: "created_at", Type: model.Int(), NotNull: true},
    },
}
```
Generate `model_orm.go` with `go install webtyp.com/ormc/cmd/ormc@latest && ormc` at the repo
root and commit it. The generated struct is `Mutation`.

## 2. Target API (exactly this is exported, plus the generated `Mutation` helpers)

```go
package outbox

type State string
const (
    StatePending   State = "pending"
    StateDelivered State = "delivered"
    StateRejected  State = "rejected"
)

type Verdict uint8
const (
    Delivered Verdict = iota + 1 // the server applied it (or had already applied it)
    Rejected                     // the server refused it for good (conflict, permission, validation)
    Retry                        // not now (offline, timeout, 5xx): try again later, keep order
)

// Result is what a Sender answers for one mutation.
type Result struct {
    Verdict Verdict
    Reason  string // shown to the user for Rejected; kept as the last error for Retry
}

// Sender delivers ONE mutation and blocks until it knows the verdict.
type Sender interface {
    Send(m *Mutation) Result
}

// Entry is what the caller enqueues.
type Entry struct {
    Op       string // required: qualified operation name, e.g. "patient_directory.upsert_patient"
    Payload  []byte // the encoded arguments; never decoded by this package
    Coalesce string // optional: entries with the same Op and Coalesce replace each other while still pending
}

// Report summarises one Deliver call.
type Report struct {
    Delivered int
    Rejected  int
    Blocked   bool // stopped at a mutation that must be retried later; nothing after it was sent
}

type Outbox struct { /* unexported */ }

// New opens the outbox stored in db (the outbox_mutation table must exist; see migrate).
// ids mints mutation ids; now returns unix nanoseconds (pass webtyp.com/time.Now).
func New(db *orm.DB, ids model.IDGenerator, now func() int64) (*Outbox, error)

func (o *Outbox) Enqueue(e Entry) (id string, err error)
func (o *Outbox) Deliver(s Sender) (Report, error)
func (o *Outbox) RetryNow() error                       // clears every pending next_attempt_at (connectivity came back)
func (o *Outbox) Pending() (int, error)                 // count of StatePending
func (o *Outbox) Rejected() ([]Mutation, error)         // StateRejected, seq ascending
func (o *Outbox) Dismiss(id string) error               // deletes ONE rejected mutation (the user resolved it)
func (o *Outbox) Redeliver(since int64) (int, error)    // delivered with delivered_at >= since → pending again
func (o *Outbox) Prune(before int64) (int, error)       // deletes delivered with delivered_at < before
```

Package `webtyp.com/outbox/migrate`: `func Migrate(conn ddl.Execer, compiler ddl.Compiler) error`
→ `ddl.New(conn, compiler).CreateTable(&outbox.Mutation{})` (same convention as
<https://github.com/veltylabs/clinical_encounter/blob/main/migrate/migrate.go>). In the browser,
`webtyp/indexdb` creates the store from the model instead; the README says so.

## 3. Behaviour (normative)

Errors are unexported typed constants (`type boxError string` + `Error()`), never `errors.New`.

- **New**: `db`, `ids` or `now` nil → `outbox: db, ids and now are required`. Reads the highest
  `seq` (ReadAll ordered `Desc("seq")`, `Limit: 1`; empty → 0) into an in-memory counter.
- **One writer.** All methods take the `Outbox`'s `sync.Mutex` except while `Deliver` is
  waiting on `Send` (see below). One `Outbox` per database at a time: the app guarantees a single
  tab (documented in the README; this package does not detect other tabs).
- **Enqueue**: `Op == ""` → `outbox: entry op is required`.
  - If `Coalesce != ""` and there is a mutation with the same `Op` and `Coalesce`, state
    `pending`, that is **not** the one currently being sent → update only its `payload` (and
    `reason = ""`); return its id. Its position (`seq`) does not change.
  - Otherwise insert a new row: `id = ids.NewID()`, `seq = counter+1`, `state = pending`,
    `attempts = 0`, `next_attempt_at = 0`, `delivered_at = 0`, `created_at = now()`.
- **Deliver**:
  - If another `Deliver` is running → return `Report{}` and `outbox: delivery already in progress`.
  - Loop: take the pending mutation with the lowest `seq`. None → return. Its
    `next_attempt_at > now()` → return with `Blocked = true` (order is preserved: later mutations
    are not sent before it).
  - Record it as in flight, **release the mutex**, call `s.Send(m)`, re-take the mutex.
  - `Delivered` → `state = delivered`, `delivered_at = now()`, `reason = ""`; continue.
  - `Rejected` → `state = rejected`, `reason = Result.Reason`; continue with the next one (a later
    mutation that depends on it will be rejected by the server too).
  - `Retry` → `attempts++`, `next_attempt_at = now() + backoff(attempts)`, `reason = Result.Reason`;
    return with `Blocked = true`.
  - `backoff(n) = min(1s × 2^(n−1), 60s)`; both bounds are unexported constants.
  - Any other `Verdict` value → treat as `Retry` and set `reason` to
    `outbox: sender returned an unknown verdict`.
  - Doc comment must say: **blocks until every due mutation was sent; never call it from a
    JavaScript event callback in WASM — run it in its own goroutine.**
- **RetryNow**: `next_attempt_at = 0` on every pending mutation.
- **Dismiss**: the id must exist and be `rejected`; otherwise
  `outbox: only a rejected mutation can be dismissed`.
- **Redeliver(since)**: every `delivered` mutation with `delivered_at >= since` → `pending`,
  `delivered_at = 0`, `attempts = 0`, `next_attempt_at = 0`, keeping its `seq`. Returns the count.
  (Used after a server failover: the receiving side is idempotent by mutation id.)
- **Prune(before)**: deletes `delivered` with `delivered_at < before`; never touches pending or
  rejected. Returns the count.

## 4. Stages

| Stage | Files | Content |
|---|---|---|
| 1 | `go.mod`, `model.go`, `model_orm.go` | `go get webtyp.com/orm webtyp.com/storage webtyp.com/model webtyp.com/fmt webtyp.com/ddl`; §1; run `ormc`. Replace the placeholder `outbox.go` gonew created |
| 2 | `outbox.go` | types and methods of §2/§3 |
| 3 | `errors.go` | typed errors |
| 4 | `migrate/migrate.go` | `Migrate` |
| 5 | `tests/*.go` | §5 |
| 6 | `README.md`, `docs/ARCHITECTURE.md` | email-outbox metaphor, "I want X → use Y" table, one-tab rule, the WASM blocking warning, example below |

README example:
```go
box, _ := outbox.New(orm.New(conn), ids, time.Now)
id, _ := box.Enqueue(outbox.Entry{Op: "clinical_encounter.save_draft", Payload: b, Coalesce: visitID})
go func() { report, err := box.Deliver(sender) /* … */ }()
```

## 5. Tests (`tests/`, external package; `gotest`)

Backend: `orm.New(mem.New())` (create the table with `migrate.Migrate` if `mem` needs it). A fake
clock (`func() int64` over a variable) and a fake `Sender` that answers from a script and records
what it received. Consumer-shaped: every case goes through the public API only.

1. Order: enqueue A, B, C → `Deliver` sends A, B, C in that order; `Report{Delivered: 3}`;
   `Pending() == 0`.
2. Retry blocks order: script A=Delivered, B=Retry → C is **not** sent; `Blocked`; B's
   `attempts == 1`. Advance the clock less than 1 s → `Deliver` sends nothing; advance past it →
   B and C are sent.
3. Backoff caps at 60 s after many retries.
4. Rejected does not block: A=Rejected("slot taken"), B=Delivered → `Rejected()` returns A with
   that reason; `Dismiss(A)` removes it; `Dismiss(B)` errors (B is delivered).
5. Coalesce: enqueue `{Op: "x", Coalesce: "v1", Payload: "1"}`, then `"2"` → one pending
   mutation with payload `"2"`, same id. During a `Deliver` whose fake sender enqueues the same
   key from inside `Send` (simulating the user typing while it is sent) → a **new** mutation is
   created instead of changing the in-flight one, and both payloads reach the sender in order.
6. `RetryNow` makes a backed-off mutation due immediately.
7. Redeliver: deliver A at t=10, B at t=20 → `Redeliver(15)` returns 1 and B is sent again by the
   next `Deliver`; A is not.
8. Prune: deletes only delivered rows older than the bound.
9. Restart: a second `New` over the same db continues `seq` above the existing maximum.
10. Concurrent `Deliver` → the second returns the "in progress" error.
11. `New` with nil arguments → error; `Enqueue` without `Op` → error.

## 6. Code rules (non-negotiable)
- Compiles to TinyGo WASM: `webtyp.com/fmt` for formatting/errors; no `errors`, `strconv`,
  `strings`, `time`, `encoding/json` from the standard library. `sync` is allowed.
- Every repeated string (states, column names) is a constant. No `any` in the public API.
- No exported symbol beyond §2 and the generated model. Tests in `tests/`; never export for a test.

## 7. Acceptance criteria
- `gotest ./...` green (stdlib and wasm lanes).
- `grep -rn "TODO\|FIXME" --include=*.go .` → empty.
- `grep -rn "\"encoding/json\"\|\"net/http\"" --include=*.go .` → empty.
