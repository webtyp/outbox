---
PLAN: "refactor: the retry delays are time.Duration (backoff v0.2.0)"
EXECUTOR: jules
REVIEWER: none
---

> Read AGENTS.md at the repo root before any change.
> **Finish by yourself:** when the delivery check passes, commit, push and open the pull request. Never ask whether to
> proceed or finalize — nobody answers, and a session that waits ends without your work. Commit after every stage.
> Never run `git reset`, `git restore`, `git checkout -- <file>` or `git stash` on your own work.
> **Publishing:** you publish only by OPENING THE PULL REQUEST (a `git push` from your sandbox does not reach GitHub, and work not submitted is lost when the session ends).
> **Never ask a yes/no question about progress**; the answer is always yes. Ask only when a signature in this plan is wrong, and name the symbol.
> Never write `go.mod`/`go.sum` lines by hand: only `go get` and `go mod tidy` do. No scratch files and no scripts over the source.

# Plan: `outbox` on `backoff` v0.2.0

## Why (decided: P47 B of the architecture master)

`webtyp.com/time` v0.6.5 has a type for a wait, `time.Duration` (nanoseconds, never an instant), with `time.Nanosecond`, `time.Microsecond`, `time.Millisecond`, `time.Second`, `(Duration).Milliseconds() int64` and `(Nano).Add(Duration) Nano`. `webtyp.com/backoff` v0.2.0 takes and answers `time.Duration` instead of `time.Nano` (an instant): `New(first, longest time.Duration) (Policy, error)` and `(Policy).DelayAfter(failures int64) (time.Duration, error)`. This repo called the v0.1.0 API with `time.Nano`; this plan moves it to the new one. Behaviour does not change.

## Tools

```bash
go install webtyp.com/devflow/cmd/gotest@latest
go get webtyp.com/backoff@v0.2.0 webtyp.com/time@v0.6.5
go mod tidy
```
`GOPRIVATE=webtyp.com/*,github.com/veltylabs/*`, `GOFLAGS=-mod=mod`; no `replace` in `go.mod`. If a module cannot be fetched, stop and say so in the PR; never invent a hash. If a signature in the published tags differs from this plan, stop and name the symbol in the PR; never adapt it by guessing.

## Stage 1 — `outbox.go`

1. The two delay constants (today `outbox.go:250-251`) become, written exactly:

```go
	firstRetryDelay   time.Duration = time.Second      // 1 s after the first failure
	longestRetryDelay time.Duration = 60 * time.Second // 60 s: the delay never grows past this
```
(`time` here is `webtyp.com/time`, already imported.)
2. The line that schedules the next attempt (today `outbox.go:225`, inside `Deliver`): `m.NextAttemptAt = o.now() + delay` becomes `m.NextAttemptAt = o.now().Add(delay)`.
3. Nothing else of `outbox.go` changes: `now func() time.Nano` stays (an instant), `Redeliver(since time.Nano)` and `Prune(before time.Nano)` stay.

Deletions: `grep -n "1000000000\|60000000000" outbox.go` → empty.

## Stage 2 — tests and docs

`tests/outbox_test.go` should compile unchanged (it only does arithmetic on `time.Nano` instants; `diff != time.Nano(60000000000)` still compares two `time.Nano`). If a test does not compile, change only the type of the delay and say which test in the PR. Any `README.md` / `docs/*.md` line that shows `backoff.New(` with a nanosecond literal becomes `backoff.New(time.Second, 60*time.Second)`; `grep -rn "backoff.New(" README.md docs` shows no literal number.

`gotest` must end green on every destination it runs; a tree that does not compile or whose tests did not run is a failed delivery.
`git diff --stat main` lists `outbox.go`, `go.mod`, `go.sum` (and a doc only if Stage 2 changed one); nothing is deleted.
