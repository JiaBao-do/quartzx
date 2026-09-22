# quartzx

Quartz-style job scheduler for Go: cron/interval/calendar triggers, persistent job store, misfire policies, calendars,
restart recovery. Go library, module `github.com/JiaBao-do/quartzx`. Fills the gap left by Quartz (Java) in the Go
ecosystem; go-quartz and gocron cover basic scheduling, see README for the honest comparison.

## Commands
- Test: `go test -race -shuffle=on ./...` (examples are run by `./examples/`)
- Lint: `golangci-lint run`
- Bench: `go test -run=^$ -bench=. -benchmem ./...`
- Fuzz: `go test -run=^$ -fuzz=FuzzParseCron -fuzztime=30s .`
- Vuln: `govulncheck ./...`
- Hook: `git config core.hooksPath .githooks` (pre-push checks the pushed commit from a clean checkout; never bypass)

## Architecture
- `cron.go`: CronSchedule parser and DST-aware `Next` (civil-time iteration, bitsets).
- `trigger.go`: Trigger/TriggerSpec (cron, interval, calendar-interval, once); `calendar.go`: exclusion calendars.
- `clock.go`: Clock, SystemClock, FakeClock (BlockUntilTimers/Advance/Set). `NewTimerAt(t)` decides "already due" and
  "arm relative to now" as one atomic clock read; `Scheduler.loop` uses it (not a separate `Now()` + `NewTimer(d)`) so
  arming a timer can never straddle a concurrent Set/Advance. Added as defensive hardening after a single unreproduced
  CI deadlock — see STATUS.md and oss/LEARNINGS.md before touching loop()'s arming logic.
- `store.go` / `filestore.go`: Store interface, JobRecord, MemoryStore, atomic JSON FileStore.
- `job.go`: Job, JobSpec, Execution, events/listeners. `scheduler.go`: single loop goroutine; `processDue` (under lock)
  computes misfires/dispatches, persistence and events happen after unlock; `runJob` per dispatch.
- `examples/*`: deterministic demos verified against `expected_output.txt`. `docs/PITFALLS.md`: claims backed by tests.

## Conventions
- Requires Go 1.24+ (no `sync.WaitGroup.Go`, no `errors.AsType`); zero third-party dependencies.
- Tests use FakeClock, never sleeps; wait on listener events. Windows race runs need the hook's gcc PATH fix.
- Exported API needs doc comments and Example tests; errors wrapped with %w, sentinels in errors.go.
- No absolute local paths in tracked files. Conventional Commits; update CHANGELOG.md.

## Invariants (do not break)
- Scheduler/Store/Clock/Calendar/Trigger are goroutine-safe; listeners never block the scheduler lock (events emitted after unlock).
- A job never overlaps itself unless AllowConcurrent. Record is advanced at dispatch, before the run (at-most-once for in-flight runs).
- DST: nonexistent local times skipped, repeated ones fire on each occurrence.
- FileStore writes are atomic (temp+fsync+rename); file format is versioned.

## Roadmap
- Heap-based next-fire lookup for very large job counts; store implementations for SQL (separate module).
- Cluster-safe store contract (leader election hook); retry policies; `@every` descriptor; metrics via slog/expvar hook.
