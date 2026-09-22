# quartzx status

| stage | attempts | updated | evidence |
|-------|----------|---------|----------|
| tagged v0.1.0 | 0 | 2026-09-21 | Built by agent quartzx-builder. Gate: gofmt, tidy, vet, test -race -shuffle (root 95.2% coverage), bench, fuzz 20s, golangci-lint 0 issues, govulncheck clean; CI green on 3 OS x (1.24, stable) at 8fe8473. Commits: d21c4d1 04de440 dfaf717 ac33794 8fe8473. |
| hardened (no tag) | 0 | 2026-09-22 | Post-tag CI incident investigated and closed with defensive hardening, not a confirmed fix. See "2026-09-22 deadlock investigation" below. |

## 2026-09-22 deadlock investigation (quartzx-fixer)

One CI run (`35575841733`, commit `7115c087a9`, ubuntu-latest go1.24, shuffle seed `1789977792365790829`, 2026-09-21T08:03:18Z)
failed `TestExamples/persistence` with `fatal error: all goroutines are asleep - deadlock!`: `main.main()` blocked on a
channel receive (`examples/persistence/main.go:59`) while `Scheduler.loop` sat parked in its timer-arming `select`
(`scheduler.go:526`) with nothing left to wake it.

Investigation: audited the entire CI history (`gh run list` + per-job logs) — this is the **only** occurrence ever; every
other historical "failure" was a different, already-fixed issue (the `go.sum` tidy-pathspec bug x4, `examples/` not
existing yet x2, the unquoted `-coverprofile` Windows-splitting bug x1). Attempted reproduction: ~2,900 runs of the
persistence example (Windows native and GOMAXPROCS 1/2, Linux/WSL at 16 cores and 2-core `taskset`-pinned) plus ~260 full
`go test -race -shuffle=on` runs, including 100 runs using the *exact* failing seed pinned to 2 cores with
`GOMAXPROCS=2` to match the runner. Also deliberately widened the 3 candidate race windows in `Scheduler.loop`'s old
timer-arming code (`Now()` read, `NewTimer(d)`, second `Now()` check) with injected sleeps in a scratch build. Zero
reproductions. Formal analysis: `examples/persistence/main.go` only ever calls `clock.Set`/`Advance` after receiving on
its `completed` channel, which is only sent after `e.running` is decremented under `s.mu` — this gate is airtight for
that example regardless of timing, so the "overlap skip" theory is structurally ruled out; the old "became due while
arming" self-heal check was shown (by proof and by forced-delay testing) to catch every interleaving of its two `Now()`
reads.

**Outcome: root cause not confirmed.** Per the user's direction, applied defensive hardening rather than an unconfirmed
fix: `Scheduler.loop` now arms its timer from a single atomic clock read (new `Clock.NewTimerAt` method) instead of a
separate `Now()` plus a relative duration, removing the class of split-read races even though none was proven live. Added
`TestFakeClockNewTimerAtArmRaceStress` (concurrent arm/advance stress on the new method) and
`TestPersistenceExampleShapeStress` (300-iteration regression mirroring the exact persistence-example shape — FileStore,
PersistData, non-overlapping cron, Set-gated-on-completion) for ongoing coverage. See `oss/LEARNINGS.md` for the entry.
No v0.1.1 tag: a single unreproduced occurrence with no confirmed root cause does not warrant a patch release per the
user's decision; the hardening ships on `main` only.

## Step 0 evidence (2026-09-21)

Read READMEs and shallow-cloned sources of gocron and go-quartz (grep of non-test .go files).

| lib | stars / last push | deps | persistence | misfire | calendars | cron |
|-----|-------------------|------|-------------|---------|-----------|------|
| reugn/go-quartz v0.15.2 (MIT) | 2012 / 2026-01-20 | 0 | `JobQueue` interface, only an example file-system queue in examples/ | `WithMisfiredChan` notification channel and `WithOutdatedThreshold`; no policies | none (0 matches) | full Quartz: sec, L, W, #, ?, year |
| go-co-op/gocron v2.22.0 (MIT) | 7166 / 2026-09-01 | 3 non-test (uuid, clockwork, robfig/cron) + testify, goleak | none (0 matches; elector/locker for HA only) | none (0 matches) | none | via robfig/cron |
| robfig/cron v3 (MIT) | 14188 / 2024-07-08 (no release since) | 0 | none | none | none | 5 field, optional seconds, no L/W/#/? year |

Others seen and out of scope: dkron, dagu (services), river, asynq (queues), flier/quartz (2015).
GitHub search API hit the rate limit after the first round of queries; repo view / contents API and clones were used instead.

Differentiator (what neither covers): named misfire policies (fire-now, skip, reschedule, fire-all) with threshold;
first-class `Store` interface with in-memory and atomic-file JSON stores plus restart recovery of missed fires;
calendars/holiday exclusions; calendar-interval triggers; non-overlap per job by default; injectable fake clock shipped;
documented DST semantics with reference table; Go 1.24 floor, zero deps.

Verdict: PARTIAL, continue.

## Board

agentboard RUNNING.md did not exist at check time (2026-09-21); nothing synced.

## Log
