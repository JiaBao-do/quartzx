# Things to care about

Every claim here is backed by a test in this repository (test names in brackets) or by an example you can run.

## 1. Time zones and daylight saving time

Cron fields match wall clock time in the trigger's location (default UTC, so set `WithLocation`).

- A wall time that does not exist is **skipped**. In America/New_York `0 30 2 * * ?` does not fire on 2026-03-08; the
  next fire is 2026-03-09 02:30. [`TestCronNextTable`]
- A wall time that occurs twice fires on **both** occurrences. `0 30 1 * * ?` fires twice on 2026-11-01, once at
  -04:00 and once at -05:00. Hourly jobs fire in both 01:00 hours. [`TestCronNextTable`]
- Wrong: a daily job at 02:30 in a DST zone and the belief it runs every day. Right: schedule at a time that always
  exists (for example 03:30 or 04:00), or in UTC, or accept the skipped day.
- `Every(24*time.Hour)` is 24 elapsed hours, so its wall clock time shifts by one hour across DST; use
  `EveryCalendar(1, quartzx.Day, WithLocation(loc))` to keep 09:00 at 09:00. [`TestCalendarIntervalKeepsWallClockAcrossDST`]
- `time.LoadLocation` needs zone data. On Windows or in minimal containers add `import _ "time/tzdata"`.

## 2. Cron dialects

quartzx accepts both dialects and decides by field count. [`TestPitfallSecondsFieldDialects`]

| | 5 fields (classic) | 6 or 7 fields (Quartz) |
|---|---|---|
| fields | min hour dom month dow | sec min hour dom month dow [year] |
| seconds | fixed at 0 | field 1 |
| day-of-week | 0-6, 0 and 7 are Sunday | 1-7, **1 is Sunday** |
| dom and dow both set | matches if **either** matches | error, use `?` in one of them |

`* * * * *` fires every minute; `* * * * * ?` fires every second. Descriptors (`@daily`) are accepted, `@every`
is not (use `Every`). robfig/cron's optional-seconds and `TZ=` prefix are not supported.

## 3. Misfires: defaults and surprises

A fire is a misfire when it is later than the threshold (default 5s, `WithMisfireThreshold`). Then the job's policy
applies; the default is `MisfireFireNow`. [`TestMisfirePoliciesAfterRestart`, `examples/misfire`]

- `fire-now` runs once and drops the other missed fires. Surprise: after a 10 hour outage an hourly job runs once.
- `skip` loses the run for good. A one-shot `Once` trigger with `skip` is deleted without running.
- `reschedule` re-anchors interval triggers at now (next fire is one period later, off the original grid).
- `fire-all` replays each missed fire, capped by `WithMaxCatchUp` (default 100) and run one after another. Surprise: a
  backlog of slow jobs delays everything behind it.
- A fire that is late by less than the threshold is not a misfire. [`TestNoMisfireWithinThreshold`]
- A paused job that is resumed after its fire time misfires. [`TestPauseAndResume`]

## 4. Recovery is not exactly-once: make jobs idempotent

- Fires missed while the process was down are recovered according to the policy above.
- The record (next fire time, counters) is saved when a fire is **dispatched**, before the job finishes. If the
  process dies while a job runs, that run is **not** replayed on restart. [`TestPitfallInFlightRunIsNotRetried`]
  A job that must not lose work needs its own durable progress or outbox; quartzx does not retry failed runs.
- With `fire-all` a job can run several times back to back after a restart, with `Execution.Misfired == true`.
- One-shot triggers (`Once`) are deleted after their run finishes. A crash before that leaves the record, and the next
  start treats it as a misfire.
- A failing job (error or panic) is reported as `EventFailed` and is not retried; the next fire happens normally.
  [`TestFailurePanicAndListenerPanic`]

## 5. Long jobs and overlap

By default a job never runs concurrently with itself: a fire that arrives while the previous run is still going is
**skipped** and reported as `EventSkipped`, not queued. [`TestOverlapIsSkippedByDefault`]

```go
// Wrong: assuming every fire runs, even when jobs are slow.
// Right: watch EventSkipped, keep jobs shorter than the interval, or opt in explicitly:
quartzx.JobSpec{Key: "k", Handler: "h", Trigger: t, AllowConcurrent: true} // [TestAllowConcurrent]
```

Use `Timeout` on the job so a hung run cannot block its own schedule forever [`TestJobTimeout`], and
`WithMaxConcurrent` to bound total parallelism. [`TestMaxConcurrent`]

## 6. Clock changes

Fire times are compared with `Clock.Now()`. If the system clock jumps forward, due fires look late and may misfire; if it
jumps back, fires are delayed (this follows from the comparison; it is not tested against a real clock jump). Keep
servers on NTP with slew. In tests use `FakeClock`; `FakeClock.Set` may move backwards.

## 7. Store durability and limits

- `FileStore` rewrites the whole file on every change (temp file, fsync, rename): crash safe, but O(jobs) per write.
  Fine for hundreds to a few thousand jobs. [`TestFileStore`, `TestFileStoreWriteFailureRollsBack`]
- It does not lock across processes. **Do not run two schedulers on one file** (they will duplicate fires and overwrite
  each other). quartzx has no clustering or leader election; use an external lock or implement `Store` on a database
  and add your own leader election.
- A store failure while running is reported as `EventError`; scheduling continues from memory. Listen for it.
- Job `Data` is JSON: numbers come back as `float64`, and values must be encodable. [`TestFileStore`]
  Wrong: `n := e.Data["n"].(int)`. Right: `n, _ := e.Data["n"].(float64)`.
- Calendars and handlers are code, not data. Register them on every start; a job whose calendar is missing is not
  loaded and reported as `EventError`. [`TestLoadProblemsAreReportedNotFatal`]
- The file format is versioned (`"version": 1`); a newer version is refused with `ErrStoreFormat`.

## 8. Graceful shutdown

`Stop(ctx)` stops new fires and waits for running jobs. If ctx expires, job contexts are cancelled and Stop returns
`ctx.Err()`. [`TestGracefulStopWaitsForRunningJob`, `TestStopDeadlineCancelsJobs`] Jobs must watch their context. Cancelling
the context passed to `Start` cancels running jobs immediately. [`TestParentContextCancelStopsEverything`]

## 9. Concurrency

`Scheduler`, stores, clocks, calendars and triggers are safe for concurrent use. Listeners run on scheduler goroutines
and must not block. `Execution.Data` is a shallow copy: do not mutate nested maps/slices in place.

## 10. Not supported

Clustering/leader election, retries with backoff, job chaining/workflows, sub-second cron, `@every`, JDBC-style
stores (write your own `Store`), time zones for `Every` (it is elapsed time). The scheduler scans all jobs to find the
next fire, so it is not meant for hundreds of thousands of jobs.

## Go version

Requires Go 1.24+. CI runs 1.24 and the latest release on Linux, macOS and Windows.

## Upgrade notes

v0.x: the API may change between minor versions; see CHANGELOG.md.
