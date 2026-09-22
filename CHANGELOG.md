# Changelog

All notable changes are documented here. Format: [Keep a Changelog](https://keepachangelog.com/), versioning: [SemVer](https://semver.org/).

## [Unreleased]

### Changed
- `Scheduler.loop` now arms its next-fire timer from a single atomic clock
  read (`Clock.NewTimerAt`) instead of a separate `Now()` call plus a
  relative duration passed to `NewTimer`. This is defensive hardening, not a
  confirmed bug fix: a single CI run once deadlocked in this exact code path
  (see `STATUS.md` and `oss/LEARNINGS.md`), but the deadlock was never
  reproduced despite an extensive investigation, and no live race was
  confirmed. `Clock` gained a new required method, `NewTimerAt(t time.Time)
  Timer`; both `SystemClock` and `FakeClock` implement it. Any external
  `Clock` implementation must add it (acceptable pre-1.0 per the versioning
  policy).

## [0.1.0] - 2026-09-21

### Added
- Quartz-compatible cron parser (`ParseCron`) with seconds, `? L W #`, `L-n`, `LW`, year field, names and descriptors.
- Injectable `Clock` with a deterministic `FakeClock`.
- Triggers: cron, fixed interval, calendar interval, one-shot; serializable through TriggerSpec.
- Calendars: holiday, weekly, annual, daily window, cron.
- Job stores: MemoryStore and atomic JSON FileStore.
- Scheduler with misfire policies (fire-now, skip, reschedule, fire-all), restart recovery, pause/resume, non-overlapping runs, listeners, graceful Stop.
- Examples (quickstart, persistence, misfire, calendar, demo), docs/PITFALLS.md, benchmarks, fuzz test.
