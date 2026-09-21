# Changelog

All notable changes are documented here. Format: [Keep a Changelog](https://keepachangelog.com/), versioning: [SemVer](https://semver.org/).

## [Unreleased]

### Added
- Quartz-compatible cron parser (`ParseCron`) with seconds, `? L W #`, `L-n`, `LW`, year field, names and descriptors.
- Injectable `Clock` with a deterministic `FakeClock`.
- Triggers: cron, fixed interval, calendar interval, one-shot; serializable through TriggerSpec.
- Calendars: holiday, weekly, annual, daily window, cron.
- Job stores: MemoryStore and atomic JSON FileStore.
- Scheduler with misfire policies (fire-now, skip, reschedule, fire-all), restart recovery, pause/resume, non-overlapping runs, listeners, graceful Stop.
- Examples (quickstart, persistence, misfire, calendar, demo), docs/PITFALLS.md, benchmarks, fuzz test.
