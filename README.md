# quartzx

[![ci](https://github.com/JiaBao-do/quartzx/actions/workflows/ci.yml/badge.svg)](https://github.com/JiaBao-do/quartzx/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/JiaBao-do/quartzx.svg)](https://pkg.go.dev/github.com/JiaBao-do/quartzx)

A Quartz-style job scheduler for Go with a persistent job store, named **misfire policies**, **calendars** (holiday and
weekend exclusion) and **recovery of missed fires after a restart**. Standard library only, no CGO, Go 1.24+.

**Requires Go 1.24+.**

```sh
go get github.com/JiaBao-do/quartzx
```

## Quick start

This is `examples/quickstart/main.go` (a test keeps it in sync); it runs on a fake clock so the output is deterministic.

```go
// Quickstart: a cron job on a fake clock. Run: go run ./examples/quickstart
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/JiaBao-do/quartzx"
)

func main() {
	ctx := context.Background()
	clock := quartzx.NewFakeClock(time.Date(2026, 1, 1, 8, 59, 50, 0, time.UTC))
	s := quartzx.New(quartzx.WithClock(clock)) // use the default real clock in production
	ran := make(chan struct{})
	s.RegisterFunc("hello", func(_ context.Context, e quartzx.Execution) error {
		fmt.Println("hello at", e.ScheduledFor.Format(time.TimeOnly))
		close(ran)
		return nil
	})
	_ = s.Start(ctx)
	trigger, _ := quartzx.Cron("0 0 9 * * ?") // every day at 09:00:00
	_ = s.Schedule(ctx, quartzx.JobSpec{Key: "greeting", Handler: "hello", Trigger: trigger})
	clock.BlockUntilTimers(1)
	clock.Advance(10 * time.Second) // 09:00:00
	<-ran
	_ = s.Stop(ctx)
}
```

Output: `hello at 09:00:00`. In production leave out `WithClock` and the real clock is used.

## What quartzx adds, honestly

Scheduling libraries for Go already exist and two are good. [reugn/go-quartz](https://github.com/reugn/go-quartz)
(about 2000 stars) is also Quartz inspired, zero-dependency, and has a complete Quartz cron parser. [go-co-op/gocron](https://github.com/go-co-op/gocron)
(about 7000 stars) is the popular general purpose choice. quartzx overlaps with them on basic scheduling;
the difference is in the areas below. Compared on 2026-09-21 from their READMEs and source (grep of non-test files);
"no" means not found there, not proof of impossibility.

| | quartzx | go-quartz v0.15 | gocron v2 |
|---|---|---|---|
| Persistence | `Store` interface, in-memory and atomic JSON file store shipped | `JobQueue` interface; a file example lives in `examples/` | none (only elector/locker hooks for HA) |
| Restart recovery of missed fires | yes: state (next fire, counts, pause, job data) is reloaded and missed fires handled | you implement it inside your queue | no |
| Misfire handling | policies per job: fire-now, skip, reschedule, fire-all, with threshold | notification channel (`WithMisfiredChan`) and an outdated threshold, no policies | no |
| Calendars / holiday exclusion | holiday, weekly, annual, daily window, cron calendars, per job | no | no |
| Calendar-interval trigger (every N months, end-of-month clamp) | yes | no | monthly/weekly helpers |
| Cron dialect | Quartz (`? L W # L-n`, year) and classic 5 field, DST rules documented and table tested | Quartz | robfig/cron dialect |
| No overlap with itself | default, per job | `IsolatedJob` wrapper | singleton mode |
| Fake clock included for tests | yes (`FakeClock`) | no | via clockwork dependency |
| Dependencies | 0 | 0 | 3 non-test |

Choose **go-quartz** if you want a mature, smaller API and are fine writing your own persistence. Choose **gocron**
for a popular fluent API, in-process scheduling and its distributed locker/elector integrations (quartzx has no clustering).
Choose **quartzx** if you need jobs to survive restarts with defined catch-up behaviour, business calendars, or
deterministic tests with a fake clock. quartzx is v0.x and much younger than both.

## Features

- Triggers: `Cron`, `Every`, `EveryCalendar`, `Once`; start/end, time zones. All serializable (`TriggerSpec`).
- `Store` interface with `MemoryStore` and `FileStore` (one versioned JSON file, atomic rewrite).
- Misfire policies and a configurable threshold; recovery of missed fires at `Start`.
- Calendars: `NewHolidayCalendar`, `NewWeeklyCalendar`, `NewAnnualCalendar`, `NewDailyCalendar`, `NewCronCalendar`.
- Job data maps, optionally persisted after each run; listeners for every event; pause/resume, `RunNow`.
- Concurrency control: no self-overlap by default, `AllowConcurrent`, `WithMaxConcurrent`, per-job `Timeout`.
- Graceful shutdown through `Stop(ctx)`. Panics in jobs and listeners are contained.

## Examples

All examples use the fake clock, print deterministic output, and are verified by `go test ./examples/`
against `expected_output.txt`.

| | |
|---|---|
| `go run ./examples/quickstart` | cron job in under 30 lines |
| `go run ./examples/persistence` | file store, crash, restart, catch-up of missed hours |
| `go run ./examples/misfire` | the four policies side by side |
| `go run ./examples/calendar` | New York 09:00 job skipping weekends and holidays across DST |
| `go run ./examples/demo` | several jobs, pause/resume, persisted job data |

## Things to care about

The full list with wrong/right snippets is in [docs/PITFALLS.md](docs/PITFALLS.md). The ones that bite first:

- **DST**: nonexistent local times are skipped, repeated ones fire twice. Schedule in UTC or at safe hours.
- **Cron dialects**: 5 fields is classic (seconds fixed at 0, Sunday is 0), 6 or 7 fields is Quartz (Sunday is 1).
- **Misfire defaults**: a fire more than 5s late is a misfire; the default policy `fire-now` runs the job once and drops the rest.
- **Not exactly once**: a run in flight when the process dies is not replayed; missed fires are. Make jobs idempotent.
- **Overlap**: a fire that arrives while the job still runs is skipped (`EventSkipped`), not queued.
- **One scheduler per store file**: no cross-process locking, no clustering.
- **Job data is JSON**: numbers come back as `float64`.
- **Time zone data**: on Windows or minimal containers `import _ "time/tzdata"`.

## Portability

Standard library only, no CGO, no network, no telemetry, plain versioned JSON on disk. Uses Go 1.24 features only
(`go.mod` says `go 1.24`; CI runs 1.24 and stable on Linux, macOS and Windows).

## License

MIT
