// Package quartzx is a Quartz-style job scheduler for Go: cron, interval and
// calendar-interval triggers, a persistent job store, misfire policies,
// calendars for holiday exclusion, job data, listeners, pause and resume,
// non-overlapping runs, and recovery of missed fires after a restart.
//
// It uses only the standard library, needs Go 1.24 or newer, and takes an
// injectable [Clock] so schedules can be tested without sleeping.
//
// # Overview
//
// Create a [Scheduler] with [New], register named handlers with
// [Scheduler.Register], call [Scheduler.Start], then [Scheduler.Schedule]
// jobs described by a [JobSpec] and a [Trigger] ([Cron], [Every],
// [EveryCalendar], [Once]). Jobs are saved in a [Store] ([MemoryStore] or the
// atomic JSON [FileStore]); after a restart Start reloads them and applies each
// job's [MisfirePolicy] to fires missed while the process was down.
//
// # Concurrency
//
// [Scheduler], [Store] implementations, [Clock] implementations, [CronSchedule]
// and all [Calendar] implementations in this package are safe for concurrent
// use. Triggers are immutable. Listeners are called synchronously from
// scheduler goroutines and must be safe for concurrent use.
//
// # Time zones
//
// Cron and calendar-interval triggers evaluate wall clock time in their
// location. See [CronSchedule] for the exact daylight saving rules. Loading a
// zone by name needs zone data; on systems without it, import time/tzdata.
package quartzx
