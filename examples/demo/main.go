// A realistic multi-job app on a fake clock: an interval heartbeat, a nightly
// cron report that keeps a counter in its job data, and a monthly billing job
// (calendar interval, end-of-month clamping). One job is paused for two days;
// on resume the missed fires are handled by its misfire policy. A JSON file
// store makes it restart-safe. Output is deterministic.
//
// Run: go run ./examples/demo
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/JiaBao-do/quartzx"
)

func day(m time.Month, d, h int) time.Time { return time.Date(2026, m, d, h, 0, 0, 0, time.UTC) }

func main() {
	ctx := context.Background()
	dir, _ := os.MkdirTemp("", "quartzx-demo")
	defer os.RemoveAll(dir)
	store, _ := quartzx.NewFileStore(filepath.Join(dir, "jobs.json"))

	clock := quartzx.NewFakeClock(day(time.January, 30, 0))
	var (
		mu      sync.Mutex
		billing []string
		done    = make(chan quartzx.Event, 64) // completed / skipped / misfired
	)
	s := quartzx.New(quartzx.WithClock(clock), quartzx.WithStore(store),
		quartzx.WithListener(quartzx.ListenerFunc(func(e quartzx.Event) {
			switch e.Type {
			case quartzx.EventCompleted, quartzx.EventSkipped, quartzx.EventFailed, quartzx.EventMisfired:
				done <- e
			}
		})))
	s.RegisterFunc("heartbeat", func(context.Context, quartzx.Execution) error { return nil })
	s.RegisterFunc("report", func(_ context.Context, e quartzx.Execution) error {
		n, _ := e.Data["sent"].(float64)
		e.Data["sent"] = n + 1
		return nil
	})
	s.RegisterFunc("billing", func(_ context.Context, e quartzx.Execution) error {
		mu.Lock()
		billing = append(billing, e.ScheduledFor.Format("2006-01-02"))
		mu.Unlock()
		return nil
	})
	_ = s.Start(ctx)

	every12h, _ := quartzx.Every(12 * time.Hour)
	nightly, _ := quartzx.Cron("0 0 2 * * ?")
	monthly, _ := quartzx.EveryCalendar(1, quartzx.Month, quartzx.WithStart(day(time.January, 31, 9)))
	must := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	must(s.Schedule(ctx, quartzx.JobSpec{Key: "heartbeat", Handler: "heartbeat", Trigger: every12h, Misfire: quartzx.MisfireSkip}))
	must(s.Schedule(ctx, quartzx.JobSpec{Key: "report", Handler: "report", Trigger: nightly,
		Data: map[string]any{"sent": 0}, PersistData: true}))
	must(s.Schedule(ctx, quartzx.JobSpec{Key: "billing", Handler: "billing", Trigger: monthly}))

	// run steps the fake clock exactly to each next fire time, waiting for
	// every job due at that instant to finish, until the given time.
	run := func(until time.Time) {
		for {
			var next time.Time
			for _, j := range s.Jobs() {
				if !j.Paused && (next.IsZero() || j.NextFire.Before(next)) {
					next = j.NextFire
				}
			}
			if next.After(until) {
				return
			}
			due := 0
			for _, j := range s.Jobs() {
				if !j.Paused && j.NextFire.Equal(next) {
					due++
				}
			}
			clock.BlockUntilTimers(1)
			clock.Set(next)
			for range due {
				<-done
			}
		}
	}

	run(day(time.February, 10, 0))
	must(s.Pause(ctx, "heartbeat"))
	fmt.Println("paused heartbeat at", clock.Now().Format("Jan 2 15:04"))
	run(day(time.February, 12, 0))
	clock.BlockUntilTimers(1)
	clock.Set(day(time.February, 12, 1))
	must(s.Resume(ctx, "heartbeat"))
	e := <-done
	fmt.Printf("resumed: %s misfired, %d missed since %s, policy %s\n", e.Key, e.Missed, e.ScheduledFor.Format("Jan 2 15:04"), e.Policy)
	run(day(time.March, 1, 0))
	_ = s.Stop(ctx)

	fmt.Println("billing dates:", billing)
	for _, j := range s.Jobs() {
		fmt.Printf("%-9s fires=%-3d next=%s\n", j.Key, j.FireCount, j.NextFire.Format("Jan 2 15:04"))
	}
	report, _ := s.Job("report")
	fmt.Println("reports sent (persisted job data):", report.Data["sent"])
}
