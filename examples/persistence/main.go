// Persistence and restart recovery. A scheduler saves its jobs to a JSON file,
// "crashes", and a second scheduler started from the same file catches up on
// the fires it missed while it was down. The clock is fake, so the output is
// deterministic.
//
// Run: go run ./examples/persistence
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/JiaBao-do/quartzx"
)

func at(h, m int) time.Time { return time.Date(2026, 3, 2, h, m, 0, 0, time.UTC) }

func report(name string, out chan<- string) func(context.Context, quartzx.Execution) error {
	return func(_ context.Context, e quartzx.Execution) error {
		n, _ := e.Data["reports"].(float64) // JSON numbers come back as float64
		e.Data["reports"] = n + 1           // PersistData saves this after a successful run
		out <- fmt.Sprintf("%s: run #%d for %s misfired=%v reports=%d",
			name, e.Count, e.ScheduledFor.Format("15:04"), e.Misfired, int(n)+1)
		return nil
	}
}

func main() {
	ctx := context.Background()
	dir, _ := os.MkdirTemp("", "quartzx-demo")
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "jobs.json")

	// Process 1: schedule an hourly job and let it run twice.
	store, _ := quartzx.NewFileStore(path)
	clock := quartzx.NewFakeClock(at(8, 30))
	out := make(chan string, 16)
	completed := make(chan struct{}, 4)
	s := quartzx.New(quartzx.WithClock(clock), quartzx.WithStore(store),
		quartzx.WithListener(quartzx.ListenerFunc(func(e quartzx.Event) {
			if e.Type == quartzx.EventCompleted {
				completed <- struct{}{}
			}
		})))
	s.RegisterFunc("report", report("process 1", out))
	_ = s.Start(ctx)
	trigger, _ := quartzx.Cron("0 0 * * * ?")
	_ = s.Schedule(ctx, quartzx.JobSpec{
		Key: "hourly-report", Handler: "report", Trigger: trigger,
		Data: map[string]any{"reports": 0}, PersistData: true,
		Misfire: quartzx.MisfireFireAll, // replay every missed hour after a restart
	})
	for _, to := range []time.Time{at(9, 0), at(10, 0)} { // advance exactly to each fire
		clock.BlockUntilTimers(1)
		clock.Set(to)
		fmt.Println(<-out)
		<-completed // a run still executing would make the next fire an overlap
	}
	_ = s.Stop(ctx) // the process "crashes" at 10:30 with 11:00 due next

	// Process 2: starts at 13:30 from the same file.
	store, _ = quartzx.NewFileStore(path)
	clock = quartzx.NewFakeClock(at(13, 30))
	s = quartzx.New(quartzx.WithClock(clock), quartzx.WithStore(store),
		quartzx.WithListener(quartzx.ListenerFunc(func(e quartzx.Event) {
			if e.Type == quartzx.EventMisfired {
				out <- fmt.Sprintf("misfire: %d fires missed since %s, policy %s", e.Missed, e.ScheduledFor.Format("15:04"), e.Policy)
			}
		})))
	s.RegisterFunc("report", report("process 2", out))
	_ = s.Start(ctx) // loads hourly-report from the file
	for range 4 {
		fmt.Println(<-out)
	}
	_ = s.Stop(ctx)
	job, _ := s.Job("hourly-report")
	fmt.Println("next fire:", job.NextFire.Format("15:04"), "total fires:", job.FireCount)
}
