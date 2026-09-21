// Misfire policies side by side. The same hourly job misses every fire from
// 09:00 to 13:00 (the process was down until 13:30). Each policy reacts
// differently. The clock is fake, so the output is deterministic.
//
// Run: go run ./examples/misfire
package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/JiaBao-do/quartzx"
)

func at(h, m int) time.Time { return time.Date(2026, 3, 2, h, m, 0, 0, time.UTC) }

func main() {
	ctx := context.Background()
	for _, policy := range []quartzx.MisfirePolicy{
		quartzx.MisfireFireNow, quartzx.MisfireSkip, quartzx.MisfireReschedule, quartzx.MisfireFireAll,
	} {
		store := quartzx.NewMemoryStore()

		// Before the outage: schedule an hourly interval job at 08:00.
		before := quartzx.New(quartzx.WithClock(quartzx.NewFakeClock(at(8, 0))), quartzx.WithStore(store))
		before.RegisterFunc("tick", func(context.Context, quartzx.Execution) error { return nil })
		every, _ := quartzx.Every(time.Hour)
		_ = before.Schedule(ctx, quartzx.JobSpec{Key: "tick", Handler: "tick", Trigger: every, Misfire: policy})

		// After the outage: a new scheduler starts at 13:30 on the same store.
		var mu sync.Mutex
		var runs []string
		misfired := make(chan struct{}, 1)
		after := quartzx.New(quartzx.WithClock(quartzx.NewFakeClock(at(13, 30))), quartzx.WithStore(store),
			quartzx.WithListener(quartzx.ListenerFunc(func(e quartzx.Event) {
				if e.Type == quartzx.EventMisfired {
					misfired <- struct{}{}
				}
			})))
		after.RegisterFunc("tick", func(_ context.Context, e quartzx.Execution) error {
			mu.Lock()
			runs = append(runs, e.ScheduledFor.Format("15:04"))
			mu.Unlock()
			return nil
		})
		_ = after.Start(ctx)
		<-misfired
		_ = after.Stop(ctx) // waits for the catch-up runs
		job, _ := after.Job("tick")

		mu.Lock()
		fmt.Printf("%-10s runs=[%s] next=%s\n", policy, strings.Join(runs, " "), job.NextFire.Format("15:04"))
		mu.Unlock()
	}
}
