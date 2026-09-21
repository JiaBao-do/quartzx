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
