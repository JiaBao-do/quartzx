package quartzx_test

import (
	"context"
	"fmt"
	"time"

	"github.com/JiaBao-do/quartzx"
)

func ExampleParseCron() {
	c, _ := quartzx.ParseCron("0 0 9 ? * MON-FRI")                  // 09:00 on weekdays
	next, _ := c.Next(time.Date(2026, 9, 4, 10, 0, 0, 0, time.UTC)) // a Friday
	fmt.Println(next.Format("Mon 2006-01-02 15:04"))
	// Output: Mon 2026-09-07 09:00
}

func ExampleCronSchedule_Next_lastFriday() {
	c, _ := quartzx.ParseCron("0 0 17 ? * FRIL") // last Friday of the month, 17:00
	t := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	for range 2 {
		t, _ = c.Next(t)
		fmt.Println(t.Format("2006-01-02 15:04"))
	}
	// Output:
	// 2026-09-25 17:00
	// 2026-10-30 17:00
}

func ExampleEveryCalendar() {
	start := time.Date(2026, 1, 31, 9, 0, 0, 0, time.UTC)
	t, _ := quartzx.EveryCalendar(1, quartzx.Month, quartzx.WithStart(start))
	next := start
	for range 3 {
		next, _ = t.Next(next)
		fmt.Println(next.Format("2006-01-02"))
	}
	// Output:
	// 2026-02-28
	// 2026-03-31
	// 2026-04-30
}

func ExampleNewHolidayCalendar() {
	cal := quartzx.NewHolidayCalendar(time.UTC, time.Date(2026, 12, 25, 0, 0, 0, 0, time.UTC))
	fmt.Println(cal.Included(time.Date(2026, 12, 25, 9, 0, 0, 0, time.UTC)))
	fmt.Println(cal.Included(time.Date(2026, 12, 26, 9, 0, 0, 0, time.UTC)))
	// Output:
	// false
	// true
}

func ExampleScheduler() {
	ctx := context.Background()
	clock := quartzx.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	s := quartzx.New(quartzx.WithClock(clock))
	ran := make(chan struct{})
	s.RegisterFunc("job", func(_ context.Context, e quartzx.Execution) error {
		fmt.Println("ran", e.ScheduledFor.Format(time.TimeOnly))
		close(ran)
		return nil
	})
	_ = s.Start(ctx)
	every, _ := quartzx.Every(time.Minute)
	_ = s.Schedule(ctx, quartzx.JobSpec{Key: "k", Handler: "job", Trigger: every})
	clock.BlockUntilTimers(1)
	clock.Advance(time.Minute)
	<-ran
	_ = s.Stop(ctx)
	// Output: ran 00:01:00
}
