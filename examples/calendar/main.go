// Calendars: a 09:00 New York job that skips weekends and holidays,
// and keeps 09:00 local time across the daylight saving change. The clock is
// fake, so the output is deterministic.
//
// Run: go run ./examples/calendar
package main

import (
	"context"
	"fmt"
	"time"
	_ "time/tzdata" // zone data on systems without it (Windows, minimal containers)

	"github.com/JiaBao-do/quartzx"
)

func main() {
	ctx := context.Background()
	ny, _ := time.LoadLocation("America/New_York")

	holidays := quartzx.NewHolidayCalendar(ny,
		time.Date(2026, 11, 2, 0, 0, 0, 0, ny), // company day off
		time.Date(2026, 11, 4, 0, 0, 0, 0, ny), // public holiday
	)
	weekends := quartzx.NewWeeklyCalendar(ny, time.Saturday, time.Sunday)

	clock := quartzx.NewFakeClock(time.Date(2026, 10, 29, 12, 0, 0, 0, ny)) // Thursday
	ran := make(chan time.Time, 1)
	s := quartzx.New(quartzx.WithClock(clock),
		quartzx.WithCalendar("weekends", weekends), quartzx.WithCalendar("holidays", holidays))
	s.RegisterFunc("open", func(_ context.Context, e quartzx.Execution) error { ran <- e.ScheduledFor; return nil })
	_ = s.Start(ctx)

	trigger, _ := quartzx.Cron("0 0 9 * * ?", quartzx.WithLocation(ny))
	_ = s.Schedule(ctx, quartzx.JobSpec{Key: "open", Handler: "open", Trigger: trigger,
		Calendars: []string{"weekends", "holidays"}})

	for range 8 {
		job, _ := s.Job("open")
		clock.BlockUntilTimers(1)
		clock.Set(job.NextFire)
		fmt.Println(job.NextFire.In(ny).Format("Mon 2006-01-02 15:04 MST"), "|", (<-ran).UTC().Format("15:04Z"))
	}
	_ = s.Stop(ctx)
}
