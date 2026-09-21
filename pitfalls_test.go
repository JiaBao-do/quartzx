package quartzx

import (
	"context"
	"testing"
	"time"
)

// TestPitfallInFlightRunIsNotRetried backs docs/PITFALLS.md: the record is
// advanced when a fire is dispatched, before the job finishes, so a crash while
// a job runs does not replay that run after restart.
func TestPitfallInFlightRunIsNotRetried(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	h := newHarness(t, t0, WithStore(store))
	started := make(chan struct{})
	release := make(chan struct{})
	h.s.RegisterFunc("h", func(context.Context, Execution) error { close(started); <-release; return nil })
	h.start()
	tr := mustTrig(t)(Cron("0 * * * * ?"))
	if err := h.s.Schedule(context.Background(), JobSpec{Key: "j", Handler: "h", Trigger: tr}); err != nil {
		t.Fatal(err)
	}
	h.advance(time.Minute)
	<-started
	// the job is still running; this is what a restart would find on disk
	recs, _ := store.List(context.Background())
	if want := t0.Add(2 * time.Minute); !recs[0].NextFire.Equal(want) || recs[0].FireCount != 1 {
		t.Fatalf("store = %+v, want NextFire %v and FireCount 1", recs[0], want)
	}
	close(release)
}

// TestPitfallSecondsFieldDialects backs the dialect table in docs/PITFALLS.md.
func TestPitfallSecondsFieldDialects(t *testing.T) {
	t.Parallel()
	after := mustTime(t, "2026-01-01T00:00:00Z")
	five, _ := ParseCron("* * * * *")  // classic: every minute, at second 0
	six, _ := ParseCron("* * * * * ?") // Quartz: every second
	n5, _ := five.Next(after)
	n6, _ := six.Next(after)
	if n5.Sub(after) != time.Minute || n6.Sub(after) != time.Second {
		t.Fatalf("five field = %v, six field = %v", n5.Sub(after), n6.Sub(after))
	}
	// day-of-week numbering differs: 1 is Sunday in Quartz, Monday in classic
	q, _ := ParseCron("0 0 9 ? * 1")
	c, _ := ParseCron("0 9 * * 1")
	nq, _ := q.Next(after)
	nc, _ := c.Next(after)
	if nq.Weekday() != time.Sunday || nc.Weekday() != time.Monday {
		t.Fatalf("quartz dow 1 = %v, classic dow 1 = %v", nq.Weekday(), nc.Weekday())
	}
}
