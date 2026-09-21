package quartzx

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// harness bundles a scheduler, a fake clock and an event channel.
type harness struct {
	t      *testing.T
	fc     *FakeClock
	s      *Scheduler
	events chan Event
}

func newHarness(t *testing.T, start time.Time, opts ...Option) *harness {
	t.Helper()
	h := &harness{t: t, fc: NewFakeClock(start), events: make(chan Event, 4096)}
	all := append([]Option{
		WithClock(h.fc),
		WithListener(ListenerFunc(func(e Event) { h.events <- e })),
	}, opts...)
	h.s = New(all...)
	return h
}

func (h *harness) start() {
	h.t.Helper()
	if err := h.s.Start(context.Background()); err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = h.s.Stop(context.Background()) })
}

// next returns the next event matching typ (and key when non-empty).
func (h *harness) next(typ EventType, key string) Event {
	h.t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case e := <-h.events:
			if e.Type == typ && (key == "" || e.Key == key) {
				return e
			}
		case <-timeout:
			h.t.Fatalf("timed out waiting for %s %s", typ, key)
			return Event{}
		}
	}
}

// drain returns all events currently queued.
func (h *harness) drain() []Event {
	var out []Event
	for {
		select {
		case e := <-h.events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func (h *harness) advance(d time.Duration) {
	h.t.Helper()
	h.fc.BlockUntilTimers(1)
	h.fc.Advance(d)
}

func noop(context.Context, Execution) error { return nil }

func mustTrig(t *testing.T) func(Trigger, error) Trigger {
	return func(tr Trigger, err error) Trigger {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return tr
	}
}

func TestCronJobFiresOnFakeClock(t *testing.T) {
	t.Parallel()
	h := newHarness(t, t0)
	runs := make(chan Execution, 10)
	h.s.RegisterFunc("h", func(_ context.Context, e Execution) error { runs <- e; return nil })
	h.start()
	tr := mustTrig(t)(Cron("*/10 * * * * ?"))
	if err := h.s.Schedule(context.Background(), JobSpec{Key: "j", Handler: "h", Trigger: tr, Data: map[string]any{"a": "b"}}); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		h.advance(10 * time.Second)
		e := <-runs
		if want := t0.Add(time.Duration(i) * 10 * time.Second); !e.ScheduledFor.Equal(want) {
			t.Fatalf("run %d scheduled for %v want %v", i, e.ScheduledFor, want)
		}
		if e.Count != int64(i) || e.Misfired || e.Key != "j" || e.Handler != "h" || e.Data["a"] != "b" {
			t.Fatalf("bad execution %+v", e)
		}
		h.next(EventCompleted, "j")
	}
	rec, err := h.s.Job("j")
	if err != nil {
		t.Fatal(err)
	}
	if rec.FireCount != 3 || !rec.LastFire.Equal(t0.Add(30*time.Second)) || !rec.NextFire.Equal(t0.Add(40*time.Second)) {
		t.Fatalf("bad record %+v", rec)
	}
}

func TestIntervalJobAnchorsAfterOnePeriod(t *testing.T) {
	t.Parallel()
	h := newHarness(t, t0)
	h.s.RegisterFunc("h", noop)
	tr := mustTrig(t)(Every(time.Minute))
	if err := h.s.Schedule(context.Background(), JobSpec{Key: "j", Handler: "h", Trigger: tr}); err != nil {
		t.Fatal(err)
	}
	rec, _ := h.s.Job("j")
	if !rec.NextFire.Equal(t0.Add(time.Minute)) || !rec.Trigger.Start.Equal(t0.Add(time.Minute)) {
		t.Fatalf("bad anchor %+v", rec)
	}
}

func TestOverlapIsSkippedByDefault(t *testing.T) {
	t.Parallel()
	h := newHarness(t, t0)
	release := make(chan struct{})
	started := make(chan struct{}, 10)
	var running, peak atomic.Int32
	h.s.RegisterFunc("slow", func(_ context.Context, _ Execution) error {
		n := running.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		started <- struct{}{}
		<-release
		running.Add(-1)
		return nil
	})
	h.start()
	tr := mustTrig(t)(Cron("*/10 * * * * ?"))
	if err := h.s.Schedule(context.Background(), JobSpec{Key: "j", Handler: "slow", Trigger: tr}); err != nil {
		t.Fatal(err)
	}
	h.advance(10 * time.Second)
	<-started
	h.advance(10 * time.Second) // job still running: this fire is skipped
	ev := h.next(EventSkipped, "j")
	if !ev.ScheduledFor.Equal(t0.Add(20 * time.Second)) {
		t.Fatalf("skipped %v", ev.ScheduledFor)
	}
	close(release)
	h.next(EventCompleted, "j")
	if peak.Load() != 1 {
		t.Fatalf("peak concurrency %d", peak.Load())
	}
}

func TestAllowConcurrent(t *testing.T) {
	t.Parallel()
	h := newHarness(t, t0)
	release := make(chan struct{})
	started := make(chan struct{}, 10)
	h.s.RegisterFunc("slow", func(context.Context, Execution) error { started <- struct{}{}; <-release; return nil })
	h.start()
	tr := mustTrig(t)(Cron("*/10 * * * * ?"))
	if err := h.s.Schedule(context.Background(), JobSpec{Key: "j", Handler: "slow", Trigger: tr, AllowConcurrent: true}); err != nil {
		t.Fatal(err)
	}
	h.advance(10 * time.Second)
	<-started
	h.advance(10 * time.Second)
	<-started // second run started while the first is still blocked
	close(release)
}

// restartSetup schedules a 1 minute interval job in one scheduler, stops it,
// and returns the store, ready for a second scheduler to "restart" from it.
func restartSetup(t *testing.T, store Store, policy MisfirePolicy) {
	t.Helper()
	fc := NewFakeClock(t0)
	s := New(WithClock(fc), WithStore(store))
	s.RegisterFunc("h", noop)
	tr := mustTrig(t)(Every(time.Minute))
	if err := s.Schedule(context.Background(), JobSpec{Key: "j", Handler: "h", Trigger: tr, Misfire: policy}); err != nil {
		t.Fatal(err)
	}
}

func TestMisfirePoliciesAfterRestart(t *testing.T) {
	t.Parallel()
	// Job anchored at t0+1m, every minute. Process comes back at t0+10m30s:
	// ten fires (1m..10m) were missed.
	back := t0.Add(10*time.Minute + 30*time.Second)
	cases := []struct {
		name     string
		policy   MisfirePolicy
		opts     []Option
		wantRuns []time.Duration // ScheduledFor offsets from t0
		wantNext time.Duration
	}{
		{"default is fire-now", "", nil, []time.Duration{time.Minute}, 11 * time.Minute},
		{"fire-now", MisfireFireNow, nil, []time.Duration{time.Minute}, 11 * time.Minute},
		{"skip", MisfireSkip, nil, nil, 11 * time.Minute},
		{"reschedule", MisfireReschedule, nil, []time.Duration{time.Minute}, 11*time.Minute + 30*time.Second},
		{"fire-all", MisfireFireAll, nil, []time.Duration{
			1 * time.Minute, 2 * time.Minute, 3 * time.Minute, 4 * time.Minute, 5 * time.Minute,
			6 * time.Minute, 7 * time.Minute, 8 * time.Minute, 9 * time.Minute, 10 * time.Minute}, 11 * time.Minute},
		{"fire-all capped", MisfireFireAll, []Option{WithMaxCatchUp(3)},
			[]time.Duration{time.Minute, 2 * time.Minute, 3 * time.Minute}, 11 * time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := NewMemoryStore()
			restartSetup(t, store, tc.policy)

			h := newHarness(t, back, append([]Option{WithStore(store)}, tc.opts...)...)
			var mu sync.Mutex
			var got []Execution
			h.s.RegisterFunc("h", func(_ context.Context, e Execution) error {
				mu.Lock()
				got = append(got, e)
				mu.Unlock()
				return nil
			})
			h.start()
			mis := h.next(EventMisfired, "j")
			if mis.Policy == "" || mis.Missed < 1 {
				t.Fatalf("bad misfire event %+v", mis)
			}
			for range tc.wantRuns {
				h.next(EventCompleted, "j")
			}
			mu.Lock()
			defer mu.Unlock()
			if len(got) != len(tc.wantRuns) {
				t.Fatalf("runs = %d want %d", len(got), len(tc.wantRuns))
			}
			for i, w := range tc.wantRuns {
				if !got[i].ScheduledFor.Equal(t0.Add(w)) || !got[i].Misfired {
					t.Errorf("run %d = %+v want scheduled %v misfired", i, got[i], t0.Add(w))
				}
			}
			rec, _ := h.s.Job("j")
			if !rec.NextFire.Equal(t0.Add(tc.wantNext)) {
				t.Errorf("next = %v want %v", rec.NextFire, t0.Add(tc.wantNext))
			}
			// no extra runs were dispatched
			if extra := h.drain(); countType(extra, EventFired) != 0 {
				t.Errorf("unexpected extra fires: %v", extra)
			}
		})
	}
}

func countType(evs []Event, typ EventType) int {
	n := 0
	for _, e := range evs {
		if e.Type == typ {
			n++
		}
	}
	return n
}

func TestNoMisfireWithinThreshold(t *testing.T) {
	t.Parallel()
	h := newHarness(t, t0)
	h.s.RegisterFunc("h", noop)
	h.start()
	tr := mustTrig(t)(Cron("*/10 * * * * ?"))
	if err := h.s.Schedule(context.Background(), JobSpec{Key: "j", Handler: "h", Trigger: tr, Misfire: MisfireSkip}); err != nil {
		t.Fatal(err)
	}
	h.advance(14 * time.Second) // 4s late, threshold is 5s
	e := h.next(EventFired, "j")
	if !e.ScheduledFor.Equal(t0.Add(10 * time.Second)) {
		t.Fatalf("scheduled %v", e.ScheduledFor)
	}
	for _, ev := range h.drain() {
		if ev.Type == EventMisfired {
			t.Fatal("late but within threshold must not misfire")
		}
	}
}

func TestCustomThreshold(t *testing.T) {
	t.Parallel()
	h := newHarness(t, t0, WithMisfireThreshold(time.Second))
	h.s.RegisterFunc("h", noop)
	h.start()
	tr := mustTrig(t)(Cron("*/10 * * * * ?"))
	if err := h.s.Schedule(context.Background(), JobSpec{Key: "j", Handler: "h", Trigger: tr, Misfire: MisfireSkip}); err != nil {
		t.Fatal(err)
	}
	h.advance(13 * time.Second)
	if e := h.next(EventMisfired, "j"); e.Policy != MisfireSkip {
		t.Fatalf("%+v", e)
	}
}

func TestFileStoreRestartRecoveryAndPersistData(t *testing.T) {
	t.Parallel()
	path := t.TempDir() + "/jobs.json"
	ctx := context.Background()

	fs1, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	h1 := newHarness(t, t0, WithStore(fs1))
	h1.s.RegisterFunc("count", func(_ context.Context, e Execution) error {
		n, _ := e.Data["n"].(float64) // JSON numbers come back as float64
		e.Data["n"] = n + 1
		return nil
	})
	if err := h1.s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	tr := mustTrig(t)(Cron("0 * * * * ?")) // every minute
	if err := h1.s.Schedule(ctx, JobSpec{Key: "j", Handler: "count", Trigger: tr,
		Data: map[string]any{"n": 0}, PersistData: true, Misfire: MisfireFireAll}); err != nil {
		t.Fatal(err)
	}
	h1.advance(time.Minute)
	h1.next(EventCompleted, "j")
	h1.advance(time.Minute)
	h1.next(EventCompleted, "j")
	if err := h1.s.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	// "restart" 5 minutes later from the same file
	fs2, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	h2 := newHarness(t, t0.Add(2*time.Minute+5*time.Minute), WithStore(fs2))
	h2.s.RegisterFunc("count", func(_ context.Context, e Execution) error {
		n, _ := e.Data["n"].(float64)
		e.Data["n"] = n + 1
		return nil
	})
	h2.start()
	if err := h2.s.Schedule(ctx, JobSpec{Key: "j", Handler: "count", Trigger: tr}); !errors.Is(err, ErrExists) {
		t.Fatalf("recovered job must exist, got %v", err)
	}
	// missed: minute 3,4,5,6,7 (fire-all).
	for range 5 {
		h2.next(EventCompleted, "j")
	}
	rec, _ := h2.s.Job("j")
	if rec.Data["n"] != float64(7) || rec.FireCount != 7 {
		t.Fatalf("state not carried across restart: %+v", rec)
	}
}

func TestCalendarSkipsExcludedDays(t *testing.T) {
	t.Parallel()
	// 2026-09-18 is a Friday.
	start := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	hol := NewHolidayCalendar(nil, time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)) // Monday holiday
	h := newHarness(t, start,
		WithCalendar("weekends", NewWeeklyCalendar(nil, time.Saturday, time.Sunday)),
		WithCalendar("holidays", hol))
	h.s.RegisterFunc("h", noop)
	h.start()
	tr := mustTrig(t)(Cron("0 0 9 * * ?"))
	if err := h.s.Schedule(context.Background(), JobSpec{Key: "j", Handler: "h", Trigger: tr,
		Calendars: []string{"weekends", "holidays"}}); err != nil {
		t.Fatal(err)
	}
	// Sat 19th 09:00 skipped, Sun 20th skipped, Mon 21st holiday skipped -> Tue 22nd.
	rec, _ := h.s.Job("j")
	if want := time.Date(2026, 9, 19+0, 9, 0, 0, 0, time.UTC).AddDate(0, 0, 3); !rec.NextFire.Equal(want) {
		t.Fatalf("next = %v want %v", rec.NextFire, want)
	}
	h.advance(3*24*time.Hour + 21*time.Hour)
	e := h.next(EventFired, "j")
	if e.ScheduledFor.Weekday() != time.Tuesday {
		t.Fatalf("fired for %v", e.ScheduledFor)
	}
}

func TestCalendarMissingIsRejected(t *testing.T) {
	t.Parallel()
	h := newHarness(t, t0)
	h.s.RegisterFunc("h", noop)
	tr := mustTrig(t)(Cron("* * * * * ?"))
	err := h.s.Schedule(context.Background(), JobSpec{Key: "j", Handler: "h", Trigger: tr, Calendars: []string{"nope"}})
	if !errors.Is(err, ErrNoCalendar) {
		t.Fatalf("err = %v", err)
	}
}

func TestPauseAndResume(t *testing.T) {
	t.Parallel()
	h := newHarness(t, t0)
	h.s.RegisterFunc("h", noop)
	h.start()
	ctx := context.Background()
	tr := mustTrig(t)(Cron("*/10 * * * * ?"))
	if err := h.s.Schedule(ctx, JobSpec{Key: "j", Handler: "h", Trigger: tr, Misfire: MisfireSkip}); err != nil {
		t.Fatal(err)
	}
	h.fc.BlockUntilTimers(1)
	if err := h.s.Pause(ctx, "j"); err != nil {
		t.Fatal(err)
	}
	h.fc.Advance(time.Minute)
	if err := h.s.Resume(ctx, "j"); err != nil {
		t.Fatal(err)
	}
	// on resume the paused-through fire is a misfire, handled by policy skip
	ev := h.next(EventMisfired, "j")
	if ev.Policy != MisfireSkip {
		t.Fatalf("%+v", ev)
	}
	if n := countType(h.drain(), EventFired); n != 0 {
		t.Fatalf("paused job fired %d times", n)
	}
	rec, _ := h.s.Job("j")
	if rec.Paused || !rec.NextFire.Equal(t0.Add(70*time.Second)) {
		t.Fatalf("%+v", rec)
	}
	if err := h.s.Pause(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
	if err := h.s.Resume(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestPausedStateIsPersisted(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	h := newHarness(t, t0, WithStore(store))
	h.s.RegisterFunc("h", noop)
	tr := mustTrig(t)(Cron("*/10 * * * * ?"))
	ctx := context.Background()
	if err := h.s.Schedule(ctx, JobSpec{Key: "j", Handler: "h", Trigger: tr}); err != nil {
		t.Fatal(err)
	}
	if err := h.s.Pause(ctx, "j"); err != nil {
		t.Fatal(err)
	}
	h2 := newHarness(t, t0, WithStore(store))
	h2.start()
	rec, err := h2.s.Job("j")
	if err != nil || !rec.Paused {
		t.Fatalf("%+v %v", rec, err)
	}
}

func TestOnceJobIsRemovedWhenExhausted(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	h := newHarness(t, t0, WithStore(store))
	h.s.RegisterFunc("h", noop)
	h.start()
	tr := mustTrig(t)(Once(t0.Add(time.Minute)))
	if err := h.s.Schedule(context.Background(), JobSpec{Key: "j", Handler: "h", Trigger: tr}); err != nil {
		t.Fatal(err)
	}
	h.advance(time.Minute)
	h.next(EventCompleted, "j")
	h.next(EventExhausted, "j")
	if _, err := h.s.Job("j"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
	recs, _ := store.List(context.Background())
	if len(recs) != 0 {
		t.Fatalf("store still has %v", recs)
	}
}

func TestOnceInThePastFiresImmediatelyOrIsSkipped(t *testing.T) {
	t.Parallel()
	for _, policy := range []MisfirePolicy{MisfireFireNow, MisfireSkip} {
		h := newHarness(t, t0)
		h.s.RegisterFunc("h", noop)
		h.start()
		tr := mustTrig(t)(Once(t0.Add(-time.Hour)))
		if err := h.s.Schedule(context.Background(), JobSpec{Key: "j", Handler: "h", Trigger: tr, Misfire: policy}); err != nil {
			t.Fatal(err)
		}
		h.next(EventMisfired, "j")
		if policy == MisfireFireNow {
			h.next(EventCompleted, "j")
		}
		h.next(EventExhausted, "j")
	}
}

func TestScheduleValidationAndErrors(t *testing.T) {
	t.Parallel()
	h := newHarness(t, t0)
	h.s.RegisterFunc("h", noop)
	ctx := context.Background()
	tr := mustTrig(t)(Cron("* * * * * ?"))
	short := mustTrig(t)(Every(time.Microsecond))
	cases := []struct {
		name string
		spec JobSpec
		want error
	}{
		{"empty key", JobSpec{Handler: "h", Trigger: tr}, ErrInvalidJob},
		{"empty handler", JobSpec{Key: "k", Trigger: tr}, ErrInvalidJob},
		{"nil trigger", JobSpec{Key: "k", Handler: "h"}, ErrInvalidJob},
		{"bad policy", JobSpec{Key: "k", Handler: "h", Trigger: tr, Misfire: "wat"}, ErrInvalidJob},
		{"negative timeout", JobSpec{Key: "k", Handler: "h", Trigger: tr, Timeout: -1}, ErrInvalidJob},
		{"unknown handler", JobSpec{Key: "k", Handler: "zzz", Trigger: tr}, ErrNoHandler},
		{"tiny interval", JobSpec{Key: "k", Handler: "h", Trigger: short}, ErrInvalidTrigger},
	}
	for _, c := range cases {
		if err := h.s.Schedule(ctx, c.spec); !errors.Is(err, c.want) {
			t.Errorf("%s: err = %v want %v", c.name, err, c.want)
		}
	}
	// a cron that can never fire
	never := mustTrig(t)(Cron("0 0 0 31 2 ?"))
	if err := h.s.Schedule(ctx, JobSpec{Key: "k", Handler: "h", Trigger: never}); !errors.Is(err, ErrInvalidTrigger) {
		t.Errorf("never: err = %v", err)
	}

	spec := JobSpec{Key: "k", Handler: "h", Trigger: tr}
	if err := h.s.Schedule(ctx, spec); err != nil {
		t.Fatal(err)
	}
	if err := h.s.Schedule(ctx, spec); !errors.Is(err, ErrExists) {
		t.Errorf("dup: err = %v", err)
	}
	spec.Data = map[string]any{"v": 2}
	if err := h.s.Replace(ctx, spec); err != nil {
		t.Errorf("Replace: %v", err)
	}
	if rec, _ := h.s.Job("k"); rec.Data["v"] != 2 {
		t.Errorf("Replace did not take effect: %+v", rec)
	}
	if got := len(h.s.Jobs()); got != 1 {
		t.Errorf("Jobs = %d", got)
	}
	if err := h.s.Unschedule(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if err := h.s.Unschedule(ctx, "k"); !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v", err)
	}
	if _, err := h.s.Job("k"); !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v", err)
	}
}

func TestScheduleDoesNotAliasCallerData(t *testing.T) {
	t.Parallel()
	h := newHarness(t, t0)
	h.s.RegisterFunc("h", noop)
	tr := mustTrig(t)(Cron("* * * * * ?"))
	data := map[string]any{"k": "v"}
	if err := h.s.Schedule(context.Background(), JobSpec{Key: "j", Handler: "h", Trigger: tr, Data: data}); err != nil {
		t.Fatal(err)
	}
	data["k"] = "changed"
	rec, _ := h.s.Job("j")
	if rec.Data["k"] != "v" {
		t.Fatalf("scheduler shares caller map: %v", rec.Data)
	}
	rec.Data["k"] = "mutated"
	rec2, _ := h.s.Job("j")
	if rec2.Data["k"] != "v" {
		t.Fatal("Job returned an aliased map")
	}
}

func TestStartStop(t *testing.T) {
	t.Parallel()
	h := newHarness(t, t0)
	ctx := context.Background()
	if err := h.s.Stop(ctx); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("err = %v", err)
	}
	if err := h.s.RunNow(ctx, "x"); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("err = %v", err)
	}
	if err := h.s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.s.Start(ctx); !errors.Is(err, ErrStarted) {
		t.Fatalf("err = %v", err)
	}
	if err := h.s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.s.Start(ctx); err != nil { // restartable
		t.Fatal(err)
	}
	if err := h.s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestGracefulStopWaitsForRunningJob(t *testing.T) {
	t.Parallel()
	h := newHarness(t, t0)
	started := make(chan struct{})
	release := make(chan struct{})
	var finished atomic.Bool
	h.s.RegisterFunc("h", func(ctx context.Context, _ Execution) error {
		close(started)
		<-release
		finished.Store(true)
		return ctx.Err()
	})
	h.start()
	tr := mustTrig(t)(Cron("*/10 * * * * ?"))
	if err := h.s.Schedule(context.Background(), JobSpec{Key: "j", Handler: "h", Trigger: tr}); err != nil {
		t.Fatal(err)
	}
	h.advance(10 * time.Second)
	<-started
	stopped := make(chan error, 1)
	go func() { stopped <- h.s.Stop(context.Background()) }()
	select {
	case err := <-stopped:
		t.Fatalf("Stop returned before the job finished: %v", err)
	default:
	}
	close(release)
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	if !finished.Load() {
		t.Fatal("job did not finish before Stop returned")
	}
}

func TestStopDeadlineCancelsJobs(t *testing.T) {
	t.Parallel()
	h := newHarness(t, t0)
	started := make(chan struct{})
	cancelled := make(chan struct{})
	h.s.RegisterFunc("h", func(ctx context.Context, _ Execution) error {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return ctx.Err()
	})
	h.start()
	tr := mustTrig(t)(Cron("*/10 * * * * ?"))
	if err := h.s.Schedule(context.Background(), JobSpec{Key: "j", Handler: "h", Trigger: tr}); err != nil {
		t.Fatal(err)
	}
	h.advance(10 * time.Second)
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already expired
	if err := h.s.Stop(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	<-cancelled
}

func TestParentContextCancelStopsEverything(t *testing.T) {
	t.Parallel()
	h := newHarness(t, t0)
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	h.s.RegisterFunc("h", func(ctx context.Context, _ Execution) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	if err := h.s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	tr := mustTrig(t)(Cron("*/10 * * * * ?"))
	if err := h.s.Schedule(ctx, JobSpec{Key: "j", Handler: "h", Trigger: tr}); err != nil {
		t.Fatal(err)
	}
	h.advance(10 * time.Second)
	<-started
	cancel()
	h.next(EventFailed, "j")
	if err := h.s.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestFailurePanicAndListenerPanic(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	h := newHarness(t, t0, WithListener(ListenerFunc(func(Event) { panic("listener bug") })))
	h.s.RegisterFunc("err", func(context.Context, Execution) error { return boom })
	h.s.RegisterFunc("panic", func(context.Context, Execution) error { panic("kaboom") })
	h.start()
	ctx := context.Background()
	tr := mustTrig(t)(Cron("*/10 * * * * ?"))
	for _, k := range []string{"err", "panic"} {
		if err := h.s.Schedule(ctx, JobSpec{Key: k, Handler: k, Trigger: tr}); err != nil {
			t.Fatal(err)
		}
	}
	h.advance(10 * time.Second)
	failed := map[string]Event{}
	for len(failed) < 2 { // the two jobs run concurrently: collect in any order
		ev := h.next(EventFailed, "")
		failed[ev.Key] = ev
	}
	if !errors.Is(failed["err"].Err, boom) {
		t.Fatalf("%v", failed["err"].Err)
	}
	var pe *PanicError
	if !errors.As(failed["panic"].Err, &pe) || pe.Value != "kaboom" {
		t.Fatalf("%v", failed["panic"].Err)
	}
	// the scheduler is still alive: next round runs too
	h.advance(10 * time.Second)
	h.next(EventFailed, "err")
}

func TestRunNow(t *testing.T) {
	t.Parallel()
	h := newHarness(t, t0)
	ran := make(chan Execution, 1)
	h.s.RegisterFunc("h", func(_ context.Context, e Execution) error { ran <- e; return nil })
	h.start()
	ctx := context.Background()
	tr := mustTrig(t)(Cron("0 0 0 1 1 ?"))
	if err := h.s.Schedule(ctx, JobSpec{Key: "j", Handler: "h", Trigger: tr}); err != nil {
		t.Fatal(err)
	}
	if err := h.s.RunNow(ctx, "j"); err != nil {
		t.Fatal(err)
	}
	e := <-ran
	if !e.ScheduledFor.Equal(t0) {
		t.Fatalf("%+v", e)
	}
	h.next(EventCompleted, "j")
	if rec, _ := h.s.Job("j"); rec.FireCount != 0 {
		t.Fatalf("RunNow must not change the schedule: %+v", rec)
	}
	if err := h.s.RunNow(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestRunNowRespectsOverlap(t *testing.T) {
	t.Parallel()
	h := newHarness(t, t0)
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	h.s.RegisterFunc("h", func(context.Context, Execution) error { started <- struct{}{}; <-release; return nil })
	h.start()
	ctx := context.Background()
	tr := mustTrig(t)(Cron("0 0 0 1 1 ?"))
	if err := h.s.Schedule(ctx, JobSpec{Key: "j", Handler: "h", Trigger: tr}); err != nil {
		t.Fatal(err)
	}
	_ = h.s.RunNow(ctx, "j")
	<-started
	_ = h.s.RunNow(ctx, "j")
	h.next(EventSkipped, "j")
	close(release)
}

func TestMaxConcurrent(t *testing.T) {
	t.Parallel()
	h := newHarness(t, t0, WithMaxConcurrent(1))
	var running, peak atomic.Int32
	var done sync.WaitGroup
	done.Add(3)
	h.s.RegisterFunc("h", func(context.Context, Execution) error {
		n := running.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		running.Add(-1)
		done.Done()
		return nil
	})
	h.start()
	tr := mustTrig(t)(Cron("*/10 * * * * ?"))
	for _, k := range []string{"a", "b", "c"} {
		if err := h.s.Schedule(context.Background(), JobSpec{Key: k, Handler: "h", Trigger: tr}); err != nil {
			t.Fatal(err)
		}
	}
	h.advance(10 * time.Second)
	done.Wait()
	if peak.Load() != 1 {
		t.Fatalf("peak = %d", peak.Load())
	}
}

func TestJobTimeout(t *testing.T) {
	t.Parallel()
	h := newHarness(t, t0)
	h.s.RegisterFunc("h", func(ctx context.Context, _ Execution) error { <-ctx.Done(); return ctx.Err() })
	h.start()
	tr := mustTrig(t)(Cron("*/10 * * * * ?"))
	if err := h.s.Schedule(context.Background(), JobSpec{Key: "j", Handler: "h", Trigger: tr, Timeout: 20 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	h.advance(10 * time.Second)
	if ev := h.next(EventFailed, "j"); !errors.Is(ev.Err, context.DeadlineExceeded) {
		t.Fatalf("%v", ev.Err)
	}
}

type failingStore struct {
	Store
	failSave, failDelete, failList bool
}

func (f *failingStore) Save(ctx context.Context, r JobRecord) error {
	if f.failSave {
		return errors.New("disk full")
	}
	return f.Store.Save(ctx, r)
}

func (f *failingStore) Delete(ctx context.Context, k string) error {
	if f.failDelete {
		return errors.New("disk gone")
	}
	return f.Store.Delete(ctx, k)
}

func (f *failingStore) List(ctx context.Context) ([]JobRecord, error) {
	if f.failList {
		return nil, errors.New("list broke")
	}
	return f.Store.List(ctx)
}

func TestStoreFailures(t *testing.T) {
	t.Parallel()
	fs := &failingStore{Store: NewMemoryStore(), failSave: true}
	h := newHarness(t, t0, WithStore(fs))
	h.s.RegisterFunc("h", noop)
	tr := mustTrig(t)(Cron("*/10 * * * * ?"))
	ctx := context.Background()
	if err := h.s.Schedule(ctx, JobSpec{Key: "j", Handler: "h", Trigger: tr}); err == nil {
		t.Fatal("Schedule must report the store error")
	}
	if _, err := h.s.Job("j"); !errors.Is(err, ErrNotFound) {
		t.Fatal("failed Schedule must not leave the job behind")
	}
	fs.failSave = false
	if err := h.s.Schedule(ctx, JobSpec{Key: "j", Handler: "h", Trigger: tr}); err != nil {
		t.Fatal(err)
	}
	h.start()
	fs.failSave = true // now runtime persistence fails: reported as events, scheduling continues
	h.advance(10 * time.Second)
	if ev := h.next(EventError, "j"); ev.Err == nil {
		t.Fatal("no error")
	}
	h.next(EventCompleted, "j")

	fs.failList = true
	h2 := newHarness(t, t0, WithStore(fs))
	if err := h2.s.Start(ctx); err == nil {
		t.Fatal("Start must fail when the store cannot be listed")
	}
}

func TestLoadProblemsAreReportedNotFatal(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	ctx := context.Background()
	good := JobRecord{Key: "good", Handler: "h", Trigger: TriggerSpec{Type: TriggerCron, Cron: "*/10 * * * * ?"}}
	_ = store.Save(ctx, good)
	_ = store.Save(ctx, JobRecord{Key: "badtrig", Handler: "h", Trigger: TriggerSpec{Type: "wat"}})
	_ = store.Save(ctx, JobRecord{Key: "badcal", Handler: "h", Calendars: []string{"gone"}, Trigger: good.Trigger})
	_ = store.Save(ctx, JobRecord{Key: "badpol", Handler: "h", Misfire: "wat", Trigger: good.Trigger})
	_ = store.Save(ctx, JobRecord{Key: "badtimeout", Handler: "h", Timeout: "soon", Trigger: good.Trigger})
	_ = store.Save(ctx, JobRecord{Key: "nohandler", Handler: "missing", Trigger: good.Trigger})

	h := newHarness(t, t0, WithStore(store))
	h.s.RegisterFunc("h", noop)
	h.start()
	seen := map[string]bool{}
	for range 4 {
		seen[h.next(EventError, "").Key] = true
	}
	for _, k := range []string{"badtrig", "badcal", "badpol", "badtimeout"} {
		if !seen[k] {
			t.Errorf("no error event for %s (got %v)", k, seen)
		}
	}
	if got := len(h.s.Jobs()); got != 2 { // good + nohandler
		t.Fatalf("Jobs = %d", got)
	}
	// a job with a missing handler reports an error instead of running
	h.advance(10 * time.Second)
	// events of one pass are emitted before the runs start, so the error comes first
	if ev := h.next(EventError, "nohandler"); !errors.Is(ev.Err, ErrNoHandler) {
		t.Fatalf("%v", ev.Err)
	}
	h.next(EventCompleted, "good")
}

func TestScheduleWhileRunningWakesLoop(t *testing.T) {
	t.Parallel()
	h := newHarness(t, t0)
	h.s.RegisterFunc("h", noop)
	h.start()
	ctx := context.Background()
	slow := mustTrig(t)(Cron("0 0 0 1 1 ?"))
	if err := h.s.Schedule(ctx, JobSpec{Key: "slow", Handler: "h", Trigger: slow}); err != nil {
		t.Fatal(err)
	}
	h.fc.BlockUntilTimers(1)
	fast := mustTrig(t)(Cron("*/5 * * * * ?"))
	if err := h.s.Schedule(ctx, JobSpec{Key: "fast", Handler: "h", Trigger: fast}); err != nil {
		t.Fatal(err)
	}
	// the loop must re-arm for the earlier job
	h.fc.BlockUntilTimers(1)
	h.fc.Advance(5 * time.Second)
	h.next(EventFired, "fast")
}

func TestCalendarCanExhaustATrigger(t *testing.T) {
	t.Parallel()
	// every day excluded: the job can never fire
	all := CalendarFunc(func(time.Time) bool { return false })
	h := newHarness(t, t0, WithCalendar("all", all))
	h.s.RegisterFunc("h", noop)
	tr := mustTrig(t)(Cron("0 0 12 * * ?"))
	err := h.s.Schedule(context.Background(), JobSpec{Key: "j", Handler: "h", Trigger: tr, Calendars: []string{"all"}})
	if !errors.Is(err, ErrInvalidTrigger) {
		t.Fatalf("err = %v", err)
	}
}
