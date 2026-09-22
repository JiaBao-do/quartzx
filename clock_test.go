package quartzx

import (
	"testing"
	"time"
)

func TestFakeClockTimers(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fc := NewFakeClock(start)
	if !fc.Now().Equal(start) {
		t.Fatal("Now")
	}
	t1 := fc.NewTimer(10 * time.Second)
	t2 := fc.NewTimer(5 * time.Second)
	if fc.Timers() != 2 {
		t.Fatalf("Timers = %d", fc.Timers())
	}
	fc.Advance(4 * time.Second)
	select {
	case <-t2.C():
		t.Fatal("fired early")
	default:
	}
	fc.Advance(time.Second)
	if got := <-t2.C(); !got.Equal(start.Add(5 * time.Second)) {
		t.Fatalf("t2 fired at %v", got)
	}
	select {
	case <-t1.C():
		t.Fatal("t1 fired early")
	default:
	}
	if !t1.Stop() {
		t.Fatal("Stop should report an active timer")
	}
	if t1.Stop() {
		t.Fatal("second Stop should report inactive")
	}
	fc.Advance(time.Hour)
	select {
	case <-t1.C():
		t.Fatal("stopped timer fired")
	default:
	}
	if fc.Timers() != 0 {
		t.Fatalf("Timers = %d", fc.Timers())
	}
}

func TestFakeClockImmediateAndSet(t *testing.T) {
	t.Parallel()
	fc := NewFakeClock(time.Unix(100, 0))
	tm := fc.NewTimer(0)
	select {
	case <-tm.C():
	default:
		t.Fatal("zero timer must fire immediately")
	}
	tm2 := fc.NewTimer(time.Minute)
	fc.Set(time.Unix(100, 0).Add(2 * time.Minute))
	select {
	case <-tm2.C():
	default:
		t.Fatal("Set must fire due timers")
	}
}

func TestFakeClockBlockUntilTimers(t *testing.T) {
	t.Parallel()
	fc := NewFakeClock(time.Unix(0, 0))
	done := make(chan struct{})
	go func() { fc.BlockUntilTimers(2); close(done) }()
	fc.NewTimer(time.Second)
	fc.NewTimer(time.Second)
	<-done
}

func TestSystemClock(t *testing.T) {
	t.Parallel()
	var c Clock = SystemClock{}
	before := time.Now()
	if c.Now().Before(before) {
		t.Fatal("Now went backwards")
	}
	tm := c.NewTimer(time.Millisecond)
	<-tm.C()
	if tm.Stop() {
		t.Fatal("fired timer should not be active")
	}
}

func TestFakeClockNewTimerAt(t *testing.T) {
	t.Parallel()
	start := time.Unix(1000, 0)
	fc := NewFakeClock(start)

	// A deadline that is already due (equal to or before Now) fires
	// immediately and is never counted as an active timer.
	due := fc.NewTimerAt(start)
	select {
	case got := <-due.C():
		if !got.Equal(start) {
			t.Fatalf("due fired at %v, want %v", got, start)
		}
	default:
		t.Fatal("a deadline at Now must fire immediately")
	}
	past := fc.NewTimerAt(start.Add(-time.Second))
	select {
	case <-past.C():
	default:
		t.Fatal("a deadline before Now must fire immediately")
	}
	if fc.Timers() != 0 {
		t.Fatalf("Timers = %d, want 0 (no immediate-fire timers tracked)", fc.Timers())
	}

	// A future deadline arms and fires exactly at that instant.
	future := fc.NewTimerAt(start.Add(time.Minute))
	if fc.Timers() != 1 {
		t.Fatalf("Timers = %d, want 1", fc.Timers())
	}
	fc.Set(start.Add(30 * time.Second))
	select {
	case <-future.C():
		t.Fatal("fired early")
	default:
	}
	fc.Set(start.Add(time.Minute))
	select {
	case got := <-future.C():
		if !got.Equal(start.Add(time.Minute)) {
			t.Fatalf("future fired at %v", got)
		}
	default:
		t.Fatal("future deadline did not fire on Set")
	}
}

func TestSystemClockNewTimerAt(t *testing.T) {
	t.Parallel()
	var c Clock = SystemClock{}
	tm := c.NewTimerAt(time.Now().Add(time.Millisecond))
	<-tm.C()
	if tm.Stop() {
		t.Fatal("fired timer should not be active")
	}
	// An already-past deadline fires on the next tick rather than blocking.
	past := c.NewTimerAt(time.Now().Add(-time.Hour))
	select {
	case <-past.C():
	case <-time.After(time.Second):
		t.Fatal("a past deadline should fire promptly")
	}
}

// TestFakeClockNewTimerAtArmRaceStress is a regression test for the
// investigation recorded in STATUS.md / LEARNINGS.md: a single CI run once
// deadlocked in Scheduler.loop after arming a timer from a Now() read and a
// separately computed duration, which a concurrent Set could in theory
// interleave with. That exact deadlock was never reproduced (see the
// investigation notes), but NewTimerAt replaces the split read with one
// atomic operation; this stress test hammers NewTimerAt concurrently with
// Set/Advance under -race to keep that guarantee honest. It must never hang
// (enforced by the test's own deadline) and must never trip the race
// detector.
func TestFakeClockNewTimerAtArmRaceStress(t *testing.T) {
	t.Parallel()
	start := time.Unix(0, 0)
	fc := NewFakeClock(start)

	const arms = 500
	fired := make(chan struct{}, arms)
	armDone := make(chan struct{})
	stopAdvancing := make(chan struct{})

	go func() {
		defer close(armDone)
		for i := range arms {
			// Alternate between deadlines that are already due and ones
			// slightly in the future, so both of NewTimerAt's paths (fire
			// now vs. arm-and-wait) run concurrently with the advancer
			// below, which is exactly the interleaving NewTimerAt makes
			// atomic (see its doc comment and Scheduler.loop's use of it).
			var target time.Time
			if i%2 == 0 {
				target = fc.Now()
			} else {
				target = fc.Now().Add(time.Millisecond)
			}
			tm := fc.NewTimerAt(target)
			go func(tm Timer) {
				<-tm.C()
				fired <- struct{}{}
			}(tm)
		}
	}()
	// Keep advancing until every timer has been armed AND fired; this cannot
	// stop early just because arming finished, since a "future" timer armed
	// near the end still needs another Advance to cross its deadline.
	go func() {
		for {
			select {
			case <-stopAdvancing:
				return
			default:
				fc.Advance(time.Microsecond * 500)
			}
		}
	}()
	<-armDone
	for i := 0; i < arms; i++ {
		select {
		case <-fired:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d/%d timers fired", i, arms)
		}
	}
	close(stopAdvancing)
}
