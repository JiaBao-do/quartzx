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
