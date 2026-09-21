package quartzx

import (
	"sort"
	"sync"
	"time"
)

// Clock abstracts time so schedulers can be tested deterministically.
// Implementations must be safe for concurrent use.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// NewTimer returns a timer that fires once after d.
	NewTimer(d time.Duration) Timer
}

// Timer is the subset of [time.Timer] used by the scheduler.
type Timer interface {
	// C delivers the fire time.
	C() <-chan time.Time
	// Stop prevents the timer from firing and reports whether it was active.
	Stop() bool
}

// SystemClock is the real clock. The zero value is ready to use and it is
// safe for concurrent use.
type SystemClock struct{}

// Now returns time.Now().
func (SystemClock) Now() time.Time { return time.Now() }

// NewTimer wraps time.NewTimer.
func (SystemClock) NewTimer(d time.Duration) Timer { return sysTimer{time.NewTimer(d)} }

type sysTimer struct{ t *time.Timer }

func (s sysTimer) C() <-chan time.Time { return s.t.C }
func (s sysTimer) Stop() bool          { return s.t.Stop() }

// FakeClock is a manually advanced clock for deterministic tests. Time only
// moves when Advance or Set is called. It is safe for concurrent use.
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*fakeTimer
	waiters []chan struct{}
}

type fakeTimer struct {
	clock    *FakeClock
	deadline time.Time
	ch       chan time.Time
	active   bool
}

// NewFakeClock returns a FakeClock set to start.
func NewFakeClock(start time.Time) *FakeClock { return &FakeClock{now: start} }

// Now returns the fake time.
func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// NewTimer creates a timer that fires when the fake time reaches Now()+d.
// A timer with d <= 0 fires immediately.
func (f *FakeClock) NewTimer(d time.Duration) Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &fakeTimer{clock: f, deadline: f.now.Add(d), ch: make(chan time.Time, 1), active: true}
	if d <= 0 {
		t.active = false
		t.ch <- f.now
		return t
	}
	f.timers = append(f.timers, t)
	f.notifyLocked()
	return t
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() bool {
	f := t.clock
	f.mu.Lock()
	defer f.mu.Unlock()
	was := t.active
	t.active = false
	for i, x := range f.timers {
		if x == t {
			f.timers = append(f.timers[:i], f.timers[i+1:]...)
			break
		}
	}
	return was
}

// Advance moves the clock forward by d and fires every timer whose deadline
// has been reached, in deadline order.
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	f.setLocked(f.now.Add(d))
	f.mu.Unlock()
}

// Set moves the clock to t (which may be earlier than the current time) and
// fires due timers.
func (f *FakeClock) Set(t time.Time) {
	f.mu.Lock()
	f.setLocked(t)
	f.mu.Unlock()
}

func (f *FakeClock) setLocked(t time.Time) {
	f.now = t
	sort.SliceStable(f.timers, func(i, j int) bool { return f.timers[i].deadline.Before(f.timers[j].deadline) })
	keep := f.timers[:0]
	for _, tm := range f.timers {
		if !tm.deadline.After(t) {
			tm.active = false
			select {
			case tm.ch <- t:
			default:
			}
			continue
		}
		keep = append(keep, tm)
	}
	f.timers = keep
}

// Timers returns the number of active timers.
func (f *FakeClock) Timers() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.timers)
}

// BlockUntilTimers blocks until at least n timers are active. Tests call it
// before Advance so the scheduler has armed its timer.
func (f *FakeClock) BlockUntilTimers(n int) {
	for {
		f.mu.Lock()
		if len(f.timers) >= n {
			f.mu.Unlock()
			return
		}
		w := make(chan struct{})
		f.waiters = append(f.waiters, w)
		f.mu.Unlock()
		<-w
	}
}

func (f *FakeClock) notifyLocked() {
	for _, w := range f.waiters {
		close(w)
	}
	f.waiters = nil
}
