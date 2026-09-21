package quartzx

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"
)

const (
	defaultMisfireThreshold = 5 * time.Second
	defaultMaxCatchUp       = 100
	maxCalendarSkips        = 100_000
	minInterval             = time.Millisecond
)

// Option configures a [Scheduler].
type Option func(*Scheduler)

// WithStore sets the job store. The default is a new [MemoryStore].
func WithStore(st Store) Option { return func(s *Scheduler) { s.store = st } }

// WithClock sets the clock. Use [FakeClock] in tests.
func WithClock(c Clock) Option { return func(s *Scheduler) { s.clock = c } }

// WithListener adds a listener. It can be given more than once.
func WithListener(l Listener) Option {
	return func(s *Scheduler) { s.listeners = append(s.listeners, l) }
}

// WithMisfireThreshold sets how late a fire may be before it counts as a
// misfire and the job's [MisfirePolicy] applies (default 5s). A fire that is
// later than this because the process was down or overloaded is a misfire; a
// fire that is merely a few milliseconds late is not.
func WithMisfireThreshold(d time.Duration) Option {
	return func(s *Scheduler) { s.threshold = d }
}

// WithMaxCatchUp limits how many missed fires MisfireFireAll replays per job
// and misfire (default 100).
func WithMaxCatchUp(n int) Option { return func(s *Scheduler) { s.maxCatchUp = n } }

// WithMaxConcurrent limits how many jobs execute at the same time across the
// scheduler. Zero (the default) means unlimited.
func WithMaxConcurrent(n int) Option { return func(s *Scheduler) { s.maxConc = n } }

// WithCalendar registers a named [Calendar] that jobs can reference in
// JobSpec.Calendars. Calendars are code, not data, and are not persisted:
// register them on every start before calling Start.
func WithCalendar(name string, c Calendar) Option {
	return func(s *Scheduler) { s.calendars[name] = c }
}

// Scheduler runs jobs according to their triggers, persists them in a
// [Store], and recovers missed fires after a restart. Create one with [New].
//
// A Scheduler is safe for concurrent use by multiple goroutines. Handlers
// may be registered and jobs scheduled before or after Start. Listeners and
// calendars are called from scheduler goroutines and must not call back into
// the scheduler's mutating methods while holding locks of their own.
//
// The scheduler keeps every job in memory and scans them to find the next
// fire time, which is appropriate for up to a few thousand jobs.
type Scheduler struct {
	clock      Clock
	store      Store
	listeners  []Listener
	threshold  time.Duration
	maxCatchUp int
	maxConc    int
	sem        chan struct{}

	mu        sync.Mutex
	handlers  map[string]Job
	calendars map[string]Calendar
	entries   map[string]*entry
	wake      chan struct{}
	run       *runState
}

type runState struct {
	jobCtx     context.Context
	cancelJobs context.CancelFunc
	cancelLoop context.CancelFunc
	loopDone   chan struct{}
	wg         sync.WaitGroup
}

type entry struct {
	rec       JobRecord
	trig      Trigger
	cals      []Calendar
	timeout   time.Duration
	next      time.Time
	running   int
	exhausted bool
}

func (e *entry) snapshot() JobRecord {
	r := e.rec
	r.NextFire = e.next
	return r
}

// New returns a Scheduler. It does nothing until [Scheduler.Start].
func New(opts ...Option) *Scheduler {
	s := &Scheduler{
		clock:      SystemClock{},
		threshold:  defaultMisfireThreshold,
		maxCatchUp: defaultMaxCatchUp,
		handlers:   map[string]Job{},
		calendars:  map[string]Calendar{},
		entries:    map[string]*entry{},
		wake:       make(chan struct{}, 1),
	}
	for _, o := range opts {
		o(s)
	}
	if s.store == nil {
		s.store = NewMemoryStore()
	}
	if s.maxConc > 0 {
		s.sem = make(chan struct{}, s.maxConc)
	}
	return s
}

// Register binds a handler name to a job. Persisted jobs refer to handlers by
// name, so register every handler before Start. Registering a name again
// replaces the handler.
func (s *Scheduler) Register(name string, j Job) {
	s.mu.Lock()
	s.handlers[name] = j
	s.mu.Unlock()
}

// RegisterFunc is Register for a plain function.
func (s *Scheduler) RegisterFunc(name string, f func(ctx context.Context, e Execution) error) {
	s.Register(name, JobFunc(f))
}

func (s *Scheduler) emit(ev Event) {
	if ev.Time.IsZero() {
		ev.Time = s.clock.Now()
	}
	for _, l := range s.listeners {
		func() {
			defer func() { _ = recover() }()
			l.OnEvent(ev)
		}()
	}
}

func (s *Scheduler) poke() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Start loads the jobs from the store and starts scheduling in the
// background. Fires that were missed while the scheduler was not running are
// handled by each job's misfire policy as soon as the loop starts.
// Cancelling ctx stops the scheduler abruptly, including running jobs;
// prefer [Scheduler.Stop] for a graceful shutdown.
func (s *Scheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.run != nil {
		s.mu.Unlock()
		return ErrStarted
	}
	s.mu.Unlock()

	recs, err := s.store.List(ctx)
	if err != nil {
		return fmt.Errorf("quartzx: load jobs: %w", err)
	}
	now := s.clock.Now()
	var loadErrs []Event
	s.mu.Lock()
	if s.run != nil {
		s.mu.Unlock()
		return ErrStarted
	}
	for _, r := range recs {
		if _, ok := s.entries[r.Key]; ok {
			continue
		}
		e, err := s.newEntry(r, now)
		if err != nil {
			loadErrs = append(loadErrs, Event{Type: EventError, Key: r.Key, Err: err})
			continue
		}
		s.entries[r.Key] = e
	}
	jobCtx, cancelJobs := context.WithCancel(ctx)
	loopCtx, cancelLoop := context.WithCancel(ctx)
	rs := &runState{jobCtx: jobCtx, cancelJobs: cancelJobs, cancelLoop: cancelLoop, loopDone: make(chan struct{})}
	s.run = rs
	s.mu.Unlock()

	for _, ev := range loadErrs {
		s.emit(ev)
	}
	go s.loop(loopCtx, rs)
	return nil
}

// Stop stops scheduling new fires, then waits for running jobs to finish. If
// ctx expires first the jobs' contexts are cancelled, Stop still waits for
// them to return, and it returns ctx.Err(). Stop on a scheduler that is not
// running returns [ErrNotStarted]. The scheduler can be started again.
func (s *Scheduler) Stop(ctx context.Context) error {
	s.mu.Lock()
	rs := s.run
	s.mu.Unlock()
	if rs == nil {
		return ErrNotStarted
	}
	rs.cancelLoop()
	<-rs.loopDone

	done := make(chan struct{})
	go func() { rs.wg.Wait(); close(done) }()
	var err error
	select {
	case <-done:
	case <-ctx.Done():
		err = ctx.Err()
		rs.cancelJobs()
		<-done
	}
	rs.cancelJobs()
	s.mu.Lock()
	if s.run == rs {
		s.run = nil
	}
	s.mu.Unlock()
	return err
}

func (s *Scheduler) newEntry(r JobRecord, now time.Time) (*entry, error) {
	trig, err := NewTrigger(r.Trigger)
	if err != nil {
		return nil, err
	}
	if !r.Misfire.valid() {
		return nil, fmt.Errorf("%w: unknown misfire policy %q", ErrInvalidJob, r.Misfire)
	}
	e := &entry{rec: r, trig: trig, next: r.NextFire}
	for _, name := range r.Calendars {
		c, ok := s.calendars[name]
		if !ok {
			return nil, fmt.Errorf("%w: %q", ErrNoCalendar, name)
		}
		e.cals = append(e.cals, c)
	}
	if r.Timeout != "" {
		if e.timeout, err = time.ParseDuration(r.Timeout); err != nil {
			return nil, fmt.Errorf("%w: timeout: %w", ErrInvalidJob, err)
		}
	}
	if e.next.IsZero() && !r.Paused {
		e.next = s.first(e, now)
	}
	return e, nil
}

// after returns the next fire time after t honouring calendars, or zero.
func (s *Scheduler) after(e *entry, t time.Time) time.Time {
	for range maxCalendarSkips {
		n, ok := e.trig.Next(t)
		if !ok {
			return time.Time{}
		}
		if included(e.cals, n) {
			return n
		}
		t = n
	}
	return time.Time{}
}

func included(cals []Calendar, t time.Time) bool {
	for _, c := range cals {
		if !c.Included(t) {
			return false
		}
	}
	return true
}

func (s *Scheduler) first(e *entry, now time.Time) time.Time {
	if sp := e.trig.Spec(); sp.Type == TriggerOnce {
		if included(e.cals, sp.Start) {
			return sp.Start
		}
		return time.Time{}
	}
	return s.after(e, now)
}

// Schedule adds a job. It returns [ErrExists] if the key is taken, which is
// the normal outcome on a restart when the job was recovered from the store.
func (s *Scheduler) Schedule(ctx context.Context, spec JobSpec) error {
	return s.add(ctx, spec, false)
}

// Replace adds a job, replacing any existing job with the same key.
func (s *Scheduler) Replace(ctx context.Context, spec JobSpec) error {
	return s.add(ctx, spec, true)
}

func (s *Scheduler) add(ctx context.Context, spec JobSpec, replace bool) error {
	switch {
	case spec.Key == "":
		return fmt.Errorf("%w: empty key", ErrInvalidJob)
	case spec.Handler == "":
		return fmt.Errorf("%w: empty handler", ErrInvalidJob)
	case spec.Trigger == nil:
		return fmt.Errorf("%w: nil trigger", ErrInvalidJob)
	case !spec.Misfire.valid():
		return fmt.Errorf("%w: unknown misfire policy %q", ErrInvalidJob, spec.Misfire)
	case spec.Timeout < 0:
		return fmt.Errorf("%w: negative timeout", ErrInvalidJob)
	}
	if sp := spec.Trigger.Spec(); sp.Type == TriggerInterval {
		if d, _ := time.ParseDuration(sp.Every); d < minInterval {
			return fmt.Errorf("%w: interval below %v", ErrInvalidTrigger, minInterval)
		}
	}
	now := s.clock.Now()
	trig, err := normalize(spec.Trigger, now)
	if err != nil {
		return err
	}
	spec.Trigger = trig
	spec.Data = maps.Clone(spec.Data)

	s.mu.Lock()
	if _, ok := s.handlers[spec.Handler]; !ok {
		s.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrNoHandler, spec.Handler)
	}
	if _, ok := s.entries[spec.Key]; ok && !replace {
		s.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrExists, spec.Key)
	}
	e, err := s.newEntry(spec.record(), now)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if e.next.IsZero() {
		s.mu.Unlock()
		return fmt.Errorf("%w: trigger never fires", ErrInvalidTrigger)
	}
	prev := s.entries[spec.Key]
	s.entries[spec.Key] = e
	rec := e.snapshot()
	s.mu.Unlock()

	if err := s.store.Save(ctx, rec); err != nil {
		s.mu.Lock()
		if s.entries[spec.Key] == e {
			if prev != nil {
				s.entries[spec.Key] = prev
			} else {
				delete(s.entries, spec.Key)
			}
		}
		s.mu.Unlock()
		return fmt.Errorf("quartzx: save job %q: %w", spec.Key, err)
	}
	s.poke()
	return nil
}

// Unschedule removes a job from the scheduler and the store. A run that is
// already executing is not interrupted.
func (s *Scheduler) Unschedule(ctx context.Context, key string) error {
	s.mu.Lock()
	if _, ok := s.entries[key]; !ok {
		s.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrNotFound, key)
	}
	delete(s.entries, key)
	s.mu.Unlock()
	s.poke()
	return s.store.Delete(ctx, key)
}

// Pause stops a job from firing. Its next fire time is kept, so on
// [Scheduler.Resume] fires that were missed meanwhile are handled by the
// job's misfire policy. The paused state is persisted.
func (s *Scheduler) Pause(ctx context.Context, key string) error { return s.setPaused(ctx, key, true) }

// Resume undoes [Scheduler.Pause].
func (s *Scheduler) Resume(ctx context.Context, key string) error {
	return s.setPaused(ctx, key, false)
}

func (s *Scheduler) setPaused(ctx context.Context, key string, p bool) error {
	s.mu.Lock()
	e, ok := s.entries[key]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrNotFound, key)
	}
	e.rec.Paused = p
	if !p && e.next.IsZero() && !e.exhausted {
		e.next = s.first(e, s.clock.Now())
	}
	rec := e.snapshot()
	s.mu.Unlock()
	s.poke()
	return s.store.Save(ctx, rec)
}

// RunNow executes a job immediately, once, without changing its schedule. It
// requires a started scheduler and follows the job's overlap rule: if the job
// is running and does not allow concurrency, an EventSkipped is emitted and
// nil is returned. It does not wait for the job to finish.
func (s *Scheduler) RunNow(_ context.Context, key string) error {
	now := s.clock.Now()
	s.mu.Lock()
	rs := s.run
	if rs == nil {
		s.mu.Unlock()
		return ErrNotStarted
	}
	e, ok := s.entries[key]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrNotFound, key)
	}
	job, ok := s.handlers[e.rec.Handler]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrNoHandler, e.rec.Handler)
	}
	if e.running > 0 && !e.rec.AllowConcurrent {
		s.mu.Unlock()
		s.emit(Event{Type: EventSkipped, Key: key, ScheduledFor: now})
		return nil
	}
	e.running++
	rs.wg.Add(1)
	items := []fireItem{{at: now, count: e.rec.FireCount}}
	s.mu.Unlock()
	go s.runJob(rs, e, job, items)
	return nil
}

// Job returns a copy of the job's current state.
func (s *Scheduler) Job(key string) (JobRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[key]
	if !ok {
		return JobRecord{}, fmt.Errorf("%w: %q", ErrNotFound, key)
	}
	r := e.snapshot()
	r.Data = maps.Clone(r.Data)
	return r, nil
}

// Jobs returns copies of all jobs' current state, sorted by key.
func (s *Scheduler) Jobs() []JobRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]JobRecord, 0, len(s.entries))
	for _, e := range s.entries {
		r := e.snapshot()
		r.Data = maps.Clone(r.Data)
		out = append(out, r)
	}
	slices.SortFunc(out, func(a, b JobRecord) int {
		switch {
		case a.Key < b.Key:
			return -1
		case a.Key > b.Key:
			return 1
		}
		return 0
	})
	return out
}

func (s *Scheduler) earliest() (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var best time.Time
	for _, e := range s.entries {
		if e.rec.Paused || e.exhausted || e.next.IsZero() {
			continue
		}
		if best.IsZero() || e.next.Before(best) {
			best = e.next
		}
	}
	return best, !best.IsZero()
}

func (s *Scheduler) loop(ctx context.Context, rs *runState) {
	defer close(rs.loopDone)
	for {
		if ctx.Err() != nil {
			return
		}
		s.processDue(rs)
		next, ok := s.earliest()
		var tc <-chan time.Time
		var tm Timer
		if ok {
			d := next.Sub(s.clock.Now())
			tm = s.clock.NewTimer(d)
			if !s.clock.Now().Before(next) { // became due while arming
				tm.Stop()
				continue
			}
			tc = tm.C()
		}
		select {
		case <-ctx.Done():
		case <-s.wake:
		case <-tc:
		}
		if tm != nil {
			tm.Stop()
		}
	}
}

type fireItem struct {
	at       time.Time
	misfired bool
	count    int64
}

type batch struct {
	saves   []JobRecord
	deletes []string
	events  []Event
	runs    []func()
}

func (s *Scheduler) processDue(rs *runState) {
	now := s.clock.Now()
	var b batch
	s.mu.Lock()
	var due []*entry
	for _, e := range s.entries {
		if !e.rec.Paused && !e.exhausted && !e.next.IsZero() && !e.next.After(now) {
			due = append(due, e)
		}
	}
	slices.SortFunc(due, func(a, c *entry) int {
		if d := a.next.Compare(c.next); d != 0 {
			return d
		}
		switch {
		case a.rec.Key < c.rec.Key:
			return -1
		case a.rec.Key > c.rec.Key:
			return 1
		}
		return 0
	})
	for _, e := range due {
		s.advance(rs, e, now, &b)
	}
	s.mu.Unlock()

	for _, ev := range b.events {
		s.emit(ev)
	}
	for _, key := range b.deletes {
		if err := s.store.Delete(context.Background(), key); err != nil {
			s.emit(Event{Type: EventError, Key: key, Err: fmt.Errorf("quartzx: delete job: %w", err)})
		}
	}
	for _, r := range b.saves {
		if err := s.store.Save(context.Background(), r); err != nil {
			s.emit(Event{Type: EventError, Key: r.Key, Err: fmt.Errorf("quartzx: save job: %w", err)})
		}
	}
	for _, f := range b.runs {
		go f()
	}
}

// advance handles every due fire of e. s.mu is held.
func (s *Scheduler) advance(rs *runState, e *entry, now time.Time, b *batch) {
	var items []fireItem
	key := e.rec.Key
	for !e.next.IsZero() && !e.next.After(now) {
		sched := e.next
		if now.Sub(sched) <= s.threshold {
			items = append(items, fireItem{at: sched})
			e.next = s.after(e, sched)
			continue
		}
		policy := e.rec.Misfire
		if policy == "" {
			policy = MisfireFireNow
		}
		missed := 1
		switch policy {
		case MisfireSkip:
			e.next = s.after(e, now)
		case MisfireFireNow:
			items = append(items, fireItem{at: sched, misfired: true})
			e.next = s.after(e, now)
		case MisfireReschedule:
			items = append(items, fireItem{at: sched, misfired: true})
			if nt, err := rebase(e.trig, now); err == nil {
				e.trig = nt
				e.rec.Trigger = nt.Spec()
			}
			e.next = s.after(e, now)
		case MisfireFireAll:
			t, n := sched, 0
			for !t.IsZero() && !t.After(now) && n < s.maxCatchUp {
				items = append(items, fireItem{at: t, misfired: true})
				n++
				t = s.after(e, t)
			}
			missed = n
			if !t.IsZero() && !t.After(now) {
				t = s.after(e, now)
			}
			e.next = t
		}
		b.events = append(b.events, Event{Type: EventMisfired, Key: key, ScheduledFor: sched, Policy: policy, Missed: missed, Time: now})
	}

	if len(items) > 0 {
		job, ok := s.handlers[e.rec.Handler]
		switch {
		case !ok:
			b.events = append(b.events, Event{Type: EventError, Key: key, Time: now,
				Err: fmt.Errorf("%w: %q", ErrNoHandler, e.rec.Handler)})
		case e.running > 0 && !e.rec.AllowConcurrent:
			for _, it := range items {
				b.events = append(b.events, Event{Type: EventSkipped, Key: key, ScheduledFor: it.at, Time: now})
			}
		default:
			for i := range items {
				items[i].count = e.rec.FireCount + int64(i) + 1
			}
			e.rec.FireCount += int64(len(items))
			e.rec.LastFire = items[len(items)-1].at
			e.running++
			rs.wg.Add(1)
			its := items
			b.runs = append(b.runs, func() { s.runJob(rs, e, job, its) })
		}
	}

	if e.next.IsZero() {
		e.exhausted = true
		if e.running == 0 {
			s.finishLocked(e, b)
		}
		return
	}
	b.saves = append(b.saves, e.snapshot())
}

// finishLocked removes an exhausted entry. s.mu is held.
func (s *Scheduler) finishLocked(e *entry, b *batch) {
	if s.entries[e.rec.Key] == e {
		delete(s.entries, e.rec.Key)
		b.deletes = append(b.deletes, e.rec.Key)
		b.events = append(b.events, Event{Type: EventExhausted, Key: e.rec.Key})
	}
}

func (s *Scheduler) runJob(rs *runState, e *entry, job Job, items []fireItem) {
	defer rs.wg.Done()
	// The terminal event of the last run is emitted only after the job is no
	// longer marked running, so a listener that reacts to it (a test advancing
	// a fake clock) never races with the overlap check.
	var final *Event
	defer func() {
		var b batch
		s.mu.Lock()
		e.running--
		if e.exhausted && e.running == 0 {
			s.finishLocked(e, &b)
		}
		s.mu.Unlock()
		for _, k := range b.deletes {
			if err := s.store.Delete(context.Background(), k); err != nil {
				s.emit(Event{Type: EventError, Key: k, Err: fmt.Errorf("quartzx: delete job: %w", err)})
			}
		}
		if final != nil {
			s.emit(*final)
		}
		for _, ev := range b.events {
			s.emit(ev)
		}
		s.poke()
	}()
	if s.sem != nil {
		select {
		case s.sem <- struct{}{}:
			defer func() { <-s.sem }()
		case <-rs.jobCtx.Done():
			return
		}
	}
	key := e.rec.Key
	for i, it := range items {
		if rs.jobCtx.Err() != nil {
			return
		}
		terminal := func(ev Event) {
			if i == len(items)-1 {
				final = &ev
				return
			}
			s.emit(ev)
		}
		s.mu.Lock()
		data := maps.Clone(e.rec.Data)
		handler := e.rec.Handler
		persist := e.rec.PersistData
		s.mu.Unlock()
		exec := Execution{Key: key, Handler: handler, ScheduledFor: it.at, FiredAt: s.clock.Now(),
			Misfired: it.misfired, Count: it.count, Data: data}
		s.emit(Event{Type: EventFired, Key: key, ScheduledFor: it.at})
		err := s.call(rs.jobCtx, job, exec, e.timeout)
		if err != nil {
			terminal(Event{Type: EventFailed, Key: key, ScheduledFor: it.at, Err: err})
			continue
		}
		if persist {
			s.mu.Lock()
			var rec JobRecord
			save := s.entries[key] == e
			if save {
				e.rec.Data = exec.Data
				rec = e.snapshot()
			}
			s.mu.Unlock()
			if save {
				if err := s.store.Save(context.Background(), rec); err != nil {
					s.emit(Event{Type: EventError, Key: key, Err: fmt.Errorf("quartzx: save job data: %w", err)})
				}
			}
		}
		terminal(Event{Type: EventCompleted, Key: key, ScheduledFor: it.at})
	}
}

func (s *Scheduler) call(ctx context.Context, job Job, exec Execution, timeout time.Duration) (err error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Value: r}
		}
	}()
	return job.Execute(ctx, exec)
}
