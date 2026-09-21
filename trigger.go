package quartzx

import (
	"fmt"
	"time"
)

// Trigger types used in [TriggerSpec.Type].
const (
	TriggerCron     = "cron"
	TriggerInterval = "interval"
	TriggerCalendar = "calendar"
	TriggerOnce     = "once"
)

// CalendarUnit is the unit of a calendar-interval trigger.
type CalendarUnit string

// Units for [EveryCalendar].
const (
	Day   CalendarUnit = "day"
	Week  CalendarUnit = "week"
	Month CalendarUnit = "month"
	Year  CalendarUnit = "year"
)

// TriggerSpec is the serializable description of a [Trigger]. It is what a
// [Store] persists. The zero Start of an interval or calendar trigger is
// filled in by the scheduler when the job is scheduled.
type TriggerSpec struct {
	// Type is one of TriggerCron, TriggerInterval, TriggerCalendar, TriggerOnce.
	Type string `json:"type"`
	// Cron is the expression for TriggerCron.
	Cron string `json:"cron,omitempty"`
	// Every is a [time.ParseDuration] string for TriggerInterval.
	Every string `json:"every,omitempty"`
	// N and Unit describe a TriggerCalendar: every N units.
	N    int          `json:"n,omitempty"`
	Unit CalendarUnit `json:"unit,omitempty"`
	// Start is the first possible fire time (the anchor for interval and
	// calendar triggers, the fire time for TriggerOnce).
	Start time.Time `json:"start,omitzero"`
	// End, if set, is the last instant at which the trigger may fire.
	End time.Time `json:"end,omitzero"`
	// Location is an IANA zone name (default UTC). It matters for cron
	// and calendar triggers.
	Location string `json:"location,omitempty"`
}

// Trigger computes fire times. Implementations returned by this package are
// immutable and safe for concurrent use.
type Trigger interface {
	// Next returns the first fire time strictly after the given time, or
	// false when the trigger is exhausted.
	Next(after time.Time) (time.Time, bool)
	// Spec returns the serializable description of the trigger.
	Spec() TriggerSpec
}

// TriggerOption customizes a trigger constructor.
type TriggerOption func(*TriggerSpec)

// WithStart sets the earliest fire time (or anchor).
func WithStart(t time.Time) TriggerOption { return func(s *TriggerSpec) { s.Start = t } }

// WithEnd sets the last instant at which the trigger may fire.
func WithEnd(t time.Time) TriggerOption { return func(s *TriggerSpec) { s.End = t } }

// WithLocation sets the time zone in which cron and calendar triggers are
// evaluated.
func WithLocation(loc *time.Location) TriggerOption {
	return func(s *TriggerSpec) {
		if loc != nil {
			s.Location = loc.String()
		}
	}
}

// Cron returns a trigger for a cron expression. See [CronSchedule].
func Cron(expr string, opts ...TriggerOption) (Trigger, error) {
	return build(TriggerSpec{Type: TriggerCron, Cron: expr}, opts)
}

// Every returns a fixed-interval trigger. Fire times are Start + k*d. If no
// start is given the scheduler anchors it at schedule time plus d.
func Every(d time.Duration, opts ...TriggerOption) (Trigger, error) {
	return build(TriggerSpec{Type: TriggerInterval, Every: d.String()}, opts)
}

// EveryCalendar returns a trigger that fires every n calendar units (days,
// weeks, months, years) from its start, keeping the wall clock time of the
// start in the trigger's location, also across DST changes. Month and year
// steps clamp to the end of shorter months (Jan 31 + 1 month = Feb 28/29)
// and never drift.
func EveryCalendar(n int, unit CalendarUnit, opts ...TriggerOption) (Trigger, error) {
	return build(TriggerSpec{Type: TriggerCalendar, N: n, Unit: unit}, opts)
}

// Once returns a trigger that fires a single time at t.
func Once(t time.Time, opts ...TriggerOption) (Trigger, error) {
	return build(TriggerSpec{Type: TriggerOnce, Start: t}, opts)
}

// NewTrigger rebuilds a trigger from its spec, for example one loaded from a
// [Store].
func NewTrigger(spec TriggerSpec) (Trigger, error) { return build(spec, nil) }

type trigger struct {
	spec  TriggerSpec
	loc   *time.Location
	cron  *CronSchedule
	every time.Duration
}

func build(spec TriggerSpec, opts []TriggerOption) (Trigger, error) {
	for _, o := range opts {
		o(&spec)
	}
	t := &trigger{spec: spec, loc: time.UTC}
	if spec.Location != "" {
		loc, err := time.LoadLocation(spec.Location)
		if err != nil {
			return nil, fmt.Errorf("%w: location %q: %w", ErrInvalidTrigger, spec.Location, err)
		}
		t.loc = loc
	}
	if !spec.Start.IsZero() && !spec.End.IsZero() && spec.End.Before(spec.Start) {
		return nil, fmt.Errorf("%w: end before start", ErrInvalidTrigger)
	}
	switch spec.Type {
	case TriggerCron:
		c, err := ParseCron(spec.Cron)
		if err != nil {
			return nil, err
		}
		t.cron = c.In(t.loc)
	case TriggerInterval:
		d, err := time.ParseDuration(spec.Every)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("%w: bad interval %q", ErrInvalidTrigger, spec.Every)
		}
		t.every = d
	case TriggerCalendar:
		if spec.N <= 0 {
			return nil, fmt.Errorf("%w: calendar step must be positive", ErrInvalidTrigger)
		}
		switch spec.Unit {
		case Day, Week, Month, Year:
		default:
			return nil, fmt.Errorf("%w: unknown unit %q", ErrInvalidTrigger, spec.Unit)
		}
	case TriggerOnce:
		if spec.Start.IsZero() {
			return nil, fmt.Errorf("%w: once needs a time", ErrInvalidTrigger)
		}
	default:
		return nil, fmt.Errorf("%w: unknown type %q", ErrInvalidTrigger, spec.Type)
	}
	return t, nil
}

func (t *trigger) Spec() TriggerSpec { return t.spec }

func (t *trigger) Next(after time.Time) (time.Time, bool) {
	var (
		r  time.Time
		ok bool
	)
	start := t.spec.Start
	switch t.spec.Type {
	case TriggerOnce:
		r, ok = start, start.After(after)
	case TriggerCron:
		from := after
		if !start.IsZero() && after.Before(start) {
			from = start.Add(-time.Nanosecond)
		}
		r, ok = t.cron.Next(from)
	case TriggerInterval:
		switch {
		case start.IsZero():
			r, ok = after.Add(t.every), true
		case start.After(after):
			r, ok = start, true
		default:
			k := int64(after.Sub(start)/t.every) + 1
			r, ok = start.Add(time.Duration(k)*t.every), true
		}
	case TriggerCalendar:
		r, ok = t.nextCalendar(after)
	}
	if ok && !t.spec.End.IsZero() && r.After(t.spec.End) {
		return time.Time{}, false
	}
	return r, ok
}

func (t *trigger) nextCalendar(after time.Time) (time.Time, bool) {
	start := t.spec.Start
	if start.IsZero() {
		return addUnits(after.In(t.loc), t.spec.N, t.spec.Unit), true
	}
	start = start.In(t.loc)
	if start.After(after) {
		return start, true
	}
	var unit time.Duration
	switch t.spec.Unit {
	case Day:
		unit = 25 * time.Hour // longest possible day, so k is never overestimated
	case Week:
		unit = 7 * 25 * time.Hour
	case Month:
		unit = 31 * 25 * time.Hour
	default:
		unit = 366 * 25 * time.Hour
	}
	k := int(after.Sub(start)/(unit*time.Duration(t.spec.N))) - 1
	if k < 0 {
		k = 0
	}
	for ; ; k++ {
		c := addUnits(start, k*t.spec.N, t.spec.Unit)
		if c.After(after) {
			return c, true
		}
	}
}

// addUnits adds m units to t, clamping months and years to the end of month.
func addUnits(t time.Time, m int, u CalendarUnit) time.Time {
	switch u {
	case Day:
		return t.AddDate(0, 0, m)
	case Week:
		return t.AddDate(0, 0, 7*m)
	case Year:
		m *= 12
	}
	y, mo, d := t.Date()
	h, mi, s := t.Clock()
	total := int(mo) - 1 + m
	ny, nm := y+total/12, total%12+1
	if dm := daysIn(ny, nm); d > dm {
		d = dm
	}
	return time.Date(ny, time.Month(nm), d, h, mi, s, t.Nanosecond(), t.Location())
}

// normalize fills in the anchor of interval and calendar triggers that have
// none, so a persisted trigger keeps its grid across restarts.
func normalize(tr Trigger, now time.Time) (Trigger, error) {
	sp := tr.Spec()
	if !sp.Start.IsZero() || (sp.Type != TriggerInterval && sp.Type != TriggerCalendar) {
		return tr, nil
	}
	next, ok := tr.Next(now)
	if !ok {
		return tr, nil
	}
	sp.Start = next
	return NewTrigger(sp)
}

// rebase re-anchors an interval or calendar trigger so its next fire is one
// period after now. Other triggers are returned unchanged.
func rebase(tr Trigger, now time.Time) (Trigger, error) {
	sp := tr.Spec()
	if sp.Type != TriggerInterval && sp.Type != TriggerCalendar {
		return tr, nil
	}
	sp.Start = time.Time{}
	fresh, err := NewTrigger(sp)
	if err != nil {
		return nil, err
	}
	return normalize(fresh, now)
}
