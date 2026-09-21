package quartzx

import "time"

// Calendar excludes times from a schedule, like a Quartz Calendar. A job
// scheduled with calendar names never fires at an instant for which any of
// those calendars reports Included == false; the fire is skipped and the
// next candidate is used. Implementations must be safe for concurrent use;
// all calendars in this package are immutable.
type Calendar interface {
	// Included reports whether t is allowed to fire.
	Included(t time.Time) bool
}

// CalendarFunc adapts a function to a [Calendar].
type CalendarFunc func(t time.Time) bool

// Included calls f.
func (f CalendarFunc) Included(t time.Time) bool { return f(t) }

func inLoc(t time.Time, loc *time.Location) time.Time {
	if loc == nil {
		return t.UTC()
	}
	return t.In(loc)
}

type dayKey struct {
	y int
	m time.Month
	d int
}

// HolidayCalendar excludes whole calendar days (for example public holidays).
type HolidayCalendar struct {
	loc  *time.Location
	days map[dayKey]struct{}
}

// NewHolidayCalendar returns a calendar that excludes the calendar days of
// dates, judged in loc (nil means UTC). The time of day in dates is ignored.
func NewHolidayCalendar(loc *time.Location, dates ...time.Time) *HolidayCalendar {
	h := &HolidayCalendar{loc: loc, days: make(map[dayKey]struct{}, len(dates))}
	for _, d := range dates {
		y, m, dd := d.In(orUTC(loc)).Date()
		h.days[dayKey{y, m, dd}] = struct{}{}
	}
	return h
}

func orUTC(loc *time.Location) *time.Location {
	if loc == nil {
		return time.UTC
	}
	return loc
}

// Included reports whether t falls on a day that is not a listed holiday.
func (h *HolidayCalendar) Included(t time.Time) bool {
	y, m, d := inLoc(t, h.loc).Date()
	_, hit := h.days[dayKey{y, m, d}]
	return !hit
}

// WeeklyCalendar excludes days of the week, for example weekends.
type WeeklyCalendar struct {
	loc      *time.Location
	excluded [7]bool
}

// NewWeeklyCalendar returns a calendar that excludes the given weekdays,
// judged in loc (nil means UTC).
func NewWeeklyCalendar(loc *time.Location, excluded ...time.Weekday) *WeeklyCalendar {
	w := &WeeklyCalendar{loc: loc}
	for _, d := range excluded {
		w.excluded[d%7] = true
	}
	return w
}

// Included reports whether the weekday of t is not excluded.
func (w *WeeklyCalendar) Included(t time.Time) bool {
	return !w.excluded[inLoc(t, w.loc).Weekday()]
}

// MonthDay is a month and day of month, ignoring the year.
type MonthDay struct {
	Month time.Month
	Day   int
}

// AnnualCalendar excludes the same month/day every year (for example
// December 25).
type AnnualCalendar struct {
	loc  *time.Location
	days map[MonthDay]struct{}
}

// NewAnnualCalendar returns a calendar that excludes the given days every
// year, judged in loc (nil means UTC).
func NewAnnualCalendar(loc *time.Location, days ...MonthDay) *AnnualCalendar {
	a := &AnnualCalendar{loc: loc, days: make(map[MonthDay]struct{}, len(days))}
	for _, d := range days {
		a.days[d] = struct{}{}
	}
	return a
}

// Included reports whether the month and day of t are not excluded.
func (a *AnnualCalendar) Included(t time.Time) bool {
	_, m, d := inLoc(t, a.loc).Date()
	_, hit := a.days[MonthDay{m, d}]
	return !hit
}

// TimeOfDay is a wall clock time of day.
type TimeOfDay struct{ Hour, Minute, Second int }

func (t TimeOfDay) secs() int { return t.Hour*3600 + t.Minute*60 + t.Second }

// DailyCalendar excludes a window of the day, [From, To). If To is not after
// From the window wraps past midnight (for example 22:00 to 06:00).
type DailyCalendar struct {
	loc      *time.Location
	from, to int
}

// NewDailyCalendar returns a calendar that excludes the time-of-day window
// [from, to), judged in loc (nil means UTC).
func NewDailyCalendar(loc *time.Location, from, to TimeOfDay) *DailyCalendar {
	return &DailyCalendar{loc: loc, from: from.secs(), to: to.secs()}
}

// Included reports whether the time of day of t is outside the window.
func (c *DailyCalendar) Included(t time.Time) bool {
	h, m, s := inLoc(t, c.loc).Clock()
	x := h*3600 + m*60 + s
	if c.from < c.to {
		return x < c.from || x >= c.to
	}
	return x < c.from && x >= c.to
}

// CronCalendar excludes every second matched by a cron expression, for
// example "* * 9-11 * * ?" excludes 09:00 to 11:59:59.
type CronCalendar struct{ c *CronSchedule }

// NewCronCalendar returns a calendar that excludes the times matched by expr,
// evaluated in loc (nil means UTC).
func NewCronCalendar(expr string, loc *time.Location) (*CronCalendar, error) {
	c, err := ParseCron(expr)
	if err != nil {
		return nil, err
	}
	return &CronCalendar{c.In(orUTC(loc))}, nil
}

// Included reports whether t is not matched by the expression.
func (c *CronCalendar) Included(t time.Time) bool {
	t = t.Truncate(time.Second)
	n, ok := c.c.Next(t.Add(-time.Second))
	return !ok || !n.Equal(t)
}
