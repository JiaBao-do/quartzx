package quartzx

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestIntervalTrigger(t *testing.T) {
	t.Parallel()
	start := mustTime(t, "2026-01-01T00:00:00Z")
	tr, err := Every(10*time.Second, WithStart(start), WithEnd(start.Add(35*time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		after string
		want  string // "" = exhausted
	}{
		{"2025-12-31T00:00:00Z", "2026-01-01T00:00:00Z"}, // before start: the start itself
		{"2026-01-01T00:00:00Z", "2026-01-01T00:00:10Z"},
		{"2026-01-01T00:00:09.9Z", "2026-01-01T00:00:10Z"},
		{"2026-01-01T00:00:10Z", "2026-01-01T00:00:20Z"},
		{"2026-01-01T00:00:29Z", "2026-01-01T00:00:30Z"},
		{"2026-01-01T00:00:30Z", ""}, // 40s is after End
	}
	for _, c := range cases {
		got, ok := tr.Next(mustTime(t, c.after))
		if c.want == "" {
			if ok {
				t.Errorf("Next(%s) = %v, want exhausted", c.after, got)
			}
			continue
		}
		if !ok || !got.Equal(mustTime(t, c.want)) {
			t.Errorf("Next(%s) = %v,%v want %s", c.after, got, ok, c.want)
		}
	}
}

func TestIntervalWithoutStart(t *testing.T) {
	t.Parallel()
	tr, _ := Every(time.Minute)
	after := mustTime(t, "2026-01-01T00:00:00Z")
	got, _ := tr.Next(after)
	if !got.Equal(after.Add(time.Minute)) {
		t.Fatalf("got %v", got)
	}
}

func TestOnceTrigger(t *testing.T) {
	t.Parallel()
	at := mustTime(t, "2026-05-05T05:05:05Z")
	tr, err := Once(at)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := tr.Next(at.Add(-time.Hour)); !ok || !got.Equal(at) {
		t.Fatalf("got %v %v", got, ok)
	}
	if _, ok := tr.Next(at); ok {
		t.Fatal("once must not fire again")
	}
}

func TestCronTriggerStartEnd(t *testing.T) {
	t.Parallel()
	start := mustTime(t, "2026-01-10T00:00:00Z")
	end := mustTime(t, "2026-01-12T12:00:00Z")
	tr, err := Cron("0 0 12 * * ?", WithStart(start), WithEnd(end))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"2026-01-10T12:00:00Z", "2026-01-11T12:00:00Z", "2026-01-12T12:00:00Z"}
	cur := mustTime(t, "2026-01-01T00:00:00Z") // before Start: skips ahead to Start
	for _, w := range want {
		got, ok := tr.Next(cur)
		if !ok || !got.Equal(mustTime(t, w)) {
			t.Fatalf("got %v %v want %s", got, ok, w)
		}
		cur = got
	}
	if got, ok := tr.Next(cur); ok {
		t.Fatalf("expected exhausted after End, got %v", got)
	}
}

func TestCronTriggerStartIsInclusive(t *testing.T) {
	t.Parallel()
	start := mustTime(t, "2026-01-10T12:00:00Z")
	tr, _ := Cron("0 0 12 * * ?", WithStart(start))
	got, _ := tr.Next(mustTime(t, "2026-01-01T00:00:00Z"))
	if !got.Equal(start) {
		t.Fatalf("a fire exactly at Start must be kept, got %v", got)
	}
}

func TestCalendarIntervalMonthClamps(t *testing.T) {
	t.Parallel()
	start := mustTime(t, "2026-01-31T09:00:00Z")
	tr, err := EveryCalendar(1, Month, WithStart(start))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"2026-02-28T09:00:00Z", "2026-03-31T09:00:00Z", "2026-04-30T09:00:00Z", "2026-05-31T09:00:00Z"}
	cur := start
	for _, w := range want {
		got, ok := tr.Next(cur)
		if !ok || !got.Equal(mustTime(t, w)) {
			t.Fatalf("got %v want %s (no drift expected)", got, w)
		}
		cur = got
	}
}

func TestCalendarIntervalUnits(t *testing.T) {
	t.Parallel()
	start := mustTime(t, "2024-02-29T00:00:00Z")
	cases := []struct {
		n    int
		unit CalendarUnit
		want string
	}{
		{1, Day, "2024-03-01T00:00:00Z"},
		{2, Week, "2024-03-14T00:00:00Z"},
		{3, Month, "2024-05-29T00:00:00Z"},
		{1, Year, "2025-02-28T00:00:00Z"}, // leap day clamps
	}
	for _, c := range cases {
		tr, err := EveryCalendar(c.n, c.unit, WithStart(start))
		if err != nil {
			t.Fatal(err)
		}
		got, ok := tr.Next(start)
		if !ok || !got.Equal(mustTime(t, c.want)) {
			t.Errorf("EveryCalendar(%d,%s).Next = %v want %s", c.n, c.unit, got, c.want)
		}
	}
	// far in the future: exercises the estimate-then-step search
	tr, _ := EveryCalendar(1, Month, WithStart(start))
	got, _ := tr.Next(mustTime(t, "2126-06-15T00:00:00Z"))
	if !got.Equal(mustTime(t, "2126-06-29T00:00:00Z")) {
		t.Fatalf("got %v", got)
	}
}

func TestCalendarIntervalKeepsWallClockAcrossDST(t *testing.T) {
	t.Parallel()
	ny := mustLoc(t, "America/New_York")
	start := time.Date(2026, 3, 7, 9, 0, 0, 0, ny)
	tr, _ := EveryCalendar(1, Day, WithStart(start), WithLocation(ny))
	got, _ := tr.Next(start)
	if want := mustTime(t, "2026-03-08T09:00:00-04:00"); !got.Equal(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	// an interval trigger, by contrast, is exactly 24h of elapsed time
	iv, _ := Every(24*time.Hour, WithStart(start))
	got, _ = iv.Next(start)
	if want := mustTime(t, "2026-03-08T10:00:00-04:00"); !got.Equal(want) {
		t.Fatalf("interval got %v want %v", got, want)
	}
}

func TestTriggerSpecRoundTrip(t *testing.T) {
	t.Parallel()
	ny := mustLoc(t, "America/New_York")
	start := mustTime(t, "2026-01-01T00:00:00Z")
	mk := []func() (Trigger, error){
		func() (Trigger, error) { return Cron("0 0 12 * * ?", WithLocation(ny), WithStart(start)) },
		func() (Trigger, error) { return Every(90*time.Second, WithStart(start)) },
		func() (Trigger, error) { return EveryCalendar(2, Week, WithStart(start), WithLocation(ny)) },
		func() (Trigger, error) { return Once(start) },
	}
	for _, f := range mk {
		tr, err := f()
		if err != nil {
			t.Fatal(err)
		}
		b, err := json.Marshal(tr.Spec())
		if err != nil {
			t.Fatal(err)
		}
		var sp TriggerSpec
		if err := json.Unmarshal(b, &sp); err != nil {
			t.Fatal(err)
		}
		tr2, err := NewTrigger(sp)
		if err != nil {
			t.Fatalf("%s: %v", b, err)
		}
		a, _ := tr.Next(start.Add(-time.Hour))
		c, _ := tr2.Next(start.Add(-time.Hour))
		if !a.Equal(c) {
			t.Errorf("%s: round trip changed next fire: %v vs %v", b, a, c)
		}
	}
}

func TestTriggerValidation(t *testing.T) {
	t.Parallel()
	now := time.Now()
	bad := []TriggerSpec{
		{Type: "nope"},
		{Type: TriggerCron, Cron: "not cron"},
		{Type: TriggerInterval, Every: "abc"},
		{Type: TriggerInterval, Every: "-1s"},
		{Type: TriggerCalendar, N: 0, Unit: Day},
		{Type: TriggerCalendar, N: 1, Unit: "fortnight"},
		{Type: TriggerOnce},
		{Type: TriggerCron, Cron: "* * * * *", Location: "Mars/Olympus"},
		{Type: TriggerCron, Cron: "* * * * *", Start: now, End: now.Add(-time.Hour)},
	}
	for _, sp := range bad {
		_, err := NewTrigger(sp)
		if err == nil {
			t.Errorf("NewTrigger(%+v) succeeded", sp)
			continue
		}
		if !errors.Is(err, ErrInvalidTrigger) && !errors.Is(err, ErrInvalidCron) {
			t.Errorf("NewTrigger(%+v) err = %v, want a sentinel", sp, err)
		}
	}
	if _, err := Cron("bad"); !errors.Is(err, ErrInvalidCron) {
		t.Errorf("err = %v", err)
	}
}

func TestNormalizeAndRebase(t *testing.T) {
	t.Parallel()
	now := mustTime(t, "2026-01-01T00:00:00Z")
	iv, _ := Every(time.Hour)
	n, err := normalize(iv, now)
	if err != nil {
		t.Fatal(err)
	}
	if got := n.Spec().Start; !got.Equal(now.Add(time.Hour)) {
		t.Fatalf("anchor = %v", got)
	}
	// already anchored: unchanged
	n2, _ := normalize(n, now.Add(time.Hour))
	if !n2.Spec().Start.Equal(n.Spec().Start) {
		t.Fatal("normalize changed an existing anchor")
	}
	cr, _ := Cron("* * * * * ?")
	if got, _ := normalize(cr, now); got != cr {
		t.Fatal("cron must be untouched")
	}
	later := now.Add(10 * time.Hour)
	rb, err := rebase(n, later)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := rb.Next(later); !got.Equal(later.Add(time.Hour)) {
		t.Fatalf("rebased next = %v", got)
	}
	if got, _ := rebase(cr, later); got != cr {
		t.Fatal("cron must not be rebased")
	}
}

func TestCalendars(t *testing.T) {
	t.Parallel()
	ny := mustLoc(t, "America/New_York")
	xmas := time.Date(2026, 12, 25, 0, 0, 0, 0, ny)
	hol := NewHolidayCalendar(ny, xmas)
	if hol.Included(time.Date(2026, 12, 25, 23, 59, 0, 0, ny)) {
		t.Error("holiday must be excluded all day")
	}
	if !hol.Included(time.Date(2026, 12, 26, 0, 0, 0, 0, ny)) {
		t.Error("day after must be included")
	}
	// the same instant judged in the calendar's zone, not UTC
	if hol.Included(mustTime(t, "2026-12-26T03:00:00Z")) { // 22:00 Dec 25 in New York
		t.Error("zone must be the calendar's")
	}

	wk := NewWeeklyCalendar(nil, time.Saturday, time.Sunday)
	if wk.Included(mustTime(t, "2026-09-19T12:00:00Z")) || wk.Included(mustTime(t, "2026-09-20T12:00:00Z")) {
		t.Error("weekend must be excluded")
	}
	if !wk.Included(mustTime(t, "2026-09-21T12:00:00Z")) {
		t.Error("Monday must be included")
	}

	an := NewAnnualCalendar(nil, MonthDay{time.January, 1}, MonthDay{time.December, 25})
	if an.Included(mustTime(t, "2031-01-01T05:00:00Z")) || !an.Included(mustTime(t, "2031-01-02T05:00:00Z")) {
		t.Error("annual calendar wrong")
	}

	day := NewDailyCalendar(nil, TimeOfDay{9, 0, 0}, TimeOfDay{17, 0, 0})
	for h, want := range map[int]bool{8: true, 9: false, 16: false, 17: true} {
		if got := day.Included(time.Date(2026, 1, 1, h, 0, 0, 0, time.UTC)); got != want {
			t.Errorf("daily %02d:00 = %v want %v", h, got, want)
		}
	}
	night := NewDailyCalendar(nil, TimeOfDay{22, 0, 0}, TimeOfDay{6, 0, 0}) // wraps midnight
	for h, want := range map[int]bool{21: true, 22: false, 3: false, 6: true, 12: true} {
		if got := night.Included(time.Date(2026, 1, 1, h, 0, 0, 0, time.UTC)); got != want {
			t.Errorf("night %02d:00 = %v want %v", h, got, want)
		}
	}

	cc, err := NewCronCalendar("* * 9-11 * * ?", nil)
	if err != nil {
		t.Fatal(err)
	}
	if cc.Included(mustTime(t, "2026-01-01T10:30:15Z")) || !cc.Included(mustTime(t, "2026-01-01T12:00:00Z")) {
		t.Error("cron calendar wrong")
	}
	if _, err := NewCronCalendar("bad", nil); !errors.Is(err, ErrInvalidCron) {
		t.Errorf("err = %v", err)
	}

	fn := CalendarFunc(func(t time.Time) bool { return t.Hour() != 3 })
	if fn.Included(mustTime(t, "2026-01-01T03:00:00Z")) {
		t.Error("CalendarFunc")
	}
}
