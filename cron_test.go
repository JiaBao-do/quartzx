package quartzx

import (
	"errors"
	"testing"
	"time"
	_ "time/tzdata" // zone data for Windows and minimal containers
)

func mustLoc(t testing.TB, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

func mustTime(t testing.TB, s string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// TestCronNextTable is the reference table: expression, zone, start, and the
// exact fire times that follow (RFC 3339, so offsets prove DST handling).
func TestCronNextTable(t *testing.T) {
	ny := "America/New_York"
	tests := []struct {
		name  string
		expr  string
		zone  string
		after string
		want  []string
	}{
		{"daily noon", "0 0 12 * * ?", "UTC", "2026-01-01T00:00:00Z",
			[]string{"2026-01-01T12:00:00Z", "2026-01-02T12:00:00Z"}},
		{"strictly after", "0 0 12 * * ?", "UTC", "2026-01-01T12:00:00Z",
			[]string{"2026-01-02T12:00:00Z"}},
		{"sub-second after", "* * * * * ?", "UTC", "2026-01-01T00:00:00.5Z",
			[]string{"2026-01-01T00:00:01Z"}},
		{"every 15 min classic", "*/15 * * * *", "UTC", "2026-01-01T10:07:00Z",
			[]string{"2026-01-01T10:15:00Z", "2026-01-01T10:30:00Z", "2026-01-01T10:45:00Z", "2026-01-01T11:00:00Z"}},
		{"seconds step", "0/20 * * * * ?", "UTC", "2026-01-01T00:00:00Z",
			[]string{"2026-01-01T00:00:20Z", "2026-01-01T00:00:40Z", "2026-01-01T00:01:00Z"}},
		{"last day of month", "0 0 0 L * ?", "UTC", "2026-01-01T00:00:00Z",
			[]string{"2026-01-31T00:00:00Z", "2026-02-28T00:00:00Z", "2026-03-31T00:00:00Z"}},
		{"third to last day", "0 0 0 L-3 * ?", "UTC", "2026-02-01T00:00:00Z",
			[]string{"2026-02-25T00:00:00Z", "2026-03-28T00:00:00Z"}},
		{"nearest weekday to 15th", "0 0 9 15W * ?", "UTC", "2026-02-01T00:00:00Z",
			// Feb 15 Sun -> Mon 16; Mar 15 Sun -> Mon 16; Apr 15 Wed stays
			[]string{"2026-02-16T09:00:00Z", "2026-03-16T09:00:00Z", "2026-04-15T09:00:00Z"}},
		{"1W on Saturday moves to Monday", "0 0 9 1W * ?", "UTC", "2026-07-15T00:00:00Z",
			[]string{"2026-08-03T09:00:00Z", "2026-09-01T09:00:00Z"}},
		{"15W on Saturday moves to Friday", "0 0 9 15W * ?", "UTC", "2026-08-01T00:00:00Z",
			[]string{"2026-08-14T09:00:00Z"}},
		{"last weekday", "0 0 0 LW * ?", "UTC", "2026-02-01T00:00:00Z",
			// Feb 28 2026 is Saturday -> Fri 27; Mar 31 Tue; Aug 31 Mon
			[]string{"2026-02-27T00:00:00Z", "2026-03-31T00:00:00Z"}},
		{"third friday", "0 0 0 ? * 6#3", "UTC", "2026-09-01T00:00:00Z",
			[]string{"2026-09-18T00:00:00Z", "2026-10-16T00:00:00Z"}},
		{"last friday", "0 0 0 ? * FRIL", "UTC", "2026-09-01T00:00:00Z",
			[]string{"2026-09-25T00:00:00Z", "2026-10-30T00:00:00Z"}},
		{"fifth monday only when it exists", "0 0 0 ? * MON#5", "UTC", "2026-01-01T00:00:00Z",
			[]string{"2026-03-30T00:00:00Z", "2026-06-29T00:00:00Z"}},
		{"weekdays", "0 0 9 ? * MON-FRI", "UTC", "2026-09-04T10:00:00Z", // Fri
			[]string{"2026-09-07T09:00:00Z", "2026-09-08T09:00:00Z"}},
		{"quartz dow 1 is sunday", "0 0 9 ? * 1", "UTC", "2026-09-01T00:00:00Z",
			[]string{"2026-09-06T09:00:00Z"}},
		{"classic dow 0 is sunday", "0 9 * * 0", "UTC", "2026-09-01T00:00:00Z",
			[]string{"2026-09-06T09:00:00Z"}},
		{"classic dow 7 is sunday", "0 9 * * 7", "UTC", "2026-09-01T00:00:00Z",
			[]string{"2026-09-06T09:00:00Z"}},
		{"year field", "0 0 0 1 1 ? 2030", "UTC", "2026-01-01T00:00:00Z",
			[]string{"2030-01-01T00:00:00Z"}},
		{"year step", "0 0 0 1 1 ? 2026/2", "UTC", "2026-01-01T00:00:00Z",
			[]string{"2028-01-01T00:00:00Z", "2030-01-01T00:00:00Z"}},
		{"leap day", "0 0 0 29 2 ?", "UTC", "2026-01-01T00:00:00Z",
			[]string{"2028-02-29T00:00:00Z", "2032-02-29T00:00:00Z"}},
		{"classic day OR semantics", "0 13 13 * 5", "UTC", "2026-02-01T00:00:00Z", // Sun; fri or 13th
			[]string{"2026-02-06T13:00:00Z", "2026-02-13T13:00:00Z", "2026-02-20T13:00:00Z"}},
		{"names are case insensitive", "0 0 0 1 jan-mar ?", "UTC", "2026-01-01T00:00:00Z",
			[]string{"2026-02-01T00:00:00Z", "2026-03-01T00:00:00Z"}},
		{"descriptor hourly", "@hourly", "UTC", "2026-01-01T00:30:00Z",
			[]string{"2026-01-01T01:00:00Z"}},
		{"descriptor weekly", "@weekly", "UTC", "2026-09-02T00:00:00Z",
			[]string{"2026-09-06T00:00:00Z"}},

		// DST, America/New_York 2026: spring forward Mar 8 02:00->03:00 (EST -5 -> EDT -4),
		// fall back Nov 1 02:00->01:00 (EDT -4 -> EST -5).
		{"daily 02:30 skips the missing wall time", "0 30 2 * * ?", ny, "2026-03-07T12:00:00-05:00",
			[]string{"2026-03-09T02:30:00-04:00"}},
		{"daily 12:00 keeps wall clock across spring forward", "0 0 12 * * ?", ny, "2026-03-07T13:00:00-05:00",
			[]string{"2026-03-08T12:00:00-04:00", "2026-03-09T12:00:00-04:00"}},
		{"hourly across spring forward", "0 0 * * * ?", ny, "2026-03-08T00:30:00-05:00",
			[]string{"2026-03-08T01:00:00-05:00", "2026-03-08T03:00:00-04:00", "2026-03-08T04:00:00-04:00"}},
		{"hourly across fall back fires in both hours", "0 0 * * * ?", ny, "2026-11-01T00:30:00-04:00",
			[]string{"2026-11-01T01:00:00-04:00", "2026-11-01T01:00:00-05:00", "2026-11-01T02:00:00-05:00"}},
		{"daily 01:30 fires twice on fall back day", "0 30 1 * * ?", ny, "2026-11-01T00:00:00-04:00",
			[]string{"2026-11-01T01:30:00-04:00", "2026-11-01T01:30:00-05:00", "2026-11-02T01:30:00-05:00"}},
		{"second occurrence found when starting inside the overlap", "0 30 1 * * ?", ny, "2026-11-01T01:45:00-04:00",
			[]string{"2026-11-01T01:30:00-05:00", "2026-11-02T01:30:00-05:00"}},
		{"first occurrence found while inside the overlap", "0 50 1 * * ?", ny, "2026-11-01T01:45:00-04:00",
			[]string{"2026-11-01T01:50:00-04:00", "2026-11-01T01:50:00-05:00"}},
		{"every 30 minutes across fall back", "0 0/30 * * * ?", ny, "2026-11-01T00:45:00-04:00",
			[]string{"2026-11-01T01:00:00-04:00", "2026-11-01T01:30:00-04:00", "2026-11-01T01:00:00-05:00", "2026-11-01T01:30:00-05:00", "2026-11-01T02:00:00-05:00"}},
		{"Europe/London spring forward gap 01:30 skipped", "0 30 1 * * ?", "Europe/London", "2026-03-28T12:00:00Z",
			[]string{"2026-03-30T01:30:00+01:00"}},
		{"zone without DST", "0 0 6 * * ?", "Asia/Kolkata", "2026-01-01T00:00:00Z",
			[]string{"2026-01-01T06:00:00+05:30"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, err := ParseCron(tc.expr)
			if err != nil {
				t.Fatal(err)
			}
			c = c.In(mustLoc(t, tc.zone))
			cur := mustTime(t, tc.after)
			for i, w := range tc.want {
				got, ok := c.Next(cur)
				if !ok {
					t.Fatalf("fire %d: no next after %v", i, cur)
				}
				if want := mustTime(t, w); !got.Equal(want) {
					t.Fatalf("fire %d after %v:\n got  %v\n want %v", i, cur, got.Format(time.RFC3339), w)
				}
				cur = got
			}
		})
	}
}

func TestCronExhausted(t *testing.T) {
	t.Parallel()
	c, err := ParseCron("0 0 0 1 1 ? 2026")
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := c.Next(mustTime(t, "2026-01-01T00:00:00Z")); ok {
		t.Fatalf("expected exhausted, got %v", got)
	}
	// impossible date: Feb 31
	c, err = ParseCron("0 0 0 31 2 ?")
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := c.Next(mustTime(t, "2026-01-01T00:00:00Z")); ok {
		t.Fatalf("Feb 31 should never fire, got %v", got)
	}
}

func TestCronParseErrors(t *testing.T) {
	t.Parallel()
	bad := []string{
		"", "* * *", "* * * * * * * *", "60 * * * * ?", "* 60 * * * ?", "* * 24 * * ?", "* * * 0 * ?",
		"* * * 32 * ?", "* * * * 13 ?", "* * * * * 8", "* * * * * 0", "0 0 0 1 1 6 2026", // both day fields restricted
		"* * * * * ? 1969", "@nope", "0/0 * * * * ?", "5-2 * * * * ?", "a * * * * ?", "* * * L-0 * ?",
		"* * * 40W * ?", "* * * ? * 6#9", "* * * ? * 9L", "* * * ? ?/2 ?", "-1 * * * * ?",
		"0 0 0 ?/2 * *", "0 0 0 1 * ? 2026-2020",
	}
	for _, e := range bad {
		if _, err := ParseCron(e); !errors.Is(err, ErrInvalidCron) {
			t.Errorf("ParseCron(%q) err = %v, want ErrInvalidCron", e, err)
		}
	}
}

func TestCronStringAndNilLocation(t *testing.T) {
	t.Parallel()
	c, err := ParseCron("  @daily ")
	if err != nil {
		t.Fatal(err)
	}
	if c.String() != "@daily" {
		t.Fatalf("String = %q", c.String())
	}
	n, ok := c.In(nil).Next(mustTime(t, "2026-01-01T00:00:00Z"))
	if !ok || n.Location() != time.UTC {
		t.Fatalf("got %v %v", n, ok)
	}
}

// TestCronMonotonic checks, across whole years in DST zones, that fire times
// are strictly increasing and that every result satisfies the expression.
func TestCronMonotonic(t *testing.T) {
	t.Parallel()
	zones := []string{"America/New_York", "Europe/London", "Australia/Lord_Howe", "America/Sao_Paulo", "UTC"}
	exprs := []string{"0 0/20 * * * ?", "0 15 * * * ?", "0 30 1 * * ?", "0 0 2 * * ?", "*/7 * * * * *"}
	for _, z := range zones {
		loc := mustLoc(t, z)
		for _, e := range exprs {
			c, _ := ParseCron(e)
			c = c.In(loc)
			cur := time.Date(2026, 1, 1, 0, 0, 0, 0, loc)
			end := cur.AddDate(1, 0, 0)
			if e == "*/7 * * * * *" {
				end = cur.AddDate(0, 0, 1)
			}
			n := 0
			for cur.Before(end) {
				nx, ok := c.Next(cur)
				if !ok || !nx.After(cur) {
					t.Fatalf("%s %s: Next(%v) = %v,%v not increasing", z, e, cur, nx, ok)
				}
				if e == "0 0/20 * * * ?" && (nx.Minute()%20 != 0 || nx.Second() != 0) {
					t.Fatalf("%s %s: %v violates fields", z, e, nx)
				}
				cur = nx
				n++
			}
			if n == 0 {
				t.Fatalf("%s %s: no fires", z, e)
			}
		}
	}
}
