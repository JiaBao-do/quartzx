package quartzx

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// maxCronYear bounds the search for the next fire time.
const maxCronYear = 2199

// CronSchedule is a parsed cron expression. It is immutable after
// ParseCron and safe for concurrent use by multiple goroutines.
//
// Supported syntax (Quartz compatible):
//
//	seconds minutes hours day-of-month month day-of-week [year]
//
// A five field expression (minutes hours day-of-month month day-of-week)
// is treated as classic cron: seconds are fixed at 0, day-of-week is 0-6
// (0 and 7 are Sunday) and, when both day fields are restricted, a day
// matches if either field matches. In six and seven field expressions
// day-of-week is 1-7 (1 = Sunday) and restricting both day fields is an
// error, as in Quartz; use ? in one of them.
//
// Special characters: * , - / ? L W # as in Quartz, including L-n,
// LW, nW, nL and n#m. Month and weekday names (JAN, MON) are case
// insensitive. The descriptors @yearly, @annually, @monthly, @weekly,
// @daily, @midnight and @hourly are accepted.
//
// Daylight saving time: fields are matched against wall clock time in the
// schedule's location. A wall time that does not exist (spring forward gap)
// is skipped. A wall time that occurs twice (fall back) fires on each
// occurrence.
type CronSchedule struct {
	expr  string
	loc   *time.Location
	sec   uint64
	min   uint64
	hour  uint32
	month uint16
	dom   domSpec
	dow   dowSpec
	years []yearRange
	or    bool // classic cron day semantics: dom OR dow
}

type domSpec struct {
	any        bool
	bits       uint32 // bit d = day d
	last       bool   // L
	lastOffset []int  // L-n
	lastWeekd  bool   // LW
	nearest    []int  // nW
}

type dowSpec struct {
	any  bool
	bits uint8 // bit 0 = Sunday
	nth  []nthDow
	last []int // nL: last weekday n of month
}

type nthDow struct{ dow, n int }

type yearRange struct{ lo, hi, step int }

var monthNames = map[string]int{
	"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
	"JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

var dowNames = map[string]int{
	"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6,
}

var descriptors = map[string]string{
	"@yearly":   "0 0 0 1 1 ?",
	"@annually": "0 0 0 1 1 ?",
	"@monthly":  "0 0 0 1 * ?",
	"@weekly":   "0 0 0 ? * SUN",
	"@daily":    "0 0 0 * * ?",
	"@midnight": "0 0 0 * * ?",
	"@hourly":   "0 0 * * * ?",
}

// ParseCron parses expr and returns a schedule evaluated in UTC. Use
// [CronSchedule.In] to evaluate it in another location.
// Errors wrap [ErrInvalidCron].
func ParseCron(expr string) (*CronSchedule, error) {
	c, err := parseCron(expr)
	if err != nil {
		return nil, fmt.Errorf("%w: %q: %w", ErrInvalidCron, expr, err)
	}
	return c, nil
}

// In returns a copy of the schedule that is evaluated in loc.
func (c *CronSchedule) In(loc *time.Location) *CronSchedule {
	cp := *c
	if loc == nil {
		loc = time.UTC
	}
	cp.loc = loc
	return &cp
}

// String returns the expression the schedule was parsed from.
func (c *CronSchedule) String() string { return c.expr }

func parseCron(expr string) (*CronSchedule, error) {
	orig := strings.TrimSpace(expr)
	e := orig
	if strings.HasPrefix(e, "@") {
		d, ok := descriptors[strings.ToLower(e)]
		if !ok {
			return nil, fmt.Errorf("unknown descriptor")
		}
		e = d
	}
	f := strings.Fields(e)
	c := &CronSchedule{expr: orig, loc: time.UTC}
	classic := false
	switch len(f) {
	case 5:
		classic = true
		f = append([]string{"0"}, f...)
	case 6:
	case 7:
	default:
		return nil, fmt.Errorf("expected 5, 6 or 7 fields, got %d", len(f))
	}
	var err error
	if c.sec, err = simpleField(f[0], 0, 59, nil, "seconds"); err != nil {
		return nil, err
	}
	if c.min, err = simpleField(f[1], 0, 59, nil, "minutes"); err != nil {
		return nil, err
	}
	h, err := simpleField(f[2], 0, 23, nil, "hours")
	if err != nil {
		return nil, err
	}
	c.hour = uint32(h)
	m, err := simpleField(f[4], 1, 12, monthNames, "month")
	if err != nil {
		return nil, err
	}
	c.month = uint16(m)
	if c.dom, err = parseDom(f[3]); err != nil {
		return nil, err
	}
	if c.dow, err = parseDow(f[5], classic); err != nil {
		return nil, err
	}
	if len(f) == 7 {
		if c.years, err = parseYears(f[6]); err != nil {
			return nil, err
		}
	}
	if !c.dom.any && !c.dow.any {
		if !classic {
			return nil, fmt.Errorf("day-of-month and day-of-week cannot both be restricted; use ? in one")
		}
		c.or = true
	}
	return c, nil
}

func parseNum(s string, names map[string]int) (int, error) {
	if v, ok := names[strings.ToUpper(s)]; ok {
		return v, nil
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < 0 {
		return 0, fmt.Errorf("bad value %q", s)
	}
	return v, nil
}

// rangeItem parses "*", "a", "a-b", each optionally followed by "/step",
// and returns the values it denotes.
func rangeItem(item string, lo, hi int, names map[string]int, allowQ bool) ([]int, error) {
	step := 1
	hasStep := false
	if i := strings.IndexByte(item, '/'); i >= 0 {
		s, err := strconv.Atoi(item[i+1:])
		if err != nil || s <= 0 {
			return nil, fmt.Errorf("bad step in %q", item)
		}
		step, hasStep = s, true
		item = item[:i]
	}
	var a, b int
	switch {
	case item == "*" || (allowQ && item == "?"):
		if item == "?" && hasStep {
			return nil, fmt.Errorf("? cannot have a step")
		}
		a, b = lo, hi
	case strings.Contains(item, "-"):
		p := strings.SplitN(item, "-", 2)
		var err error
		if a, err = parseNum(p[0], names); err != nil {
			return nil, err
		}
		if b, err = parseNum(p[1], names); err != nil {
			return nil, err
		}
	default:
		var err error
		if a, err = parseNum(item, names); err != nil {
			return nil, err
		}
		b = a
		if hasStep {
			b = hi
		}
	}
	if a < lo || b > hi || a > b {
		return nil, fmt.Errorf("value out of range %d-%d in %q", lo, hi, item)
	}
	var out []int
	for v := a; v <= b; v += step {
		out = append(out, v)
	}
	return out, nil
}

func simpleField(s string, lo, hi int, names map[string]int, what string) (uint64, error) {
	var bits uint64
	for _, item := range strings.Split(s, ",") {
		vals, err := rangeItem(item, lo, hi, names, false)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", what, err)
		}
		for _, v := range vals {
			bits |= 1 << uint(v)
		}
	}
	return bits, nil
}

func parseDom(s string) (domSpec, error) {
	var d domSpec
	if s == "*" || s == "?" {
		d.any = true
		return d, nil
	}
	for _, item := range strings.Split(s, ",") {
		u := strings.ToUpper(item)
		switch {
		case u == "L":
			d.last = true
		case u == "LW":
			d.lastWeekd = true
		case strings.HasPrefix(u, "L-"):
			n, err := strconv.Atoi(u[2:])
			if err != nil || n < 1 || n > 30 {
				return d, fmt.Errorf("day-of-month: bad %q", item)
			}
			d.lastOffset = append(d.lastOffset, n)
		case strings.HasSuffix(u, "W"):
			n, err := strconv.Atoi(u[:len(u)-1])
			if err != nil || n < 1 || n > 31 {
				return d, fmt.Errorf("day-of-month: bad %q", item)
			}
			d.nearest = append(d.nearest, n)
		default:
			vals, err := rangeItem(item, 1, 31, nil, false)
			if err != nil {
				return d, fmt.Errorf("day-of-month: %w", err)
			}
			for _, v := range vals {
				d.bits |= 1 << uint(v)
			}
		}
	}
	return d, nil
}

func parseDow(s string, classic bool) (dowSpec, error) {
	var d dowSpec
	if s == "*" || s == "?" {
		d.any = true
		return d, nil
	}
	lo, hi := 1, 7
	if classic {
		lo, hi = 0, 7
	}
	norm := func(v int) int { // to 0=Sunday
		if classic {
			return v % 7
		}
		return v - 1
	}
	names := map[string]int{}
	for k, v := range dowNames {
		if classic {
			names[k] = v
		} else {
			names[k] = v + 1
		}
	}
	for _, item := range strings.Split(s, ",") {
		u := strings.ToUpper(item)
		switch {
		case u == "L" && !classic:
			d.last = append(d.last, 6) // last day of the week: Saturday
		case strings.Contains(u, "#"):
			p := strings.SplitN(u, "#", 2)
			w, err := parseNum(p[0], names)
			n, err2 := strconv.Atoi(p[1])
			if err != nil || err2 != nil || w < lo || w > hi || n < 1 || n > 5 {
				return d, fmt.Errorf("day-of-week: bad %q", item)
			}
			d.nth = append(d.nth, nthDow{norm(w), n})
		case len(u) > 1 && strings.HasSuffix(u, "L"):
			w, err := parseNum(u[:len(u)-1], names)
			if err != nil || w < lo || w > hi {
				return d, fmt.Errorf("day-of-week: bad %q", item)
			}
			d.last = append(d.last, norm(w))
		default:
			vals, err := rangeItem(item, lo, hi, names, false)
			if err != nil {
				return d, fmt.Errorf("day-of-week: %w", err)
			}
			for _, v := range vals {
				d.bits |= 1 << uint(norm(v))
			}
		}
	}
	return d, nil
}

func parseYears(s string) ([]yearRange, error) {
	if s == "*" {
		return nil, nil
	}
	var out []yearRange
	for _, item := range strings.Split(s, ",") {
		step := 1
		if i := strings.IndexByte(item, '/'); i >= 0 {
			n, err := strconv.Atoi(item[i+1:])
			if err != nil || n <= 0 {
				return nil, fmt.Errorf("year: bad step in %q", item)
			}
			step, item = n, item[:i]
		}
		lo, hi := 0, 0
		var err error
		switch {
		case item == "*":
			lo, hi = 1970, maxCronYear
		case strings.Contains(item, "-"):
			p := strings.SplitN(item, "-", 2)
			if lo, err = strconv.Atoi(p[0]); err == nil {
				hi, err = strconv.Atoi(p[1])
			}
		default:
			if lo, err = strconv.Atoi(item); err == nil {
				hi = lo
				if step > 1 {
					hi = maxCronYear
				}
			}
		}
		if err != nil || lo < 1970 || hi > maxCronYear || lo > hi {
			return nil, fmt.Errorf("year: bad %q", item)
		}
		out = append(out, yearRange{lo, hi, step})
	}
	return out, nil
}

func (c *CronSchedule) yearOK(y int) bool {
	if len(c.years) == 0 {
		return true
	}
	for _, r := range c.years {
		if y >= r.lo && y <= r.hi && (y-r.lo)%r.step == 0 {
			return true
		}
	}
	return false
}

func daysIn(y, m int) int {
	return time.Date(y, time.Month(m)+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// matchingDays returns the days of month m in year y (ascending) on which the
// day fields match, as a bit set (bit d = day d).
func (c *CronSchedule) matchingDays(y, m int) uint32 {
	n := daysIn(y, m)
	all := uint32(1<<uint(n+1)) - 2 // bits 1..n
	if c.dom.any && c.dow.any {
		return all
	}
	wd := int(time.Date(y, time.Month(m), 1, 0, 0, 0, 0, time.UTC).Weekday())
	weekday := func(d int) int { return (wd + d - 1) % 7 }
	var dm, dw uint32
	if !c.dom.any {
		dm = c.dom.bits & all
		if c.dom.last {
			dm |= 1 << uint(n)
		}
		for _, k := range c.dom.lastOffset {
			if n-k >= 1 {
				dm |= 1 << uint(n-k)
			}
		}
		if c.dom.lastWeekd {
			d := n
			for weekday(d) == 0 || weekday(d) == 6 {
				d--
			}
			dm |= 1 << uint(d)
		}
		for _, d := range c.dom.nearest {
			if d > n {
				continue
			}
			t := d
			switch weekday(d) {
			case 6: // Saturday
				if d > 1 {
					t = d - 1
				} else {
					t = d + 2
				}
			case 0: // Sunday
				if d < n {
					t = d + 1
				} else {
					t = d - 2
				}
			}
			dm |= 1 << uint(t)
		}
	}
	if !c.dow.any {
		for d := 1; d <= n; d++ {
			w := weekday(d)
			if c.dow.bits&(1<<uint(w)) != 0 {
				dw |= 1 << uint(d)
			}
			for _, x := range c.dow.nth {
				if x.dow == w && (d-1)/7+1 == x.n {
					dw |= 1 << uint(d)
				}
			}
			for _, x := range c.dow.last {
				if x == w && d+7 > n {
					dw |= 1 << uint(d)
				}
			}
		}
	}
	switch {
	case c.dom.any:
		return dw
	case c.dow.any:
		return dm
	case c.or:
		return dm | dw
	default:
		return dm & dw
	}
}

// Next returns the first fire time strictly after the given time, or false
// if the schedule has no further fire times before year 2200.
// The result is expressed in the schedule's location.
func (c *CronSchedule) Next(after time.Time) (time.Time, bool) {
	loc := c.loc
	if loc == nil {
		loc = time.UTC
	}
	after = after.Truncate(time.Second)
	// Near a zone offset change (within 3 hours either side) scan from 3
	// hours earlier so the second occurrence of a repeated wall time that lies
	// after the given time is not missed. Otherwise start right after it.
	start := after.Add(time.Second).In(loc)
	_, offBack := after.Add(-3 * time.Hour).In(loc).Zone()
	_, offNow := start.Zone()
	_, offFwd := after.Add(3 * time.Hour).In(loc).Zone()
	if offBack != offNow || offNow != offFwd {
		start = after.Add(-3 * time.Hour).In(loc)
	}
	sy, sm, sd := start.Date()
	sh, smi, ss := start.Clock()

	var best time.Time
	haveBest := false
	consider := func(cand time.Time) (time.Time, bool) {
		if cand.After(after) {
			if haveBest && best.Before(cand) {
				return best, true
			}
			return cand, true
		}
		return time.Time{}, false
	}

	for y := sy; y <= maxCronYear; y++ {
		if !c.yearOK(y) {
			continue
		}
		sameY := y == sy
		for m := 1; m <= 12; m++ {
			if c.month&(1<<uint(m)) == 0 || (sameY && m < int(sm)) {
				continue
			}
			sameM := sameY && m == int(sm)
			days := c.matchingDays(y, m)
			for d := 1; d <= 31; d++ {
				if days&(1<<uint(d)) == 0 || (sameM && d < sd) {
					continue
				}
				sameD := sameM && d == sd
				for h := 0; h < 24; h++ {
					if c.hour&(1<<uint(h)) == 0 || (sameD && h < sh) {
						continue
					}
					sameH := sameD && h == sh
					for mi := 0; mi < 60; mi++ {
						if c.min&(1<<uint(mi)) == 0 || (sameH && mi < smi) {
							continue
						}
						sameMi := sameH && mi == smi
						for s := 0; s < 60; s++ {
							if c.sec&(1<<uint(s)) == 0 || (sameMi && s < ss) {
								continue
							}
							c1 := time.Date(y, time.Month(m), d, h, mi, s, 0, loc)
							if c1.Hour() != h || c1.Minute() != mi || c1.Day() != d {
								continue // nonexistent wall time (DST gap)
							}
							// Second occurrence during a fall back overlap.
							_, off := c1.Zone()
							if _, offP := c1.Add(3 * time.Hour).Zone(); offP < off {
								alt := c1.Add(time.Duration(off-offP) * time.Second)
								if alt.Hour() == h && alt.Minute() == mi && alt.Day() == d && alt.After(after) {
									if !haveBest || alt.Before(best) {
										best, haveBest = alt, true
									}
								}
							}
							if r, ok := consider(c1); ok {
								return r, true
							}
						}
					}
				}
			}
		}
	}
	return best, haveBest
}
