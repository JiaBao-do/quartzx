package quartzx

import (
	"testing"
	"time"
)

// FuzzParseCron checks that arbitrary input never panics and that anything
// accepted yields strictly increasing fire times.
func FuzzParseCron(f *testing.F) {
	for _, s := range []string{"* * * * * ?", "0 0 12 * * ?", "*/5 * * * *", "0 0 0 L-3 * ?", "0 0 0 15W * ?",
		"0 0 0 ? * 6#3", "0 0 0 ? * FRIL", "0 0 0 1 1 ? 2030", "@daily", "0/0 * * * * ?", "a-b/c d e f g h"} {
		f.Add(s)
	}
	base := time.Date(2026, 3, 7, 12, 0, 0, 0, time.UTC)
	ny, _ := time.LoadLocation("America/New_York")
	f.Fuzz(func(t *testing.T, expr string) {
		c, err := ParseCron(expr)
		if err != nil {
			return
		}
		for _, loc := range []*time.Location{time.UTC, ny} {
			cur := base
			z := c.In(loc)
			for range 5 {
				next, ok := z.Next(cur)
				if !ok {
					break
				}
				if !next.After(cur) {
					t.Fatalf("%q: Next(%v) = %v not increasing", expr, cur, next)
				}
				cur = next
			}
		}
	})
}
