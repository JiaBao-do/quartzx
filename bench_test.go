package quartzx

import (
	"context"
	"testing"
	"time"
)

func BenchmarkCronNext(b *testing.B) {
	c, _ := ParseCron("0 30 9 ? * MON-FRI")
	t := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b.ReportAllocs()
	for b.Loop() {
		t, _ = c.Next(t)
	}
}

func BenchmarkCronNextDST(b *testing.B) {
	loc, _ := time.LoadLocation("America/New_York")
	c, _ := ParseCron("0 0/20 * * * ?")
	c = c.In(loc)
	t := time.Date(2026, 1, 1, 0, 0, 0, 0, loc)
	b.ReportAllocs()
	for b.Loop() {
		t, _ = c.Next(t)
	}
}

func BenchmarkParseCron(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_, _ = ParseCron("0 0/15 8-18 ? * MON-FRI")
	}
}

func BenchmarkMemoryStoreSave(b *testing.B) {
	s := NewMemoryStore()
	r := sampleRecord("k")
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_ = s.Save(ctx, r)
	}
}
