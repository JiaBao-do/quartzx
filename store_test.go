package quartzx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func sampleRecord(key string) JobRecord {
	return JobRecord{
		Key: key, Handler: "h",
		Trigger:   TriggerSpec{Type: TriggerCron, Cron: "0 0 12 * * ?", Location: "UTC"},
		Data:      map[string]any{"n": 1, "s": "x", "nested": map[string]any{"a": []any{1, "b"}}},
		Calendars: []string{"c"}, Misfire: MisfireSkip, AllowConcurrent: true, PersistData: true, Timeout: "5s",
		Paused: true, NextFire: t0, LastFire: t0.Add(-time.Hour), FireCount: 9,
	}
}

func testStoreContract(t *testing.T, newStore func(t *testing.T) Store) {
	ctx := context.Background()
	t.Run("save list delete", func(t *testing.T) {
		s := newStore(t)
		for _, k := range []string{"b", "c", "a"} {
			if err := s.Save(ctx, sampleRecord(k)); err != nil {
				t.Fatal(err)
			}
		}
		recs, err := s.List(ctx)
		if err != nil || len(recs) != 3 || recs[0].Key != "a" || recs[2].Key != "c" {
			t.Fatalf("List = %v, %v (want sorted a b c)", recs, err)
		}
		if err := s.Delete(ctx, "b"); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(ctx, "missing"); err != nil {
			t.Fatalf("deleting a missing key must not fail: %v", err)
		}
		recs, _ = s.List(ctx)
		if len(recs) != 2 {
			t.Fatalf("List = %v", recs)
		}
	})
	t.Run("upsert and copies", func(t *testing.T) {
		s := newStore(t)
		r := sampleRecord("k")
		if err := s.Save(ctx, r); err != nil {
			t.Fatal(err)
		}
		r.FireCount = 10
		r.Data["n"] = 99 // mutating the caller's map after Save must not leak in
		if err := s.Save(ctx, JobRecord{Key: "k", Handler: "other", Trigger: r.Trigger}); err != nil {
			t.Fatal(err)
		}
		recs, _ := s.List(ctx)
		if len(recs) != 1 || recs[0].Handler != "other" {
			t.Fatalf("upsert failed: %+v", recs)
		}
		recs[0].Data = map[string]any{"x": 1} // mutating a listed record must not leak back
		again, _ := s.List(ctx)
		if again[0].Data != nil {
			t.Fatalf("List returned aliased data: %+v", again[0].Data)
		}
	})
	t.Run("round trip fidelity", func(t *testing.T) {
		s := newStore(t)
		want := sampleRecord("k")
		if err := s.Save(ctx, want); err != nil {
			t.Fatal(err)
		}
		got, _ := s.List(ctx)
		a, _ := json.Marshal(want)
		b, _ := json.Marshal(got[0])
		if string(a) != string(b) {
			t.Fatalf("round trip changed the record:\n%s\n%s", a, b)
		}
		if _, ok := got[0].Data["n"].(float64); !ok {
			t.Fatalf("Data numbers must come back as float64, got %T", got[0].Data["n"])
		}
	})
	t.Run("concurrent use", func(t *testing.T) {
		s := newStore(t)
		var wg sync.WaitGroup
		for i := range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				k := fmt.Sprintf("k%d", i%4)
				_ = s.Save(ctx, sampleRecord(k))
				_, _ = s.List(ctx)
				if i%3 == 0 {
					_ = s.Delete(ctx, k)
				}
			}()
		}
		wg.Wait()
	})
}

func TestMemoryStore(t *testing.T) {
	t.Parallel()
	testStoreContract(t, func(*testing.T) Store { return NewMemoryStore() })
	var zero MemoryStore // zero value is usable
	if err := zero.Save(context.Background(), sampleRecord("z")); err != nil {
		t.Fatal(err)
	}
	if err := zero.Delete(context.Background(), "nothing"); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryStoreUnencodableData(t *testing.T) {
	t.Parallel()
	r := sampleRecord("k")
	r.Data = map[string]any{"ch": make(chan int)}
	if err := NewMemoryStore().Save(context.Background(), r); err == nil {
		t.Fatal("expected an encoding error")
	}
}

func TestFileStore(t *testing.T) {
	t.Parallel()
	testStoreContract(t, func(t *testing.T) Store {
		s, err := NewFileStore(filepath.Join(t.TempDir(), "jobs.json"))
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

func TestFileStorePersistsAcrossOpens(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "jobs.json")
	ctx := context.Background()
	s1, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err == nil {
		t.Fatal("file must not exist before the first Save")
	}
	if err := s1.Save(ctx, sampleRecord("a")); err != nil {
		t.Fatal(err)
	}
	if err := s1.Save(ctx, sampleRecord("b")); err != nil {
		t.Fatal(err)
	}
	if err := s1.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	s2, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	recs, _ := s2.List(ctx)
	if len(recs) != 1 || recs[0].Key != "b" {
		t.Fatalf("reopened store = %+v", recs)
	}
	// plain, versioned, human readable JSON
	raw, _ := os.ReadFile(path)
	var doc struct {
		Version int
		Jobs    []map[string]any
	}
	if err := json.Unmarshal(raw, &doc); err != nil || doc.Version != 1 || len(doc.Jobs) != 1 {
		t.Fatalf("file format: %v %s", err, raw)
	}
	// no temp files are left behind
	entries, _ := os.ReadDir(filepath.Dir(path))
	for _, e := range entries {
		if strings.Contains(e.Name(), ".tmp-") {
			t.Fatalf("leftover temp file %s", e.Name())
		}
	}
}

func TestFileStoreRejectsBadFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for name, content := range map[string]string{
		"corrupt.json":   `{"version": 1, "jobs": [`,
		"future.json":    `{"version": 2, "jobs": []}`,
		"noversion.json": `{"jobs": []}`,
	} {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := NewFileStore(p); !errors.Is(err, ErrStoreFormat) {
			t.Errorf("%s: err = %v, want ErrStoreFormat", name, err)
		}
	}
	// a directory is not a readable store file
	if _, err := NewFileStore(dir); err == nil {
		t.Error("expected an error opening a directory")
	}
}

func TestFileStoreWriteFailureRollsBack(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "jobs.json") // parent directory does not exist
	s, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := s.Save(ctx, sampleRecord("a")); err == nil {
		t.Fatal("expected write error")
	}
	if recs, _ := s.List(ctx); len(recs) != 0 {
		t.Fatalf("failed Save must roll back memory, got %v", recs)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, sampleRecord("a")); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Dir(path)); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "a"); err == nil {
		t.Fatal("expected write error")
	}
	if recs, _ := s.List(ctx); len(recs) != 1 {
		t.Fatalf("failed Delete must roll back, got %v", recs)
	}
}

func TestFileStoreContextCancelled(t *testing.T) {
	t.Parallel()
	s, _ := NewFileStore(filepath.Join(t.TempDir(), "jobs.json"))
	s.wmu <- struct{}{} // simulate a writer in progress
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Save(ctx, sampleRecord("a")); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	<-s.wmu
}
