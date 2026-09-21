package quartzx

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"
)

// MisfirePolicy says what the scheduler does when a fire time was missed by
// more than the misfire threshold, because the scheduler was down, paused, or
// too busy.
type MisfirePolicy string

// Misfire policies. The zero value behaves as MisfireFireNow.
const (
	// MisfireFireNow runs the job once immediately and then continues on the
	// original schedule. Any further missed fires are dropped.
	MisfireFireNow MisfirePolicy = "fire-now"
	// MisfireSkip drops every missed fire and waits for the next scheduled
	// time in the future. A one-shot trigger that misfires is dropped.
	MisfireSkip MisfirePolicy = "skip"
	// MisfireReschedule runs the job once immediately and re-anchors
	// interval and calendar-interval triggers at now, so the next fire is one
	// full period later. For cron triggers it is the same as MisfireFireNow.
	MisfireReschedule MisfirePolicy = "reschedule"
	// MisfireFireAll runs the job once for every missed fire, in order, up to
	// the scheduler's catch-up limit; the rest are dropped.
	MisfireFireAll MisfirePolicy = "fire-all"
)

func (p MisfirePolicy) valid() bool {
	switch p {
	case "", MisfireFireNow, MisfireSkip, MisfireReschedule, MisfireFireAll:
		return true
	}
	return false
}

// JobRecord is the persisted state of one scheduled job. JSON encodes it in
// the file store. Data values follow encoding/json rules, so numbers come
// back as float64.
type JobRecord struct {
	Key             string         `json:"key"`
	Handler         string         `json:"handler"`
	Trigger         TriggerSpec    `json:"trigger"`
	Data            map[string]any `json:"data,omitempty"`
	Calendars       []string       `json:"calendars,omitempty"`
	Misfire         MisfirePolicy  `json:"misfire,omitempty"`
	AllowConcurrent bool           `json:"allowConcurrent,omitempty"`
	PersistData     bool           `json:"persistData,omitempty"`
	Timeout         string         `json:"timeout,omitempty"`
	Paused          bool           `json:"paused,omitempty"`
	// NextFire is the next scheduled fire time; zero when none.
	NextFire time.Time `json:"nextFire,omitzero"`
	// LastFire is the scheduled time of the most recent dispatched fire.
	LastFire time.Time `json:"lastFire,omitzero"`
	// FireCount counts dispatched fires.
	FireCount int64 `json:"fireCount,omitempty"`
}

// Store persists job records. Implementations must be safe for concurrent use
// and must return records that the caller can modify freely.
type Store interface {
	// Save inserts or replaces the record with rec.Key.
	Save(ctx context.Context, rec JobRecord) error
	// Delete removes a record. Deleting a missing key is not an error.
	Delete(ctx context.Context, key string) error
	// List returns all records sorted by key.
	List(ctx context.Context) ([]JobRecord, error)
}

// MemoryStore is a [Store] that keeps records in memory. It is safe for
// concurrent use. Records are stored as JSON, so it behaves exactly like the
// file store with respect to Data types. The zero value is ready to use.
type MemoryStore struct {
	mu   sync.Mutex
	recs map[string][]byte
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

// Save implements [Store].
func (m *MemoryStore) Save(_ context.Context, rec JobRecord) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("quartzx: encode job %q: %w", rec.Key, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.recs == nil {
		m.recs = make(map[string][]byte)
	}
	m.recs[rec.Key] = b
	return nil
}

// Delete implements [Store].
func (m *MemoryStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.recs, key)
	return nil
}

// List implements [Store].
func (m *MemoryStore) List(_ context.Context) ([]JobRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]JobRecord, 0, len(m.recs))
	for k, b := range m.recs {
		var r JobRecord
		if err := json.Unmarshal(b, &r); err != nil {
			return nil, fmt.Errorf("%w: job %q: %w", ErrStoreFormat, k, err)
		}
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
	return out, nil
}
