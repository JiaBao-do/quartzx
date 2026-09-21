package quartzx

import (
	"context"
	"fmt"
	"time"
)

// Job is the work a schedule triggers. Register implementations by name with
// [Scheduler.Register]; a persisted job refers to its handler by that name,
// which is what lets it survive a restart.
//
// Execute is called on its own goroutine. Unless the job was scheduled with
// AllowConcurrent, two calls for the same job key never overlap.
// The context is cancelled when the scheduler is stopped with an expired
// deadline or its parent context is cancelled; long jobs should honour it.
type Job interface {
	Execute(ctx context.Context, e Execution) error
}

// JobFunc adapts a function to a [Job].
type JobFunc func(ctx context.Context, e Execution) error

// Execute calls f.
func (f JobFunc) Execute(ctx context.Context, e Execution) error { return f(ctx, e) }

// Execution describes one run of a job.
type Execution struct {
	// Key is the job key and Handler the registered handler name.
	Key, Handler string
	// ScheduledFor is the fire time the trigger produced. FiredAt is the
	// clock time at which the run started. They differ for late and
	// misfired runs.
	ScheduledFor, FiredAt time.Time
	// Misfired is true when this run happened because of a misfire policy.
	Misfired bool
	// Count is the number of fires dispatched for this job, including this one.
	Count int64
	// Data is a private copy of the job data map. If the job was scheduled
	// with PersistData and Execute returns nil, the (modified) map is saved.
	Data map[string]any
}

// JobSpec describes a job to schedule.
type JobSpec struct {
	// Key uniquely identifies the job. Required.
	Key string
	// Handler is the name passed to Register. Required.
	Handler string
	// Trigger says when the job fires. Required.
	Trigger Trigger
	// Data is the job data map, copied into every Execution. Values must be
	// JSON encodable.
	Data map[string]any
	// Calendars lists names registered with WithCalendar; a fire time is
	// skipped when any of them excludes it.
	Calendars []string
	// Misfire is the policy for missed fires (default MisfireFireNow).
	Misfire MisfirePolicy
	// AllowConcurrent lets a new fire start while the previous run of this
	// job is still executing. By default such a fire is skipped and reported
	// as EventSkipped.
	AllowConcurrent bool
	// PersistData saves Execution.Data back to the store after a
	// successful run, so state carries across runs and restarts.
	PersistData bool
	// Timeout, if positive, cancels the job's context after that long.
	Timeout time.Duration
}

func (s JobSpec) record() JobRecord {
	r := JobRecord{
		Key: s.Key, Handler: s.Handler, Trigger: s.Trigger.Spec(), Data: s.Data,
		Calendars: s.Calendars, Misfire: s.Misfire, AllowConcurrent: s.AllowConcurrent,
		PersistData: s.PersistData,
	}
	if s.Timeout > 0 {
		r.Timeout = s.Timeout.String()
	}
	return r
}

// PanicError is the error reported when a job panics.
type PanicError struct{ Value any }

// Error implements error.
func (p *PanicError) Error() string { return fmt.Sprintf("quartzx: job panicked: %v", p.Value) }

// EventType classifies an [Event].
type EventType string

// Event types.
const (
	// EventFired is sent when a run starts.
	EventFired EventType = "fired"
	// EventCompleted is sent when a run returns nil.
	EventCompleted EventType = "completed"
	// EventFailed is sent when a run returns an error or panics (Err is set).
	EventFailed EventType = "failed"
	// EventMisfired is sent when a fire time was missed beyond the threshold.
	// Policy and Missed describe the outcome.
	EventMisfired EventType = "misfired"
	// EventSkipped is sent when a fire is skipped because the previous run of
	// the job is still executing.
	EventSkipped EventType = "skipped"
	// EventExhausted is sent when a job's trigger has no more fire times and
	// the job was removed.
	EventExhausted EventType = "exhausted"
	// EventError is sent for problems that have no caller to return to: a
	// store failure, an unknown handler, a record that cannot be loaded.
	EventError EventType = "error"
)

// Event is passed to listeners.
type Event struct {
	Type EventType
	Key  string
	// Time is the clock time of the event.
	Time time.Time
	// ScheduledFor is the fire time concerned, when there is one.
	ScheduledFor time.Time
	// Err is set for EventFailed and EventError.
	Err error
	// Policy and Missed are set for EventMisfired: the policy applied and
	// how many scheduled fires were missed (1 unless the policy counted more).
	Policy MisfirePolicy
	Missed int
}

// Listener receives scheduler events. OnEvent is called synchronously from
// scheduler goroutines, possibly concurrently, so it must be safe for
// concurrent use and must not block. A panic in a listener is recovered.
type Listener interface {
	OnEvent(Event)
}

// ListenerFunc adapts a function to a [Listener].
type ListenerFunc func(Event)

// OnEvent calls f.
func (f ListenerFunc) OnEvent(e Event) { f(e) }
