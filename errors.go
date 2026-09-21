package quartzx

import "errors"

// Sentinel errors. Test for them with [errors.Is].
var (
	// ErrInvalidCron is wrapped by errors from [ParseCron].
	ErrInvalidCron = errors.New("quartzx: invalid cron expression")
	// ErrInvalidTrigger is returned for a malformed trigger or trigger spec.
	ErrInvalidTrigger = errors.New("quartzx: invalid trigger")
	// ErrInvalidJob is returned for a malformed [JobSpec].
	ErrInvalidJob = errors.New("quartzx: invalid job")
	// ErrNotFound is returned when a job key is unknown.
	ErrNotFound = errors.New("quartzx: job not found")
	// ErrExists is returned by Schedule when the job key is already in use.
	ErrExists = errors.New("quartzx: job already exists")
	// ErrNoHandler is returned when a job names a handler that was not registered.
	ErrNoHandler = errors.New("quartzx: handler not registered")
	// ErrNoCalendar is returned when a job names a calendar that was not registered.
	ErrNoCalendar = errors.New("quartzx: calendar not registered")
	// ErrStarted is returned by Start when the scheduler is already running.
	ErrStarted = errors.New("quartzx: scheduler already started")
	// ErrNotStarted is returned when an operation needs a running scheduler.
	ErrNotStarted = errors.New("quartzx: scheduler not started")
	// ErrStoreFormat is returned when a persisted file has an unknown version or is corrupt.
	ErrStoreFormat = errors.New("quartzx: unsupported store format")
)
