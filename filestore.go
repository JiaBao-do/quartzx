package quartzx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// fileFormatVersion is the version field of the JSON file written by FileStore.
const fileFormatVersion = 1

type fileDoc struct {
	Version int         `json:"version"`
	Jobs    []JobRecord `json:"jobs"`
}

// FileStore is a [Store] that keeps all records in one JSON file. Every
// mutation rewrites the file atomically (write to a temporary file in the same
// directory, fsync, rename), so a crash leaves either the old or the new
// file, never a torn one. It is safe for concurrent use within a process;
// it does not lock against other processes. Use one FileStore per file.
//
// The format is plain, versioned JSON:
//
//	{"version": 1, "jobs": [ {"key": "...", ...} ]}
type FileStore struct {
	path string
	mem  MemoryStore
	// wmu serializes mutations so file content matches memory.
	wmu chan struct{}
}

// NewFileStore opens the store at path. A missing file is an empty store; it
// is created on the first Save. An unreadable or unsupported file yields an
// error wrapping [ErrStoreFormat].
func NewFileStore(path string) (*FileStore, error) {
	s := &FileStore{path: path, wmu: make(chan struct{}, 1)}
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("quartzx: read %s: %w", path, err)
	}
	var doc fileDoc
	if err := json.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrStoreFormat, path, err)
	}
	if doc.Version != fileFormatVersion {
		return nil, fmt.Errorf("%w: %s: version %d", ErrStoreFormat, path, doc.Version)
	}
	for _, r := range doc.Jobs {
		if err := s.mem.Save(context.Background(), r); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Save implements [Store].
func (f *FileStore) Save(ctx context.Context, rec JobRecord) error {
	return f.mutate(ctx, rec.Key, func() error { return f.mem.Save(ctx, rec) })
}

// Delete implements [Store].
func (f *FileStore) Delete(ctx context.Context, key string) error {
	return f.mutate(ctx, key, func() error { return f.mem.Delete(ctx, key) })
}

// List implements [Store].
func (f *FileStore) List(ctx context.Context) ([]JobRecord, error) { return f.mem.List(ctx) }

func (f *FileStore) mutate(ctx context.Context, key string, apply func() error) error {
	select {
	case f.wmu <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-f.wmu }()

	f.mem.mu.Lock()
	prev, had := f.mem.recs[key]
	f.mem.mu.Unlock()
	if err := apply(); err != nil {
		return err
	}
	if err := f.flush(ctx); err != nil {
		f.mem.mu.Lock() // roll back so memory matches the file
		if had {
			f.mem.recs[key] = prev
		} else {
			delete(f.mem.recs, key)
		}
		f.mem.mu.Unlock()
		return err
	}
	return nil
}

func (f *FileStore) flush(ctx context.Context) error {
	recs, err := f.mem.List(ctx)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(fileDoc{Version: fileFormatVersion, Jobs: recs}); err != nil {
		return fmt.Errorf("quartzx: encode store: %w", err)
	}
	dir := filepath.Dir(f.path)
	tmp, err := os.CreateTemp(dir, filepath.Base(f.path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("quartzx: write store: %w", err)
	}
	name := tmp.Name()
	fail := func(err error) error {
		tmp.Close()
		os.Remove(name)
		return fmt.Errorf("quartzx: write store: %w", err)
	}
	if _, err := tmp.Write(buf.Bytes()); err != nil {
		return fail(err)
	}
	if err := tmp.Sync(); err != nil {
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(name)
		return fmt.Errorf("quartzx: write store: %w", err)
	}
	if err := os.Rename(name, f.path); err != nil {
		os.Remove(name)
		return fmt.Errorf("quartzx: write store: %w", err)
	}
	return nil
}
