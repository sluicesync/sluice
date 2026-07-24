// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package telemetrysink

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// File-sink rotation defaults. 64 MiB × 5 retained files caps a watch at
// ~320 MiB on disk — roughly a year of one-minute samples for a 40-database
// fleet — which is small enough to leave unattended and large enough that
// rotation is rare.
const (
	defaultFileMaxBytes int64 = 64 << 20
	defaultFileMaxFiles       = 5

	// filePerm / dirPerm keep the sink's output owner-only: a metrics row is
	// not a secret, but the PATH an operator chooses may sit somewhere they
	// would rather not widen (matches internal/pipeline/blobcodec).
	filePerm os.FileMode = 0o600
	dirPerm  os.FileMode = 0o700
)

// FileConfig configures a [FileSink].
type FileConfig struct {
	// Path is the CURRENT file. Rotated generations are Path.1 (newest) …
	// Path.N. Its parent directory is created if missing.
	Path string

	// MaxBytes is the size at which the current file rotates. 0 ⇒
	// defaultFileMaxBytes. A negative value disables rotation entirely
	// (the operator owns the file's growth — e.g. an external logrotate).
	MaxBytes int64

	// MaxFiles is how many ROTATED generations to retain (the current file is
	// not counted). 0 ⇒ defaultFileMaxFiles.
	MaxFiles int
}

// FileSink appends each polled sample to a local JSONL file, rotating by
// size. One record per line ([EncodeRecord]), so the output streams into
// anything line-oriented — `jq`, DuckDB's `read_json_auto`, a portal's
// ingest — without a framing format of its own.
//
// A batch is written ATOMICALLY with respect to rotation: the whole tick's
// records land in one file, never split across a rotation boundary, so a
// consumer never sees half a fleet's tick in each of two files.
type FileSink struct {
	cfg FileConfig

	mu   sync.Mutex
	f    *os.File
	size int64
}

// Compile-time proof the file sink satisfies the seam.
var _ Sink = (*FileSink)(nil)

// NewFileSink constructs a file sink for cfg. It creates the parent
// directory and OPENS the file eagerly, so a bad path is a loud startup
// refusal rather than a swallowed per-tick warning — the sink is opt-in, and
// an opt-in that silently never wrote anything would be worse than none.
func NewFileSink(cfg FileConfig) (*FileSink, error) {
	if cfg.Path == "" {
		return nil, errors.New("telemetrysink: file sink requires a path")
	}
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = defaultFileMaxBytes
	}
	if cfg.MaxFiles <= 0 {
		cfg.MaxFiles = defaultFileMaxFiles
	}
	s := &FileSink{cfg: cfg}
	if err := s.open(); err != nil {
		return nil, err
	}
	return s, nil
}

// Name implements [Sink].
func (s *FileSink) Name() string { return "file" }

// Write encodes and appends recs. Encoding is done BEFORE any file I/O so a
// refused row (see [EncodeRecord]) never leaves a partial line behind; the
// refusal is returned to the caller, which logs and swallows it.
func (s *FileSink) Write(ctx context.Context, recs []Record) error {
	if len(recs) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var (
		buf  []byte
		errs []error
	)
	for _, r := range recs {
		line, err := EncodeRecord(r)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		buf = append(buf, line...)
	}
	if len(buf) == 0 {
		return errors.Join(errs...)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		if err := s.open(); err != nil {
			return errors.Join(append(errs, err)...)
		}
	}
	// Rotate BEFORE the batch when it would push the file past the limit, so
	// a tick's records stay together in one generation.
	if s.cfg.MaxBytes > 0 && s.size > 0 && s.size+int64(len(buf)) > s.cfg.MaxBytes {
		if err := s.rotate(); err != nil {
			return errors.Join(append(errs, err)...)
		}
	}
	n, err := s.f.Write(buf)
	s.size += int64(n)
	if err != nil {
		return errors.Join(append(errs, fmt.Errorf("telemetrysink: write %s: %w", s.cfg.Path, err))...)
	}
	return errors.Join(errs...)
}

// Close closes the current file. Safe to call more than once.
func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}

// open creates the parent directory if needed and opens Path for append,
// seeding size from the existing file so a restart does not lose track of
// how full the current generation is.
func (s *FileSink) open() error {
	if dir := filepath.Dir(s.cfg.Path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, dirPerm); err != nil {
			return fmt.Errorf("telemetrysink: create directory for %s: %w", s.cfg.Path, err)
		}
	}
	f, err := os.OpenFile(s.cfg.Path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, filePerm) //nolint:gosec // path is operator-supplied by construction (--sink-file)
	if err != nil {
		return fmt.Errorf("telemetrysink: open %s: %w", s.cfg.Path, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("telemetrysink: stat %s: %w", s.cfg.Path, err)
	}
	s.f = f
	s.size = info.Size()
	return nil
}

// rotate closes the current file, shifts the retained generations down
// (Path.N-1 → Path.N, …, Path → Path.1), drops anything past MaxFiles, and
// reopens a fresh Path. Called with the mutex held.
//
// Numbered generations (rather than timestamped names) are deliberate: the
// newest rotated file is ALWAYS Path.1, so a consumer tailing the set needs
// no name parsing and no clock, and two rotations in the same second cannot
// collide.
func (s *FileSink) rotate() error {
	if err := s.f.Close(); err != nil {
		return fmt.Errorf("telemetrysink: close %s for rotation: %w", s.cfg.Path, err)
	}
	s.f = nil
	// Drop the generation that is about to fall off the end, then shift.
	_ = os.Remove(generationPath(s.cfg.Path, s.cfg.MaxFiles))
	for i := s.cfg.MaxFiles - 1; i >= 1; i-- {
		from := generationPath(s.cfg.Path, i)
		if _, err := os.Stat(from); err != nil {
			continue
		}
		if err := os.Rename(from, generationPath(s.cfg.Path, i+1)); err != nil {
			return fmt.Errorf("telemetrysink: rotate %s: %w", from, err)
		}
	}
	if err := os.Rename(s.cfg.Path, generationPath(s.cfg.Path, 1)); err != nil {
		return fmt.Errorf("telemetrysink: rotate %s: %w", s.cfg.Path, err)
	}
	return s.open()
}

// generationPath renders the Nth rotated generation's filename.
func generationPath(base string, n int) string {
	return fmt.Sprintf("%s.%d", base, n)
}
