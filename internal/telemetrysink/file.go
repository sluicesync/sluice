// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package telemetrysink

import (
	"bytes"
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
	before := s.size
	n, err := s.f.Write(buf)
	s.size += int64(n)
	if err != nil {
		// A SHORT write (classically ENOSPC) stops mid-record, leaving the
		// file ending in a partial line with no newline. Left alone, the next
		// successful write appends at that offset and GLUES a fully-valid
		// record onto the fragment: one unparseable line that swallows a
		// record this sink already reported as written (audit 2026-07-26
		// SL-5, observed on a full tmpfs).
		//
		// So roll the file back to the last COMPLETE record boundary inside
		// what was actually written. Whole records that made it survive; only
		// the torn one is discarded — and it is discarded LOUDLY, since the
		// write error is still returned. Truncating also releases the bytes
		// the failed write consumed, which is exactly what a full disk needs.
		if keep, ok := lastRecordBoundary(buf, n, before); ok {
			if terr := s.f.Truncate(keep); terr == nil {
				s.size = keep
			} else {
				err = errors.Join(err, fmt.Errorf("could not truncate the torn record away: %w", terr))
			}
		}
		return errors.Join(append(errs, fmt.Errorf("telemetrysink: write %s: %w", s.cfg.Path, err))...)
	}
	return errors.Join(errs...)
}

// lastRecordBoundary reports the file size to truncate back to after a short
// write of n bytes that began at file offset before. Records are
// newline-terminated, so the boundary is just past the last newline inside the
// written prefix; if no record completed, the whole prefix goes.
func lastRecordBoundary(buf []byte, n int, before int64) (int64, bool) {
	if n <= 0 {
		// Nothing landed — there is still a boundary to restore only if the
		// offset moved, which it did not.
		return before, before >= 0
	}
	if n > len(buf) {
		n = len(buf)
	}
	if i := bytes.LastIndexByte(buf[:n], '\n'); i >= 0 {
		return before + int64(i) + 1, true
	}
	return before, true
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
	// O_RDWR rather than O_WRONLY so the torn-tail check below can read the
	// file's last byte.
	f, err := os.OpenFile(s.cfg.Path, os.O_RDWR|os.O_APPEND|os.O_CREATE, filePerm) //nolint:gosec // path is operator-supplied by construction (--sink-file)
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
	// Heal a tail torn by something this process cannot roll back — a kill
	// -9, a power loss, a container OOM mid-write. Truncate-on-short-write
	// handles the errors we SEE; this handles the ones we never got to
	// observe, and without it a restart re-opens at the torn offset and the
	// first new record is glued onto the fragment (audit 2026-07-26 SL-5).
	//
	// Terminating the fragment rather than truncating it is deliberate: the
	// bytes are the operator's, they may be diagnostically useful, and a
	// reader already has to tolerate one unparseable line for the tear
	// itself. Destroying data to tidy up would be the worse trade.
	if s.size > 0 {
		var last [1]byte
		if _, rerr := f.ReadAt(last[:], s.size-1); rerr == nil && last[0] != '\n' {
			n, werr := f.Write([]byte{'\n'})
			s.size += int64(n)
			if werr != nil {
				_ = f.Close()
				s.f = nil
				return fmt.Errorf("telemetrysink: terminate torn record in %s: %w", s.cfg.Path, werr)
			}
		}
	}
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
