// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"errors"
	"io"
)

// ErrPathExists is the sentinel a [ConditionalPutter.PutIfAbsent]
// failure wraps when the path is already occupied. Callers arbitrating
// a create-only claim (the chain concurrent-writer guard, ADR-0160)
// match it with errors.Is to distinguish "another writer won the slot"
// from transport/auth failures.
var ErrPathExists = errors.New("path already exists")

// ConditionalPutter is an OPTIONAL [Store] capability: write the
// contents of r to path only if nothing exists there, atomically at
// the storage layer (local FS O_EXCL create; object stores a
// conditional PUT — S3/GCS/Azure `If-None-Match: *` / generation-0
// preconditions). Exactly one of any number of concurrent PutIfAbsent
// calls for the same path succeeds; the losers fail with an error
// wrapping [ErrPathExists].
//
// Same optional-capability shape as [Appender]: callers type-assert
// and degrade gracefully when the store doesn't implement it. The
// backup-chain concurrent-writer guard (ADR-0160) rides on this to
// turn the lineage catalog's read-modify-write into a compare-and-swap.
type ConditionalPutter interface {
	// PutIfAbsent writes the contents of r to the named path within
	// the store, failing with an error wrapping [ErrPathExists] when
	// the path is already occupied. Any other error is a transport /
	// auth / capability failure, NOT a claim verdict.
	PutIfAbsent(ctx context.Context, path string, r io.Reader) error
}
