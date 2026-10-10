// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"
)

// The column-order record (RENAME-SWAP-FORWARD).
//
// A source change that flips the declared order of two columns is either a
// RENAME swap or rotation (the values stayed put, the names moved) or a
// reorder (the values moved with the names), and nothing in the boundary
// tells them apart. sluice applies neither; it accepts the boundary once the
// operator has applied the same change on the target, which it proves by
// the target's own column order MOVING from the order it held to the
// source's new order. The order it held cannot be read off the target after
// the fact — a target that already held the new order for another reason (an
// `ADD COLUMN … AFTER` forwarded to the end of a table) looks exactly like
// one the operator changed — so it is recorded, together with the source's
// order at the same moment: at the cold start, and at every accepted
// boundary that changes either.
//
// The record is persisted state read back across restarts and binaries, so
// its column lists go through one codec ([EncodeColumnNames] /
// [DecodeColumnNames]): a value that would not round-trip exactly refuses on
// write, and one this codec did not write refuses on read.

// ColumnOrderRecord is one table's recorded column orders. Schema and Table
// are the SOURCE naming a boundary carries ([SchemaSnapshot.Schema],
// [SchemaSnapshot.Table]); SourceColumns is the source table's declared
// order and TargetColumns the target table's, both as of the same accepted
// moment.
type ColumnOrderRecord struct {
	Schema        string
	Table         string
	SourceColumns []string
	TargetColumns []string
}

// ErrColumnOrderStoreUnavailable reports that the target cannot keep column
// order records (its role could not create the table, a safe-migrations
// branch refused the DDL). Not fatal: a stream without records refuses a
// column-order change as unprovable instead of accepting it.
var ErrColumnOrderStoreUnavailable = errors.New("COLUMN-ORDER-RECORD-UNAVAILABLE")

// ColumnOrderStore is the optional [ChangeApplier] surface that keeps the
// records. An applier without it records nothing. The applier ALSO persists
// the record a [SchemaSnapshot] carries in [SchemaSnapshot.TargetColumns],
// on the same transaction as the boundary's schema-history row and position,
// so a record can never run ahead of the boundary that justified it.
type ColumnOrderStore interface {
	// WriteColumnOrders upserts each record for streamID — the cold start's
	// record of every table it created or adopted. Returns an error wrapping
	// [ErrColumnOrderStoreUnavailable] when the target keeps none.
	WriteColumnOrders(ctx context.Context, streamID string, records []ColumnOrderRecord) error
	// ReadColumnOrders returns every record for streamID: an empty slice
	// when there is none (a stream from before records existed), an error
	// wrapping [ErrColumnOrderStoreUnavailable] when the target keeps none.
	ReadColumnOrders(ctx context.Context, streamID string) ([]ColumnOrderRecord, error)
}

// ColumnOrderKey is the fixed-width primary key of one record: a SHA-256 over
// (streamID, schemaName, tableName), each part length-prefixed so no two
// distinct tuples share a key whatever bytes a part holds. Same rationale as
// [SchemaVersionKey]: InnoDB cannot index three utf8mb4 VARCHAR(255)s with
// room to spare, and a prefix index would let two long names collide and
// overwrite each other.
func ColumnOrderKey(streamID, schemaName, tableName string) string {
	h := sha256.New()
	for _, part := range []string{streamID, schemaName, tableName} {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(part)))
		_, _ = h.Write(n[:])
		_, _ = h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ColumnNames is t's column names in declared order (nil for a nil table).
func ColumnNames(t *Table) []string {
	if t == nil {
		return nil
	}
	out := make([]string, 0, len(t.Columns))
	for _, c := range t.Columns {
		if c != nil {
			out = append(out, c.Name)
		}
	}
	return out
}

// EncodeColumnNames renders cols as the stored form, a JSON array of
// strings. It refuses, rather than alter, what the encoding would not carry
// exactly: a name that is not valid UTF-8 (encoding/json would silently
// replace it with U+FFFD) and a nil slice, which means "not recorded" and
// must never be written as a record. An empty slice is a record and encodes
// as "[]".
func EncodeColumnNames(cols []string) (string, error) {
	if cols == nil {
		return "", errors.New("column order: nothing to record (nil column list)")
	}
	for i, c := range cols {
		if !utf8.ValidString(c) {
			return "", fmt.Errorf("column order: column %d (%q) is not valid UTF-8 and would not round-trip", i, c)
		}
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(cols); err != nil {
		return "", fmt.Errorf("column order: encode: %w", err)
	}
	return string(bytes.TrimSuffix(buf.Bytes(), []byte("\n"))), nil
}

// DecodeColumnNames parses the stored form back. It accepts only what
// [EncodeColumnNames] writes — a JSON array of strings that re-encodes to
// exactly the stored bytes — so a JSON null, a non-array, trailing data or
// an escape that decodes lossily (a lone surrogate becomes U+FFFD) refuses,
// and a damaged or foreign record is never read as one.
func DecodeColumnNames(stored string) ([]string, error) {
	var cols []string
	dec := json.NewDecoder(bytes.NewReader([]byte(stored)))
	if err := dec.Decode(&cols); err != nil {
		return nil, fmt.Errorf("column order: decode %q: %w", truncateForError(stored), err)
	}
	if dec.More() {
		return nil, fmt.Errorf("column order: decode %q: trailing data", truncateForError(stored))
	}
	if cols == nil {
		return nil, fmt.Errorf("column order: decode %q: not a column list", truncateForError(stored))
	}
	again, err := EncodeColumnNames(cols)
	if err != nil || again != stored {
		return nil, fmt.Errorf("column order: %q was not written by this codec (it does not re-encode to itself)", truncateForError(stored))
	}
	return cols, nil
}

// truncateForError bounds a stored value quoted in an error.
func truncateForError(s string) string {
	const limit = 120
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "…"
}
