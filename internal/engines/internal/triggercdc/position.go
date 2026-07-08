// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package triggercdc is the shared, engine-neutral core of the trigger-CDC
// engines (pgtrigger, sqlite-trigger + its d1-trigger transport sibling, and
// any future `*-trigger` engine). It owns exactly the pieces whose logic is
// byte-identical across those engines behind a small DIALECT SEAM:
//
//   - the change-log-`id` position codec (the `{"last_id":N}` token this file),
//     parameterised by a dialect-provided ACCEPTED-ENGINE-NAME family so a new
//     trigger engine widens the family cleanly (the Bug-166 lesson);
//   - the batched keyset-DELETE prune loop + the auto-prune remaining-rows
//     bookkeeper (prune.go).
//
// It deliberately does NOT own the pieces whose SEMANTICS diverge — most
// importantly the snapshot→CDC handoff ANCHOR: pgtrigger's contiguous-committed-
// prefix + txid safety-lag anchor vs sqlite-trigger's single-writer MAX(id)
// anchor are different correctness arguments (ADR-0135 §4 vs the Bug-94 formula);
// merging them would be a silent-loss regression, so they stay in their engines.
// Likewise the setup DDL, the poll SQL, and the value-image decode are dialect
// and stay engine-side. The package imports only the core IR, so both engine
// packages can depend on it without a cycle.
package triggercdc

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// Pos is the engine-side representation of a trigger-CDC polling position: the
// most-recently-applied `id` value from `sluice_change_log`. It serialises as
// JSON in [ir.Position.Token] (`{"last_id":N}`) so a future schema bump doesn't
// break the wire shape — the IR contract treats Token as opaque — and the shape
// is identical across every trigger engine for operator familiarity.
type Pos struct {
	// LastID is the change-log id of the last successfully-applied change. The
	// polling reader resumes by scanning rows with id > LastID (each engine
	// layers its own gap-freedom predicate on top: PG's txid safety-lag, SQLite's
	// single-writer total order).
	LastID int64 `json:"last_id"`
}

// Codec encodes and decodes the trigger-CDC position token. It is the single
// owner of the `{"last_id":N}` wire shape; each trigger engine constructs one
// with its own error-message prefix, the engine name it WRITES, and the
// engine-name FAMILY it ACCEPTS on decode.
//
// The accepted set is the dialect seam for family acceptance (Bug-166 / Bug-2 /
// Bug-20): the pipeline re-stamps a persisted position's Engine with the source
// engine's own Name() on warm-resume, and transport siblings share the identical
// token semantics (sqlite-trigger accepts both `sqlite-trigger` and `d1-trigger`).
// Rejecting a same-family tag would make every restart a poison-pill; a future
// `mysql-trigger` widens its family here with no change to this codec.
type Codec struct {
	// ErrPrefix is the short engine label prefixed on every error this codec
	// returns (e.g. "pgtrigger", "sqlite-trigger"). It need not equal WriteEngine
	// — pgtrigger's package label differs from its "postgres-trigger" engine name.
	ErrPrefix string

	// WriteEngine is the [ir.Position.Engine] tag [Codec.Encode] stamps. It MUST
	// be a member of Accept.
	WriteEngine string

	// Accept is the trigger-CDC engine-name FAMILY [Codec.Decode] accepts. A
	// position tagged with any member decodes; a foreign tag is refused loudly.
	// Order is preserved in the "want …" clause of the mismatch error.
	Accept []string
}

// Encode marshals lastID into an [ir.Position] tagged with WriteEngine.
func (c Codec) Encode(lastID int64) (ir.Position, error) {
	b, err := json.Marshal(Pos{LastID: lastID})
	if err != nil {
		return ir.Position{}, fmt.Errorf("%s: encode position: %w", c.ErrPrefix, err)
	}
	return ir.Position{Engine: c.WriteEngine, Token: string(b)}, nil
}

// Decode parses an [ir.Position] back into the change-log id. The zero value of
// [ir.Position] (empty Engine + Token) is the "from now" sentinel and is reported
// via ok=false with a nil error; callers distinguish it from a malformed token by
// checking ok before err.
func (c Codec) Decode(p ir.Position) (lastID int64, ok bool, err error) {
	if p.Engine == "" && p.Token == "" {
		return 0, false, nil
	}
	if !c.accepts(p.Engine) {
		return 0, false, fmt.Errorf(
			"%s: decode position: engine = %q; want %s",
			c.ErrPrefix, p.Engine, quotedList(c.Accept),
		)
	}
	if p.Token == "" {
		return 0, false, fmt.Errorf("%s: decode position: token is empty", c.ErrPrefix)
	}
	var decoded Pos
	if err := json.Unmarshal([]byte(p.Token), &decoded); err != nil {
		return 0, false, fmt.Errorf("%s: decode position: %w", c.ErrPrefix, err)
	}
	if decoded.LastID < 0 {
		return 0, false, fmt.Errorf("%s: decode position: last_id = %d; must be >= 0", c.ErrPrefix, decoded.LastID)
	}
	return decoded.LastID, true, nil
}

// AppliedLastID extracts the durably-applied change-log id (the `{"last_id":N}`
// token shape) from a persisted trigger-CDC position TOKEN. `sluice trigger
// prune` (ADR-0137) calls it to derive the prune bound from the TARGET's durable
// frontier — the only safe lower bound. It reuses [Codec.Decode] so the wire
// shape has a single owner; the token is re-stamped with WriteEngine first
// because the TARGET re-stamps the position's Engine with its OWN name when it
// persists the source token (so the raw token alone carries the id).
//
// An empty, malformed, or FOREIGN token is a loud error (a durable position the
// prune can trust must decode cleanly and actually be a trigger-CDC token —
// never prune blind against a garbled or wrong-engine watermark).
func (c Codec) AppliedLastID(token string) (int64, error) {
	if token == "" {
		return 0, fmt.Errorf("%s: durable position token is empty (no applied watermark)", c.ErrPrefix)
	}
	// Require the last_id key. A FOREIGN (non-trigger-CDC) stream's token — a
	// vanilla-PG pgoutput {slot,lsn}, a mysql-gtid set, a broker envelope — would
	// otherwise json.Unmarshal cleanly into {LastID:0} and look like "nothing to
	// prune" against the wrong stream; refuse loudly instead. Decode into a *int64
	// so an absent key is distinguishable from an explicit 0.
	var probe struct {
		LastID *int64 `json:"last_id"`
	}
	if err := json.Unmarshal([]byte(token), &probe); err != nil || probe.LastID == nil {
		return 0, fmt.Errorf("%s: position token has no last_id — the stream is not a trigger-CDC stream", c.ErrPrefix)
	}
	lastID, ok, err := c.Decode(ir.Position{Engine: c.WriteEngine, Token: token})
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("%s: durable position token is empty (no applied watermark)", c.ErrPrefix)
	}
	return lastID, nil
}

// accepts reports whether engine belongs to this codec's trigger-CDC family.
func (c Codec) accepts(engine string) bool {
	for _, e := range c.Accept {
		if engine == e {
			return true
		}
	}
	return false
}

// quotedList renders the accepted-engine names as a %q-quoted "want …" clause:
// one name → `"a"`, two → `"a" or "b"`, three+ → `"a", "b", or "c"`. This
// reproduces the exact single-engine (pgtrigger) and two-engine (sqlite-trigger
// family) mismatch messages the engines emitted before this codec centralised
// them.
func quotedList(items []string) string {
	switch len(items) {
	case 0:
		return ""
	case 1:
		return strconv.Quote(items[0])
	case 2:
		return strconv.Quote(items[0]) + " or " + strconv.Quote(items[1])
	default:
		var b strings.Builder
		for i, it := range items {
			switch {
			case i == len(items)-1:
				b.WriteString("or " + strconv.Quote(it))
			default:
				b.WriteString(strconv.Quote(it) + ", ")
			}
		}
		return b.String()
	}
}
