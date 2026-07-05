// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package triggercdc

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// pgCodec / sqliteCodec mirror the two shipped trigger engines' codecs so the
// shared owner is pinned against BOTH the single-engine (pgtrigger) and the
// family (sqlite-trigger + d1-trigger) shapes, plus a hypothetical 3-engine
// family (a future mysql-trigger) — the "pin the class, not the representative"
// discipline applied to the accepted-engine-name dialect seam.
var (
	pgCodec     = Codec{ErrPrefix: "pgtrigger", WriteEngine: "postgres-trigger", Accept: []string{"postgres-trigger"}}
	sqliteCodec = Codec{ErrPrefix: "sqlite-trigger", WriteEngine: "sqlite-trigger", Accept: []string{"sqlite-trigger", "d1-trigger"}}
	threeCodec  = Codec{ErrPrefix: "x-trigger", WriteEngine: "x-trigger", Accept: []string{"x-trigger", "y-trigger", "z-trigger"}}
)

// TestCodec_EncodeDecodeRoundTrip pins the wire shape (`{"last_id":N}`, tagged
// with WriteEngine) and the round-trip for every codec.
func TestCodec_EncodeDecodeRoundTrip(t *testing.T) {
	for _, c := range []Codec{pgCodec, sqliteCodec, threeCodec} {
		pos, err := c.Encode(1234567890)
		if err != nil {
			t.Fatalf("%s: Encode: %v", c.ErrPrefix, err)
		}
		if pos.Engine != c.WriteEngine {
			t.Errorf("%s: pos.Engine = %q; want %q", c.ErrPrefix, pos.Engine, c.WriteEngine)
		}
		if pos.Token != `{"last_id":1234567890}` {
			t.Errorf("%s: token = %q; want the {\"last_id\":N} shape", c.ErrPrefix, pos.Token)
		}
		id, ok, err := c.Decode(pos)
		if err != nil || !ok {
			t.Fatalf("%s: Decode: (ok=%v, err=%v); want (true, nil)", c.ErrPrefix, ok, err)
		}
		if id != 1234567890 {
			t.Errorf("%s: decoded id = %d; want 1234567890", c.ErrPrefix, id)
		}
	}
}

// TestCodec_FamilyAcceptance is the Bug-166 pin: a codec decodes a token tagged
// with ANY member of its accepted family (not just the one it writes), because
// the pipeline re-stamps a persisted position with the source engine's own name
// on warm-resume. Rejecting a same-family tag would poison every restart.
func TestCodec_FamilyAcceptance(t *testing.T) {
	// sqlite-trigger MUST accept a position that came back tagged "d1-trigger"
	// (and vice versa) — the transport-sibling re-stamp.
	for _, engine := range sqliteCodec.Accept {
		pos := ir.Position{Engine: engine, Token: `{"last_id":42}`}
		id, ok, err := sqliteCodec.Decode(pos)
		if err != nil || !ok || id != 42 {
			t.Errorf("sqliteCodec.Decode(engine=%q) = (%d, %v, %v); want (42, true, nil)", engine, id, ok, err)
		}
	}
	// The 3-engine family accepts all three of its members.
	for _, engine := range threeCodec.Accept {
		if _, ok, err := threeCodec.Decode(ir.Position{Engine: engine, Token: `{"last_id":7}`}); err != nil || !ok {
			t.Errorf("threeCodec.Decode(engine=%q) = (ok=%v, err=%v); want accepted", engine, ok, err)
		}
	}
}

// TestCodec_ForeignEngineRefused pins that a position tagged with an engine
// OUTSIDE the family is refused loudly, with the exact "want …" clause each
// family shape produced before the codec was centralised (byte-identical
// messages so no test/log downstream regresses).
func TestCodec_ForeignEngineRefused(t *testing.T) {
	cases := []struct {
		codec    Codec
		wantWant string // the trailing "want …" clause
	}{
		{pgCodec, `want "postgres-trigger"`},
		{sqliteCodec, `want "sqlite-trigger" or "d1-trigger"`},
		{threeCodec, `want "x-trigger", "y-trigger", or "z-trigger"`},
	}
	for _, tc := range cases {
		_, ok, err := tc.codec.Decode(ir.Position{Engine: "mysql", Token: `{"last_id":1}`})
		if ok || err == nil {
			t.Fatalf("%s: Decode(foreign engine) = (ok=%v, err=%v); want a loud refuse", tc.codec.ErrPrefix, ok, err)
		}
		if !strings.Contains(err.Error(), "engine = \"mysql\"") || !strings.Contains(err.Error(), tc.wantWant) {
			t.Errorf("%s: err = %q; want it to name the bad engine and the clause %q", tc.codec.ErrPrefix, err, tc.wantWant)
		}
	}
}

// TestCodec_DecodeSentinelAndInvalid pins the "from now" sentinel (zero position
// → ok=false, nil err), the empty-token and malformed-token refusals, and the
// last_id >= 0 invariant.
func TestCodec_DecodeSentinelAndInvalid(t *testing.T) {
	if _, ok, err := pgCodec.Decode(ir.Position{}); ok || err != nil {
		t.Errorf("Decode(zero) = (ok=%v, err=%v); want (false, nil) — the from-now sentinel", ok, err)
	}
	if _, _, err := pgCodec.Decode(ir.Position{Engine: pgCodec.WriteEngine, Token: ""}); err == nil {
		t.Error("Decode(empty token) returned nil; want a loud error")
	}
	if _, _, err := pgCodec.Decode(ir.Position{Engine: pgCodec.WriteEngine, Token: "{bad"}); err == nil {
		t.Error("Decode(malformed token) returned nil; want a loud error")
	}
	if _, _, err := pgCodec.Decode(ir.Position{Engine: pgCodec.WriteEngine, Token: `{"last_id":-5}`}); err == nil {
		t.Error("Decode(negative last_id) returned nil; want a loud error (persisted watermark must be >= 0)")
	}
}

// TestCodec_AppliedLastID pins the prune-bound decode: a valid token yields the
// id; empty / malformed / negative / FOREIGN tokens all refuse loudly. The
// foreign-token cases are the load-bearing silent-loss guard — a pgoutput
// {slot,lsn}, a mysql-gtid set, or a broker envelope unmarshals cleanly into
// {LastID:0} and would look like "nothing to prune" against the WRONG stream.
func TestCodec_AppliedLastID(t *testing.T) {
	for _, c := range []Codec{pgCodec, sqliteCodec} {
		got, err := c.AppliedLastID(`{"last_id":99}`)
		if err != nil || got != 99 {
			t.Fatalf("%s: AppliedLastID(valid) = (%d, %v); want (99, nil)", c.ErrPrefix, got, err)
		}
		for _, bad := range []string{"", "{bad", `{"last_id":-5}`} {
			if _, err := c.AppliedLastID(bad); err == nil {
				t.Errorf("%s: AppliedLastID(%q) returned nil; want a loud error", c.ErrPrefix, bad)
			}
		}
		for _, foreign := range []string{
			`{"slot":"sluice_slot","lsn":"0/16B3748"}`,
			`{"gtid":"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"}`,
			`{"chain_id":"c1","segment":3}`,
		} {
			if _, err := c.AppliedLastID(foreign); err == nil {
				t.Errorf("%s: AppliedLastID(%q) returned nil; want a loud refuse (no last_id key)", c.ErrPrefix, foreign)
			}
		}
	}
}
