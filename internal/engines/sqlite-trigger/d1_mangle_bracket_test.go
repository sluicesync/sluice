// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestD1CapturedImageBytes_BracketsTheServerSideRewrite pins the SQT-1 mangle
// bracket for the d1-trigger change-log poll.
//
// THE MEASUREMENT THIS ENCODES (live Cloudflare D1, 2026-09-06). D1 stores
// invalid UTF-8 verbatim — a cell written as `CAST(x'41ff42' AS TEXT)` reads
// back `hex(c)` = `41FF42` with `length(CAST(c AS BLOB))` = 3 — but its HTTP
// query response rewrites that byte to U+FFFD, so `SELECT c` delivers
// `41 EF BF BD 42`, five bytes for three.
//
// That rewrite is SERVER-SIDE, and it is why the lane's existing guard cannot
// help: `sqlite.JSONStringValue` sees the payload after the rewrite, when it
// is already valid UTF-8. The code said so itself and called the behaviour an
// unmeasured premise (backlog 2026-08-12); the measurement settled it against
// us, and the 2026-09-06 audit had filed the consequence as a HIGH.
//
// The numbers below are the measured ones, not invented: 3 stored, 5
// delivered, for the exact byte sequence probed.
func TestD1CapturedImageBytes_BracketsTheServerSideRewrite(t *testing.T) {
	// What D1 actually delivered for the probe cell: "A", U+FFFD, "B".
	mangled := "A�B"
	if len(mangled) != 5 {
		t.Fatalf("fixture is not the measured shape: %d bytes, want 5", len(mangled))
	}

	t.Run("a rewritten cell is refused, naming both counts", func(t *testing.T) {
		err := checkD1CapturedImageBytes(42, "after", sql.NullString{String: mangled, Valid: true}, json.RawMessage("3"), json.RawMessage("0"))
		if err == nil {
			t.Fatal("a captured image delivered as 5 bytes where D1 stores 3 was accepted — this is the " +
				"server-side U+FFFD rewrite, and applying it writes the rewritten text to the target while " +
				"the source still holds the original")
		}
		for _, want := range []string{"id=42", `"after"`, "5 bytes", "stores 3", "U+FFFD"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal missing %q; got: %v", want, err)
			}
		}
		// The CODE and the REMEDY live on the CodedError, not in Error() —
		// asserted here because an operator greps the code and follows the
		// hint, and a refusal that carries neither is just a stack trace.
		ce, ok := sluicecode.FromError(err)
		if !ok {
			t.Fatalf("refusal carries no sluice code: %v", err)
		}
		if ce.Code != sluicecode.CodeD1TextMangled {
			t.Errorf("code = %q; want %q", ce.Code, sluicecode.CodeD1TextMangled)
		}
		if !strings.Contains(ce.Hint, "hex(col)") {
			t.Errorf("remedy does not tell the operator how to see the real bytes; got: %q", ce.Hint)
		}
	})

	t.Run("an intact cell passes", func(t *testing.T) {
		// The no-false-fire floor. Without it, "refuses the mangled case" is
		// indistinguishable from "refuses everything", which would wedge
		// every d1-trigger stream on its first change row.
		if err := checkD1CapturedImageBytes(1, "before", sql.NullString{String: "AB", Valid: true}, json.RawMessage("2"), json.RawMessage("0")); err != nil {
			t.Errorf("an intact 2-byte cell was refused: %v", err)
		}
		// Multi-byte but VALID UTF-8 must also pass: the bracket compares
		// bytes, and a 3-byte character is 3 stored bytes.
		if err := checkD1CapturedImageBytes(2, "after", sql.NullString{String: "€", Valid: true}, json.RawMessage("3"), json.RawMessage("0")); err != nil {
			t.Errorf("a valid multi-byte character was refused: %v", err)
		}
	})

	t.Run("a NULL image is not compared", func(t *testing.T) {
		// An INSERT has no before image. Comparing would refuse every insert.
		if err := checkD1CapturedImageBytes(3, "before", sql.NullString{}, json.RawMessage("0"), json.RawMessage("0")); err != nil {
			t.Errorf("a SQL NULL captured image was refused: %v", err)
		}
	})

	t.Run("a MISSING length refuses rather than passing", func(t *testing.T) {
		// The bracket's own liveness. If the query shape drifts and D1 stops
		// returning the length column, silently skipping the check turns this
		// gate into decoration — which is the failure mode it exists to stop.
		err := checkD1CapturedImageBytes(4, "after", sql.NullString{String: mangled, Valid: true}, nil, json.RawMessage("0"))
		if err == nil {
			t.Fatal("an absent stored-byte length was treated as clean — the bracket is then inoperative " +
				"and nothing on this transport can see a server-side rewrite")
		}
		if !strings.Contains(err.Error(), "not optional") {
			t.Errorf("refusal does not say the bracket is load-bearing; got: %v", err)
		}
	})

	t.Run("the length decoder takes both JSON shapes", func(t *testing.T) {
		for _, raw := range []string{"3", `"3"`} {
			v, ok := d1JSONInt64(json.RawMessage(raw))
			if !ok || v != 3 {
				t.Errorf("d1JSONInt64(%s) = (%d, %v); want (3, true)", raw, v, ok)
			}
		}
		if _, ok := d1JSONInt64(json.RawMessage(`"not-a-number"`)); ok {
			t.Error("a non-numeric string decoded as a length; that would silence the bracket")
		}
	})
}

// TestD1CapturedImageBytes_CatchesTheLengthPreservingRewrite pins the
// blind spot the pre-tag value-fidelity review found in the byte-length
// bracket (audit 2026-09-06, MEDIUM).
//
// D1 substitutes one U+FFFD -- three bytes -- per MAXIMAL INVALID
// SUBPART, and a subpart is one, two or three bytes. At exactly three
// the substitution is byte-length PRESERVING and the length bracket sees
// nothing. That shape is not exotic: a 4-byte UTF-8 sequence severed at
// a byte boundary IS a 3-byte maximal subpart, and severing at a byte
// boundary is what fixed-width truncation does to an emoji.
//
// So the fixture here is the realistic one: a cell holding 'A' followed
// by the first three bytes of a 4-byte emoji. Stored: 4 bytes, zero
// U+FFFD. Delivered: "A" + U+FFFD, also 4 bytes, one U+FFFD. The length
// check passes by construction -- that is the point -- and the count
// check is the only thing standing between this row and a silent
// rewrite on the target.
func TestD1CapturedImageBytes_CatchesTheLengthPreservingRewrite(t *testing.T) {
	delivered := "A�"
	if len(delivered) != 4 {
		t.Fatalf("fixture is not length-preserving: delivered is %d bytes, want 4", len(delivered))
	}

	t.Run("equal byte counts do not make it clean", func(t *testing.T) {
		// stored bytes = 4 (matches!), stored U+FFFD = 0 (does not).
		err := checkD1CapturedImageBytes(7, "after",
			sql.NullString{String: delivered, Valid: true},
			json.RawMessage("4"), json.RawMessage("0"))
		if err == nil {
			t.Fatal("a length-preserving U+FFFD rewrite was accepted. A severed 4-byte sequence is three " +
				"bytes -- exactly the width of the replacement it becomes -- so the byte-length bracket " +
				"passes and only the replacement COUNT can see it. Without this check the captured row is " +
				"applied with the source's bytes replaced, at exit 0.")
		}
		for _, want := range []string{"id=7", `"after"`, "1 U+FFFD", "stores 0"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal missing %q; got: %v", want, err)
			}
		}
	})

	t.Run("a cell that legitimately holds U+FFFD passes", func(t *testing.T) {
		// The control that keeps this from being a blanket ban on U+FFFD.
		// A source may legitimately store the replacement character; it is
		// counted on both sides and the counts agree.
		if err := checkD1CapturedImageBytes(8, "before",
			sql.NullString{String: delivered, Valid: true},
			json.RawMessage("4"), json.RawMessage("1")); err != nil {
			t.Fatalf("a value that genuinely stores U+FFFD must pass; got: %v", err)
		}
	})

	t.Run("a missing count is refused, not skipped", func(t *testing.T) {
		// Same rule as the byte half: an absent column means the query
		// shape drifted, and skipping the check is how a bracket becomes
		// decorative.
		err := checkD1CapturedImageBytes(9, "after",
			sql.NullString{String: delivered, Valid: true},
			json.RawMessage("4"), nil)
		if err == nil {
			t.Fatal("a captured image with no stored U+FFFD count was accepted; the count half of the " +
				"bracket must refuse rather than silently degrade to the length check it is there to cover")
		}
		if !strings.Contains(err.Error(), "no stored U+FFFD count") {
			t.Errorf("refusal does not name the missing count: %v", err)
		}
	})
}

// TestD1ReplacementCountExpr_ShapeIsServerSide pins that the count is
// computed on the STORED value, which is the property that makes it
// independent evidence rather than this reader re-deriving its own
// answer from the payload it is grading.
func TestD1ReplacementCountExpr_ShapeIsServerSide(t *testing.T) {
	got := d1ReplacementCountExpr("before")
	for _, want := range []string{"length(CAST(before AS BLOB))", "replace(before, char(65533), '')", "/ 3"} {
		if !strings.Contains(got, want) {
			t.Errorf("expression missing %q — it must measure the STORED value server-side, "+
				"not the delivered text; got: %s", want, got)
		}
	}
}
