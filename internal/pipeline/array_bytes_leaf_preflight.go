// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// The CDC half of the MySQL array-leaf fix, closed by REFUSAL for the
// same reason [preflightBinaryTargetColumnsOnCDC] next door is — on this
// lane there is nothing left to disambiguate with.
//
// # The ambiguity
//
// An [ir.Array] value reaches a target with no array type (MySQL) as a
// JSON document, and every element is rendered by its family's rule.
// Two families have a `[]byte` IR leaf and DIFFERENT rules: a
// json/jsonb element is carried as the document's text, a bytea element
// as base64 (see arrayLeafForJSON in internal/engines/mysql). Which one
// applies is decided by the SOURCE column's element type.
//
// # Why the CDC lane cannot use that answer
//
// A ChangeApplier resolves its column descriptors from the TARGET's
// information_schema. MySQL renders BOTH `json[]` and `bytea[]` as the
// same `JSON` column, so the applier reads `ir.JSON` for either and the
// element type is simply not there — while the CDC READER types its
// values from the SOURCE and hands both families the same `[]byte`
// leaf. Nothing on the applier side can tell them apart, and the
// content cannot decide it either: a bytea whose bytes happen to spell
// a JSON document is an ordinary bytea (item 135's rule, one layer up).
//
// Without a refusal the failure is a divergence WITHIN one table: the
// cold copy — which does see the source element type — stores the
// document text, and the first CDC update to the same column stores
// something else, with no error on either side.
//
// # Why refusal, and the over-reach stated rather than implied
//
// The writer refuses the leaf loudly (it will not guess), so the
// alternative to this preflight is not silence — it is the same refusal
// arriving MID-STREAM, after the cold copy has already completed. Item
// 145 set the precedent on the other target: pgtrigger.Setup refuses a
// json[]/jsonb[] column outright so the operator hears it before any
// value moves. This is that, for this pair.
//
// The over-reach: `bytea[]` is refused too, and its two lanes would in
// fact have AGREED (base64 either way). Letting it through would mean
// the applier deciding "these bytes are a bytea, not a document", which
// is exactly the guess that has no evidence behind it — so the honest
// scope is the family SET whose leaf is byte-shaped, not the one member
// of it whose answer happens to be stable. An operator who needs the
// column copied has `sluice migrate`, which has no CDC lane and carries
// every family faithfully; the message says so.

// errArrayBytesLeafOnCDC is the sentinel cause. Wrapped with per-column
// detail naming each offending column.
var errArrayBytesLeafOnCDC = errors.New(
	"pipeline: a json[]/jsonb[]/bytea[] source column cannot be applied faithfully by a continuous-sync " +
		"run against a target that has no array type",
)

// preflightArrayBytesLeafOnCDC refuses a CDC-lane run whose SOURCE holds
// an array column with a byte-shaped element leaf (json, jsonb, bytea)
// when the TARGET ENGINE has no array type.
//
// # Why it keys on the target ENGINE and not on a target catalog read
//
// The sibling [preflightBinaryTargetColumnsOnCDC] compares the two
// catalogs, and it can: its hazard is a property of a column that
// ALREADY EXISTS on the target. This one is not — the offending column
// is usually one sluice is about to CREATE, so on an ordinary fresh
// cold start a catalog read returns nothing and a catalog-keyed check
// would silently no-op on exactly the runs that matter. That was this
// function's first cut, and the real-server test
// TestSync_PGToMySQL_ByteShapedArrayLeafRefusedAtPreflight caught it:
// the sync ran to completion instead of refusing.
//
// So the question asked here is about the TARGET ENGINE's type surface,
// which needs no connection: a MySQL-family target renders every
// [ir.Array] as a scalar JSON column, which is what erases the element
// family the applier would need. [migcore.IsMySQLFamilyEngine] is the
// registry-parity-tested spelling of that set, and the same predicate
// every other PG→MySQL cross-engine refusal uses. Postgres targets keep
// the column an array and have a real per-family writer arm on both
// sides; SQLite refuses [ir.Array] outright at schema emit, which is
// louder and earlier than anything here.
//
// # The independent expected value (the 2026-08-01 rule)
//
// The source schema is read from the SOURCE server; the target's
// array-lessness is a declared property of the target engine. Neither
// derives from the other, and neither derives from the value path this
// protects.
//
// # What it reaches, stated so the name cannot be read as broader
//
//   - `sync` single-database cold start, on every branch of
//     coldStartGatePreflight.
//   - `schema add-table`, against the target the live stream applies to.
//   - A column declared as a DOMAIN over such an array
//     (`CREATE DOMAIN d AS json[]`) — see [unwrapDomainToArray]. The
//     first cut asked `src.(ir.Array)` and skipped it.
//   - NOT the multi-database cold-start fan-out, which has its own
//     schema plumbing; filed rather than half-built.
//   - NOT warm resume, which reads no schema by design. Nor a
//     mid-stream `ALTER TABLE … ADD COLUMN bytea[]`, which no preflight
//     sees.
//
// # What the blind halves cost, which is NOT uniformly a late error
//
// The writer's leaf refusal is the backstop for the halves above, and
// its own scope is narrower than "the CDC lane": it fires on a `[]byte`
// leaf, so it reaches the doors that produce one.
//
//   - Multi-database fan-out — still LOUD. Its source is one of the two
//     [ir.MultiDatabaseSnapshotOpener] implementors: MySQL, whose
//     schema reader never produces an [ir.Array] at all, or Postgres,
//     whose pgoutput door decodes both byte-shaped families to []byte
//     and so trips the refusal.
//   - Warm resume / mid-stream ADD COLUMN over the pgoutput or scan
//     doors — still LOUD, at the applier's leaf, naming the column.
//   - Warm resume / mid-stream ADD COLUMN over the PGTRIGGER door —
//     SILENTLY DIVERGENT. That reader's payload decode is a `UseNumber`
//     [encoding/json] unmarshal, whose leaves can never be a []byte, so
//     the refusal cannot fire: a `bytea[]` element arrives as PG's
//     `\x`-hex string and lands as `["\\xdead"]` where the cold copy
//     stored `["3q0="]`. Cold start is covered — this preflight reads
//     the SOURCE schema whatever the source engine — so the exposure is
//     a stream STARTED on a sluice older than v0.116.0 and resumed on a
//     newer one, or a column added to a live stream. It is not closed
//     at the leaf because on that door the value is byte-identical to a
//     jsonb array DOCUMENT and to a `text[]`/`int[]` CDC value, both of
//     which are correct today; arrayLeafForJSON's scope section carries
//     the full argument and the pin.
//
// So: the blind halves cost a LATE error on every door but one, and a
// silent divergence on that one. The earlier form of this sentence said
// "never a silent one", which was the claim the whole preflight-scope
// argument rested on and was false for pgtrigger.
func preflightArrayBytesLeafOnCDC(schema *ir.Schema, targetEngine, mode string) error {
	if schema == nil || !migcore.IsMySQLFamilyEngine(targetEngine) {
		return nil
	}
	var offenders []string
	for _, t := range schema.Tables {
		if t == nil {
			continue
		}
		for _, c := range t.Columns {
			if c == nil {
				continue
			}
			src := c.Type
			if c.SourceColumnType != nil {
				src = c.SourceColumnType
			}
			arr, isArray := unwrapDomainToArray(src)
			if !isArray || !isByteShapedArrayLeaf(arr.Element) {
				continue
			}
			offenders = append(offenders, fmt.Sprintf("%s.%s (array of %T)", t.Name, c.Name, arr.Element))
		}
	}
	if len(offenders) == 0 {
		return nil
	}
	sort.Strings(offenders)
	return fmt.Errorf(
		"%w — refusing %s before any data moves: %s. The %s target has no array type, so the column holds a "+
			"JSON document and each element is encoded by its element family: a json/jsonb element as the "+
			"document's own text, a bytea element as base64. The change applier reads column types from the "+
			"TARGET, where both families are the same JSON column, so on the CDC lane it cannot tell which "+
			"encoding to use — and the bytes cannot decide it either, since a bytea may legitimately spell a "+
			"JSON document. The cold copy, which does see the source element type, would encode one way and "+
			"every later CDC apply the other, disagreeing about the same column with no error on either "+
			"side. sluice refuses the value rather than guessing, so the alternative to this message is the "+
			"same refusal arriving mid-stream. Remedies: exclude the table with --exclude-table, or use "+
			"`sluice migrate` for a one-shot copy — it has no CDC lane and carries every array element "+
			"family faithfully",
		errArrayBytesLeafOnCDC, mode, strings.Join(offenders, ", "), targetEngine,
	)
}

// engineNameOrEmpty resolves an engine's registered name, tolerating the
// nil Target that partial-construction unit tests hand the cold-start
// gate. An empty name matches no engine family, so the preflight
// no-ops — the right answer when there is no target to decide about.
func engineNameOrEmpty(e ir.Engine) string {
	if e == nil {
		return ""
	}
	return e.Name()
}

// unwrapDomainToArray resolves a column's source type to the [ir.Array]
// underneath it, seeing through any depth of [ir.Domain].
//
// `CREATE DOMAIN d AS json[]` is a real Postgres shape and the schema
// reader carries it as `ir.Domain{BaseType: ir.Array{...}}`, so a bare
// `src.(ir.Array)` skipped the cold-start refusal for exactly the
// columns it exists to catch and left them to fail mid-stream at the
// writer's leaf instead. The MySQL writer already unwraps domains the
// same way before dispatching a value ([prepareValue]'s Bug-122 arm), so
// the value path and this preflight now ask the same question.
//
// Recursion is bounded by nilling out a base type that is not itself a
// domain; a self-referential domain is not constructible in Postgres, and
// the loop bound below makes a malformed IR terminate rather than hang.
func unwrapDomainToArray(t ir.Type) (ir.Array, bool) {
	// 16 is far past any real nesting (PG's own domain-over-domain chains
	// are shallow); the bound exists so a cyclic IR built by a test or a
	// future reader bug fails closed rather than spinning.
	for range 16 {
		switch v := t.(type) {
		case ir.Array:
			return v, true
		case ir.Domain:
			if v.BaseType == nil {
				return ir.Array{}, false
			}
			t = v.BaseType
		default:
			return ir.Array{}, false
		}
	}
	return ir.Array{}, false
}

// isByteShapedArrayLeaf reports whether an array ELEMENT type's
// IR-canonical leaf is a `[]byte` — the property that makes the element
// family unrecoverable from a target that flattens arrays into JSON.
//
// It is the two families the Postgres reader decodes to bytes: json /
// jsonb (decodeBytes) and bytea (decodeBytea). Kept as its own named
// predicate so the roster gate in internal/engines/mysql and this
// refusal name the same set rather than each carrying a list.
func isByteShapedArrayLeaf(elem ir.Type) bool {
	switch elem.(type) {
	case ir.JSON, ir.Binary, ir.Varbinary, ir.Blob:
		return true
	}
	return false
}
