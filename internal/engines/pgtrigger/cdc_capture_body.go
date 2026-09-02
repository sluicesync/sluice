// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sort"
	"strings"
)

// The capture-shape door's BODY arm (audit 2026-08-31 SL-5).
//
// The door graded the bound function's NAME (`proname`) and the trigger's
// shape (`tgtype`) and stopped, while its own header called it the mirror
// of sqlite-trigger's door — which compares the installed `CREATE`
// statement against the statement THIS binary renders. Two populations rode
// through that gap:
//
//   - TAMPER. `CREATE OR REPLACE FUNCTION sluice_capture_change() … AS $$
//     BEGIN RETURN NULL; END $$` leaves every trigger in place, correctly
//     named, correctly shaped, pointing at a function that records nothing.
//     Every subsequent source DML is silently absent from the stream, at
//     exit 0. Nothing in the engine noticed.
//   - VINTAGE. The function body carries fixes the trigger does not:
//     `SET extra_float_digits = 3` (Bug 194), `SET bytea_output = hex`
//     (audit 2026-08-05 B-1), the SEC-1 `SET search_path` pin on the DDL
//     function, the Bug 257 suppression check. `CREATE OR REPLACE` is the
//     ONLY thing that installs them — upgrading the binary does not — so an
//     install set up by an older sluice keeps capturing floats and byteas
//     through the FIRING session's GUCs, which is the corruption those pins
//     were added to stop, and no door said so.
//
// # What is compared, and what is NOT
//
// PostgreSQL splits a function definition across three catalog columns and
// only one of them is the body: `prosrc` holds the text between the dollar
// quotes, the `SET` clauses land in `proconfig`, and `SECURITY DEFINER` in
// `prosecdef`. A `prosrc`-only comparison — the literal shape SL-5
// proposed — would MISS its own headline scenario, because
// `bytea_output`/`extra_float_digits`/`search_path` live in proconfig. All
// three are compared ([captureFunctionShape]).
//
// # Normalization: exactly the two transforms that cannot hide a change
//
// PostgreSQL stores prosrc byte-verbatim, so exact equality against this
// binary's render is achievable and is the default. The only normalization
// applied is CRLF→LF plus trailing-whitespace-per-line and surrounding
// blank lines — the transforms a `--dry-run` plan pasted through a Windows
// editor or psql undergoes, which change no SQL semantics. Nothing looser:
// collapsing internal whitespace would let a body differing only inside a
// string literal or an identifier compare equal. proconfig entries are
// normalized on BOTH sides to `name=value` with the spacing PostgreSQL
// itself applies. That round trip — prosrc byte-identical, proconfig in the
// form the door builds — is the environmental premise the whole comparison
// rests on, so it is CHECKED against a real server for all four functions
// by TestCaptureFunctionBodyDoor's "PREMISE" subtest rather than asserted
// here.
//
// The row function is compared against the render of EVERY capture-payload
// mode (ADR-0068), because the installed mode is recorded nowhere; a match
// against any one is a match. That is not a weakening — the three renders
// differ only in the UPDATE/DELETE assignment block, and all three are
// legitimate installs of this binary.
//
// # Refuse vs WARN: how an OLD install is told from a TAMPERED one
//
// A body that differs is, on its own, ambiguous — and the ambiguity cannot
// be resolved from the body. So the door resolves it from PROVENANCE that
// setup records ([metaCaptureDigestCol], schema_version 5): the digest of
// the function definitions the plan installed. Then:
//
//   - The body no longer records into this install's change log →
//     REFUSE, whatever the provenance. No sluice version ever rendered such
//     a body (verified back to the engine's first commit, 9f220c47: every
//     rendering has carried `INSERT INTO <change log>`), so vintage cannot
//     explain it. This is the arm that covers the whole PRE-v5 population —
//     i.e. every install that exists today — against the attack.
//   - The definitions disagree with the digest setup recorded →
//     REFUSE. Something replaced them AFTER setup: a hand `CREATE OR
//     REPLACE`, an operator-edited dry-run plan, a third party.
//   - The definitions AGREE with the recorded digest but not with this
//     binary's render → WARN [staleCaptureFunctionMarker]. The install is
//     untampered and older than the binary; the remedy is a `trigger setup`
//     re-run.
//   - No trustworthy provenance (pre-v5 install, or a v5 install whose
//     schema_version was regressed by an older binary's setup run — that is
//     what makes the version, not merely the column's presence, the trust
//     signal) → WARN, saying in those words that old-vs-edited cannot be
//     distinguished here.
//
// WARN rather than refuse for the vintage cases is the SEC-1 /
// DROP-CAPTURE-ABSENT posture, for the same reason: this runs at every CDC
// open, so refusing would convert a binary upgrade into an outage on every
// running sync, for a state the operator has been in all along. The
// direction that is NOT tolerated — a definition that changed after setup,
// or one that records nothing — refuses.
//
// # Honest residual
//
// A tamperer who leaves a dead `INSERT INTO … sluice_change_log` in an
// otherwise gutted body defeats the capture-defeat arm, and on a PRE-v5
// install (no recorded digest) that lands on a WARN rather than a refusal.
// Closing it needs the provenance every new install now records. Stated
// here rather than implied.

// staleCaptureFunctionMarker is the grep-stable prefix the body arm's WARN
// carries; the pins and the mutation run key on it.
const staleCaptureFunctionMarker = "STALE-CAPTURE-FUNCTION"

// captureFunctionShape is one capture function's definition as PostgreSQL
// stores it — the three columns that together are "what this function
// does".
type captureFunctionShape struct {
	name     string
	body     string   // pg_proc.prosrc, normalized (see the file header)
	settings []string // pg_proc.proconfig, normalized "name=value", sorted
	definer  bool     // pg_proc.prosecdef
}

// digest is the shape's provenance fingerprint: what setup records and what
// the door re-derives from the catalog.
func (s captureFunctionShape) digest() string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%t\x00%s\x00%s", s.name, s.definer, strings.Join(s.settings, "\x1f"), s.body)
	return hex.EncodeToString(h.Sum(nil))
}

func (s captureFunctionShape) equal(other captureFunctionShape) bool {
	return s.body == other.body && s.definer == other.definer &&
		strings.Join(s.settings, "\x1f") == strings.Join(other.settings, "\x1f")
}

// recordsIntoChangeLog is the capture-defeat predicate: a capture function
// that does not write into this install's change log captures nothing, and
// no sluice rendering has ever lacked either half of it (checked against
// the engine's first commit, not assumed). Deliberately looser than an
// exact match — it must hold for EVERY vintage, including ones this binary
// cannot render — and deliberately not a parser: its job is to separate
// "records something" from "records nothing", and its limit is stated in
// the file header.
func (s captureFunctionShape) recordsIntoChangeLog() bool {
	lower := strings.ToLower(s.body)
	return strings.Contains(lower, "insert into") && strings.Contains(lower, strings.ToLower(ChangeLogTable))
}

// captureFunctionShapeOfRender extracts the shape from one rendered
// `CREATE OR REPLACE FUNCTION` statement — the same three pieces the
// catalog stores, so both sides of the comparison are built the same way.
// Reports false if the statement is not in the shape every renderer in
// setup.go emits; [TestCaptureFunctionShape_ParsesEveryRender] makes that a
// build failure rather than something a runtime can meet.
func captureFunctionShapeOfRender(name, stmt string) (captureFunctionShape, bool) {
	const open = "\nAS $sluice$\n"
	const closer = "\n$sluice$;"
	// Line endings are normalized before the cut, not after: a plan pasted
	// through a Windows editor carries CRLF in its FRAMING too, and a cut
	// that missed would report "unparseable" for a perfectly good render.
	stmt = strings.ReplaceAll(stmt, "\r\n", "\n")
	header, rest, ok := strings.Cut(stmt, open)
	if !ok || !strings.HasSuffix(rest, closer) {
		return captureFunctionShape{}, false
	}
	shape := captureFunctionShape{
		name: name,
		body: normalizeFunctionBody(strings.TrimSuffix(rest, closer)),
	}
	for _, line := range strings.Split(header, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "SECURITY DEFINER":
			shape.definer = true
		case strings.HasPrefix(line, "SET "):
			shape.settings = append(shape.settings, normalizeFunctionSetting(strings.TrimPrefix(line, "SET ")))
		}
	}
	sort.Strings(shape.settings)
	return shape, true
}

// normalizeFunctionBody applies the two transforms that cannot hide a
// semantic change: line-ending normalization and trailing whitespace. See
// the file header for why nothing looser is applied.
func normalizeFunctionBody(body string) string {
	lines := strings.Split(strings.ReplaceAll(body, "\r\n", "\n"), "\n")
	for i, l := range lines {
		lines[i] = strings.TrimRight(l, " \t\r")
	}
	return strings.Trim(strings.Join(lines, "\n"), "\n")
}

// normalizeFunctionSetting renders one GUC pin the way PostgreSQL stores it
// in proconfig: `name=value`, no spaces around the `=`, list items joined
// with `, `. Applied to both the rendered clause and the catalog value, so
// a server that spaced a list differently cannot be read as drift — the
// pairing is checked against a real server by TestCaptureFunctionBodyDoor's
// "PREMISE" subtest, which requires the pins to survive the round trip
// rather than merely to compare equal because both sides were empty.
func normalizeFunctionSetting(s string) string {
	name, value, ok := strings.Cut(s, "=")
	if !ok {
		return strings.TrimSpace(s)
	}
	items := strings.Split(value, ",")
	for i, it := range items {
		items[i] = strings.TrimSpace(it)
	}
	return strings.TrimSpace(name) + "=" + strings.Join(items, ", ")
}

// expectedCaptureFunctionShapes is what THIS binary would install in the
// schema, per function name. The row function carries three alternatives —
// one per ADR-0068 capture-payload mode — because the installed mode is
// recorded nowhere and all three are legitimate.
func expectedCaptureFunctionShapes(schema string) map[string][]captureFunctionShape {
	changeLog := quoteIdent(schema) + "." + quoteIdent(ChangeLogTable)
	meta := quoteIdent(schema) + "." + quoteIdent(ChangeLogMetaTable)
	renders := map[string][]string{
		CaptureFunctionRow: {
			renderCaptureRowFunction(schema, changeLog, CapturePayloadFull),
			renderCaptureRowFunction(schema, changeLog, CapturePayloadChanged),
			renderCaptureRowFunction(schema, changeLog, CapturePayloadMinimal),
		},
		CaptureFunctionTruncate: {renderCaptureTruncateFunction(schema, changeLog)},
		CaptureFunctionDDL:      {renderCaptureDDLFunction(schema, changeLog, meta)},
		CaptureFunctionDrop:     {renderCaptureDropFunction(schema, changeLog, meta)},
	}
	out := make(map[string][]captureFunctionShape, len(renders))
	for name, stmts := range renders {
		for _, stmt := range stmts {
			if shape, ok := captureFunctionShapeOfRender(name, stmt); ok {
				out[name] = append(out[name], shape)
			}
		}
	}
	return out
}

// captureFunctionDigests is the value setup records in the meta table: the
// installed definitions' fingerprints, `name=<sha256>` sorted and comma
// joined. Per FUNCTION rather than one blob for the whole install, so a
// plan that installs fewer functions than the schema happens to carry (a
// re-run under --allow-polled-fingerprint over an install that once had
// event triggers) leaves the others' provenance unknown instead of making
// every one of them look replaced.
func captureFunctionDigests(rendered map[string]string) string {
	names := make([]string, 0, len(rendered))
	for name := range rendered {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		shape, ok := captureFunctionShapeOfRender(name, rendered[name])
		if !ok {
			continue // unreachable; pinned by TestCaptureFunctionShape_ParsesEveryRender
		}
		parts = append(parts, name+"="+shape.digest())
	}
	return strings.Join(parts, ",")
}

// parseCaptureFunctionDigests reads back what [captureFunctionDigests]
// wrote, ALL OR NOTHING: a record with any unparseable entry yields no
// provenance at all, and the second return reports that.
//
// Per-entry dropping was the first cut and it was the permissive direction
// wearing conservative clothing. Trust is decided by
// [installMeta.captureDigestTrusted], which only asks whether the recorded
// string is non-empty — so garbling ONE entry left the record "trusted" while
// that function's provenance quietly went missing, and the grading below took
// the trusted-but-unrecorded arm: a WARN saying the function is "outside the
// set the last setup run installed". That is both a misattribution and a
// downgrade of the tamper REFUSAL to a WARN, available to exactly the
// adversary this door exists to catch — anyone who can replace a function
// body can also edit one byte of the digest column. A record that does not
// parse is evidence of nothing, so it is treated as nothing.
func parseCaptureFunctionDigests(recorded string) (map[string]string, bool) {
	out := map[string]string{}
	if strings.TrimSpace(recorded) == "" {
		return out, true
	}
	for _, part := range strings.Split(recorded, ",") {
		name, digest, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || name == "" || digest == "" {
			return map[string]string{}, false
		}
		out[name] = digest
	}
	return out, true
}

// # Which functions the body arm reads: the ones the triggers EXECUTE
//
// The read below is scoped by pg_proc OID — the set of `tgfoid` /
// `evtfoid` values the installed capture triggers carry
// ([boundCaptureFunctionOIDs]) — and by the sluice schema. It is NOT scoped
// by name, because `proname` does not identify a function, and two decoy
// classes have ridden through a name-scoped read:
//
//   - OVERLOAD (pre-publish value-fidelity review of v0.137.0). PostgreSQL
//     permits overloading, and with `check_function_bodies = off` any
//     `RETURNS void` plpgsql function stores `prosrc` verbatim. Gut the real
//     0-arg function, plant a same-named 1-arg decoy carrying a healthy
//     body: the last row won the map collapse, the door graded the decoy,
//     and passed. The first fix scoped the read by `pronargs = 0`.
//   - NAMESPACE (audit 2026-09-01 SLP-1). Plant `decoy.sluice_capture_change()`
//     in another schema and rebind the trigger to it: the schema-scoped read
//     graded the untouched `public.sluice_capture_change`, which is pristine
//     and digest-matching, and passed — 3 INSERTs + 1 UPDATE recorded 0
//     change-log rows at a healthy stream. The event-trigger arms and the
//     `sql_drop` arm had the identical shape.
//
// Both are the same defect — grading something other than the function
// the trigger points at — and the OID is what the trigger points at, so
// keying on it closes the class rather than its members: a decoy of any
// name, arity or schema either IS the bound function (and is graded) or is
// not (and is irrelevant). The arity scope is subsumed: an OID names one
// pg_proc row. The sluice-schema filter stays as belt-and-braces for the
// trigger grade's own namespace refusal ([gradeCaptureShape]) — a bound OID
// that does not come back is refused below, so this arm cannot silently
// grade fewer functions than the triggers execute.

// captureFunctionShapeQuery is package-level so
// [TestLoadInstalledCaptureFunctionShapes_KeysOnBoundOIDs] can grade the
// predicate itself: the one thing that must never come back is a read
// scoped by proname (see the block above for why).
func captureFunctionShapeQuery() string {
	return `
SELECT p.oid,
       p.proname,
       p.prosrc,
       p.prosecdef,
       pg_catalog.array_to_string(COALESCE(p.proconfig, '{}'::text[]), E'\n')
  FROM pg_catalog.pg_proc      p
  JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
 WHERE n.nspname = $1
   AND p.oid = ANY($2::oid[])
 ORDER BY p.proname`
}

// loadInstalledCaptureFunctionShapes reads the three definition columns for
// exactly the functions in bound — the OIDs the installed capture triggers
// execute — within the sluice schema, and refuses if any of them does not
// come back (it resolves outside the schema, or vanished between the
// trigger read and this one). proconfig is joined server-side rather than
// scanned as an array: the entries are `name=value` strings that cannot
// contain a newline, and this keeps the read off any driver array-codec
// behaviour.
func loadInstalledCaptureFunctionShapes(ctx context.Context, db *sql.DB, schema string, bound []uint32) (map[string]captureFunctionShape, error) {
	rows, err := db.QueryContext(ctx, captureFunctionShapeQuery(), schema, bound)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]captureFunctionShape{}
	missing := make(map[uint32]bool, len(bound))
	for _, oid := range bound {
		missing[oid] = true
	}
	for rows.Next() {
		var (
			oid    uint32
			shape  captureFunctionShape
			config string
		)
		if err := rows.Scan(&oid, &shape.name, &shape.body, &shape.definer, &config); err != nil {
			return nil, err
		}
		delete(missing, oid)
		// Refuse rather than collapse. Two bound OIDs sharing a name within
		// one schema is a state PostgreSQL does not produce for trigger
		// functions (0 declared arguments, so name + schema is the whole
		// signature); if it ever fires, the read is ambiguous and this door
		// must not pick a winner — silently keeping one of two definitions
		// is the exact defect the OID scope closes.
		if _, dup := out[shape.name]; dup {
			return nil, fmt.Errorf("pgtrigger: capture function %q resolves to more than one "+
				"bound definition in schema %q — the installed capture shape is ambiguous "+
				"and cannot be graded; inspect pg_proc for duplicates before resuming",
				shape.name, schema)
		}
		shape.body = normalizeFunctionBody(shape.body)
		for _, entry := range strings.Split(config, "\n") {
			if entry = strings.TrimSpace(entry); entry != "" {
				shape.settings = append(shape.settings, normalizeFunctionSetting(entry))
			}
		}
		sort.Strings(shape.settings)
		out[shape.name] = shape
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(missing) != 0 {
		oids := make([]string, 0, len(missing))
		for oid := range missing {
			oids = append(oids, fmt.Sprint(oid))
		}
		sort.Strings(oids)
		return nil, fmt.Errorf("pgtrigger: capture trigger(s) are bound to function OIDs %s that do not resolve to a function "+
			"in schema %q — the trigger executes something this door cannot grade (a function in another schema, or one dropped "+
			"between reads); refusing to stream. Re-run `sluice trigger setup --dsn=... --tables=...` to rebind the triggers to "+
			"the real capture functions",
			strings.Join(oids, ", "), schema)
	}
	return out, nil
}

// captureFunctionDrift is one function whose installed definition is not
// what this binary renders, with the provenance verdict for it.
type captureFunctionDrift struct {
	name string
	why  string
}

// gradeCaptureFunctionShapes is the body arm's pure grade: it REFUSES the
// two classes vintage cannot explain and returns the rest as drift for the
// caller to WARN about. See the file header for the whole decision table.
func gradeCaptureFunctionShapes(schema string, installed map[string]captureFunctionShape, meta installMeta) ([]captureFunctionDrift, error) {
	expected := expectedCaptureFunctionShapes(schema)
	recorded, wholeRecordParsed := parseCaptureFunctionDigests(meta.captureFnDigest)
	// A record that does not parse is evidence of nothing — see
	// [parseCaptureFunctionDigests] for why a partial read is the permissive
	// direction here, not the conservative one.
	trusted := meta.captureDigestTrusted() && wholeRecordParsed

	names := make([]string, 0, len(installed))
	for name := range installed {
		names = append(names, name)
	}
	sort.Strings(names)

	var drift []captureFunctionDrift
	for _, name := range names {
		got := installed[name]
		if !got.recordsIntoChangeLog() {
			return nil, fmt.Errorf(
				"pgtrigger: capture function %s.%s no longer writes into %s.%s — its body records NOTHING, so every change on "+
					"every table whose capture trigger calls it is silently absent from the stream (the triggers themselves are "+
					"intact, which is why the rest of this door passes). No sluice version has ever rendered such a body, so this "+
					"is a hand `CREATE OR REPLACE` (or something else editing the function), not an old install; refusing to "+
					"stream. Re-run `sluice trigger setup --dsn=... --tables=...` to reinstall the real definition (the change "+
					"log, its resume watermark and the consumer registry are preserved), and find out who replaced it",
				schema, name, schema, ChangeLogTable,
			)
		}
		// An install whose definitions are byte-identical to this binary's
		// render is SILENT here even when setup never recorded provenance
		// for them, and that is deliberate — but it is not free, so the
		// residual is stated rather than implied (Bug 259, found by the
		// v0.137.0 regression cycle; v0.137.0's own notes over-claimed a
		// warning on every not-yet-re-set-up install and were corrected).
		//
		// The cost: exactly those installs are the ones whose tamper
		// detection stays on the WARN arm below rather than the REFUSE arm,
		// because there is no recorded digest for a later edit to fail
		// against — and nothing here prompts the operator to fix that.
		//
		// Warning here would be the more protective behaviour and is NOT
		// taken, DECIDED by the operator 2026-09-01 rather than defaulted
		// into: [warnStaleCaptureFunctions]' text says the functions "are
		// not what this sluice renders", which is false for this case, so
		// warning would mean a new operator-facing surface firing on every
		// open for every existing install.
		//
		// What makes the residual small is that it cannot GROW: a fresh
		// `trigger setup` on this binary records the digest at
		// schema_version 5, so every new install is fully provenanced and
		// lands on the REFUSE arm. Only an install created by a pre-v5
		// binary that nobody re-runs setup against sits here — and the
		// SEC-1 INSECURE-CAPTURE-FUNCTION warning already steers most of
		// those to the same single re-run.
		//
		// The decision is a function of the INSTALL BASE, not of this code,
		// so it has a revisit trigger rather than being closed: see
		// docs/dev/audit-backlog.md, which keeps the three alternatives.
		if matchesAnyShape(got, expected[name]) {
			continue
		}
		want, haveRecord := recorded[name]
		switch {
		case trusted && haveRecord && want != got.digest():
			return nil, fmt.Errorf(
				"pgtrigger: capture function %s.%s does not match the definition `sluice trigger setup` installed — its body, its "+
					"`SET` pins or its SECURITY DEFINER flag were CHANGED after setup recorded them (a hand `CREATE OR REPLACE`, an "+
					"edited --dry-run plan, or a third party), so what it captures is not what this install verified. Refusing to "+
					"stream rather than forwarding whatever it now records. Re-run `sluice trigger setup --dsn=... --tables=...` to "+
					"reinstall this binary's definition (the change log, its resume watermark and the consumer registry are "+
					"preserved); if the edit was intentional, note that setup will overwrite it",
				schema, name,
			)
		case trusted && haveRecord:
			drift = append(drift, captureFunctionDrift{
				name: name,
				why:  "installed by a DIFFERENT sluice binary (it still matches the definition that install recorded, so nothing has edited it since)",
			})
		case trusted:
			// Provenance IS trustworthy, but this function is not in it —
			// the last setup run did not install it (an
			// --allow-polled-fingerprint re-run over an install that once
			// had event triggers is the shape that produces this).
			drift = append(drift, captureFunctionDrift{
				name: name,
				why:  "outside the set the last `sluice trigger setup` run installed, so no provenance was recorded for it and an older rendering CANNOT be told apart from a hand edit here",
			})
		default:
			drift = append(drift, captureFunctionDrift{
				name: name,
				// captureDigestMinSchemaVer, not ChangeLogSchemaVer: this
				// sentence describes the TRUST FLOOR, which is frozen at the
				// version that introduced the digest. Spelling it with the
				// moving constant makes the message wrong at the next
				// unrelated meta-table bump — the same defect the floor
				// itself was extracted to fix, one layer out in the prose.
				why: "installed before sluice recorded capture-function provenance (schema_version < " + fmt.Sprint(captureDigestMinSchemaVer) + "), so an older rendering and a hand edit CANNOT be told apart here",
			})
		}
	}
	return drift, nil
}

func matchesAnyShape(got captureFunctionShape, want []captureFunctionShape) bool {
	for _, w := range want {
		if got.equal(w) {
			return true
		}
	}
	return false
}

// warnStaleCaptureFunctions is the drift half's operator surface. Named
// fixes are listed because they are the reason the drift matters: an old
// body captures floats and byteas through the FIRING session's GUCs, which
// is silent value corruption rather than a cosmetic difference.
func warnStaleCaptureFunctions(ctx context.Context, schema string, drift []captureFunctionDrift) {
	if len(drift) == 0 {
		return
	}
	detail := make([]string, 0, len(drift))
	for _, d := range drift {
		detail = append(detail, d.name+" ("+d.why+")")
	}
	slog.WarnContext(ctx,
		"pgtrigger: "+staleCaptureFunctionMarker+": the installed capture function(s) are not what this sluice renders. Only a "+
			"`sluice trigger setup` re-run replaces a function body (CREATE OR REPLACE) — upgrading the binary does NOT — so an "+
			"install made by an older sluice keeps capturing through the OLD body, including before the extra_float_digits pin "+
			"(Bug 194: every captured float silently rounded when the writing session's setting is lower), before the "+
			"bytea_output pin (every captured bytea silently corrupted on the way to a MySQL/SQLite target), before the SEC-1 "+
			"search_path pin on the DDL capture function, and before the Bug 257 setup-DDL suppression. Re-run `sluice trigger "+
			"setup --dsn=... --tables=...` at your next window (seconds; the change log, its resume watermark and the consumer "+
			"registry are preserved, and the stream resumes where it left off)",
		slog.String("schema", schema),
		slog.String("functions", strings.Join(detail, ", ")))
}
