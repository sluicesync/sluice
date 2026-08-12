// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	driver "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// loadDataReaderPrefix names the handler-registry slots this engine
// installs into go-sql-driver/mysql for LOAD DATA LOCAL INFILE
// streaming. Each WriteRows call mints a unique suffix so concurrent
// writers don't collide on the package-level registry.
const loadDataReaderPrefix = "sluice_loaddata_"

// writeLoadData streams rows to MySQL via LOAD DATA LOCAL INFILE using
// the go-sql-driver Reader-handler protocol. The serializer encodes
// rows as TSV with MySQL's default escape rules (tab/newline/backslash
// escaped, NUL → \0, NULL → \N) and hands the bytes to the driver, which
// forwards them to the server. No real file is ever written.
//
// # Segmented, replayable (item 114)
//
// The table is loaded as a SEQUENCE of bounded LOAD DATA statements —
// [loadDataSegmentTarget] encoded TSV bytes each — rather than one
// statement per table, and each segment is driven through the SAME
// [RowWriter.flushWithReparentRetry] the two batched write cores use. That
// is the whole point: the batched cores ride a transient by re-driving
// their BUFFERED batch, and until this change LOAD DATA had nothing
// buffered to re-drive, so a transient minutes into a large table
// discarded the whole table's copy with no resume point. Holding one
// segment's encoded bytes gives the retry the same thing the batched
// cores have, and the retry policy, the ADR-0110 grow-gate Await/Trip, the
// wall-clock bound and the ADR-0108 keyless carve-out all come from the
// shared helper rather than being re-derived here.
//
// # Why the replay is exactly-once and not at-least-once
//
// LOAD DATA *LOCAL* INFILE downgrades a duplicate-key error to a warning
// and SKIPS the offending row — the server cannot stop a transfer already
// in flight, so it behaves as if IGNORE were given. Verified against a
// real MySQL 8.0, not assumed: see
// TestLoadDataLocal_DuplicateKeyIsAWarningAndSkipsTheRow. That makes a
// byte-identical replay CONVERGENT on a table with a key — whether the
// prior attempt rolled back, committed fully, or committed a prefix, the
// replay inserts exactly the rows that are missing. A table with no key
// has nothing to collide on, so the replay would double its rows;
// flushWithReparentRetry refuses that case loudly ([errKeylessAmbiguousReplay])
// on this core exactly as it does on the batched ones.
//
// The wart the IGNORE behaviour costs us is the post-load warning probe:
// a replay legitimately produces one 1062 warning per already-landed row,
// and those must NOT trip the Bugs-102/103 silent-clamp refusal while a
// real coercion warning hiding among them still must. See
// [RowWriter.reportLoadDataWarnings].
//
// # One deliberate divergence from the Postgres twin
//
// The PG COPY core got this same treatment first (roadmap item 38,
// [writeViaCopyChunked]) but gates its chunked path on `growGate != nil`,
// leaving a monolithic path for no-gate constructions. This one segments
// UNCONDITIONALLY: there is one code path, so a future caller that forgets
// to wire the gate cannot silently inherit the un-resumable form, and the
// measured cost of segmenting is within noise at the shipping budget
// (see [defaultLoadDataSegmentBytes]). Two paths would also mean two
// encoders to keep byte-identical, which is the shape Bug 74 came from.
//
// # What it does NOT do
//
// Segment boundaries are an IN-RUN replay point, not a durable cursor: a
// process that dies mid-table still restarts that table (or, on the
// chunked copy path, that chunk) on the next run. Cross-run resume is the
// pipeline's job and it already has one — per-(table, chunk) progress with
// an idempotent replay writer (ADR-0043/0096). Recording a row cursor here
// would need a stable source order this writer's input channel does not
// promise.
//
// On any failure (server with local_infile=OFF, geometry column
// present, runtime serialization error) the caller falls through to
// writeBatched. The fallback is best-effort: if writeBatched then
// fails too, that error wins.
func (w *RowWriter) writeLoadData(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
	cols := nonGeneratedColumns(table.Columns)
	if len(cols) == 0 {
		return fmt.Errorf("mysql: LOAD DATA: table %q has no insertable columns", table.Name)
	}
	// LOAD DATA can't natively round-trip MySQL's geometry wire format
	// (SRID-prefixed WKB through TSV would need ST_GEOMFROMWKB on the
	// server side, which isn't expressible in column-only LOAD DATA).
	// Fall back to BatchedInsert when geometry is present.
	for _, c := range cols {
		// Storage type, not declared type (Bug 233): a DOMAIN over
		// geometry emits a MySQL GEOMETRY column exactly like a bare
		// one, so it needs the same fallback — LOAD DATA cannot express
		// the ST_GEOMFROMWKB the wire format requires either way.
		if _, isGeom := ir.UnwrapDomain(c.Type).(ir.Geometry); isGeom {
			slog.WarnContext(
				ctx, "mysql: LOAD DATA: falling back to batched INSERT (table has geometry column)",
				slog.String("table", table.Name),
				slog.String("column", c.Name),
			)
			return w.writeBatched(ctx, table, rows)
		}
	}

	enabled, err := w.checkLocalInfile(ctx, table)
	if err != nil {
		return fmt.Errorf("mysql: LOAD DATA: probe @@local_infile: %w", err)
	}
	if !enabled {
		slog.WarnContext(
			ctx, "mysql: LOAD DATA: server has local_infile=OFF; falling back to batched INSERT",
			slog.String("hint", "set local_infile=ON on the server (or pass --local-infile=ON to mysqld) for ~5–10x faster bulk load"),
			slog.String("table", table.Name),
		)
		return w.writeBatched(ctx, table, rows)
	}

	name, err := mintReaderName()
	if err != nil {
		return fmt.Errorf("mysql: LOAD DATA: generate handler name: %w", err)
	}

	// ONE registry slot for the whole call. The handler hands the driver a
	// fresh reader over the CURRENT segment's bytes every time the statement
	// executes — which is precisely what makes a retry a byte-identical
	// replay instead of a re-read of a source that has already been consumed.
	// The bytes are only mutated between flushes, never during one, and
	// WriteRowsParallel never routes here (it is batched-only), so the
	// closure is read on the calling goroutine only.
	seg := &loadDataSegment{}
	driver.RegisterReaderHandler(name, func() io.Reader { return bytes.NewReader(seg.buf.Bytes()) })
	defer driver.DeregisterReaderHandler(name)

	// Pin a single connection for the LOAD DATA + post-load warning
	// check (Bugs 102/103/106 closure, v0.92.2). @@warning_count and
	// SHOW WARNINGS are session-scoped; without connection affinity
	// the pool can hand us a different conn for the warning probe and
	// the refusal silently misses. On a RETRY flushWithReparentRetry
	// substitutes a fresh conn and the probe follows it there (ADR-0108).
	// Item 139: the acquire itself rides the same transient retry.
	conn, err := w.acquireConnWithRetry(ctx, table)
	if err != nil {
		return fmt.Errorf("mysql: LOAD DATA: pin connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	stmt := buildLoadDataStmt(w.schema, table.Name, cols, name)
	target := w.loadDataSegmentTarget()
	for segment := 1; ; segment++ {
		// Encode a bounded segment BEFORE opening the statement. The
		// encode is synchronous rather than a producer goroutine feeding
		// an io.Pipe (the pre-item-114 shape): the retry needs the whole
		// segment in hand anyway, and materializing it first removes the
		// pipe, the goroutine, and the Bug-178 liveness guard that existed
		// only because an early statement error could park an encoder on a
		// pipe write nobody was draining. Measured cost of losing the
		// encode/transfer overlap: see the item-114 throughput note in
		// row_writer_loaddata_resume_integration_test.go.
		seg.buf.Reset()
		n, drained, encErr := encodeRowsTSV(ctx, &seg.buf, cols, rows, target)
		if encErr != nil {
			return fmt.Errorf("mysql: LOAD DATA: serialize rows for %q: %w", table.Name, encErr)
		}
		seg.rows = n
		if seg.rows > 0 {
			if err := w.flushLoadDataSegment(ctx, conn, table, stmt, seg, segment); err != nil {
				return err
			}
		}
		if drained {
			return nil
		}
	}
}

// loadDataSegment is one LOAD DATA statement's worth of encoded TSV,
// retained IN FULL for the length of the flush so a classified transient
// can be ridden by re-driving the identical bytes. It is the LOAD DATA
// analogue of the batched cores' buffered batch — the thing item 114
// existed because this path did not have.
type loadDataSegment struct {
	buf  bytes.Buffer
	rows int
}

// flushLoadDataSegment executes one segment through the shared ADR-0108
// retry helper, which owns the grow-gate Await/Trip, the transient
// classification, the fresh-connection re-acquire, the wall-clock bound and
// the keyless carve-out. The only LOAD-DATA-specific parts are the
// statement itself and the replay-aware warning probe.
func (w *RowWriter) flushLoadDataSegment(ctx context.Context, conn *sql.Conn, table *ir.Table, stmt string, seg *loadDataSegment, segment int) error {
	if err := w.flushWithReparentRetry(ctx, table, seg.rows, func(c *sql.Conn, isRetry bool) error {
		if injected := injectedSegmentFailure(segment, isRetry, loadDataBeforeExec); injected != nil {
			return injected
		}
		res, err := c.ExecContext(ctx, stmt)
		if err != nil {
			return fmt.Errorf("mysql: LOAD DATA into %q (%d rows): %w", table.Name, seg.rows, err)
		}
		if injected := injectedSegmentFailure(segment, isRetry, loadDataAfterExec); injected != nil {
			return injected
		}
		// Rows the server actually INSERTED. On a replay the shortfall is
		// the duplicate-key skips, and that difference is the independent
		// number reportLoadDataWarnings uses to prove the replay's warnings
		// are all harmless. A driver that cannot report it yields -1, which
		// that accounting treats as "unprovable" and refuses on.
		inserted, raErr := res.RowsAffected()
		if raErr != nil {
			inserted = -1
		}
		// CRITICAL (Bugs 102/103 v0.92.2 root cause): LOAD DATA LOCAL
		// INFILE silently bypasses strict sql_mode for per-row type-
		// conversion errors. The session's @@sql_mode is strict (we
		// inject it in parseDSN), and a direct INSERT of an 80-digit
		// NUMERIC into DECIMAL(65,30) correctly errors 1264 — but the
		// same value through LOAD DATA with `(@var) SET col=@var`
		// indirection silently clamps to MAX and bumps @@warning_count.
		//
		// Empirically (probed against MySQL 8.0 with strict sql_mode):
		//   - Direct INSERT 80-digit → Error 1264, statement aborts
		//   - LOAD DATA same value → row inserted with MAX clamp;
		//     @@warning_count = 1; SHOW WARNINGS exposes the truncation
		//   - Same for TIMESTAMP out-of-range / zero-date
		//   - Explicit CAST(@var AS DECIMAL(65,30)) in SET still clamps
		//
		// The only reliable closure is the post-load warning probe, and it
		// runs on the conn this attempt used — every segment, never
		// sampled, because on this path the warning IS the error.
		return w.reportLoadDataWarnings(ctx, c, table.Name, seg.rows, inserted, isRetry)
	}, conn); err != nil {
		return err
	}
	if bulkFlushHookForTest != nil {
		bulkFlushHookForTest(seg.rows, int64(seg.buf.Len()))
	}
	return nil
}

// loadDataSegmentTarget is the encoded-TSV byte budget for one segment.
// Zero-value-safe: the operator knob only ever LOWERS it, so every
// construction that never sets maxBufferBytes (tests, direct API use, the
// broker) gets the package default.
func (w *RowWriter) loadDataSegmentTarget() int64 {
	if w.maxBufferBytes > 0 && w.maxBufferBytes < defaultLoadDataSegmentBytes {
		return w.maxBufferBytes
	}
	return defaultLoadDataSegmentBytes
}

// reportBulkWriteWarnings inspects the diagnostic-area warnings produced
// by the just-completed bulk write (LOAD DATA, or a batched INSERT) on the
// pinned conn, and either refuses loudly (strict sql_mode) or WARNs loudly
// once per table (relaxed sql_mode, --mysql-sql-mode=”) — closing the
// Vector B silent-clamp gap. With zero warnings it's a no-op.
//
// SHOW WARNINGS is read FIRST (before any other statement on the conn):
// reading `@@warning_count` first empties the diagnostic list, so the
// prior code's subsequent SHOW WARNINGS returned nothing and the refusal
// rendered an empty `Examples: []`. Reading SHOW WARNINGS directly gives
// both the count (row count) AND the detail. The reverse ordering — the
// list first, then the count — IS safe, and item 114's replay accounting
// depends on it; that is pinned by
// TestLoadDataLocal_TrueWarningCountSurvivesShowWarnings rather than left
// as a claim.
//
// Strict sql_mode: LOAD DATA / non-strict-INSERT downgrade per-row
// type-conversion errors to warnings; a non-empty list is silent
// corruption sluice must refuse (Bugs 102/103 / v0.92.2).
//
// Relaxed sql_mode (`--mysql-sql-mode=”`): the operator opted into
// server-side coercion (e.g. to accept legacy zero-dates, now better
// handled read-side by --zero-date). That opt-in does NOT make a silent
// numeric clamp / string truncation acceptable — MySQL still flags it
// (verified: under sql_mode=” an out-of-range value clamps AND bumps the
// warning list). So instead of the pre-Vector-B silent skip, emit a loud
// one-time-per-table WARN naming the coercions + the data-preserving
// remedy. Not a refusal: the operator chose relaxed mode deliberately.
// diagnosticQuerier is the session surface [readShowWarnings] probes: a
// pinned *sql.Conn (the bulk-write paths) or an open *sql.Tx (the CDC
// apply path, task C2). Either way the probe MUST run on the SAME
// session as the just-executed statement — the diagnostics area is
// session-scoped and per-statement.
type diagnosticQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// showWarnings is what one SHOW WARNINGS read yielded. Visible is the
// number of rows the server RETURNED, which the server caps at
// @@max_error_count (1024 by default) — it is NOT necessarily the number of
// warnings the statement produced. Anything that needs the true total must
// read @@warning_count ([readTrueWarningCount]); the item-114 replay
// accounting does, and the difference is load-bearing there.
type showWarnings struct {
	// Details holds up to 8 formatted lines for the operator message.
	Details []string
	// Visible counts the rows SHOW WARNINGS returned (capped, see above).
	Visible int
	// NonDup counts the VISIBLE rows whose code is not 1062 (duplicate
	// key). Used as a second, sample-based signal alongside the replay
	// accounting — see [replayWarningsAreOnlyDuplicates].
	NonDup int
}

// mysqlDupKeyWarningCode is the SHOW WARNINGS code column value for the
// duplicate-key warning LOAD DATA LOCAL emits instead of an error.
const mysqlDupKeyWarningCode = "1062"

// readShowWarnings reads SHOW WARNINGS on q (must be read before any
// other statement on the session, which would clear the diagnostic list),
// returning up to 8 formatted detail lines and the visible warning count.
func readShowWarnings(ctx context.Context, q diagnosticQuerier, table string) (showWarnings, error) {
	var sw showWarnings
	rows, err := q.QueryContext(ctx, "SHOW WARNINGS")
	if err != nil {
		return showWarnings{}, fmt.Errorf("mysql: write into %q: SHOW WARNINGS failed: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var level, code, msg string
		if err := rows.Scan(&level, &code, &msg); err != nil {
			return showWarnings{}, fmt.Errorf("mysql: write into %q: scan warning: %w", table, err)
		}
		sw.Visible++
		if code != mysqlDupKeyWarningCode {
			sw.NonDup++
		}
		// Cap at a few warnings — gigantic loads can emit thousands and
		// we just need enough for the operator to diagnose.
		if len(sw.Details) < 8 {
			sw.Details = append(sw.Details, fmt.Sprintf("%s %s: %s", level, code, msg))
		}
	}
	if err := rows.Err(); err != nil {
		return showWarnings{}, fmt.Errorf("mysql: write into %q: iterate warnings: %w", table, err)
	}
	return sw, nil
}

// warningCountQuerier is the session surface [readTrueWarningCount] reads:
// a pinned *sql.Conn (the bulk-write paths) or an open *sql.Tx (the CDC
// apply path) — the same session-scoping requirement as [diagnosticQuerier].
type warningCountQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// readTrueWarningCount reads @@warning_count — the number of warnings the
// last statement produced, UNCAPPED, unlike the SHOW WARNINGS row count
// which the server truncates at @@max_error_count. It must be read AFTER
// the SHOW WARNINGS that consumes the detail list: SHOW WARNINGS does not
// clear the count (verified against MySQL 8.0 in
// TestLoadDataLocal_TrueWarningCountSurvivesShowWarnings), while a
// prepared-statement read of @@warning_count DOES clear the list — which is
// why this is a no-argument query (go-sql-driver sends it as a plain
// COM_QUERY) and why it runs second.
func readTrueWarningCount(ctx context.Context, q warningCountQuerier) (int64, error) {
	var n int64
	if err := q.QueryRowContext(ctx, "SELECT @@warning_count").Scan(&n); err != nil {
		return 0, fmt.Errorf("mysql: read @@warning_count: %w", err)
	}
	return n, nil
}

func (w *RowWriter) reportBulkWriteWarnings(ctx context.Context, conn *sql.Conn, table string) error {
	sw, err := readShowWarnings(ctx, conn, table)
	if err != nil {
		return err
	}
	// Decide on the UNCAPPED @@warning_count, not the SHOW WARNINGS row count:
	// the server truncates that list at @@max_error_count, and a server tuned to
	// 0 returns an empty list while @@warning_count stays accurate (COLD-1).
	// Reading @@warning_count needs no privilege (SETTING max_error_count does,
	// which is why this reads rather than pins the session variable). A read
	// failure degrades to the visible count rather than failing a clean write.
	count := sw.Visible
	if total, terr := readTrueWarningCount(ctx, conn); terr == nil {
		count = int(total)
	}
	return w.decideBulkWriteWarnings(ctx, table, sw, count)
}

// decideBulkWriteWarnings is the refuse-or-WARN policy over an already-read
// warning list. count is the number to REPORT — the visible count for the
// ordinary paths, the true @@warning_count where the caller has read it.
func (w *RowWriter) decideBulkWriteWarnings(ctx context.Context, table string, sw showWarnings, count int) error {
	details := sw.Details
	if count == 0 {
		return nil
	}
	more := ""
	if count > 8 {
		more = fmt.Sprintf(" (… and %d more)", count-8)
	}

	if resolveSessionSQLMode(w.sqlMode) == "" {
		// Relaxed: WARN once per table, don't refuse.
		if _, seen := w.warnedClamp.LoadOrStore(table, struct{}{}); !seen {
			slog.WarnContext(
				ctx,
				"mysql: target SILENTLY coerced value(s) under --mysql-sql-mode='' — out-of-range/over-long "+
					"values were clamped or truncated on write, not refused (Vector B). The migration proceeds "+
					"with the coerced values per your relaxed sql_mode opt-in.",
				slog.String("table", table),
				slog.Int("warnings", count),
				slog.String("examples", strings.Join(details, "; ")+more),
				slog.String("hint", "to PRESERVE such values, map the column to a fitting type via "+
					"--type-override (e.g. =decimal(P,S) for a numeric overflow, =text/=varchar for an over-long "+
					"string, =datetime for an out-of-range timestamp) or fix the source; to REFUSE instead of "+
					"coerce, drop --mysql-sql-mode='' so strict mode rejects the value loudly"),
			)
		}
		return nil
	}

	return fmt.Errorf("mysql: bulk write into %q produced %d warning(s) under strict sql_mode — "+
		"LOAD DATA's per-row type-conversion errors are silently downgraded to warnings (a MySQL "+
		"behaviour quirk this refusal closes; Bugs 102/103 / v0.92.2). Examples: [%s]%s. "+
		"Recovery (data-preserving): map the column to a target type that FITS the value via "+
		"--type-override — e.g. `=datetime` for an out-of-range timestamp (MySQL DATETIME covers "+
		"1000–9999 vs TIMESTAMP's 1970–2038), `=decimal(P,S)` for a numeric overflow, "+
		"or `=text`/`=varchar` to keep the raw value — or fix the source data. "+
		"Do NOT pass --mysql-sql-mode='' to silence a range/overflow/truncation warning: under "+
		"non-strict sql_mode MySQL SILENTLY clamps or truncates the offending values (out-of-range "+
		"dates → 0000-00-00, over-long strings cut, numbers clamped) and the migration exits 0 with "+
		"corrupted data. --mysql-sql-mode='' is appropriate ONLY for accepting genuinely legacy "+
		"zero-date data as-is (see docs/operator/migrating-legacy-mysql.md)",
		table, count, strings.Join(details, "; "), more)
}

// reportLoadDataWarnings is the LOAD DATA segment's post-load warning probe
// (item 114). On a FIRST attempt it is exactly [RowWriter.reportBulkWriteWarnings]
// — any warning is the Bugs-102/103 silent-clamp signal and refuses under
// strict sql_mode.
//
// On a REPLAY it has one more job. LOAD DATA LOCAL downgrades duplicate-key
// to a warning and skips the row, so a byte-identical re-drive of a segment
// whose prior attempt landed some or all of its rows legitimately produces
// one 1062 warning PER already-landed row. Refusing on those would make the
// retry useless; tolerating warnings wholesale would let a real coercion
// hide among them. So the tolerance is bought with an INDEPENDENT number
// rather than asserted:
//
//   - duplicates skipped = rows in the segment − rows the server reports
//     INSERTED (driver-side affected-rows, not sluice's bookkeeping);
//   - total warnings = @@warning_count, the server's UNCAPPED count;
//   - anything left over is a warning duplicates cannot explain ⇒ refuse.
//
// The sampled SHOW WARNINGS list cannot serve as that evidence on its own:
// the server caps it at @@max_error_count (1024), so on a large replay a
// single truncation warning can sit entirely outside the visible window.
// That exact case is pinned in
// TestLoadDataLocal_TruncationHidesBehindDuplicateWarnings.
func (w *RowWriter) reportLoadDataWarnings(ctx context.Context, conn *sql.Conn, table string, segRows int, inserted int64, replay bool) error {
	sw, err := readShowWarnings(ctx, conn, table)
	if err != nil {
		return err
	}
	// The SHOW WARNINGS row count cannot answer "were there any warnings?": the
	// server truncates the list at @@max_error_count, and a server tuned to 0
	// returns an empty list while @@warning_count stays accurate (COLD-1). Read
	// the uncapped count (no privilege needed, unlike SETTING max_error_count)
	// and gate on it. On a read failure, degrade to the visible count rather
	// than failing a clean write.
	total, terr := readTrueWarningCount(ctx, conn)
	if terr != nil {
		if sw.Visible == 0 {
			return nil
		}
		return w.decideBulkWriteWarnings(ctx, table, sw, sw.Visible)
	}
	if total == 0 {
		return nil
	}
	if !replay {
		return w.decideBulkWriteWarnings(ctx, table, sw, int(total))
	}
	// Replay: the duplicate-key accounting needs to SEE the warning codes to
	// classify each as a tolerable dup skip. If the visible list was truncated
	// below the true count (a suppressed @@max_error_count), the hidden warnings
	// cannot be classified, so a silent coercion could sit among them — decide
	// on the true count (refuses under strict) rather than tolerating blind.
	if int64(sw.Visible) < total {
		return w.decideBulkWriteWarnings(ctx, table, sw, int(total))
	}
	if !replayWarningsAreOnlyDuplicates(segRows, inserted, total, sw.NonDup) {
		return w.decideBulkWriteWarnings(ctx, table, sw, int(total))
	}
	slog.WarnContext(
		ctx, "mysql: LOAD DATA replay after a transient re-sent rows the target had already committed; "+
			"MySQL skipped them as duplicate keys (LOAD DATA LOCAL reports duplicate-key as a warning), "+
			"so the segment landed exactly once",
		slog.String("table", table),
		slog.Int("segment_rows", segRows),
		slog.Int64("inserted", inserted),
		slog.Int64("duplicates_skipped", int64(segRows)-inserted),
	)
	return nil
}

// replayWarningsAreOnlyDuplicates reports whether every warning a replayed
// segment produced is accounted for by a duplicate-key skip.
//
// Pure, so the decision matrix is unit-pinned without a server. The
// conservative direction is always "no": an accounting that does not add up
// (unknown affected-rows, more warnings than skipped rows, a visible warning
// that is not 1062) refuses, because the cost of a false refusal is a
// restart and the cost of a false tolerance is a silently coerced value.
//
// Note a row can be BOTH truncated and duplicate — two warnings, one skip —
// which shows up here as total > skipped and refuses. That is intended.
func replayWarningsAreOnlyDuplicates(segRows int, inserted, totalWarnings int64, visibleNonDup int) bool {
	if inserted < 0 || visibleNonDup > 0 {
		return false
	}
	skipped := int64(segRows) - inserted
	if skipped <= 0 {
		return false
	}
	return totalWarnings <= skipped
}

// checkLocalInfile reports whether the server's `local_infile` system
// variable is enabled. MySQL 8.0+ ships with it OFF by default, in
// which case any LOAD DATA LOCAL INFILE statement fails server-side.
// Pre-flighting here lets the writer fall back to BatchedInsert with
// one WARN line instead of crashing mid-stream.
//
// Item 139 sibling, found by the acquire pin rather than by the field
// report: this probe runs BEFORE the pin, so on the LOAD DATA lane a
// vtgate drop killed the run one statement EARLIER than the reported
// `pin connection` site — same error, same run-ending disposition, and
// invisible to a roster that looks only at Conn() call sites. It now
// checks its connection out through [RowWriter.acquireConnWithRetry] and
// runs the probe on that session, which is also the more faithful read
// (a session variable read on the pool is a read of some other session).
func (w *RowWriter) checkLocalInfile(ctx context.Context, table *ir.Table) (bool, error) {
	conn, err := w.acquireConnWithRetry(ctx, table)
	if err != nil {
		return false, err
	}
	defer func() { _ = conn.Close() }()
	var v string
	if err := conn.QueryRowContext(ctx, "SELECT @@local_infile").Scan(&v); err != nil {
		return false, err
	}
	v = strings.TrimSpace(v)
	return v == "1" || strings.EqualFold(v, "ON"), nil
}

// buildLoadDataStmt returns the LOAD DATA LOCAL INFILE statement that
// pulls bytes from the named registered reader.
//
// Two MySQL-isms shape the statement form:
//
//   - CHARACTER SET binary disables the per-stream utf8mb4 validation
//     pass; raw bytes in BLOB/VARBINARY columns (the destination of
//     `[]byte` IR values) would otherwise trip Error 1300 ("Invalid
//     utf8mb4 character string") on the first non-ASCII byte.
//   - With CHARACTER SET binary, JSON columns reject input as
//     "Cannot create a JSON value from a string with CHARACTER SET
//     'binary'" because the JSON validator demands a Unicode charset.
//     The fix is to load every field into a user variable and assign
//     real columns via a SET clause — JSON columns get a CONVERT()
//     wrapper that re-tags the bytes as utf8mb4 (the serializer
//     already emits valid UTF-8 for JSON values per prepareValue).
//
// Generated columns are skipped upstream so the column list and the
// per-row TSV stream stay in lockstep.
//
// The FIELDS/ESCAPED/LINES framing bytes are written as HEX string
// literals (X'09' tab, X'5C' backslash, X'0A' newline) rather than
// backslash-escaped literals ('\t', '\\', '\n'). This is deliberate and
// load-bearing (Bug 178): those clauses are ordinary MySQL string
// literals, so the server parses them under the session's sql_mode. Under
// NO_BACKSLASH_ESCAPES a `'\\'` literal is TWO backslash bytes, not one —
// which trips MySQL's "ESCAPED BY must be a single character" check and
// aborts the statement with Error 1083 BEFORE it requests the LOCAL INFILE
// stream, deadlocking the writer (the driver never drains the reader; see
// writeLoadData). Hex literals are NBE-invariant — the server yields the
// same bytes regardless of sql_mode — so the framing the writer emits
// (real 0x09 / 0x5C / 0x0A via appendEscapedByte) always matches what
// LOAD DATA parses. The bytes are identical to the old escaped form; only
// the source-literal spelling changed.
func buildLoadDataStmt(schema, tableName string, cols []*ir.Column, readerName string) string {
	target := quoteIdent(tableName)
	if schema != "" {
		target = quoteIdent(schema) + "." + quoteIdent(tableName)
	}

	// Per-column user variables: `@c0, @c1, ...`. Avoids identifier
	// collisions with the columns themselves (a column literally
	// named after the original would otherwise alias the variable).
	vars := make([]string, len(cols))
	setParts := make([]string, len(cols))
	for i, c := range cols {
		vars[i] = fmt.Sprintf("@c%d", i)
		setParts[i] = quoteIdent(c.Name) + " = " + columnSetExpr(c, vars[i])
	}

	return fmt.Sprintf(
		"LOAD DATA LOCAL INFILE 'Reader::%s' INTO TABLE %s "+
			"CHARACTER SET binary "+
			"FIELDS TERMINATED BY X'09' ESCAPED BY X'5C' "+
			"LINES TERMINATED BY X'0A' "+
			"(%s) SET %s",
		readerName, target,
		strings.Join(vars, ", "),
		strings.Join(setParts, ", "),
	)
}

// columnSetExpr returns the SET-clause RHS for col, given the user
// variable that holds its raw input. JSON columns get a CONVERT()
// wrapper so MySQL's JSON parser sees utf8mb4-tagged bytes; every
// other column type takes the variable verbatim — MySQL's implicit
// conversion handles VARCHAR/TEXT (binary→utf8mb4 reinterpretation),
// numerics (string parse), and binary types (passthrough).
//
// # It dispatches on the STORAGE type, not the declared one (Bug 233)
//
// Every arm below is a statement about the MySQL column this IR type
// EMITS to, so the question it asks is "what does this land as", and a
// [ir.Domain] wrapper is not part of that answer — the DDL emitter
// downgrades a DOMAIN to its base type, so a `CREATE DOMAIN d AS
// json[]` column is a MySQL `JSON` column exactly like a bare `json[]`
// one. Dispatching on the declared type instead matched no arm for
// every DOMAIN-typed column, so the JSON ones took the bare
// `varName` fall-through under `CHARACTER SET binary` and MySQL
// answered with Error 3144, zero rows and no sluice refusal — on
// `migrate` and on `restore` alike, for EVERY array element family and
// for scalar `json`. Only the LOAD DATA core was affected: the batched
// core binds what [prepareValue] returns, and that has unwrapped
// domains since Bug 122.
func columnSetExpr(col *ir.Column, varName string) string {
	t := ir.UnwrapDomain(col.Type)
	// ir.JSON and ir.Array both emit a MySQL `JSON` column
	// (emitColumnType maps ir.Array → JSON; the IR keeps the source
	// type ir.Array). Under `CHARACTER SET binary` MySQL's JSON
	// validator rejects un-retagged bytes with Error 3144 ("Cannot
	// create a JSON value from a string with CHARACTER SET 'binary'"),
	// so both need the utf8mb4 CONVERT wrapper — the LOAD DATA half of
	// the value-side ir.Array→JSON fix in prepareValue (Bug 18). The
	// serializer already emits valid UTF-8 JSON text for an array
	// value; this is only a charset re-tag.
	switch t.(type) {
	case ir.JSON, ir.Array:
		return "CONVERT(" + varName + " USING utf8mb4)"
	}
	// catalog Bug 75: ir.Bit is streamed as the canonical '0'/'1'
	// bit-string (see encodeRowsTSV). MySQL's BIT(N) column needs the
	// numeric value, so parse the base-2 digits. NULLIF keeps a NULL
	// field (\N → SQL NULL) NULL rather than CONV('')→0. CAST to
	// UNSIGNED so the assignment to BIT(N) is the integer value, not a
	// re-stringified one.
	if _, isBit := t.(ir.Bit); isBit {
		return "CAST(CONV(NULLIF(" + varName + ", ''), 2, 10) AS UNSIGNED)"
	}
	// Text-like columns: re-tag the binary stream as utf8mb4 so
	// CHECK constraints and stored procedures see the column's
	// declared charset rather than `binary`. The bytes themselves
	// are unchanged.
	switch t.(type) {
	case ir.Varchar, ir.Text, ir.Set:
		return "CONVERT(" + varName + " USING utf8mb4)"
	}
	// Bug 48 fix: PG extensions with cross-engine default translators
	// (hstore → MySQL JSON; citext → MySQL VARCHAR-with-collation) keep
	// their `ir.ExtensionType` shape in the IR until the writer emits;
	// the value-side `prepareHstoreToJSON` translator runs at INSERT
	// time but doesn't touch the LOAD DATA path. Without the CONVERT
	// wrapper, MySQL parses the hstore→JSON bytes as charset=binary and
	// rejects with "Cannot create a JSON value from a string with
	// CHARACTER SET 'binary'" (Error 3144 / SQLSTATE 22032). Apply the
	// same utf8mb4 reinterpretation that ir.JSON / ir.Varchar / ir.Text
	// get — the bytes already are UTF-8; this is just a charset tag.
	// vector / pg_trgm / postgis don't reach this path (they refuse at
	// the cross-engine preflight); hstore and citext are the only
	// ExtensionType arms with cross-engine default translators today.
	if _, isExt := t.(ir.ExtensionType); isExt {
		return "CONVERT(" + varName + " USING utf8mb4)"
	}
	return varName
}

// mintReaderName generates a unique registry slot name for one
// WriteRows invocation. Random hex suffix keeps concurrent writers
// from colliding on the package-level handler registry.
func mintReaderName() (string, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return loadDataReaderPrefix + hex.EncodeToString(buf[:]), nil
}

// encodeRowsTSV consumes rows from the channel, runs each value
// through prepareValue + tsvEncode, and writes tab-separated lines to
// w until the byte budget is reached, the channel closes, or ctx is
// cancelled. Returns the first error it encounters.
//
// limitBytes is the item-114 segment budget: a non-positive value means
// "the whole channel" (the pre-item-114 behaviour, still what the encoder's
// own unit tests exercise). The budget is SOFT and bounds accumulation, not
// individual rows — the check runs after a row is appended, so a row larger
// than the whole budget still ships as a one-row segment. Rows are never
// split, so a segment boundary is always a row boundary, which is what
// makes a segment independently replayable.
//
// drained is true only when the row channel CLOSED; a budget-closed segment
// returns false and the caller comes back for the next one.
func encodeRowsTSV(ctx context.Context, w io.Writer, cols []*ir.Column, rows <-chan ir.Row, limitBytes int64) (rowsWritten int, drained bool, err error) {
	// One reusable byte buffer so the per-row hot path doesn't
	// allocate per-field. The serializer writes to buf, then flushes
	// the row at end-of-line.
	buf := make([]byte, 0, 4096)
	var written int64

	for {
		select {
		case row, ok := <-rows:
			if !ok {
				return rowsWritten, true, nil
			}
			buf = buf[:0]
			for i, c := range cols {
				if i > 0 {
					buf = append(buf, '\t')
				}
				raw := row[c.Name]
				// catalog Bug 75: ir.Bit. The INSERT path wants ceil(N/8)
				// big-endian bytes (what prepareValue produces), but LOAD
				// DATA reinterprets a binary field as a string and
				// corrupts it. Instead emit the IR-canonical '0'/'1'
				// bit-string verbatim and let columnSetExpr's CONV(...,2,10)
				// SET expression parse it. NULL stays NULL.
				//
				// Storage type, not declared type (Bug 233), and this half
				// must move with columnSetExpr's: the two form one
				// agreement about what the field on the wire means, so a
				// DOMAIN over BIT that took one branch and not the other
				// would write the '0'/'1' text and then not parse it.
				if _, isBit := ir.UnwrapDomain(c.Type).(ir.Bit); isBit && raw != nil {
					if s, ok := raw.(string); ok {
						buf = appendEscapedString(buf, s)
						continue
					}
				}
				v, err := prepareValue(raw, c)
				if err != nil {
					return rowsWritten, false, err
				}
				buf, err = tsvEncode(buf, v)
				if err != nil {
					return rowsWritten, false, err
				}
			}
			buf = append(buf, '\n')
			if _, err := w.Write(buf); err != nil {
				return rowsWritten, false, err
			}
			rowsWritten++
			written += int64(len(buf))
			if limitBytes > 0 && written >= limitBytes {
				return rowsWritten, false, nil
			}
		case <-ctx.Done():
			return rowsWritten, false, ctx.Err()
		}
	}
}

// tsvEncode appends one MySQL-LOAD-DATA-escaped field to dst and
// returns the new slice. NULL values emit the literal `\N` (the
// default ESCAPED BY sequence MySQL recognises). Strings and byte
// slices are escaped per MySQL's rules: tab/newline/CR/backslash/NUL
// are backslash-escaped.
func tsvEncode(dst []byte, v any) ([]byte, error) {
	if v == nil {
		return append(dst, '\\', 'N'), nil
	}
	switch x := v.(type) {
	case string:
		return appendEscapedString(dst, x), nil
	case []byte:
		return appendEscapedBytes(dst, x), nil
	case bool:
		if x {
			return append(dst, '1'), nil
		}
		return append(dst, '0'), nil
	case int:
		return strconv.AppendInt(dst, int64(x), 10), nil
	case int8:
		return strconv.AppendInt(dst, int64(x), 10), nil
	case int16:
		return strconv.AppendInt(dst, int64(x), 10), nil
	case int32:
		return strconv.AppendInt(dst, int64(x), 10), nil
	case int64:
		return strconv.AppendInt(dst, x, 10), nil
	case uint:
		return strconv.AppendUint(dst, uint64(x), 10), nil
	case uint8:
		return strconv.AppendUint(dst, uint64(x), 10), nil
	case uint16:
		return strconv.AppendUint(dst, uint64(x), 10), nil
	case uint32:
		return strconv.AppendUint(dst, uint64(x), 10), nil
	case uint64:
		return strconv.AppendUint(dst, x, 10), nil
	case float32:
		return strconv.AppendFloat(dst, float64(x), 'g', -1, 32), nil
	case float64:
		return strconv.AppendFloat(dst, x, 'g', -1, 64), nil
	case time.Time:
		// MySQL's canonical DATETIME literal format. UTC is forced
		// because the engine connects with `loc=UTC` (see
		// connect.go); preserving the original location here would
		// silently re-interpret times under the session's timezone.
		return x.UTC().AppendFormat(dst, "2006-01-02 15:04:05.999999"), nil
	default:
		return nil, fmt.Errorf("mysql: LOAD DATA: unsupported value type %T", v)
	}
}

// appendEscapedString writes s to dst with MySQL's LOAD DATA escape
// rules applied. The empty string is emitted as zero bytes between
// field separators — MySQL reads that as the column's default if NOT
// NULL (typically empty string), matching the BatchedInsert path's
// behaviour for empty-string values.
func appendEscapedString(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		dst = appendEscapedByte(dst, s[i])
	}
	return dst
}

// appendEscapedBytes is the []byte twin of appendEscapedString. Kept
// as a separate function so the hot path on string values doesn't pay
// the conversion cost.
func appendEscapedBytes(dst, b []byte) []byte {
	for i := 0; i < len(b); i++ {
		dst = appendEscapedByte(dst, b[i])
	}
	return dst
}

// appendEscapedByte applies the four MySQL LOAD DATA escapes that
// matter under the default (TAB-separated, backslash-escape) format:
//
//   - 0x00 → "\0"
//   - 0x09 → "\t"
//   - 0x0a → "\n"
//   - 0x0d → "\r"
//   - 0x5c → "\\"
//
// Other bytes pass through unchanged. This matches the inverse rules
// the MySQL server applies on input.
func appendEscapedByte(dst []byte, c byte) []byte {
	switch c {
	case 0x00:
		return append(dst, '\\', '0')
	case '\t':
		return append(dst, '\\', 't')
	case '\n':
		return append(dst, '\\', 'n')
	case '\r':
		return append(dst, '\\', 'r')
	case '\\':
		return append(dst, '\\', '\\')
	default:
		return append(dst, c)
	}
}

// defaultLoadDataSegmentBytes is the encoded-TSV byte target that closes
// one LOAD DATA segment (item 114) — the single knob trading the three
// things the segmentation moves.
//
// RESUME GRANULARITY. A transient costs one segment's re-send instead of
// the whole table. Against the defect this closes — a multi-hour copy
// discarded entirely — anything from a few MiB up is a rounding error, so
// this end of the trade is insensitive.
//
// THROUGHPUT. Measured, not assumed (TestLoadDataSegments_ThroughputCost):
// an extra segment costs exactly one extra TRANSACTION COMMIT on the target
// and nothing else. On the development box a one-row LOAD DATA and a
// one-row INSERT both cost ~53 ms — identical, so the cost is the target's
// commit, not anything LOAD DATA does — against a 0.6 ms round trip and a
// 0.35 ms added SHOW WARNINGS probe. End-to-end over a 7.2 MiB corpus that
// prices in linearly with the segment COUNT: 8 MiB budget 0.99x, 4 MiB
// 1.09x, 2 MiB 1.19x, 1 MiB 1.46x, 256 KiB 2.59x. At 16 MiB the shipping
// configuration is within measurement noise of the pre-item-114
// single-statement copy. For scale, the batched-INSERT core this same
// writer falls back to commits every ~1 MiB — a 16 MiB-segmented LOAD DATA
// commits 16x LESS often than the alternative write path, so the throughput
// advantage LOAD DATA is chosen for survives intact.
//
// MEMORY, the binding constraint and a genuinely new cost: one segment's
// bytes are held per CONCURRENT WRITER, where the streaming pre-item-114
// form held 64 KiB. A PG→MySQL cold copy can run --table-parallelism ×
// --bulk-parallelism writers (auto 4 × min(8,NumCPU)), so the worst case is
// tens of these. 16 MiB keeps each one well inside the ADR-0028 per-writer
// buffer budget (--max-buffer-bytes, 64 MiB default) that the batched cores
// already work within, and --max-buffer-bytes lowers it for a
// memory-constrained run (see [RowWriter.loadDataSegmentTarget]).
//
// 16 MiB also sits under MySQL's default 64 MiB max_allowed_packet, so a
// segment never depends on the driver's packet splitting to be deliverable.
//
// A package var, not a const, ONLY so tests can shrink it to get many
// segments out of a small table (and, in the throughput pin, raise it to
// re-create the pre-item-114 one-statement-per-table shape for the
// before/after comparison). Production never mutates it, so there is no
// config field and no zero-value path — the v0.99.51 trap does not apply.
// Same disposition as coldCopyReparentMaxWallVar next door.
var defaultLoadDataSegmentBytes int64 = 16 << 20

// loadDataSegmentPhase names where within one segment attempt the test
// failpoint fires. BOTH shapes matter and they exercise different halves of
// the replay's correctness, which is why the hook carries the phase rather
// than picking one:
//
//   - [loadDataBeforeExec] models an attempt that ROLLED BACK. The replay
//     must then carry the segment's bytes, because nothing landed — a
//     replay that sent an empty stream would pass every post-commit
//     assertion.
//   - [loadDataAfterExec] models COMMITTED-BUT-UNACKED, the shape a killed
//     connection cannot produce on demand: the server has the rows and the
//     client does not know it, so the replay must converge rather than
//     duplicate.
type loadDataSegmentPhase int

const (
	loadDataBeforeExec loadDataSegmentPhase = iota
	loadDataAfterExec
)

// loadDataSegmentFailHookForTest, when non-nil, is consulted at each phase
// of every LOAD DATA segment attempt with the 1-based segment number and
// whether this attempt is a replay; a non-nil return is injected as the
// attempt's error. nil in production — two nil checks on a per-segment (not
// per-row) path.
var loadDataSegmentFailHookForTest func(segment int, replay bool, phase loadDataSegmentPhase) error

// injectedSegmentFailure returns the test failpoint's error for this phase,
// or nil when no hook is installed (always, in production).
func injectedSegmentFailure(segment int, replay bool, phase loadDataSegmentPhase) error {
	if loadDataSegmentFailHookForTest == nil {
		return nil
	}
	return loadDataSegmentFailHookForTest(segment, replay, phase)
}
