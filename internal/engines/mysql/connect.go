// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/netkeepalive"
)

// keepaliveNet is a custom driver "network" name registered with the
// MySQL driver that routes plain-TCP connections through sluice's
// shared TCP keep-alive dialer (see [netkeepalive]). parseDSN swaps a
// `tcp` DSN onto this network so every MySQL query connection inherits
// the keep-alive policy; unix sockets and operator-specified networks
// are left untouched (TCP keep-alive is meaningless off TCP).
const keepaliveNet = "tcp+sluicekeepalive"

func init() {
	mysql.RegisterDialContext(keepaliveNet, func(ctx context.Context, addr string) (net.Conn, error) {
		return netkeepalive.Dialer().DialContext(ctx, "tcp", addr)
	})
}

// defaultStrictSQLMode is the v0.92.1 strict-by-default mode list
// applied to every MySQL connection unless the operator overrides
// it via --mysql-sql-mode (CLI) or `sql_mode` in the DSN params.
// Closes Bugs 102/103 silent-loss class by surfacing MySQL's own
// loud-error path instead of inheriting whatever sql_mode the
// server defaults to (often relaxed on dev / older / managed
// deployments).
const defaultStrictSQLMode = "STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO"

// resolveSessionSQLMode collapses an engine's --mysql-sql-mode override to the
// concrete mode sluice injects into every MySQL connection's
// `SET SESSION sql_mode = '...'` post-handshake. nil (an override-free engine,
// every bare `Engine{}` / test / non-CLI construction) resolves to
// [defaultStrictSQLMode] — the Bug 102/103 strict-by-default. A non-nil value is
// the operator's explicit --mysql-sql-mode choice, including "" (fall through to
// the server default — the legacy-data escape hatch). The override was formerly
// the process-wide MUTABLE `sessionSQLMode` global set by SetSessionSQLMode; task
// 2.5 (finding A-4) moves it onto the per-instance [engineOptions.sqlMode],
// threaded into [openDB] so a fleet `sync run` can carry a distinct mode per sync.
func resolveSessionSQLMode(sqlMode *string) string {
	if sqlMode != nil {
		return *sqlMode
	}
	return defaultStrictSQLMode
}

// backslashIsMySQLEscape reports whether MySQL's string-literal lexer treats
// a backslash as an escape introducer under the sql_mode sluice injects into
// its sessions (the engine's resolved --mysql-sql-mode, sqlMode): true unless
// the configured mode includes NO_BACKSLASH_ESCAPES. The DDL emitters key their
// string-literal quoting off this ([mysqlEmitter.quoteSQLString], SEC-1 review
// gap 2): when backslash is an escape, every literal backslash must be doubled or
// MySQL silently decodes it. The [SchemaWriter] resolves the policy ONCE at open
// from the engine's sqlMode and carries it on its emitter, so the emit surface
// stays a pure function of (policy, IR) with no package global.
//
// Two documented approximations, both conservative toward MySQL's factory
// default (backslash IS an escape):
//
//   - a "" mode means "fall through to the server default", which sluice cannot
//     see here; no stock MySQL default includes NO_BACKSLASH_ESCAPES, so
//     escaping is assumed.
//   - a DSN-level `sql_mode=` param overrides per-connection and is not
//     visible to the emitters; an operator combining a DSN-only
//     NO_BACKSLASH_ESCAPES override with backslash-bearing string values
//     should set --mysql-sql-mode instead so the emitters see it.
//
// EMIT-direction only: NO_BACKSLASH_ESCAPES changes how the server PARSES
// string literals sluice sends, not how it PRINTS them — SHOW CREATE TABLE and
// information_schema COLUMN_TYPE render literals with the same fixed escape
// discipline under every sql_mode (ground-truthed on 8.0.46/8.4.10; audit
// finding N-5, pinned by TestSchemaLiteralDecode_SQLModeMatrix_ByteExact). The
// reader-side decoders (scanMySQLQuotedString and its callers) are therefore
// deliberately unconditional and must NOT consult this policy.
func backslashIsMySQLEscape(sqlMode *string) bool {
	return !strings.Contains(strings.ToUpper(resolveSessionSQLMode(sqlMode)), "NO_BACKSLASH_ESCAPES")
}

// sqlModeHasNBE reports whether a raw sql_mode string enables
// NO_BACKSLASH_ESCAPES (case-insensitively). The DSN-param form the
// go-sql-driver stores may be single-quoted (`'...'`), comma-separated, and
// mixed-case; a substring test is exact enough because NO_BACKSLASH_ESCAPES
// is not a substring of any other MySQL sql_mode flag.
func sqlModeHasNBE(mode string) bool {
	return strings.Contains(strings.ToUpper(mode), "NO_BACKSLASH_ESCAPES")
}

// warnSQLModeNBEDisagreement logs a single WARN when an operator-supplied
// DSN `sql_mode` param disagrees, on NO_BACKSLASH_ESCAPES, with the session
// mode sluice's DDL emitters assume (the engine's resolved --mysql-sql-mode,
// sqlMode — task 2.5 replaced the former sessionSQLMode global). The emitters
// key their string-literal backslash escaping off [backslashIsMySQLEscape],
// but a DSN `sql_mode` wins on the actual connection — so a mismatch means the
// escaping decision is made against the wrong mode, silently doubling (or
// failing to double) backslashes in DDL string literals. This is a WARN, not a
// refusal: the DSN override is a legitimate operator choice, and the fix is to
// align --mysql-sql-mode with it. Called from [injectSessionSQLMode] only when
// a DSN `sql_mode` param is present (when sluice injects the mode itself, the
// two agree by construction).
func warnSQLModeNBEDisagreement(dsnSQLMode string, sqlMode *string) {
	dsnNBE := sqlModeHasNBE(dsnSQLMode)
	sessionNBE := !backslashIsMySQLEscape(sqlMode)
	if dsnNBE == sessionNBE {
		return
	}
	slog.Warn(
		"mysql: DSN sql_mode disagrees with --mysql-sql-mode on NO_BACKSLASH_ESCAPES; "+
			"sluice's DDL emitters escape string-literal backslashes against the session mode, "+
			"but the DSN sql_mode wins on the connection — a backslash-bearing DEFAULT / ENUM / "+
			"COMMENT / expression literal may be silently mis-escaped on the target. Align "+
			"--mysql-sql-mode with the DSN sql_mode (or drop the DSN override) to remove the mismatch",
		slog.String("dsn_sql_mode", strings.Trim(dsnSQLMode, "'")),
		slog.String("session_sql_mode", resolveSessionSQLMode(sqlMode)),
		slog.Bool("dsn_no_backslash_escapes", dsnNBE),
		slog.Bool("session_no_backslash_escapes", sessionNBE),
	)
}

// dsnShapeHint inspects a DSN that failed to parse and returns a
// short, leading-newline-terminated hint when sluice can recognise
// a known operator-side mistake. Returns the empty string for
// unknown shapes so the driver's own error message stays first.
//
// Currently recognises:
//
//   - "/db/branch" path segment — PlanetScale credentials are
//     branch-scoped (the branch is implicit in the user/password),
//     so the DSN path should be just the database name. The
//     driver's generic "did you forget to escape a param value?"
//     hint is misleading here.
//
// More patterns can be added as operator reports surface them.
func dsnShapeHint(dsn string) string {
	// MySQL DSN shape: `user:pw@protocol(address)/dbname?params`.
	// The path component is after `protocol(address)` — we have to
	// skip the `(...)` block because addresses can contain `/`
	// (unix sockets like `/tmp/mysql.sock`).
	at := strings.LastIndex(dsn, "@")
	if at < 0 {
		return ""
	}
	rest := dsn[at+1:]
	// Strip query string before counting path segments.
	if q := strings.Index(rest, "?"); q >= 0 {
		rest = rest[:q]
	}
	// Skip a `(...)` address block if present. This handles unix
	// sockets like `unix(/tmp/mysql.sock)/foo` whose internal `/`
	// would otherwise be mis-read as a path separator.
	if openIdx := strings.Index(rest, "("); openIdx >= 0 {
		if closeIdx := strings.Index(rest[openIdx:], ")"); closeIdx >= 0 {
			rest = rest[openIdx+closeIdx+1:]
		}
	}
	// Now `rest` is the path component (with leading `/` if present).
	if !strings.HasPrefix(rest, "/") {
		return ""
	}
	path := rest[1:]
	if strings.Contains(path, "/") {
		return "DSN path appears to contain `database/branch` (PlanetScale-style); credentials are branch-scoped so the path should be just the database name — try removing the `/branch` segment. Underlying error: "
	}
	return ""
}

// parseDSN parses and validates a MySQL DSN, applying the parameter
// adjustments sluice requires for correct behaviour:
//
//   - parseTime=true: driver returns time.Time for DATE/DATETIME/TIMESTAMP
//     instead of []byte, which lets the row pipeline use Go-native types.
//   - loc=UTC: timestamps are returned in UTC regardless of session
//     timezone, removing one source of cross-engine ambiguity.
//   - time_zone='+00:00' (issued via cfg.Params on every new connection):
//     forces the MySQL session to emit TIMESTAMP wire values in UTC
//     regardless of the server's default_time_zone or the host the
//     server is running on. Without this, a MySQL server whose session
//     time_zone inherits the host TZ (e.g. PT) converts the column's
//     UTC-stored TIMESTAMP into PT for the wire format; the driver then
//     parses that wall-clock as UTC (because of cfg.Loc), corrupting
//     the value by exactly the offset. Bug 19. The CDC binlog path is
//     immune to the SESSION time_zone variable (binlog encodes UTC
//     epoch directly) but susceptible to a separate process-local-TZ
//     formatting bug; that one is fixed in cdc_reader.go via
//     TimestampStringLocation.
//
// The DSN must include a database name; sluice operates against an
// explicit schema rather than connecting at the server level.
func parseDSN(dsn string) (*mysql.Config, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		// GitHub issue #17 papercut: the driver's error message already
		// starts with "invalid DSN: ..."; wrapping with our own
		// "mysql: invalid DSN: %w" produces a confusing double prefix
		// ("mysql: invalid DSN: invalid DSN: ..."). Strip the driver's
		// own "invalid DSN:" prefix before wrapping; if the driver
		// reports a different shape (a future driver version may
		// change the prefix), the original wrap still applies.
		msg := err.Error()
		const dupPrefix = "invalid DSN: "
		if strings.HasPrefix(msg, dupPrefix) {
			//nolint:errorlint // intentional: rewriting prefix for operator readability; original chain preserved via errors.Is below if needed
			return nil, fmt.Errorf("mysql: invalid DSN: %s%s", dsnShapeHint(dsn), msg[len(dupPrefix):])
		}
		return nil, fmt.Errorf("mysql: invalid DSN: %s%w", dsnShapeHint(dsn), err)
	}
	if cfg.DBName == "" {
		return nil, errors.New("mysql: DSN must include a database name")
	}
	// ADR-0153: the driver polices interpolation × unsafe COLLATION at
	// ParseDSN but ignores the charset= param; sluice refuses the
	// explicit unsafe-charset combination here, for every connection path.
	if err := refuseExplicitInterpolationUnsafeCharset(cfg, dsn); err != nil {
		return nil, err
	}
	return finishParseDSN(cfg), nil
}

// parseServerDSN is the database-OPTIONAL sibling of [parseDSN], used
// by the multi-database fan-out path (ADR-0074). The single-database
// migrate / sync path requires a database in the DSN ([parseDSN]); when
// the operator drives a multi-database run with `--all-databases` /
// `--include-database` / `--exclude-database`, the source DSN is a
// *server* connection whose database component may legitimately be
// empty — the orchestrator enumerates databases via [DatabaseLister]
// and re-opens a single-database reader per database. Every other DSN
// adjustment ([finishParseDSN]: keep-alive dialer, parseTime, UTC loc,
// time_zone, sql_mode, utf8mb4) applies identically; only the
// non-empty-DBName precondition is relaxed.
func parseServerDSN(dsn string) (*mysql.Config, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		msg := err.Error()
		const dupPrefix = "invalid DSN: "
		if strings.HasPrefix(msg, dupPrefix) {
			//nolint:errorlint // intentional: rewriting prefix for operator readability; matches parseDSN
			return nil, fmt.Errorf("mysql: invalid DSN: %s%s", dsnShapeHint(dsn), msg[len(dupPrefix):])
		}
		return nil, fmt.Errorf("mysql: invalid DSN: %s%w", dsnShapeHint(dsn), err)
	}
	// ADR-0153: same explicit interpolation × unsafe-charset refusal as
	// [parseDSN] — the server-DSN paths open real connections too.
	if err := refuseExplicitInterpolationUnsafeCharset(cfg, dsn); err != nil {
		return nil, err
	}
	return finishParseDSN(cfg), nil
}

// finishParseDSN applies the sluice-required parameter adjustments to a
// parsed [mysql.Config] — the shared tail of [parseDSN] and
// [parseServerDSN]. Split out so the only difference between the two
// entry points is whether an empty DBName is an error.
func finishParseDSN(cfg *mysql.Config) *mysql.Config {
	// Route plain-TCP query connections through the keep-alive dialer.
	// Long-lived pools (the change applier, schema reader) would
	// otherwise sit idle behind cloud NAT and stall on a dropped
	// mapping. Non-TCP networks (unix sockets) are left as-is.
	if cfg.Net == "tcp" {
		cfg.Net = keepaliveNet
	}

	cfg.ParseTime = true
	cfg.Loc = time.UTC

	// The driver's handleParams emits each cfg.Params entry as
	// `SET <key> = <value>` after the connection handshake. Quoting
	// is preserved verbatim, so the value must include the SQL
	// quotes for a literal time-zone offset string.
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	if _, ok := cfg.Params["time_zone"]; !ok {
		cfg.Params["time_zone"] = "'+00:00'"
	}
	// The session sql_mode injection (Bug 102/103 strict-by-default) lives in
	// [openDB] → [injectSessionSQLMode], not here: it depends on the
	// per-instance --mysql-sql-mode override the [Engine] carries (task 2.5,
	// replacing the former sessionSQLMode global), which parseDSN — a pure DSN
	// parser shared by tests and non-Engine callers — does not have. openDB is
	// the single choke point every MySQL connection passes through, so injecting
	// there covers every Open* path. The SEC-1 NBE-disagreement WARN also fires
	// from injectSessionSQLMode, for the same per-instance-mode reason.
	// Bug 106 (v0.92.1). Pre-fix the connection's default character
	// set could fall back to 3-byte utf8 on older MySQL servers /
	// managed deployments, silently corrupting 4-byte UTF-8 sequences
	// (emoji, supplementary-plane glyphs) — observed concretely when
	// MySQL → PG schema-read encountered an ENUM whose labels
	// contained 4-byte UTF-8, which arrived in sluice's IR as `?`
	// substitutes and then loud-failed at the target row INSERT (the
	// loud-fail was the visible symptom; the silent label corruption
	// was the silent class). Forcing utf8mb4 here ensures the
	// connection charset always supports the full Unicode range, so
	// 4-byte sequences round-trip cleanly. utf8mb4_general_ci is the
	// safe default — operators who need a different collation can
	// override in the DSN.
	if cfg.Collation == "" {
		cfg.Collation = "utf8mb4_general_ci"
	}

	return cfg
}

// parseDSNForFlavor is the flavor-aware sibling of [parseDSN] — the ONE choke
// point where the ADR-0153 statement-protocol default is resolved. Every
// Engine open path (schema reader/writer, row reader/writer, change applier,
// migration state, snapshot openers, VStream reader) parses through it; the
// resulting cfg carries the decision, so every connection derived from that
// cfg — pool conns, the ADR-0104 lane pool (pipelineCfg), the VStream
// shard-discovery and purged-GTID probes (Clone()s of the reader cfg) —
// inherits it with no per-site logic.
//
// The rule (audit N-15a, benched on real PlanetScale 2026-07-08): the
// PlanetScale / Vitess flavors default to client-side interpolation
// (`interpolateParams=true`, 1 COM_QUERY round trip per statement) because
// their write path is WAN-RTT-bound and the per-statement hidden
// COM_STMT_PREPARE measured −33% bulk copy / −26% CDC burst drain at ~100 ms
// RTT. Vanilla MySQL keeps the driver's binary-protocol default (typically
// LAN RTT; protocol conservatism is free there). An explicit
// `interpolateParams=` in the DSN always wins — see
// [applyFlavorInterpolationDefault].
func parseDSNForFlavor(dsn string, flavor Flavor) (*mysql.Config, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	applyFlavorInterpolationDefault(cfg, dsn, flavor)
	return cfg, nil
}

// parseServerDSNForFlavor is the database-OPTIONAL sibling of
// [parseDSNForFlavor], mirroring parseServerDSN vs parseDSN.
func parseServerDSNForFlavor(dsn string, flavor Flavor) (*mysql.Config, error) {
	cfg, err := parseServerDSN(dsn)
	if err != nil {
		return nil, err
	}
	applyFlavorInterpolationDefault(cfg, dsn, flavor)
	return cfg, nil
}

// interpolationUnsafeCollations mirrors go-sql-driver's unexported
// unsafeCollations denylist (collations whose multibyte encodings may carry
// 0x5C in trailing bytes, making backslash-escaping interpolation
// injection-unsafe). The driver REFUSES interpolateParams=true with these at
// Config.normalize(); sluice consults the copy so the FLAVOR DEFAULT can
// step aside gracefully instead of turning a previously-working
// unsafe-collation DSN into a connect failure — a perf default must never
// break a working configuration (see [applyFlavorInterpolationDefault]).
//
// Drift posture: TestInterpolationUnsafeCollations_SubsetOfDriver pins that
// every entry here is refused by the driver (this list ⊆ the driver's). If a
// future driver release ADDS a collation we don't list, the default flip on
// such a DSN fails LOUDLY at connector build ("interpolateParams can not be
// used with unsafe collations") rather than silently — the operator remedy
// (interpolateParams=false in the DSN) is in the same error path, and the
// list gains the entry on the driver upgrade pass.
var interpolationUnsafeCollations = map[string]bool{
	"big5_chinese_ci":        true,
	"sjis_japanese_ci":       true,
	"gbk_chinese_ci":         true,
	"big5_bin":               true,
	"gb2312_bin":             true,
	"gbk_bin":                true,
	"sjis_bin":               true,
	"cp932_japanese_ci":      true,
	"cp932_bin":              true,
	"gb18030_chinese_ci":     true,
	"gb18030_bin":            true,
	"gb18030_unicode_520_ci": true,
}

// interpolationProtocolLogOnce keeps the resolved-protocol INFO line to one
// per process (the resolution runs on every parse of every open path; the
// operator needs the fact once, not thirty times).
var interpolationProtocolLogOnce sync.Once

// applyFlavorInterpolationDefault resolves the ADR-0153 statement protocol
// on a parsed cfg: PlanetScale / Vitess flavors get client-side
// interpolation unless the operator said otherwise, and the RESOLVED state
// — whatever its source (flavor default, or an explicit
// interpolateParams=true DSN opt-in on ANY flavor, vanilla included; the
// documented high-RTT lever, docs/throughput-tuning.md) — is announced with
// the same once-per-process INFO line. Every guard downstream of here keys
// on the resolved cfg.InterpolateParams, never on the flavor: the driver's
// unsafe-collation refusal, its maxAllowedPacket ErrSkip→prepared fallback,
// and its NBE status-flag escaper all read the cfg/connection state, so a
// vanilla opt-in gets identical protections by construction.
//
// Explicit-DSN-wins contract: mysql.ParseDSN CONSUMES an interpolateParams
// param into a bool, collapsing "explicitly false" and "absent" — so
// explicitness is detected by inspecting the RAW DSN string
// ([dsnSetsInterpolateParams]) and any explicit setting (true or false) is
// respected verbatim. Zero-value safety (the v0.99.51 trap): there is no
// config bool whose zero value could invert — the default derives from the
// Flavor at parse time, and the zero Flavor (vanilla) means no flip.
//
// The unsafe-collation skip: the driver refuses interpolation under the
// big5/cp932/gb2312/gbk/sjis/gb18030 collation families
// ([interpolationUnsafeCollations]). An operator who pinned such a collation
// in the DSN keeps the binary protocol — WARNED, not refused, because the
// flip is sluice's perf default, not an operator request, and refusing would
// break a config that worked on every release before ADR-0153. An operator
// who EXPLICITLY combines interpolateParams=true with an unsafe collation is
// refused by the driver itself at ParseDSN, loudly, before this runs — on
// every flavor (the driver check is flavor-blind).
func applyFlavorInterpolationDefault(cfg *mysql.Config, rawDSN string, flavor Flavor) {
	if cfg == nil {
		return
	}
	if cfg.InterpolateParams {
		// Explicit DSN opt-in (any flavor) — the protocol is already
		// resolved; just announce it identically to the default path.
		noteInterpolationResolved("explicit interpolateParams=true in the DSN")
		return
	}
	if !flavor.usesVStream() || dsnSetsInterpolateParams(rawDSN) {
		return
	}
	unsafeName := cfg.Collation
	unsafe := interpolationUnsafeCollations[cfg.Collation]
	if !unsafe {
		// The driver's own denylist checks only the collation; the
		// charset= param (its unexported cfg.charsets → SET NAMES) is
		// the same injection hazard through a hole the driver does not
		// police — see [interpolationUnsafeCharsets].
		unsafeName, unsafe = dsnUnsafeInterpolationCharset(rawDSN)
	}
	if unsafe {
		slog.Warn(
			"mysql: skipping the PlanetScale/Vitess client-side-interpolation default: the DSN's collation/charset "+
				"is unsafe for interpolation (go-sql-driver denylist class: multibyte encodings that can carry 0x5C "+
				"in trailing bytes); staying on the binary protocol. Use a *_general_ci / utf8mb4 collation+charset to "+
				"regain the 1-RTT write path, or set interpolateParams=false in the DSN to silence this warning",
			slog.String("collation_or_charset", unsafeName),
			slog.String("flavor", flavor.String()),
		)
		return
	}
	cfg.InterpolateParams = true
	noteInterpolationResolved("PlanetScale/Vitess flavor default; override with interpolateParams=false in the DSN")
}

// noteInterpolationResolved emits the once-per-process resolved-protocol
// INFO line, naming how interpolation was engaged.
func noteInterpolationResolved(source string) {
	interpolationProtocolLogOnce.Do(func() {
		slog.Info("mysql: write path: client-side interpolation (1 round trip per statement; " + source + ")")
	})
}

// dsnSetsInterpolateParams reports whether the raw DSN string explicitly
// carries an `interpolateParams=` parameter. See [dsnParamValue] for the
// DSN-anatomy discipline; a segment without '=' is ignored by ParseDSN, so
// it is not explicit.
func dsnSetsInterpolateParams(dsn string) bool {
	_, ok := dsnParamValue(dsn, "interpolateParams")
	return ok
}

// dsnParamValue reports whether the named parameter is present in the
// DSN's query section, returning its FIRST occurrence's value. Presence is
// the only thing its caller ([dsnSetsInterpolateParams]) consumes — with
// duplicate params, whichever value the driver honors, the key was still
// explicitly set. Callers that care about WHICH occurrence use
// [dsnParamValues]. Needed because ParseDSN consumes recognised params
// into Config fields, some of them UNEXPORTED (cfg.charsets) or
// explicitness-collapsing (cfg.InterpolateParams).
func dsnParamValue(dsn, key string) (string, bool) {
	vals := dsnParamValues(dsn, key)
	if len(vals) == 0 {
		return "", false
	}
	return vals[0], true
}

// dsnParamValues returns EVERY occurrence of the named parameter, in DSN
// order. It mirrors the driver's DSN anatomy — everything after the LAST
// '/' is `dbname?params` (addresses and passwords may legally contain '/'
// and '?', so a whole-DSN substring search would be wrong) — and its param
// split (segments without '=' are skipped; exact case-sensitive key match). The driver's own param loop makes the LAST occurrence win for
// config-field params; callers that need driver-faithful semantics or an
// any-occurrence safety posture decide for themselves (see
// [dsnUnsafeInterpolationCharset]).
func dsnParamValues(dsn, key string) []string {
	slash := strings.LastIndexByte(dsn, '/')
	if slash < 0 {
		return nil
	}
	rest := dsn[slash+1:]
	q := strings.IndexByte(rest, '?')
	if q < 0 {
		return nil
	}
	var vals []string
	for _, seg := range strings.Split(rest[q+1:], "&") {
		if k, v, ok := strings.Cut(seg, "="); ok && k == key {
			vals = append(vals, v)
		}
	}
	return vals
}

// interpolationUnsafeCharsets is the CHARSET-family counterpart of
// [interpolationUnsafeCollations]: the multibyte connection charsets whose
// trailing bytes may carry 0x5C, making backslash-escaping interpolation
// injection-unsafe. go-sql-driver's own refusal (Config.normalize,
// v1.10.0 dsn.go:173) checks ONLY cfg.Collation and entirely ignores the
// separate `charset=` DSN param (its unexported cfg.charsets, which drive
// the connection's SET NAMES) — so `...?charset=gbk` sails through the
// driver with interpolation on. sluice closes that hole for its own
// connections: the flavor default steps aside on any unsafe entry, and an
// EXPLICIT interpolateParams=true + unsafe charset is refused at parse
// ([refuseExplicitInterpolationUnsafeCharset]), matching the loud posture
// the driver itself applies to the collation shape.
var interpolationUnsafeCharsets = map[string]bool{
	"big5":    true,
	"cp932":   true,
	"gb2312":  true,
	"gbk":     true,
	"sjis":    true,
	"gb18030": true,
}

// dsnUnsafeInterpolationCharset reports an interpolation-unsafe entry in
// the DSN's `charset=` parameter, if any. Two deliberate any-unsafe
// postures, both strictly safer than mirroring the driver's exact pick:
//
//   - ALL `charset=` OCCURRENCES are scanned, not just one. The driver's
//     param loop makes the LAST occurrence win (`?charset=utf8mb4&
//     charset=gbk` connects as gbk), so a first-occurrence scanner would
//     pass a DSN whose live connection is unsafe. Rather than replicate
//     last-wins — and silently diverge if the driver ever changes it —
//     ANY occurrence naming an unsafe charset disqualifies; the only
//     cost is refusing/stepping aside on a degenerate contradictory-
//     duplicate DSN that might have connected safe.
//   - within one occurrence, ANY entry of the comma-separated fallback
//     list disqualifies: the driver tries the SET NAMES candidates in
//     order until one succeeds, so which wins depends on the server at
//     connect time.
func dsnUnsafeInterpolationCharset(dsn string) (string, bool) {
	for _, raw := range dsnParamValues(dsn, "charset") {
		for _, cs := range strings.Split(raw, ",") {
			if interpolationUnsafeCharsets[strings.ToLower(strings.TrimSpace(cs))] {
				return cs, true
			}
		}
	}
	return "", false
}

// refuseExplicitInterpolationUnsafeCharset is the parse-time loud refusal
// for the operator-explicit combination the DRIVER fails to police:
// interpolateParams=true together with an injection-unsafe `charset=` entry
// (the driver refuses the equivalent COLLATION combination at ParseDSN but
// ignores charset= — see [interpolationUnsafeCharsets]). Called from
// [parseDSN]/[parseServerDSN] so EVERY connection path is covered,
// mirroring where the driver's own collation refusal lives. The flavor
// DEFAULT never reaches this: it steps aside with a WARN instead
// ([applyFlavorInterpolationDefault]).
func refuseExplicitInterpolationUnsafeCharset(cfg *mysql.Config, dsn string) error {
	if !cfg.InterpolateParams {
		return nil
	}
	cs, unsafe := dsnUnsafeInterpolationCharset(dsn)
	if !unsafe {
		return nil
	}
	return fmt.Errorf(
		"mysql: invalid DSN: interpolateParams=true cannot be combined with connection charset %q "+
			"(a multibyte encoding whose trailing bytes may carry 0x5C — injection-unsafe for client-side "+
			"interpolation, the same class the driver refuses for collations); remove interpolateParams "+
			"or use a safe charset such as utf8mb4", cs,
	)
}

// sourceReadSessionTimeoutSeconds is the bounded value sluice applies to
// `net_write_timeout` / `net_read_timeout` on every MySQL SOURCE read
// session it opens for a cold-copy (ADR-0109 §A — PRIMARY defense).
//
// The mechanism it prevents: a transient TARGET stall (a non-Metal
// PlanetScale storage auto-grow that BLOCKS the target's writes for
// seconds-to-minutes under semi-sync) backpressures sluice's reader/writer
// pipeline — the writer can't drain, so the reader stops consuming, so the
// SOURCE read connection sits idle. The source server's default
// `net_write_timeout` is 60s; once the idle read crosses it, the source
// CLOSES the connection (`unexpected EOF` / `invalid connection`) and the
// whole cold-copy aborts. Raising the source session's timeout to a
// generous bound lets the read survive the stall: when the target recovers,
// the writer drains, the reader resumes, and the copy continues — no
// reconnect, no re-snapshot, no consistency problem (the per-table reconnect
// (C) and the cold-start auto-restart (B) are the BACKSTOPS for a stall that
// outlives even this raised bound).
//
// 600s (10 min) is deliberately FINITE: a genuinely-dead target still
// surfaces (the read eventually drops and sluice's source-unresponsive
// detection + the (B)/(C) retries take over) rather than hanging forever.
//
// Zero-value-safe by construction: this is a package CONSTANT, not a config
// field — there is no EnableX-defaulting-true trap (the v0.99.51 lesson).
// Every construction path that opens a source read session
// ([applySourceReadSessionTimeouts] below) gets the same bound; an operator
// who needs a different value sets `net_write_timeout` / `net_read_timeout`
// directly in the source DSN params, which wins (the helper never overwrites
// an operator-supplied value).
const sourceReadSessionTimeoutSeconds = 600

// applySourceReadSessionTimeouts injects `net_write_timeout` /
// `net_read_timeout` into cfg.Params (ADR-0109 §A) so the go-sql-driver
// emits `SET <key> = <value>` on every connection in the pool at session
// init — covering EVERY source read session for free: the dedicated
// full-scan conn, the LIMIT-paged chunked ReadRowsBatch reads, and the
// snapshot path's pinned REPEATABLE-READ connection(s). Scoped to the
// SOURCE-read open paths (OpenRowReader + the binlog snapshot openers) so
// the target write/applier sessions are untouched — the timeout is a
// source-side defense, not a target one.
//
// An operator-supplied DSN value for either key wins absolutely (same
// two-tier override shape as sql_mode / time_zone above): the helper only
// sets a key that is absent, so a deliberate per-source tuning is never
// clobbered. The numeric value is emitted bare (no SQL quotes) — these are
// integer session variables, unlike the quoted string literals time_zone /
// sql_mode require.
func applySourceReadSessionTimeouts(cfg *mysql.Config) {
	if cfg == nil {
		return
	}
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	val := fmt.Sprintf("%d", sourceReadSessionTimeoutSeconds)
	if _, ok := cfg.Params["net_write_timeout"]; !ok {
		cfg.Params["net_write_timeout"] = val
	}
	if _, ok := cfg.Params["net_read_timeout"]; !ok {
		cfg.Params["net_read_timeout"] = val
	}
}

// vstreamParamPrefix is the DSN-parameter namespace sluice reserves
// for its Vitess VStream extensions (vstream_endpoint, vstream_transport,
// vstream_auth, vstream_shards, vstream_auto_discover_shards,
// vstream_insecure_tls, …). These are sluice-internal DSN flags; they
// are never valid MySQL session variables.
const vstreamParamPrefix = "vstream_"

// nativeSluiceParams are sluice-internal source-DSN knobs that are NOT under
// the vstream_ prefix but must STILL be stripped before a MySQL session, for
// the same Bug-126 reason: the go-sql-driver emits each cfg.Params entry as
// `SET <key>=<value>` at session init, and these are not valid MySQL system
// variables. copy_table_parallelism (ADR-0101) is the native-binlog
// concurrent-cold-copy reader count; it governs sluice's snapshot opener,
// never a MySQL session. Listed explicitly (an allowlist, not a prefix) so a
// real future MySQL variable starting with "copy_" is never accidentally
// swallowed. zero_date (ADR-0127) is the per-sync zero/partial-date policy
// override; readers parse it into a per-reader zeroDateMode and it is NOT a
// MySQL session variable, so it strips here for the same reason.
var nativeSluiceParams = map[string]struct{}{
	"copy_table_parallelism": {},
	"zero_date":              {},
}

// readerZeroDateMode resolves a reader's per-sync zero-date policy from the
// `zero_date` source-DSN param (ADR-0127). Absent → zeroDateInherit; the caller
// then folds the engine's --zero-date default onto it ([Engine.resolveReaderZeroDate],
// task 2.5), and a residual inherit resolves to the loud refuse default at decode
// — exactly the pre-task-2.5 behavior, byte-identical. Present-but-invalid is refused
// LOUDLY at reader construction, naming the param and the valid set (never a
// silent fallback). It reads cfg.Params directly; the param is stripped from
// the MySQL session separately at openDB via [stripVStreamParams] +
// nativeSluiceParams, so it never reaches a `SET zero_date = …`.
func readerZeroDateMode(cfg *mysql.Config) (zeroDateMode, error) {
	if cfg == nil {
		return zeroDateInherit, nil
	}
	raw, ok := cfg.Params["zero_date"]
	if !ok {
		return zeroDateInherit, nil
	}
	m, err := parseZeroDateMode(raw)
	if err != nil {
		return zeroDateInherit, fmt.Errorf("mysql: invalid zero_date DSN param %q (%w)", raw, err)
	}
	return m, nil
}

// stripVStreamParams returns a clone of cfg with every cfg.Params entry
// whose key carries the vstream_ prefix removed (plus the explicit
// nativeSluiceParams). It never mutates the caller's cfg (it Clone()s
// first), so a caller may continue to read the original cfg.Params after the
// call.
//
// Bug 126. sluice's vstream_* DSN extensions are consumed only by the
// VStream CDC reader (cdc_vstream.go), which reads them out of cfg.Params
// at openVStreamReader time and then dials vtgate over gRPC — it never
// hands these params to a MySQL connection. Every *other* path
// (schema-reader, row-reader, schema-writer, row-writer, change-applier,
// migration-state-store, and the CDC reader's own shard-discovery
// connection) opens a database/sql handle through [openDB]; the
// go-sql-driver's session init emits each cfg.Params entry as a
// `SET <key> = <value>` after the handshake. Self-hosted Vitess /
// vttestserver rejects the unknown vstream_* vars (Error 1105 for the
// IP-bearing vstream_endpoint, VT05006 unknown system variable for the
// rest), killing a planetscale-flavored cold-start at "open source
// schema reader" before any data moves. Stripping at the openDB choke
// point makes the leak impossible for any present or future Open* path,
// while leaving the CDC reader's earlier cfg.Params reads intact (it has
// already extracted them before the gRPC dial; it never reaches openDB
// except via discoverShards, which is a MySQL connection and correctly
// wants them stripped).
func stripVStreamParams(cfg *mysql.Config) *mysql.Config {
	if cfg == nil {
		return nil
	}
	clone := cfg.Clone()
	for k := range clone.Params {
		if strings.HasPrefix(k, vstreamParamPrefix) {
			delete(clone.Params, k)
			continue
		}
		if _, ok := nativeSluiceParams[k]; ok {
			delete(clone.Params, k)
		}
	}
	return clone
}

// openDB connects to MySQL and verifies the connection is usable.
// It returns a *sql.DB ready for queries; callers are responsible for
// calling Close() when finished.
//
// sluice's vstream_* DSN extensions are stripped here (see
// [stripVStreamParams]) so they never reach a MySQL session as a
// `SET vstream_* = …` statement — Bug 126. The session sql_mode is
// injected here too ([injectSessionSQLMode], sqlMode = the engine's
// resolved --mysql-sql-mode override). This is the single choke point
// every MySQL connection passes through, so both the strip and the
// injection are leak-proof against future Open* paths.
func openDB(ctx context.Context, cfg *mysql.Config, sqlMode *string) (*sql.DB, error) {
	cfg = stripVStreamParams(cfg)
	injectSessionSQLMode(cfg, sqlMode)
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("mysql: build connector: %w", err)
	}
	db := sql.OpenDB(connector)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mysql: ping: %w", err)
	}
	return db, nil
}

// injectSessionSQLMode adds `SET SESSION sql_mode='...'` to cfg.Params so the
// go-sql-driver emits it at session init on every connection in the pool. It is
// called by [openDB] on the vstream-stripped CLONE, so it never mutates the
// caller's cfg.
//
// Bug 102 + Bug 103 (CRITICAL silent-loss, v0.92.1). Pre-fix sluice inherited the
// MySQL server's sql_mode, which on dev containers and some managed deployments
// doesn't include the strict modes — so PG `NUMERIC(40,5)` values overflowing
// MySQL `DECIMAL(65,30)` silently clamped to the column max (Bug 102), and PG
// `TIMESTAMPTZ` values outside MySQL `TIMESTAMP` range silently became
// `0000-00-00 00:00:00` (Bug 103). The injected sql_mode follows a two-tier
// override policy:
//
//  1. DSN-level override (`sql_mode=...` in the connection string params) wins
//     absolutely — the helper only sets the key when absent.
//  2. Engine-level override via --mysql-sql-mode ([engineOptions.sqlMode],
//     resolved by [resolveSessionSQLMode]). "" means "fall through to server
//     default" — the legacy-data escape hatch — and injects nothing.
//
// If neither is set (an override-free engine), [defaultStrictSQLMode] applies
// (the loud-failure-tenet default). The literal-quotes pattern matches the
// time_zone override in [finishParseDSN].
//
// SEC-1 re-review follow-up: when a DSN `sql_mode` param IS present it wins on
// the wire, but the DDL emitters escape string-literal backslashes against the
// per-instance mode (sqlMode) — so a NO_BACKSLASH_ESCAPES disagreement is the
// one residual fail-open channel. [warnSQLModeNBEDisagreement] flags it (WARN,
// never blocks). Checked before the empty-mode early return so the
// --mysql-sql-mode="" escape hatch still surfaces a mismatching DSN override.
func injectSessionSQLMode(cfg *mysql.Config, sqlMode *string) {
	if dsnMode, ok := cfg.Params["sql_mode"]; ok {
		warnSQLModeNBEDisagreement(dsnMode, sqlMode)
	}
	mode := resolveSessionSQLMode(sqlMode)
	if mode == "" {
		return
	}
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	if _, ok := cfg.Params["sql_mode"]; !ok {
		cfg.Params["sql_mode"] = "'" + mode + "'"
	}
}
