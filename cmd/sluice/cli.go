package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
	units "github.com/docker/go-units"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/mysql"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline"
	pstelemetry "sluicesync.dev/sluice/internal/planetscale/telemetry"
	"sluicesync.dev/sluice/internal/redact"
)

// Globals are flags shared across every subcommand. Embedding into the
// top-level CLI makes them parse identically regardless of which
// subcommand the user runs; binding the value in main() makes it
// available to Run methods that declare a *Globals parameter.
type Globals struct {
	Config    string `help:"Path to a YAML config file." short:"c" type:"existingfile" placeholder:"PATH"`
	LogLevel  string `help:"Log verbosity." short:"l" default:"info" enum:"debug,info,warn,error" placeholder:"LEVEL"`
	LogFormat string `help:"Log output format: human-readable text or one-JSON-object-per-line (for Loki/Datadog/CloudWatch ingestion of long-running sync)." default:"text" enum:"text,json" placeholder:"FORMAT"`

	// PprofListen is the GitHub #23 Phase A operator-diagnostic hook.
	// When non-empty, starts net/http/pprof's debug endpoints at the
	// given address for the lifetime of the subcommand. Off by
	// default; opt-in. Useful for diagnosing silent stalls — the
	// operator hits /debug/pprof/goroutine?debug=2 to dump every
	// goroutine's stack, which is what's needed to localise a wedge.
	PprofListen string `help:"Bind net/http/pprof's debug endpoints at this address (e.g. ':6060', '127.0.0.1:6060') for the duration of the subcommand. Off by default. Useful for diagnosing silent stalls (GitHub #23 Phase A) — fetch /debug/pprof/goroutine?debug=2 to dump every goroutine's stack." placeholder:"ADDR"`

	// MySQLSQLMode is the v0.92.1 escape hatch for the new strict-by-
	// default mode forcing (Bugs 102/103/105). Sluice forces strict
	// modes on every MySQL connection to close the silent-clamp /
	// silent-zero-date class — but legacy MySQL data (zero-dates from
	// pre-MySQL-5.7 schemas, silently-truncated VARCHARs, etc.) was
	// already accepted under a relaxed sql_mode and would refuse
	// under strict-by-default. Operators migrating such data set
	// --mysql-sql-mode='' (explicit empty) to keep the server's
	// default sql_mode, or pass a specific mode list. The DSN-level
	// override (cfg.Params["sql_mode"] in the connection string)
	// takes precedence if both are set. See
	// docs/operator/migrating-legacy-mysql.md.
	//
	// The default value matches the strict literal so kong's "value
	// from CLI" vs "field zero-value" indistinguishability doesn't
	// matter: not-passed and passed-with-default both produce the
	// same forced strict mode. An explicit empty `--mysql-sql-mode=''`
	// is distinguishable (the field becomes the empty string, which
	// differs from the strict default) and disables forcing.
	// Explicit name:"mysql-sql-mode" overrides kong's auto-kebab
	// derivation. Without it, kong reads the field name `MySQLSQLMode`
	// as `My` + `SQLSQL` (one acronym block, no lowercase break) +
	// `Mode` and emits the flag as `--my-sqlsql-mode` — a typo that
	// contradicts the help text. v0.92.1 shipped with this defect;
	// v0.92.2 pins the public name explicitly.
	MySQLSQLMode string `name:"mysql-sql-mode" help:"Override sluice's default strict sql_mode on every MySQL connection. Pass --mysql-sql-mode='' (explicit empty) to fall through to the server's default sql_mode — required for migrating legacy MySQL data with zero-dates / silently-truncated values. Pass a specific comma-separated mode list to force exactly those modes. See docs/operator/migrating-legacy-mysql.md." default:"STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO" placeholder:"MODES"`

	// ZeroDate controls how MySQL zero and partial dates (0000-00-00,
	// YYYY-00-DD, YYYY-MM-00) are carried on the read path. These values
	// are storable only under a relaxed source sql_mode and have no
	// valid calendar meaning; read as native time values under the
	// driver's parseTime they were silently normalized to a wrong date
	// (Vector A CRITICAL silent corruption). sluice reads temporal
	// columns as raw text so it can apply this policy explicitly. The
	// default refuses loudly, naming the column.
	ZeroDate string `name:"zero-date" help:"How to carry MySQL zero/partial dates (0000-00-00, YYYY-00-DD, YYYY-MM-00): 'error' refuses loudly naming the column (default), 'null' carries them as NULL (refused on NOT NULL columns), 'epoch' substitutes 1970-01-01. See docs/operator/migrating-legacy-mysql.md." enum:"error,null,epoch" default:"error" placeholder:"MODE"`

	// MaxMemory is a hard soft-ceiling on the Go heap, applied via
	// runtime/debug.SetMemoryLimit at startup. --max-buffer-bytes only
	// caps *raw value bytes* of buffered ir.Row maps; the real Go-heap
	// footprint of those maps is ~4–5× the raw bytes, and with the
	// default GOGC the heap grows to ~2× the live set, so a large
	// --max-buffer-bytes (or many tables in flight) can drive RSS to
	// ~9× the cap (a 2 GiB raw cap → ~18 GB RSS observed). Setting
	// --max-memory makes the GC defend a real RSS target instead.
	// Default OFF (empty → SetMemoryLimit is not called, so Go honors
	// the GOMEMLIMIT env var natively if set). Sets a soft limit: the
	// GC works harder as the heap approaches it but does not hard-fail
	// — pair it with headroom over the live set. Not auto-derived from
	// system RAM (that would change behavior for everyone); a future
	// --max-memory=auto could do so.
	MaxMemory string `name:"max-memory" help:"Soft ceiling on the Go heap (e.g. '2GiB', '512MiB'), applied via runtime/debug.SetMemoryLimit at startup to bound RSS. Unlike --max-buffer-bytes (which caps only raw buffered value bytes), this bounds the whole heap, so the GC defends a real RSS target. Off by default; the GOMEMLIMIT env var is honored natively when this is unset." placeholder:"SIZE"`
}

// CLI is the root of the sluice command tree. Kong populates this from
// argv and dispatches to the matched subcommand's Run method.
type CLI struct {
	Globals

	// --version prints the build identifier and exits. The value is
	// supplied via kong.Vars{"version": ...} in main().
	Version kong.VersionFlag `help:"Print version and exit." short:"V"`

	Engines  EnginesCmd  `cmd:"" help:"List registered database engines."`
	Migrate  MigrateCmd  `cmd:"" help:"Run a one-time schema + data migration (simple mode)."`
	Sync     SyncCmd     `cmd:"" help:"Manage continuous-sync streams."`
	Slot     SlotCmd     `cmd:"" help:"Manage source-side replication slots (Postgres)."`
	Schema   SchemaCmd   `cmd:"" help:"Inspect and describe schemas (preview translation, etc.)."`
	Verify   VerifyCmd   `cmd:"" help:"Verify data integrity between source and target (v0.12.0+ count mode)."`
	Backup   BackupCmd   `cmd:"" help:"Take and verify logical backups (Phase 1: full snapshot to local filesystem)."`
	Restore  RestoreCmd  `cmd:"" help:"Restore a logical backup into a target database."`
	Matview  MatviewCmd  `cmd:"" help:"Operate on PostgreSQL materialized views (refresh; PG-only)."`
	Diagnose DiagnoseCmd `cmd:"" help:"Assemble an operator-bundle (cockroach-debug-zip-shape) for filing GitHub issues. ADR-0056."`
	Cutover  CutoverCmd  `cmd:"" help:"Two-phase sequence priming at cutover — re-read source sequence/AUTO_INCREMENT state and apply to the target with a safety margin (F10, ADR-0062)."`
	Trigger  TriggerCmd  `cmd:"" help:"Install / remove the postgres-trigger engine's source-side state (ADR-0066)."`

	MetricsWatch MetricsWatchCmd `cmd:"" help:"Watch a PlanetScale database's control-plane metrics (CPU/mem/storage/lag) and fire threshold alerts — standalone, no sync attached (ADR-0107)."`
}

// EnginesCmd lists the database engines registered in the binary,
// along with their key declared capabilities. Useful for confirming
// which 'driver:' values a config file may use.
type EnginesCmd struct{}

// Run implements the engines subcommand.
func (*EnginesCmd) Run() error {
	names := engines.Names()
	if len(names) == 0 {
		fmt.Println("(no engines registered — this binary was built without engine packages)")
		return nil
	}
	fmt.Printf("%-12s  %-18s  %s\n", "NAME", "BULK LOAD", "CDC")
	for _, n := range names {
		e, ok := engines.Get(n)
		if !ok {
			continue
		}
		caps := e.Capabilities()
		fmt.Printf("%-12s  %-18s  %s\n", n, caps.BulkLoad, caps.CDC)
	}
	return nil
}

// MigrateCmd runs a one-shot migration from a source database to a
// target database. Schema is translated and applied first, then data
// is bulk-copied. Suitable for smaller databases willing to accept a
// downtime window.
type MigrateCmd struct {
	SourceDriver string `help:"Source engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"source"`
	Source       string `help:"Source database DSN." required:"" env:"SLUICE_SOURCE" placeholder:"DSN" group:"source"`

	TargetDriver string `help:"Target engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"target"`
	Target       string `help:"Target database DSN." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`

	IncludeTable []string `help:"Only migrate these tables (comma-separated, repeatable). Glob patterns allowed (e.g. 'audit_*'). Mutually exclusive with --exclude-table." sep:"," placeholder:"TABLE"`
	ExcludeTable []string `help:"Migrate every table except these (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --include-table." sep:"," placeholder:"TABLE"`

	IncludeDatabase []string `help:"Multi-database fan-out (ADR-0074, MySQL source): migrate ONLY these source databases (comma-separated, repeatable). Glob patterns allowed (e.g. 'app_*'). Each source database routes to a same-named target namespace (PG schema / MySQL database). Mutually exclusive with --exclude-database. When any database-scope flag is set, the source DSN's database is optional (it's a server connection). System databases (information_schema, performance_schema, mysql, sys) are always excluded." sep:"," placeholder:"DATABASE"`
	ExcludeDatabase []string `help:"Multi-database fan-out (ADR-0074, MySQL source): migrate every non-system source database EXCEPT these (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --include-database." sep:"," placeholder:"DATABASE"`
	AllDatabases    bool     `help:"Multi-database fan-out (ADR-0074, MySQL source): migrate every non-system database on the source server, each to a same-named target namespace. Mutually exclusive with --include-database / --exclude-database."`

	IncludeSchema []string `help:"Multi-schema fan-out (ADR-0075, Postgres source): migrate ONLY these source schemas (comma-separated, repeatable). Glob patterns allowed (e.g. 'app_*'). Each source schema routes to a same-named target namespace (PG schema / MySQL database). Mutually exclusive with --exclude-schema. The PG-source synonym of --include-database; supplying BOTH the --*-schema and --*-database spelling in one invocation is an error. System schemas (pg_catalog, information_schema, pg_toast, pg_temp*) are always excluded." sep:"," placeholder:"SCHEMA"`
	ExcludeSchema []string `help:"Multi-schema fan-out (ADR-0075, Postgres source): migrate every non-system source schema EXCEPT these (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --include-schema. The PG-source synonym of --exclude-database." sep:"," placeholder:"SCHEMA"`
	AllSchemas    bool     `help:"Multi-schema fan-out (ADR-0075, Postgres source): migrate every non-system schema in the source database, each to a same-named target namespace. The PG-source synonym of --all-databases. Mutually exclusive with --include-schema / --exclude-schema."`

	IncludeView []string `help:"Only migrate these views (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --exclude-view." sep:"," placeholder:"VIEW"`
	ExcludeView []string `help:"Migrate every view except these (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --include-view." sep:"," placeholder:"VIEW"`
	SkipViews   bool     `help:"Skip view processing entirely; views in the source schema are not created on the target. Useful when views are managed out-of-band (Atlas / sqitch / liquibase)."`

	TypeOverride []string `help:"Force a specific target type for a column (repeatable). Format: 'TABLE.COLUMN=TYPE', e.g. 'products.attrs=text'. CLI form of the YAML 'mappings:' config; for target-type options (e.g. 'jsonb' with binary=true), use the YAML form." placeholder:"TABLE.COLUMN=TYPE" sep:"none"`

	ExprOverride []string `help:"Replace a generated column's body with operator-supplied target-dialect text (repeatable). Format: 'TABLE.COLUMN=EXPRESSION'. The expression is emitted verbatim — sluice's cross-dialect translator (ADR-0016) does NOT run on overridden columns. Escape hatch for cases the translator's hand-coded rewrites don't recognise. CLI form of the YAML 'expression_mappings:' config." placeholder:"TABLE.COLUMN=EXPRESSION" sep:"none"`

	DryRun bool `help:"Read the source schema and print the migration plan without applying changes." short:"n"`

	Resume      bool   `help:"Resume a previously-failed migration. State is read from sluice_migrate_state on the target." short:"r"`
	MigrationID string `help:"Stable migration identifier; key in sluice_migrate_state. Auto-generated from source/target host info when empty." placeholder:"ID"`

	ForceColdStart bool `help:"Skip the cold-start pre-flight check that refuses to bulk-copy into a populated target. Use with caution — INSERT into a non-empty table will collide on PRIMARY KEY. Ignored when --resume is set."`

	RawCopyFormat string `help:"Wire format for the same-engine raw-copy passthrough fast lane (ADR-0078, PG→PG). 'text' (default) is cross-major safe (pgcopydb's default); 'binary' is faster but only used when source and target server majors match (sluice probes both and downgrades to text loudly on a mismatch); 'auto' requests binary, letting the version probe decide. The lane itself engages ONLY for a same-engine, no-transform copy (no --redact / --type-override / --expr-override / --inject-shard-column); any transform present falls back to the IR copy path. The win is eliminating the per-value decode/re-encode, not text-vs-binary." default:"text" enum:"text,binary,auto" placeholder:"text|binary|auto"`

	ResetTargetData bool `help:"Destructive recovery: DELETE the migrate-state row, DROP every source-schema table on the target, then run a fresh cold-start. Use after a wedged-state recovery (e.g. slot-missing fall-through). Requires confirmation (type 'reset') unless --yes is set. Mutually exclusive with --resume. See ADR-0023."`

	Yes bool `help:"Skip the destructive-action confirmation prompt for --reset-target-data." short:"y"`

	BulkBatchSize int `help:"Bulk-copy batch size for resume-mid-table checkpointing. Each batch commits with an updated cursor in sluice_migrate_state.table_progress, so a crash mid-table resumes without re-copying the prefix. Tables without a PK fall back to truncate-and-redo regardless. Lower values shorten the replay window on crash; higher values amortise per-tx commit overhead. Only consulted on the resume path; cold-start migrations use the faster plain-INSERT / COPY path. Default 5000." default:"5000" placeholder:"N"`

	// ADR-0118 finding 1(a): lead with applicability. On `migrate` these are
	// the general-purpose copy-parallelism knobs and apply for EVERY source
	// (the within-table axis) — distinct from the `sync start` variants, which
	// are PG-source-only.
	BulkParallelism int `help:"Every source: number of parallel reader/writer pairs per table during bulk copy — the within-table axis. Tables above --bulk-parallel-min-rows are split into this many PK ranges and copied concurrently. Tables without a single integer PK fall back to single-reader. 0 means use min(8, NumCPU); 1 disables parallelism. See ADR-0019." default:"0" placeholder:"N"`

	TableParallelism int `help:"Every source: number of tables copied CONCURRENTLY during bulk copy — the cross-table axis (pgcopydb --table-jobs), composed with the within-table --bulk-parallelism axis. Closes the many-medium-table gap where each table sat below the within-table-split threshold and the table loop ran them serially, leaving cores idle. The two axes MULTIPLY: at most --table-parallelism × (effective --bulk-parallelism) connections open against the target at once, and that PRODUCT is bounded by the target's connection budget (and --max-target-connections) at a single chokepoint — within-table parallelism is satisfied first, the table axis gets whatever remains. 0 (default) = auto: 4 (pgcopydb's --table-jobs default), bounded by the budget split. 1 disables cross-table concurrency (one table at a time). Only the migrate path uses this; the sync cold-start path stays serial by design. See ADR-0076." default:"0" placeholder:"N"`

	MaxTargetConnections int `help:"Explicit ceiling on the number of connections the bulk-copy pool opens against the target (connection-resilience item 4). 0 (default) = auto: sluice probes the target's connection-slot budget (Postgres max_connections / role / database limits minus in-use and a small reserve) and caps --bulk-parallelism to fit, refusing loudly if no budget is free. When set, it's an explicit upper bound the auto-cap further bounds — it never raises --bulk-parallelism. Inert against engines without a connection-slot model (MySQL target)." default:"0" placeholder:"N"`

	ReapStaleBackends bool `help:"Terminate sluice's OWN orphaned backends on the target during the cold-start preflight (connection-resilience Phase 2, item 2). Detection runs ALWAYS and reports loudly; this flag authorises pg_terminate_backend on each orphan. An orphan is a backend whose application_name carries the 'sluice/' prefix, owned by the connecting role, NOT the current session, and either idle-in-transaction or holding a lock on a relation sluice is about to write — typically a SIGKILL'd / OOM'd prior run whose server-side COPY backend still holds a target-table lock and a connection slot. Default off — detect-and-report is the safe baseline, because a legitimately-running concurrent sluice process on the same target is a real possibility (the report is shown first so you can tell them apart). Termination is always scoped to your own sluice backends; it never touches another role's or a non-sluice session, and needs no superuser grant. Inert against engines without a backend model (MySQL target)."`

	BulkParallelMinRows int64 `help:"Every source: row-count threshold below which a table is copied with a single reader/writer pair regardless of --bulk-parallelism. Avoids per-chunk overhead on small tables. 0 (default) = auto: base 80000 (sits below 100k to absorb the InnoDB information_schema row-count undershoot), dialled DOWN on many-table schemas (base/table-count, floored at 10000) so a many-medium-table migrate engages within-table parallelism instead of copying each table serially + single-streamed (the pgcopydb many-table gap, roadmap item 3). Set an explicit N to pin the threshold (e.g. 100000 for pre-v0.62.0 behaviour); explicit values are never auto-lowered." default:"0" placeholder:"N"`

	MaxBufferBytes int64 `help:"Soft cap on per-batch buffered memory in the bulk-copy writer. The writer flushes when accumulated row-value bytes reach the cap regardless of row count, so wide-row workloads (TEXT/BYTEA/JSON at MB scale) don't blow out heap. A single row larger than the cap still applies (soft target). Default 67108864 (64 MiB). See ADR-0028." default:"67108864" placeholder:"N"`

	IndexBuildMem string `help:"Postgres-only: per-build maintenance_work_mem for the deferred secondary-index phase (CREATE INDEX runs after the bulk COPY, against an idle target). Accepts a human size ('512MB', '2GB') or a raw byte count. Default 'auto': sluice probes pg_settings (shared_buffers as the RAM proxy) and raises maintenance_work_mem well above the provider's steady-state ~4%-of-RAM default — the dominant index-build speedup — flooring at the provider's current value (sluice only ever raises). It also raises max_parallel_maintenance_workers toward the max_worker_processes ceiling. Best-effort: a denied SET logs a WARN and the build proceeds untuned, never failing the index phase. Inert on MySQL targets. See docs/dev/notes/index-build-phase-tuning.md." default:"auto" placeholder:"SIZE|auto"`

	IndexBuildParallelism int `help:"Postgres-only: number of secondary indexes to build CONCURRENTLY in the deferred index phase (Phase B). Each concurrent build runs plain CREATE INDEX on its own connection with its own maintenance_work_mem, so the aggregate memory budget is DIVIDED across the workers (total ≈ N × per-build mem). 0 (default) = auto: sluice derives a conservative N bounded by the target's spare connection-slot budget AND a memory budget (so it can't OOM a small node) AND the index count. The note's tier data shows parallelism barely helps below PS-640 (max_worker_processes flat at 4), so auto stays modest there and scales up on large instances. Set >0 to override the auto count verbatim. N=1 forces the serial single-connection build. Inert on MySQL targets. See docs/dev/notes/index-build-phase-tuning.md." default:"0" placeholder:"N"`

	TargetSchema string `help:"Per-source target schema namespace (Postgres-only). When set, every emitted CREATE TABLE / ALTER TABLE / CREATE INDEX / CREATE TYPE prefixes the table reference with this schema. Use to land multiple sluice streams on the same target without table-name collisions (Shape B microservices → analytics warehouse, ADR-0031). The schema is auto-created on the target if it doesn't exist. The control table sluice_cdc_state stays in the DSN's default schema regardless. MySQL operators use a different --target DSN database instead — schemas and databases collapse on MySQL." placeholder:"NAME"`

	EnablePGExtension []string `help:"Enable passthrough for a Postgres extension type (repeatable). Same-engine PG → PG passthrough preserves the source-native shape on the target. Cross-engine targets (MySQL) keep the loud-failure default except for hstore (→ JSON) and citext (→ VARCHAR with case-insensitive collation), which have built-in default translators. Each named extension must be installed on both source and target — sluice preflights via pg_extension before any data moves. Recognised: vector (pgvector), pg_trgm, hstore, citext. v1 shortlist per docs/research/pg-extensions-deployment-frequency.md. See ADR-0032." placeholder:"EXT"`

	InjectShardColumn string `help:"ADR-0048 Shape A — inject a sluice-managed discriminator column on the consolidated target (Format: NAME=VALUE). Each per-shard 'sluice migrate' (and 'sluice sync start') passes a distinct VALUE so per-shard rows land disjoint on the shared target via a composite PK (discriminator, …source PK). Sluice appends the column to every PK-bearing table, rewrites the PK to be composite, stamps VALUE onto every row (bulk-copy + CDC), and runs a three-point loud preflight on a non-empty target: every existing row must have the discriminator NOT NULL, the incoming VALUE must not already be present, and the composite PK must lead with the discriminator. Tables without a base PK refuse loudly (composite PK requires a base PK). Off when empty (default). The discriminator column is suppressed from 'schema diff' / 'verify' as a sluice-managed surface. See ADR-0048 for the full design." placeholder:"NAME=VALUE"`

	AllowCrossShardMerge bool `help:"Opt out of the cross-shard-collision preflight (Bug 152). By default, when the source is a multi-shard Vitess/PlanetScale keyspace (vtgate merges every shard into one logical stream) and --inject-shard-column is NOT set, sluice REFUSES to copy into a single target table that has a PK or UNIQUE — rows from different shards sharing a key value would silently overwrite each other (per-shard id ranges collide across shards). Pass this flag ONLY if the key is globally unique across shards (e.g. Vitess sequences or UUID keys) so no overwrite can occur. The structural alternative is --inject-shard-column NAME=VALUE (ADR-0048), which adds a per-shard discriminator. No effect on single-shard / non-sharded sources or when --inject-shard-column is set."`

	AllowDegradedFKs bool `help:"Tolerate dirty foreign-key sources: when ALTER TABLE ... ADD CONSTRAINT FOREIGN KEY fails with SQLSTATE 23503 (orphan rows on the child table), retry as NOT VALID and surface the degraded constraint at the end of the run. The FK is still attached on the target catalog and rejects new writes that would orphan rows; the operator runs ALTER TABLE ... VALIDATE CONSTRAINT <name> after fixing the orphans. Default off — loud-failure-on-dirty-source stays baseline. PG-target only (mirrors pgcopydb PR #27): MySQL has no per-constraint NOT VALID semantic and refuses loudly when this flag is set against a MySQL target. See docs/dev/notes/pgcopydb-planetscale-fork-review.md."`

	Redact       []string `help:"Redact a PII column (repeatable). Format: '[schema.]table.column=STRATEGY[:options]'. Strategies: null (NULLABLE columns only), static:<value>, hash:sha256, hash:hmac-sha256[:<keyname>] (requires --keyset-source), truncate:<n>, mask:inner:<m1>,<m2>[,<char>], mask:outer:<m1>,<m2>[,<char>], mask:ssn, mask:pan, mask:pan-relaxed, mask:email, mask:ca-sin, mask:uk-nin, mask:iban, mask:uuid (Phase 2.b country/format presets, v0.57.0+), randomize:int:<min>,<max>, randomize:email, randomize:us-phone, randomize:uuid (Phase 2.c first wave, v0.59.0+), randomize:ssn, randomize:pan[:<brand>], randomize:ca-sin, randomize:uk-nin, randomize:iban[:<country-code>] (Phase 2.c second wave, v0.60.0+; brand: visa|mastercard|amex; country: DE|GB|FR; all randomize:* require a PK on the source table), randomize:dict:<name>, tokenize:dict:<name>[:<keyname>] (Phase 3 v0.61.0+, keyset-sourced in Phase 4 v0.62.0+; dictionaries declared in YAML 'dictionaries:' block — CLI form REQUIRES YAML config to declare the dictionary content). Examples: --redact users.email=hash:sha256, --redact users.pan=mask:pan, --redact users.id=mask:uuid, --redact users.age=randomize:int:18,90, --redact users.first_name=tokenize:dict:first_names. Bulk-copy + CDC paths both honour --redact. YAML form available under config 'redactions:' block. See docs/dev/notes/prep-pii-redaction-phase-1.md." placeholder:"RULE" sep:"none"`
	KeysetSource string   `help:"Operator keyset source for keyset-using redaction strategies (hash:hmac-sha256, tokenize:dict). PII Phase 4 (ADR-0041). Forms: 'file:PATH' (keyset YAML on disk), 'env:VARNAME' (keyset YAML in an env var), 'db:DSN' (sluice_keysets table on the named DSN — shared across streams for cross-stream surrogate stability). Resolved ONCE at startup; rotation takes effect on next process restart only (no hot-reload). Required when any --redact / YAML rule uses hash:hmac-sha256 or tokenize:dict — the Phase 1 --redact-key-source flag and the built-in v0.61.0 tokenize key were removed." placeholder:"SRC"`

	CrashHookFlags
}

// Run implements the migrate subcommand.
func (m *MigrateCmd) Run(g *Globals) error {
	cfg, err := config.Load(g.Config)
	if err != nil {
		return err
	}

	source, err := resolveEngine(m.SourceDriver)
	if err != nil {
		return fmt.Errorf("--source-driver: %w", err)
	}
	target, err := resolveEngine(m.TargetDriver)
	if err != nil {
		return fmt.Errorf("--target-driver: %w", err)
	}

	// CLI-side mutual exclusion: catching this here means the
	// operator gets a clean "you can't do that" before any DSN
	// dialing happens. NewTableFilter also enforces it for the
	// programmatic-construction path.
	if len(m.IncludeTable) > 0 && len(m.ExcludeTable) > 0 {
		return errors.New("--include-table and --exclude-table are mutually exclusive")
	}
	includeNS, excludeNS, allNS, err := resolveNamespaceScopeArgs(
		m.IncludeDatabase, m.ExcludeDatabase, m.AllDatabases,
		m.IncludeSchema, m.ExcludeSchema, m.AllSchemas,
	)
	if err != nil {
		return err
	}
	if len(includeNS) > 0 && len(excludeNS) > 0 {
		return errors.New("--include-database/--include-schema and --exclude-database/--exclude-schema are mutually exclusive")
	}
	if allNS && (len(includeNS) > 0 || len(excludeNS) > 0) {
		return errors.New("--all-databases/--all-schemas is mutually exclusive with --include-* / --exclude-* namespace scope")
	}
	if len(m.IncludeView) > 0 && len(m.ExcludeView) > 0 {
		return errors.New("--include-view and --exclude-view are mutually exclusive")
	}
	databaseFilter, err := pipeline.NewDatabaseFilter(includeNS, excludeNS)
	if err != nil {
		return err
	}
	if m.Resume && m.ResetTargetData {
		return errors.New("--resume and --reset-target-data are mutually exclusive")
	}
	include, exclude := resolveTableFilterArgs(m.IncludeTable, m.ExcludeTable, cfg)
	filter, err := pipeline.NewTableFilter(include, exclude)
	if err != nil {
		return err
	}
	viewFilter, err := pipeline.NewViewFilter(m.IncludeView, m.ExcludeView)
	if err != nil {
		return err
	}
	indexBuildMem, err := parseIndexBuildMem(m.IndexBuildMem)
	if err != nil {
		return err
	}

	mappings, err := resolveMappings(m.TypeOverride, cfg)
	if err != nil {
		return err
	}
	exprMappings, err := resolveExpressionMappings(m.ExprOverride, cfg)
	if err != nil {
		return err
	}

	if m.ResetTargetData && !m.Yes {
		ok, err := confirmTypedDestructive(os.Stdin, os.Stdout,
			"This will DROP tables on the target. Type 'reset' to confirm: ", "reset")
		if err != nil {
			return err
		}
		if !ok {
			fmt.Fprintln(os.Stdout, "aborted")
			return nil
		}
	}

	shardSpec, err := parseInjectShardColumn(m.InjectShardColumn)
	if err != nil {
		return err
	}

	// connection-resilience (1): label every connection sluice opens
	// with the run's id (PG: application_name=sluice/<role>/<id>) so
	// the operator can find sluice's sessions in pg_stat_activity.
	// Applied once here, before any engine opens a connection; the
	// engine normalises an empty --migration-id to the "-" fallback.
	source = labelEngine(source, m.MigrationID)
	target = labelEngine(target, m.MigrationID)

	mig := &pipeline.Migrator{
		Source:                source,
		Target:                target,
		SourceDSN:             m.Source,
		TargetDSN:             m.Target,
		DryRun:                m.DryRun,
		Mappings:              mappings,
		ExpressionMappings:    exprMappings,
		Filter:                filter,
		DatabaseFilter:        databaseFilter,
		AllDatabases:          allNS,
		ViewFilter:            viewFilter,
		SkipViews:             m.SkipViews,
		Resume:                m.Resume,
		MigrationID:           m.MigrationID,
		ForceColdStart:        m.ForceColdStart,
		RawCopyFormat:         parseRawCopyFormat(m.RawCopyFormat),
		ResetTargetData:       m.ResetTargetData,
		BulkBatchSize:         m.BulkBatchSize,
		BulkParallelism:       m.BulkParallelism,
		TableParallelism:      m.TableParallelism,
		BulkParallelMinRows:   m.BulkParallelMinRows,
		MaxTargetConnections:  m.MaxTargetConnections,
		ReapStaleBackends:     m.ReapStaleBackends,
		MaxBufferBytes:        m.MaxBufferBytes,
		IndexBuildMem:         indexBuildMem,
		IndexBuildParallelism: m.IndexBuildParallelism,
		TargetSchema:          m.TargetSchema,
		EnabledPGExtensions:   m.EnablePGExtension,
		InjectShardColumn:     shardSpec,
		AllowCrossShardMerge:  m.AllowCrossShardMerge,
		AllowDegradedFKs:      m.AllowDegradedFKs,
	}
	keysetSource := m.KeysetSource
	if keysetSource == "" {
		keysetSource = cfg.KeysetSource
	}
	keyset, err := redact.LoadKeyset(kongContext(), keysetSource)
	if err != nil {
		return err
	}
	dictionaries, err := redact.LoadDictionaries(cfg.Dictionaries)
	if err != nil {
		return err
	}
	redactor, err := parseRedactFlags(m.Redact, keyset, "", dictionaries)
	if err != nil {
		return err
	}
	redactor, err = mergeYAMLRedactions(redactor, cfg.Redactions, keyset, "", dictionaries)
	if err != nil {
		return fmt.Errorf("redactions (YAML): %w", err)
	}
	mig.Redactor = redactor
	logKeysetLoaded(keyset)
	logRedactionConfig(redactor, "migrate")
	// ADR-0056 auto-on-crash hook (opt-in). When
	// --diagnose-on-crash-dir is set, the hook writes a bundle to the
	// directory if Run returns an error. The hook NEVER masks the
	// original error per the loud-failure tenet.
	crashWrap, err := installCrashHook(m.CrashHookFlags,
		crashHookRequestForStreamer(m.MigrationID, source, target, m.Source, m.Target, ""))
	if err != nil {
		return err
	}
	return crashWrap(mig.Run(kongContext()))
}

// resolveTableFilterArgs picks the include/exclude list to use,
// preferring CLI flags over YAML config when both are supplied.
// CLI takes precedence wholesale: if --include-table is set it
// replaces the config's IncludeTables (and clears any config-side
// ExcludeTables for that command run, since the operator's intent
// is unambiguous). Same shape for --exclude-table.
func resolveTableFilterArgs(cliInclude, cliExclude []string, cfg *config.Config) (include, exclude []string) {
	if len(cliInclude) > 0 {
		return cliInclude, nil
	}
	if len(cliExclude) > 0 {
		return nil, cliExclude
	}
	return cfg.IncludeTables, cfg.ExcludeTables
}

// resolveNamespaceScopeArgs merges the two spellings of the
// multi-namespace fan-out flags into the single internal
// ([DatabaseFilter] + all-flag) shape the orchestrator consumes. The
// `--*-database` form (ADR-0074) is canonical on a MySQL source; the
// `--*-schema` form (ADR-0075) is canonical on a Postgres source. They
// populate the SAME internal filter — "a MySQL database ≈ a PG schema"
// (ADR-0031) — so there is no duplicated filter logic downstream.
//
// Supplying BOTH a `--*-schema` and a `--*-database` form in one
// invocation is a loud error: the operator must pick one vocabulary
// (the two are synonyms, and mixing them is almost certainly a mistake).
// The per-form mutual-exclusion (include vs exclude, all vs include/
// exclude) is enforced by the caller separately on the merged result.
func resolveNamespaceScopeArgs(
	includeDatabase, excludeDatabase []string, allDatabases bool,
	includeSchema, excludeSchema []string, allSchemas bool,
) (include, exclude []string, all bool, err error) {
	schemaUsed := len(includeSchema) > 0 || len(excludeSchema) > 0 || allSchemas
	databaseUsed := len(includeDatabase) > 0 || len(excludeDatabase) > 0 || allDatabases
	if schemaUsed && databaseUsed {
		return nil, nil, false, errors.New(
			"--include-schema / --exclude-schema / --all-schemas and " +
				"--include-database / --exclude-database / --all-databases are synonyms; " +
				"supply only one vocabulary (use --*-schema on a Postgres source, --*-database on a MySQL source)",
		)
	}
	if schemaUsed {
		return includeSchema, excludeSchema, allSchemas, nil
	}
	return includeDatabase, excludeDatabase, allDatabases, nil
}

// resolveEngine looks up an engine by registered name and returns a
// useful error message that lists the available options when the
// name doesn't match anything.
func resolveEngine(name string) (ir.Engine, error) {
	if name == "" {
		return nil, errors.New("engine name is empty")
	}
	e, ok := engines.Get(name)
	if !ok {
		return nil, fmt.Errorf("unknown engine %q (registered: %v)", name, engines.Names())
	}
	return e, nil
}

// labelEngine returns e configured to stamp its connections with the
// run's stream-/migration-id when the engine supports connection
// labeling ([ir.ConnectionLabeler] — PG carries it in application_name);
// engines without a per-connection label pass through unchanged. The
// labeled copy is local to this run — the registry's engine value stays
// label-free.
func labelEngine(e ir.Engine, id string) ir.Engine {
	if l, ok := e.(ir.ConnectionLabeler); ok {
		return l.WithConnectionLabel(id)
	}
	return e
}

// SyncCmd groups the continuous-sync subcommands. Continuous sync is
// where source-side changes (binlog, logical replication) flow to the
// target on an ongoing basis — useful both as a low-downtime
// migration path and as a reporting/locality replication tool.
type SyncCmd struct {
	Start      SyncStartCmd         `cmd:"" help:"Start a continuous-sync stream from source to target."`
	Status     SyncStatusCmd        `cmd:"" help:"Show status of a running sync stream."`
	Stop       SyncStopCmd          `cmd:"" help:"Request a running sync stream to drain in-flight changes and exit cleanly."`
	Health     SyncHealthCmd        `cmd:"" help:"Probe a running stream's freshness against operator-supplied thresholds; cron-friendly exit codes."`
	FromBackup SyncFromBackupCmdGrp `cmd:"" name:"from-backup" help:"Replay a backup chain into a target as a long-running broker (Phase 4.5)."`
}

// SyncFromBackupCmdGrp groups `sluice sync from-backup` (run) and
// `sluice sync from-backup stop` (companion stop). Mirrors the
// BackupStreamCmdGroup shape from Phase 4: the verb without an
// explicit subcommand is `sync from-backup run` so kong dispatches
// cleanly with the sibling `stop` subcommand.
type SyncFromBackupCmdGrp struct {
	Run  SyncFromBackupCmd     `cmd:"" help:"Run the long-running broker (poll a chain and replay incrementals into a target)."`
	Stop SyncFromBackupStopCmd `cmd:"" help:"Request a running broker to commit any in-flight apply and exit cleanly."`
}

// SyncFromBackupCmd runs the Phase 4.5 broker. Polls a chain root for
// new incrementals at the configured cadence and replays each one
// into a target via the existing ChangeApplier.ApplyBatch path. The
// chain itself is the rendezvous: an upstream `sluice backup stream`
// writes incrementals to S3 / GCS / Azure / local-FS; this command
// reads from the same destination. No direct source-target
// connectivity required — the broker is a read-only consumer of the
// chain.
type SyncFromBackupCmd struct {
	BackupDir    string `help:"Directory the chain lives in (local filesystem). Mutually exclusive with --backup-target." placeholder:"DIR"`
	BackupTarget string `name:"backup-target" help:"URL of the chain (s3://, gs://, azblob://, file:///). Mutually exclusive with --backup-dir." placeholder:"URL"`

	BackupEndpoint  string `help:"Override the S3 endpoint for S3-compatible providers. Only meaningful when --backup-target is an s3:// URL." placeholder:"URL"`
	BackupRegion    string `help:"Override the S3 region. Only meaningful when --backup-target is an s3:// URL." placeholder:"REGION"`
	BackupPathStyle bool   `help:"Force path-style addressing. Only meaningful when --backup-target is an s3:// URL."`

	TargetDriver string `help:"Target engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"target"`
	Target       string `help:"Target database DSN." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`

	StreamID string `help:"Stream identifier; the key under which the broker's chain-state position is persisted on the target. Required for clean restart resume." required:"" placeholder:"ID"`

	PollInterval time.Duration `help:"Wall-clock cadence each broker tick runs at. The broker observes new incrementals + applies them within ~poll-interval of their commit on the source side." default:"30s" placeholder:"DUR"`

	ApplyBatchSize int   `help:"Batch up to N CDC changes per target transaction during incremental replay. Idempotent applier semantics (ADR-0010) keep replay-on-crash safe." default:"100" placeholder:"N"`
	MaxBufferBytes int64 `help:"Soft cap on per-batch buffered memory in the CDC applier. Default 67108864 (64 MiB). See ADR-0028." default:"67108864" placeholder:"N"`

	ApplyConcurrency int `help:"Key-hash concurrent-apply LANE count W for incremental REPLAY (ADR-0104/0105, the same machinery 'sync start --apply-concurrency' uses). Each incremental's merged change stream is fanned across W in-order PK-hash lanes committing concurrently, each with its own AIMD controller. Without this a large incremental replayed into a high-latency / cross-region target applies through a single serial pipelined stream and is RTT-bound (the broker-replay analog of the 'sync start' cross-region wedge). Exactly-once is preserved: every change in an incremental carries the same broker chain position, so the lanes persist the identical resume position the serial path does. ADR-0106 FAST BY DEFAULT: 0 (default, unset) = auto:4 (a fixed conservative ceiling — the broker does not run a connection-budget probe; per-lane AIMD backs off if the target is tight); 1 = explicit SERIAL opt-out (byte-identical to the pre-fix behaviour); W>1 honored verbatim." default:"0" placeholder:"W"`

	ResetTargetData bool `help:"Cold-start recovery: drop target tables, run a chain restore (full + every incremental up to current), then transition to live polling. Mirrors 'migrate --reset-target-data'. Mutually exclusive with --at-chain-id."`

	AtChainID string `help:"Operator-asserted resumption: the broker treats the target as currently being at chain ID <ID>; writes a fresh sluice_cdc_state row and transitions to live polling from there. Use after a manual 'sluice restore --from=<chain-url>'. Mutually exclusive with --reset-target-data." placeholder:"BACKUP-ID"`

	Yes bool `help:"Skip the destructive-action confirmation prompt for --reset-target-data." short:"y"`

	EncryptionFlags
}

// Run implements `sluice sync from-backup run`.
func (s *SyncFromBackupCmd) Run(_ *Globals) error {
	target, err := resolveEngine(s.TargetDriver)
	if err != nil {
		return fmt.Errorf("--target-driver: %w", err)
	}
	if s.BackupDir == "" && s.BackupTarget == "" {
		return errors.New("one of --backup-dir or --backup-target is required")
	}
	if s.BackupDir != "" && s.BackupTarget != "" {
		return errors.New("--backup-dir and --backup-target are mutually exclusive")
	}
	if s.ResetTargetData && s.AtChainID != "" {
		return errors.New("--reset-target-data and --at-chain-id are mutually exclusive")
	}

	ctx := kongContext()
	store, storeDesc, closer, err := openBackupStore(ctx, s.BackupDir, s.BackupTarget, pipeline.BlobStoreOptions{
		Endpoint:  s.BackupEndpoint,
		Region:    s.BackupRegion,
		PathStyle: s.BackupPathStyle,
	})
	if err != nil {
		return err
	}
	if closer != nil {
		defer func() { _ = closer() }()
	}

	if s.ResetTargetData && !s.Yes {
		ok, err := confirmTypedDestructive(os.Stdin, os.Stdout,
			"This will DROP tables on the target. Type 'reset' to confirm: ", "reset")
		if err != nil {
			return err
		}
		if !ok {
			fmt.Fprintln(os.Stdout, "aborted")
			return nil
		}
	}

	// Phase 6.1: load the chain root to extract Argon2id params for
	// the read-side envelope.
	rootManifest, err := pipeline.ReadRootManifest(ctx, store)
	if err != nil {
		return fmt.Errorf("from-backup: read root manifest: %w", err)
	}
	envelope, err := s.buildReadEnvelope(rootManifest)
	if err != nil {
		return err
	}

	broker := &pipeline.SyncFromBackup{
		Target:           target,
		TargetDSN:        s.Target,
		Store:            store,
		ChainURL:         storeDesc,
		StreamID:         s.StreamID,
		PollInterval:     s.PollInterval,
		ApplyBatchSize:   s.ApplyBatchSize,
		MaxBufferBytes:   s.MaxBufferBytes,
		ApplyConcurrency: s.ApplyConcurrency,
		ResetTargetData:  s.ResetTargetData,
		AtChainID:        s.AtChainID,
		SluiceVersion:    version,
		Envelope:         envelope,
	}
	return broker.Run(ctx)
}

// SyncFromBackupStopCmd runs `sluice sync from-backup stop`. Writes
// `stop_requested_at` to the chain destination's `broker_state.json`
// so the running broker observes the request on its next tick poll
// and exits cleanly. Cross-machine: the operator can stop a broker
// from a different host without process access — both sides agree on
// the chain destination.
type SyncFromBackupStopCmd struct {
	BackupDir    string `help:"Directory the running broker is reading from (local filesystem). Mutually exclusive with --backup-target." placeholder:"DIR"`
	BackupTarget string `name:"backup-target" help:"URL of the chain destination the running broker is reading from (s3://, gs://, azblob://, file:///). Mutually exclusive with --backup-dir." placeholder:"URL"`

	BackupEndpoint  string `help:"Override the S3 endpoint for S3-compatible providers. Only meaningful when --backup-target is an s3:// URL." placeholder:"URL"`
	BackupRegion    string `help:"Override the S3 region. Only meaningful when --backup-target is an s3:// URL." placeholder:"REGION"`
	BackupPathStyle bool   `help:"Force path-style addressing. Only meaningful when --backup-target is an s3:// URL."`
}

// Run implements `sluice sync from-backup stop`.
func (s *SyncFromBackupStopCmd) Run(_ *Globals) error {
	if s.BackupDir == "" && s.BackupTarget == "" {
		return errors.New("one of --backup-dir or --backup-target is required")
	}
	if s.BackupDir != "" && s.BackupTarget != "" {
		return errors.New("--backup-dir and --backup-target are mutually exclusive")
	}
	ctx := kongContext()
	store, storeDesc, closer, err := openBackupStore(ctx, s.BackupDir, s.BackupTarget, pipeline.BlobStoreOptions{
		Endpoint:  s.BackupEndpoint,
		Region:    s.BackupRegion,
		PathStyle: s.BackupPathStyle,
	})
	if err != nil {
		return err
	}
	if closer != nil {
		defer func() { _ = closer() }()
	}

	prior, err := pipeline.RequestSyncFromBackupStop(ctx, store, time.Now())
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "stop requested for broker on chain %q (running pid=%d host=%q stream_id=%q); broker will exit on next tick\n",
		storeDesc, prior.PID, prior.Host, prior.StreamID)
	return nil
}

// SyncStartCmd starts (or resumes) a continuous-sync stream from a
// source database to a target. The stream captures a consistent
// snapshot, bulk-copies it, then streams ongoing changes via CDC
// until the operator interrupts it (Ctrl-C). Restarts with the
// same --stream-id resume from the persisted position rather than
// re-running the snapshot+bulk-copy phase.
type SyncStartCmd struct {
	SourceDriver string `help:"Source engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"source"`
	Source       string `help:"Source database DSN." required:"" env:"SLUICE_SOURCE" placeholder:"DSN" group:"source"`

	TargetDriver string `help:"Target engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"target"`
	Target       string `help:"Target database DSN." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`

	IncludeTable []string `help:"Only stream these tables (comma-separated, repeatable). Glob patterns allowed (e.g. 'audit_*'). Mutually exclusive with --exclude-table." sep:"," placeholder:"TABLE"`
	ExcludeTable []string `help:"Stream every table except these (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --include-table." sep:"," placeholder:"TABLE"`

	IncludeDatabase []string `help:"Multi-database fan-out (ADR-0074, MySQL source): cold-start + CDC-sync ONLY these source databases (comma-separated, repeatable). Glob patterns allowed (e.g. 'app_*'). Each source database routes to a same-named target namespace (PG schema / MySQL database). The selected databases are cold-started under ONE spanning consistent snapshot, then the single server-wide binlog CDC stream is routed per-change to each namespace. Mutually exclusive with --exclude-database. When any database-scope flag is set, the source DSN's database is optional (it's a server connection). System databases (information_schema, performance_schema, mysql, sys) are always excluded. Warm-resume across N databases is not yet supported (ADR-0074 Phase 1b.3)." sep:"," placeholder:"DATABASE"`
	ExcludeDatabase []string `help:"Multi-database fan-out (ADR-0074, MySQL source): cold-start + CDC-sync every non-system source database EXCEPT these (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --include-database." sep:"," placeholder:"DATABASE"`
	AllDatabases    bool     `help:"Multi-database fan-out (ADR-0074, MySQL source): cold-start + CDC-sync every non-system database on the source server, each to a same-named target namespace. Mutually exclusive with --include-database / --exclude-database."`

	IncludeSchema []string `help:"Multi-schema fan-out (ADR-0075, Postgres source): cold-start + CDC-sync ONLY these source schemas (comma-separated, repeatable). Glob patterns allowed (e.g. 'app_*'). Each source schema routes to a same-named target namespace (PG schema / MySQL database). The PG slot is database-wide, so the selected schemas are cold-started under ONE spanning exported snapshot, then the single database-wide CDC stream is routed per-change to each namespace. The PG-source synonym of --include-database; mutually exclusive with --exclude-schema. Supplying BOTH the --*-schema and --*-database spelling in one invocation is an error." sep:"," placeholder:"SCHEMA"`
	ExcludeSchema []string `help:"Multi-schema fan-out (ADR-0075, Postgres source): cold-start + CDC-sync every non-system source schema EXCEPT these (comma-separated, repeatable). Glob patterns allowed. The PG-source synonym of --exclude-database; mutually exclusive with --include-schema." sep:"," placeholder:"SCHEMA"`
	AllSchemas    bool     `help:"Multi-schema fan-out (ADR-0075, Postgres source): cold-start + CDC-sync every non-system schema in the source database, each to a same-named target namespace. The PG-source synonym of --all-databases. Mutually exclusive with --include-schema / --exclude-schema."`

	IncludeView []string `help:"Only create these views on the target during cold-start (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --exclude-view. Views are not replicated by CDC; this filter only affects the cold-start schema-apply phase." sep:"," placeholder:"VIEW"`
	ExcludeView []string `help:"Skip these views during cold-start schema-apply (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --include-view." sep:"," placeholder:"VIEW"`
	SkipViews   bool     `help:"Skip view creation entirely on cold-start. Views are not replicated by CDC, so this only affects the initial schema-apply step."`

	TypeOverride []string `help:"Force a specific target type for a column (repeatable). Format: 'TABLE.COLUMN=TYPE', e.g. 'products.attrs=text'. CLI form of the YAML 'mappings:' config; for target-type options, use the YAML form." placeholder:"TABLE.COLUMN=TYPE" sep:"none"`

	ExprOverride []string `help:"Replace a generated column's body with operator-supplied target-dialect text (repeatable). Format: 'TABLE.COLUMN=EXPRESSION'. Emitted verbatim; ADR-0016 translator skips overridden columns. CLI form of the YAML 'expression_mappings:' config." placeholder:"TABLE.COLUMN=EXPRESSION" sep:"none"`

	StreamID string `help:"Stream identifier; the key under which position is persisted on the target. Auto-generated from source/target host info when empty." placeholder:"ID"`
	SlotName string `help:"Replication-slot name suffix for engines that have a slot concept (Postgres). Default 'sluice_slot'. Sluice prepends 'sluice_' if the supplied name doesn't already start with it (so '--slot-name shard_a' creates 'sluice_shard_a'); the convention lets operators find every sluice slot with 'pg_replication_slots WHERE slot_name LIKE sluice\\_%'. Set per-instance to run multiple concurrent sluice instances against the same source — without distinct slot names they collide on the default. Engines without slots (MySQL: binlog stream is the slot) silently ignore this flag." placeholder:"NAME"`
	DryRun   bool   `short:"n" help:"Print what would happen — cold-start vs warm-resume, source schema summary or persisted position — without modifying the target or starting the stream."`

	ForceColdStart bool `help:"Skip the cold-start pre-flight check that refuses to bulk-copy into a populated target. Use with caution — INSERT into a non-empty table will collide on PRIMARY KEY. Ignored on the warm-resume path."`

	ResetTargetData bool `help:"Destructive recovery: DELETE the cdc-state row, DROP every source-schema table on the target, then run a fresh cold-start stream. Use after slot-missing fall-through or a similar wedged-state recovery. Requires confirmation (type 'reset') unless --yes is set. See ADR-0023."`

	RestartFromScratch bool `help:"Force a fresh cold-start that re-copies from the beginning, IGNORING any persisted resume position (incl. a mid-COPY cursor). Use when a checkpoint is bad or to force a clean re-copy. The re-copy lands cleanly regardless of source: for an idempotent source (VStream/PlanetScale, Postgres) the upsert COPY absorbs the overlap WITHOUT dropping the target; for a non-idempotent source (native MySQL binlog, whose cold-copy is plain INSERT and would otherwise dup-key on the leftover rows) sluice DROPS the in-scope target tables first and recreates them empty, then re-copies (the cdc-state row is left intact — only the position is discarded). Unlike --force-cold-start (which only skips the pre-flight and still warm-resumes from a persisted position), this discards the position; unlike --reset-target-data, it never clears the cdc-state row and only drops tables on the non-idempotent path. Mutually exclusive with --reset-target-data and --position-from-manifest."`

	NoAutoResnapshot bool `help:"Suppress the automatic re-snapshot when a resume hits a purged/invalid source position. By default (parity with the self-hosted binlog path, ADR-0093) a resume from a position older than the source's retained binlogs — routine on PlanetScale's binlog-retention window — auto-recovers with a fresh cold-start re-snapshot: on an idempotent source (VStream/PlanetScale) the upsert copy absorbs the overlap and the target is NOT dropped; on a non-idempotent source (native MySQL binlog) the in-scope target tables are dropped and recreated first so the plain-INSERT copy starts clean (the cdc-state row is preserved). With this flag set, sluice instead fails LOUDLY with an actionable error naming the recovery commands (--restart-from-scratch / --reset-target-data), so the operator decides — useful when a full re-snapshot is expensive (very large tables) and should be a deliberate choice. Gates BOTH the pre-flight fall-through and the reactive VStream recovery."`

	SchemaAlreadyApplied bool `help:"Skip every DDL phase during cold-start (CREATE TABLE / CREATE INDEX / ADD FOREIGN KEY / CREATE VIEW / SyncIdentitySequences / EnsureControlTable). Operator promises the target's catalog matches the source's AND the sluice_cdc_state control table is pre-created. Use this on PlanetScale branches with Safe Migrations enabled (GitHub #17), or on Atlas/Liquibase-managed schemas where DDL goes through a separate pipeline. The cold-start preflight refusal is also skipped — bulk-copy runs into operator-prepared empty tables; sluice does NOT validate the schema match."`

	Yes bool `help:"Skip the destructive-action confirmation prompt for --reset-target-data." short:"y"`

	ApplyBatchSize string `help:"Batch up to N CDC changes per target transaction, OR 'auto' to use the engine-default ceiling (1000 mysql/postgres, 100 planetscale). Default 'auto' (ADR-0089): the ADR-0052 AIMD controller adapts the batch size within [1, ceiling] to a p95-latency target, for >10x throughput over single-row apply. Pass --apply-batch-size=1 for the pre-ADR-0089 conservative one-change-per-tx behaviour, or --no-auto-tune to keep a static cap (floor stays 1). Tables with NO usable identity key (no PRIMARY KEY and no unique index) are never batched — each such change commits alone (batch=1 semantics) so replay-on-crash cannot amplify duplicates (ADR-0089 keyless guard); PRIMARY-KEY and UNIQUE tables batch normally (ADR-0010 idempotency). Schema-change events (TRUNCATE) flush the in-progress batch." default:"auto" placeholder:"N|auto"`

	NoAutoTune bool `help:"Disable the ADR-0052 AIMD apply-batch-size controller. With this flag set, --apply-batch-size=N becomes a strictly static row-cap (the pre-v0.72.0 behaviour). Useful on workloads where the operator has hand-tuned the batch size for a specific shape and wants no auto-adaptation."`

	ApplyTuneTargetLatency time.Duration `help:"Override the AIMD controller's p95 target latency (ADR-0052 DP-2). Engine-default when zero: 5s for planetscale (Vitess 20s tx-killer + 4x headroom), 10s for mysql/postgres. Only consulted when the controller is active (default; opt-out via --no-auto-tune)." placeholder:"DUR"`

	MaxBufferBytes int64 `help:"Soft cap on per-batch buffered memory in the CDC applier (and, on the cold-start branch, the bulk-copy writer). The applier commits the in-flight target tx when accumulated row-value bytes reach the cap regardless of row count, so wide-row streams (TEXT/BYTEA/JSON at MB scale) don't blow out heap. A single change larger than the cap still applies (soft target). Default 67108864 (64 MiB). See ADR-0028." default:"67108864" placeholder:"N"`

	IndexBuildMem string `help:"Postgres-only, cold-start branch: per-build maintenance_work_mem for the deferred secondary-index phase (CREATE INDEX runs after the cold-start bulk COPY, against an idle target). Accepts a human size ('512MB', '2GB') or a raw byte count. Default 'auto': sluice probes pg_settings (shared_buffers as the RAM proxy) and raises maintenance_work_mem well above the provider's steady-state ~4%-of-RAM default — the dominant index-build speedup — flooring at the provider's current value. Best-effort: a denied SET logs a WARN and the build proceeds untuned. Only the cold-start path builds indexes; warm-resume ignores this. Inert on MySQL targets. See docs/dev/notes/index-build-phase-tuning.md." default:"auto" placeholder:"SIZE|auto"`

	IndexBuildParallelism int `help:"Postgres-only, cold-start branch: number of secondary indexes to build CONCURRENTLY in the deferred index phase (Phase B). Each concurrent build runs plain CREATE INDEX on its own connection with its own maintenance_work_mem, so the aggregate memory budget is DIVIDED across the workers. 0 (default) = auto: sluice derives a conservative N bounded by the target's spare connection-slot budget AND a memory budget (so it can't OOM a small node) AND the index count. Parallelism barely helps below PS-640 (max_worker_processes flat at 4), so auto stays modest there. Set >0 to override the auto count verbatim; N=1 forces the serial build. Only the cold-start path builds indexes; warm-resume ignores this. Inert on MySQL targets. See docs/dev/notes/index-build-phase-tuning.md." default:"0" placeholder:"N"`

	// ADR-0118 finding 1(a): the applicability clause is front-loaded as the
	// FIRST thing a --help reader sees. On a MySQL/VStream source these flags
	// are INERT (serial cold-start); --copy-fanout-degree / the DSN copy-table
	// parallelism knobs (--vstream-copy-table-parallelism /
	// --copy-table-parallelism) tune VStream/native cold-copy there. Setting
	// one explicitly on such a source emits a one-time runtime WARN (finding
	// 1(b), see warnInertParallelismFlags).
	BulkParallelism int `help:"PG source only; inert on MySQL/VStream — use --copy-fanout-degree / --vstream-copy-table-parallelism / --copy-table-parallelism there. FAST cold-start (ADR-0079): parallel reader/writer pairs PER table during the initial cold-start copy — the within-table axis (ADR-0019 PK-range chunking). Engages with the cross-table --table-parallelism axis when the PG source surfaces a shareable exported snapshot; all parallel readers are pinned to the ONE snapshot. 0 = min(8, NumCPU); 1 disables. See ADR-0079." default:"0" placeholder:"N"`

	TableParallelism int `help:"PG source only; inert on MySQL/VStream — use --copy-fanout-degree / --vstream-copy-table-parallelism / --copy-table-parallelism there. FAST cold-start (ADR-0079): tables copied CONCURRENTLY during the initial cold-start copy — the cross-table axis (pgcopydb --table-jobs), composed with within-table --bulk-parallelism. The two MULTIPLY; the product (plus the reserved CDC connection) is bounded by the target's connection budget and --max-target-connections at a single chokepoint. 0 (default) = auto: 4. 1 disables cross-table concurrency. See ADR-0076 / ADR-0079." default:"0" placeholder:"N"`

	BulkParallelMinRows int64 `help:"PG source only; inert on MySQL/VStream — use --copy-fanout-degree / --vstream-copy-table-parallelism / --copy-table-parallelism there. FAST cold-start (ADR-0079): row-count threshold below which a table is copied with a single reader/writer pair regardless of --bulk-parallelism. 0 (default) = auto (base 80000, dialled down on many-table schemas). Set an explicit N to pin it." default:"0" placeholder:"N"`

	BulkBatchSize int `help:"FAST cold-start (ADR-0079, PG source) only: bulk-copy batch size for the within-table cursor path. Default 5000. Inert on MySQL/VStream sources (serial cold-start)." default:"5000" placeholder:"N"`

	CopyFanoutDegree int `help:"VStream/CDC snapshot cold-start (ADR-0097, PlanetScale-MySQL target) only: WRITE-side fan-out — the single incoming snapshot row stream is PK-hash-partitioned out to N concurrent batched-INSERT writer workers, each on its own connection, to beat the single cross-region-RTT-bound INSERT connection vtgate forces (it blocks LOAD DATA). 0 (default) = auto: 4; 1 disables fan-out (serial). Bounded by the target connection budget / --max-target-connections. Inert on the FAST cold-start path and on no-PK tables. See ADR-0097." default:"0" placeholder:"N"`

	// ADR-0118 finding 4: promote the DSN read-axis params to first-class CLI
	// flags. Precedence is explicit CLI flag > DSN param > engine default
	// (1 = serial). 0 (the default, unset) means "fall back to the DSN param",
	// so the new flag's zero value never silently overrides a DSN value — only
	// an explicitly-set CLI flag wins (zero-value-safe, the v0.99.51 trap). The
	// DSN form (vstream_copy_table_parallelism / copy_table_parallelism in the
	// source DSN query-string) keeps working verbatim. The resolved override is
	// threaded into the mysql engine in SyncStartCmd.Run, where the engine reads
	// it ahead of the DSN param.
	VStreamCopyTableParallelism int `name:"vstream-copy-table-parallelism" help:"VStream cold-copy READ axis (Vitess/PlanetScale source): the number of CONCURRENT single-table COPY streams the auto-shard cold-copy runs (ADR-0099), the read-side sibling of the write-side --copy-fanout-degree. 0 (default) = unset — fall back to the source DSN's vstream_copy_table_parallelism param, then to the engine default (1 = serial single-stream). An explicit value here WINS over the DSN param. The DSN form keeps working verbatim. 1 = serial. Inert on PG / native-MySQL sources." default:"0" placeholder:"N"`

	CopyTableParallelism int `name:"copy-table-parallelism" help:"Native-MySQL cold-copy READ axis (self-managed, non-Vitess MySQL source): the number of CONCURRENT FTWRL-coordinated pinned-snapshot reader connections the cold-copy opens (ADR-0101). 0 (default) = unset — fall back to the source DSN's copy_table_parallelism param, then to the engine default (1 = serial single-snapshot). An explicit value here WINS over the DSN param. The DSN form keeps working verbatim. 1 = serial. Inert on PG / VStream sources." default:"0" placeholder:"N"`

	// VStreamPreserveSkew (ADR-0120, roadmap item 27 — default flipped 2026-06-26)
	// OPTS OUT of the new relaxed default and restores vtgate's MinimizeSkew hold
	// on the steady-state multi-shard VStream CDC stream. The default is now
	// relaxed (MinimizeSkew off) because a real cross-region A/B showed the old ON
	// default FREEZES the lagging shard under an apply-deficit backlog. Opt-out-
	// named: the zero value (false) keeps the new relaxed default for every
	// non-CLI caller (the v0.99.51 trap — safe behaviour is the zero value). The
	// DSN form (vstream_preserve_skew=true) also works; the explicit flag wins.
	// Threaded into the mysql engine in SyncStartCmd.Run.
	VStreamPreserveSkew bool `name:"vstream-preserve-skew" help:"VStream CDC (Vitess/PlanetScale source) only: OPT OUT of the default and restore vtgate's MinimizeSkew hold (commit-time-ordered merged stream) on the steady-state multi-shard stream. Since ADR-0120 (default flipped) MinimizeSkew is OFF by default — both shards stream and drain CONCURRENTLY — because the old ON default was shown by a real cross-region A/B to FREEZE the lagging shard under an apply-deficit backlog. Set this only if you specifically need strict cross-shard commit-time delivery and accept the catch-up wedge risk. The DSN form vstream_preserve_skew=true also works; this flag wins. Inert on PG / native-MySQL sources and on a single-shard keyspace."`

	NoIntraTableStealing bool `name:"no-intra-table-stealing" help:"Native-MySQL concurrent cold-copy (--copy-table-parallelism > 1) only: DISABLE intra-table PK-range work-stealing (ADR-0119). By default a large, chunk-eligible table (single/composite orderable PK, above the within-table row threshold) is split into PK-range chunks so idle reader connections can steal a CHUNK of the last big table — keeping the copy N-wide to the tail instead of tapering to one whole table. With this flag set, every table is copied as one whole-table work item (the prior tier-(a) whole-table-stealing behaviour). A throughput knob, not a correctness one: chunk coverage is gap-free + overlap-free either way. Inert on PG / VStream sources and on a serial (--copy-table-parallelism=1) cold-copy."`

	RawCopyFormat string `help:"FAST cold-start (ADR-0079, same-engine PG→PG) only: wire format for the raw-copy passthrough fast lane (ADR-0078). 'text' (default) is cross-major safe; 'binary' is used only when source and target server majors match (downgrades to text loudly otherwise); 'auto' requests binary. The lane engages ONLY for a no-transform copy (no --redact / --type-override / --expr-override / --inject-shard-column). Inert on MySQL/VStream sources (serial cold-start)." default:"text" enum:"text,binary,auto" placeholder:"text|binary|auto"`

	MaxTargetConnections int `help:"Explicit ceiling on the target connection budget (connection-resilience item 4). On cold-start, sluice probes the target's connection-slot budget (Postgres max_connections / role / database limits minus in-use and a small reserve) and refuses loudly if no slot is free for the copy + CDC connections. 0 (default) = auto (probe-and-refuse-on-exhaustion, no operator ceiling). On the ADR-0079 FAST cold-start (PG source) it also bounds the cross-table × within-table copy + index-build connection product (plus the reserved CDC slot); on the serial cold-start it's the loud-refusal floor plus an explicit ceiling. Inert against engines without a connection-slot model (MySQL target)." default:"0" placeholder:"N"`

	ReapStaleBackends bool `help:"Terminate sluice's OWN orphaned backends on the target during the cold-start preflight (connection-resilience Phase 2, item 2). Detection runs ALWAYS and reports loudly; this flag authorises pg_terminate_backend on each orphan. An orphan is a backend whose application_name carries the 'sluice/' prefix, owned by the connecting role, NOT the current session, and either idle-in-transaction or holding a lock on a relation sluice is about to write — typically a SIGKILL'd / OOM'd prior run whose server-side COPY backend still holds a target-table lock and a connection slot. Default off — detect-and-report is the safe baseline, because a legitimately-running concurrent sluice process on the same target is a real possibility (the report is shown first so you can tell them apart). Termination is always scoped to your own sluice backends; it never touches another role's or a non-sluice session, and needs no superuser grant. Inert against engines without a backend model (MySQL target)."`

	ApplyExecTimeout time.Duration `help:"Per-statement deadline applied to every tx.ExecContext on the apply path. GitHub #23 Phase B fix (v0.52.0): closes the silent-stall failure mode where a half-closed destination connection blocked the apply goroutine indefinitely inside the driver's TLS read path. On expiry the driver returns context.DeadlineExceeded, which is classified retriable so the runWithRetry loop reopens the applier and retries the batch on a fresh connection. 0 disables (the pre-v0.52.0 behaviour: unbounded). Tune up for legitimately slow batch upserts on slow targets; down for tighter stall detection." default:"60s" placeholder:"DUR"`

	ApplyConcurrency int `help:"MySQL or Postgres target (ADR-0104 / ADR-0105, item 23(c)/26): the key-hash apply LANE count W. The merged CDC change stream is fanned across W in-order apply lanes by primary-key hash (same key → same lane → applied in source order, so dependent INSERT→UPDATE→DELETE on a row never reorder), each lane committing CONCURRENTLY on a dedicated backend with its OWN AIMD batch-size controller. On a high-latency cross-region link a serial applier is RTT-bound and falls below the source write rate, causing the per-shard MinimizeSkew wedge; concurrent lanes lift aggregate apply throughput toward W× and keep both shards drained (live-validated ~4× on a 2-shard Vitess→PlanetScale-MySQL link). The resume position advances only to a source boundary durable across ALL lanes (exactly-once for keyed tables; keyless stays at-least-once). An in-lane abort that the target may transiently throw — a PlanetScale transaction-killer (MySQL) or a serialization/deadlock (Postgres) — is handled IN-LANE: the lane's controller shrinks and the batch is split-and-retried idempotently without restarting the stream. ADR-0106 (FAST BY DEFAULT): 0 (the default, unset) = auto:N — an adaptive, connection-budget-bounded lane count: Postgres min(4, slot-budget) via the same probe --max-target-connections drives, MySQL/PlanetScale a fixed conservative ceiling of 4 (no slot probe, --max-target-connections inert there). 1 = the explicit SERIAL opt-out (byte-identical to the pre-ADR-0106 default, for operators who want strictly serial apply). W>1 honored verbatim (you own your target's budget). The auto value matches the cold-copy axes' auto:4 so the whole pipeline fans out ~4-wide by default, bounded by the target. Keep --apply-batch-size at a sane value (the default is fine): an absurdly high ceiling can make the controller lag on an abort-heavy target (safe — no data loss — but slow). On Postgres this composes with (does not replace) the ADR-0092 statement pipelining used within each lane's transaction." default:"0" placeholder:"W"`

	// ADR-0118 finding 2: cross-add backup's --retry-* spelling as an alias
	// (identical concept + defaults, 8/100ms/30s). The primary shown in
	// --help stays the sync-stream's --apply-retry-* name. Additive —
	// no existing command line changes behaviour.
	ApplyRetryAttempts    int           `aliases:"retry-attempts" help:"Maximum consecutive retriable apply failures the streamer absorbs before exiting. ADR-0038. 1 = no retry (exit on first transient — pre-v0.42.0 behaviour). 8 = default for managed-Vitess / Vitess-flavoured MySQL where tx-killer transients are routine. Counter resets when persisted CDC position advances between attempts; a streamer surviving for hours doesn't carry retry debt. Alias: --retry-attempts (the backup-stream spelling)." default:"8" placeholder:"N"`
	ApplyRetryBackoffBase time.Duration `aliases:"retry-backoff-base" help:"Base interval for the exponential backoff between retriable apply failures. ADR-0038. Doubles on each consecutive failure, capped at --apply-retry-backoff-cap. Only consulted when --apply-retry-attempts > 1. Alias: --retry-backoff-base (the backup-stream spelling)." default:"100ms" placeholder:"DUR"`
	ApplyRetryBackoffCap  time.Duration `aliases:"retry-backoff-cap" help:"Upper bound on each per-attempt backoff interval. ADR-0038. Defaults to 30s. With 8 attempts and default base, the per-attempt sequence is: 100ms → 200ms → 400ms → 800ms → 1.6s → 3.2s → 6.4s → 12.8s, capped at the cap when it grows past. Alias: --retry-backoff-cap (the backup-stream spelling)." default:"30s" placeholder:"DUR"`

	MetricsListen string `help:"Bind a Prometheus-format /metrics endpoint at this address (e.g. ':9090' for all interfaces port 9090, '127.0.0.1:9090' for localhost only) for the duration of the stream. Off by default — opt-in. Companion to 'sluice sync health' (which is the cron-friendly one-shot probe shape). Useful for operators running Prometheus / Grafana / alertmanager." placeholder:"ADDR"`

	// ADR-0107 Phase 2: OPTIONAL PlanetScale target-health telemetry. When
	// the operator supplies the org + a read_metrics_endpoints service token,
	// sluice polls the PlanetScale metrics endpoint (CPU/mem/storage, plus
	// secondary lag/conns) off the apply hot path and feeds the Phase-1
	// advisory consumers (proactive AIMD back-off, storage-resize WARN, the
	// sluice_target_* gauges). CONTROL-PLANE credential, distinct from the
	// data-plane DSN. All-or-nothing: org without a complete token pair is a
	// loud refusal. Unset ⇒ no provider wired ⇒ byte-identical default sync.
	PlanetScaleOrg            string `name:"planetscale-org" help:"PlanetScale org slug; enables OPTIONAL target-health telemetry (CPU/mem/storage) from the PlanetScale metrics endpoint for proactive apply back-off + in-tool observability (ADR-0107). Opt-in; requires --planetscale-metrics-token-id and --planetscale-metrics-token. Control-plane only — distinct from the data-plane --target DSN. Off when unset (default sync unchanged)." placeholder:"ORG"`
	PlanetScaleMetricsTokenID string `name:"planetscale-metrics-token-id" help:"PlanetScale service-token ID (granted the read_metrics_endpoints permission) for --planetscale-org target-health telemetry. Prefer the env var so the id never lands in shell history." env:"PLANETSCALE_METRICS_TOKEN_ID" placeholder:"ID"`
	PlanetScaleMetricsToken   string `name:"planetscale-metrics-token" help:"PlanetScale service-token secret for --planetscale-org target-health telemetry. Set via the env var (never on the command line); masked in all logging." env:"PLANETSCALE_METRICS_TOKEN" placeholder:"SECRET"`
	PlanetScaleMetricsBranch  string `name:"planetscale-metrics-branch" help:"Target branch to filter telemetry series to (defaults to 'main'). Only consulted when --planetscale-org is set." placeholder:"BRANCH"`
	PlanetScaleMetricsDB      string `name:"planetscale-metrics-db" help:"Target database name to filter PlanetScale telemetry SD to. Defaults to the --target DSN's database. Only consulted when --planetscale-org is set." placeholder:"DATABASE"`

	SuppressTargetMetricsHistory bool `help:"Disable persisting polled PlanetScale target-health metrics to the sluice_target_metrics_history table on the target (ADR-0107 item 35). Only relevant when --planetscale-org telemetry is configured; recording is on by default then. The rolling history lets 'sluice diagnose' show the recent CPU/mem/storage/lag/conn trend without scripting the metrics API; the table is bounded (7-day retention, pruned). Recording is advisory and failure-isolated — it never affects the sync."`

	// ADR-0107 item 36 — sync-scoped target-metrics threshold ALERTER. Opt-in,
	// only active with --planetscale-org telemetry; advisory (never affects the
	// sync); credential-gated (sink URLs via env); failure-isolated (a dead sink
	// is logged-and-swallowed). A rule with threshold 0 is inert.
	NotifyWebhook             string        `help:"Generic webhook URL to POST target-metrics threshold alerts to as JSON (ADR-0107 item 36). Opt-in; only active with --planetscale-org telemetry AND at least one --notify-*-util/--notify-lag-seconds/--notify-storage-growth-per-min threshold set. ADVISORY — never affects the sync; a dead sink is logged-and-swallowed. A credential (set via the env var, not the command line)." env:"SLUICE_NOTIFY_WEBHOOK" placeholder:"URL"`
	NotifySlack               string        `help:"Slack incoming-webhook URL to POST target-metrics threshold alerts to (ADR-0107 item 36). Same gating + advisory + failure-isolated semantics as --notify-webhook. A credential (set via the env var)." env:"SLUICE_NOTIFY_SLACK" placeholder:"URL"`
	NotifyStorageUtil         float64       `help:"Alert when the target's storage utilisation (used/capacity, 0-1) is at or above this fraction (ADR-0107 item 36). 0 (default) disables the rule. Edge-triggered + cooldown'd. Requires --planetscale-org telemetry + a --notify-webhook/--notify-slack sink." placeholder:"FRAC"`
	NotifyCPUUtil             float64       `help:"Alert when the target's CPU utilisation (0-1) is at or above this fraction (ADR-0107 item 36). 0 disables. Same gating as --notify-storage-util." placeholder:"FRAC"`
	NotifyMemUtil             float64       `help:"Alert when the target's memory utilisation (0-1) is at or above this fraction (ADR-0107 item 36). 0 disables. Same gating as --notify-storage-util." placeholder:"FRAC"`
	NotifyLagSeconds          float64       `help:"Alert when the target's replica lag (seconds) is at or above this value (ADR-0107 item 36). 0 disables. Same gating as --notify-storage-util." placeholder:"SECONDS"`
	NotifyStorageGrowthPerMin float64       `help:"Alert when the target's storage utilisation is CLIMBING at or above this fraction-of-capacity per minute (ADR-0107 item 36) — a pre-grow early warning. e.g. 0.02 = +2%/min. 0 disables. Same gating as --notify-storage-util." placeholder:"FRAC_PER_MIN"`
	NotifyCooldown            time.Duration `help:"Minimum interval between re-fires of a STILL-breached target-metrics alert (ADR-0107 item 36). A sustained breach reminds at most once per this interval rather than every poll. Default 15m." default:"15m" placeholder:"DUR"`
	NotifySyncLagSeconds      float64       `help:"Alert when sluice's OWN sync lag — seconds the target trails the source's latest applied commit (sluice_sync_lag_seconds, roadmap item 45) — is at or above this value. 0 (default) disables. UNGATED from PlanetScale telemetry: works on MySQL and Postgres alike, needing only a --notify-webhook/--notify-slack sink (NOT --planetscale-org). Distinct from --notify-lag-seconds, which is the PlanetScale control-plane TARGET-INTERNAL replica lag. Edge-triggered + cooldown'd; advisory + failure-isolated." placeholder:"SECONDS"`

	HeartbeatInterval time.Duration `help:"Wall-clock cadence the per-stream heartbeat goroutine logs an INFO 'stream: heartbeat' line at. GitHub #23 Phase A: distinguishes silent-stall (process alive but no apply, no log) from wedge (process alive, no heartbeat either). 0 disables." default:"60s" placeholder:"DUR"`

	PollInterval time.Duration `help:"Override the CDC reader's poll cadence for poll-based engines (today: postgres-trigger; default 1s). Push-based engines (postgres pgoutput, mysql binlog, planetscale VStream) silently ignore — they have no poll loop. Operators chasing lower CDC latency on a write-heavy postgres-trigger stream tighten this to e.g. 250ms; operators trading latency for source load loosen to 5s. 0 (the default) keeps the engine's built-in cadence. ADR-0066 §6; roadmap item 18(c)." placeholder:"DUR"`

	SourceHeartbeatInterval    time.Duration `help:"ADR-0061 / F17 — enable the source-side heartbeat writer at this cadence. Sluice INSERTs a row into a sluice-owned table on the source every interval; the INSERT generates WAL (Postgres) / binlog (MySQL) so the CDC consumer's position advances even against an idle source, preventing slot eviction / binlog rotation past the consumer. 0 (default) disables — F17 is opt-in because the INSERT is a behaviour change on the source DB that operators on regulated systems must explicitly enable. Typical value 30s. The source-side table (default 'sluice_heartbeat') is auto-created; on roles without CREATE TABLE privilege the streamer WARNs once and continues without F17." default:"0s" placeholder:"DUR"`
	SourceHeartbeatPruneWindow time.Duration `help:"ADR-0061 / F17 — age threshold for the periodic DELETE that bounds heartbeat-table growth. Rows older than this duration are dropped on a periodic prune pass. 0 disables prune (table grows unbounded). Only consulted when --source-heartbeat-interval > 0." default:"1h" placeholder:"DUR"`
	SourceHeartbeatTableName   string        `help:"ADR-0061 / F17 — override the source-side heartbeat table name. Default 'sluice_heartbeat'. Operators with hostile DBA-managed namespaces can pre-create a differently-named table and point the writer at it. Only consulted when --source-heartbeat-interval > 0." default:"sluice_heartbeat" placeholder:"NAME"`
	NoSourceHeartbeat          bool          `help:"ADR-0061 / F17 — opt-out escape hatch. When set, the source-side heartbeat writer is skipped even if --source-heartbeat-interval > 0 (e.g. CLI override of YAML-configured interval). Useful on managed DBs / read-replicas where DDL is restricted; the per-permission-error WARN-once-skip path covers the same case automatically, but --no-source-heartbeat silences the warning at the source."`

	PositionFromManifest string `help:"URL of a backup chain (s3://bucket/prefix, gs://, azblob://, file:///path) whose terminal manifest's EndPosition is used as this stream's resume position. Use after 'sluice restore --from=<chain-url>' to resume CDC from the chain's tail without re-bulking. Mutually exclusive with the implicit 'resume from sluice_cdc_state' path: when set, the persisted position is bypassed and the chain's terminal becomes the source of truth. PG soft warnings (wal_keep_size, Patroni) fire as pre-flight checks; --strict-preflight promotes them to refusals. See docs/dev/design/logical-backups-phase-3.md." placeholder:"URL"`

	StrictPreflight bool `help:"Promote position-from-manifest soft warnings (wal_keep_size sufficiency, Patroni-managed source detection) to hard refusals. Default off: the warnings log but the run proceeds. Slot existence / wal_status='lost' is always a refusal regardless of this flag — the slot can't deliver what we need."`

	PatroniMode string `help:"Control the Patroni / HA-managed source detection. 'auto' (default) runs the engine heuristics + DSN hostname pattern check and warns if any signal fires; 'on' skips the heuristics and forces the warning (operator opts in regardless of detection — useful on tenant-isolated managed PG where the heuristics miss); 'off' skips the heuristics and suppresses the Patroni warning entirely (operator confirmed self-hosted single-node PG without HA). Combine with --strict-preflight=true and --patroni-mode=on to make the warning a hard refusal." default:"auto" enum:"auto,on,off" placeholder:"MODE"`

	BackupEndpoint  string `help:"Override the S3 endpoint for --position-from-manifest's S3-compatible providers. Only meaningful when --position-from-manifest is an s3:// URL." placeholder:"URL"`
	BackupRegion    string `help:"Override the S3 region for --position-from-manifest. Only meaningful when --position-from-manifest is an s3:// URL." placeholder:"REGION"`
	BackupPathStyle bool   `help:"Force path-style S3 addressing for --position-from-manifest. Only meaningful when --position-from-manifest is an s3:// URL."`

	TargetSchema string `help:"Per-source target schema namespace (Postgres-only). When set, every emitted CREATE TABLE / ALTER TABLE / CREATE INDEX / CREATE TYPE prefixes the table reference with this schema, and CDC events apply against the named schema. Use to land multiple concurrent sluice streams on the same target without table-name collisions (Shape B microservices → analytics warehouse, ADR-0031). The schema is auto-created on the target if it doesn't exist. The control table sluice_cdc_state stays in the DSN's default schema regardless — multiple target-schema streams share a single state table per target. MySQL operators use a different --target DSN database instead — schemas and databases collapse on MySQL." placeholder:"NAME"`

	EnablePGExtension []string `help:"Enable passthrough for a Postgres extension type (repeatable). Same-engine PG → PG preserves the source-native shape. Cross-engine targets (MySQL) keep the loud-failure default except for hstore (→ JSON) and citext (→ VARCHAR with case-insensitive collation). Sluice preflights extension presence on both source and target. Recognised: vector (pgvector), pg_trgm, hstore, citext. See ADR-0032." placeholder:"EXT"`

	InjectShardColumn string `help:"ADR-0048 Shape A — inject a sluice-managed discriminator column on the consolidated target (Format: NAME=VALUE). Each per-shard sync stream passes a distinct VALUE so per-shard rows land disjoint on the shared target via a composite PK. Sluice appends the column to every PK-bearing table, rewrites the PK to be composite, stamps VALUE onto every row (bulk-copy + CDC), and runs a three-point loud preflight on a non-empty target. See 'sluice migrate --inject-shard-column' help text and ADR-0048 for the full design." placeholder:"NAME=VALUE"`

	AllowCrossShardMerge bool `help:"Opt out of the cross-shard-collision preflight (Bug 152). By default, when the source is a multi-shard Vitess/PlanetScale keyspace (vtgate merges every shard into one logical stream) and --inject-shard-column is NOT set, sluice REFUSES to sync into a single target table that has a PK or UNIQUE — rows from different shards sharing a key value would silently overwrite each other. Pass this ONLY if the key is globally unique across shards (e.g. Vitess sequences / UUID keys). The structural alternative is --inject-shard-column NAME=VALUE (ADR-0048). No effect on single-shard / non-sharded sources or when --inject-shard-column is set."`

	NoCoordinateLiveDDL bool `help:"ADR-0054 Shape A Phase 2 — disable live cross-shard DDL coordination. Default is ENABLED when --inject-shard-column is set (live coordination: one shard acquires a lease, applies the DDL on the consolidated target, records the schema version + DDL checksum; peer shards verify the checksum, skip the apply, continue CDC against the migrated target). Pass --no-coordinate-live-ddl to keep the pre-v0.73 drained model (operator runs 'sync stop --wait' on every shard, runs schema migrate once, then 'sync start --resume' on every shard). A no-op when --inject-shard-column is unset."`

	SchemaChanges string `help:"ADR-0091 — online forwarding of source DDL through the live CDC apply path (single-stream / non-Shape-A). 'forward' (DEFAULT) applies every unambiguous source schema change on the target — ADD/DROP COLUMN, ALTER COLUMN TYPE, ALTER NULLABILITY, CREATE/DROP INDEX, ADD/DROP/MODIFY CHECK — logging each applied DDL at INFO, so the sync stays online through routine schema evolution. 'refuse' restores the conservative pre-v0.92 behavior: any source DDL surfaces loudly with the drained-model recovery hint (for operators who gate DDL through a separate change-management process). RENAME COLUMN and multi-shape combos always refuse loudly (rename is indistinguishable from drop+add without a stable column id — forwarding the wrong guess risks silent data loss); a computed/volatile DEFAULT on ADD COLUMN also refuses (ADR-0058 §2a). No-op when --inject-shard-column is set (Shape A already forwards every shape via the lease, ADR-0054). NOTE: 'forward' is a behavior change vs pre-v0.92 — a stream that previously refused on source DDL now forwards it." default:"forward" enum:"forward,refuse" placeholder:"MODE"`

	ForwardSchemaAddColumn bool `help:"DEPRECATED (ADR-0091) — use --schema-changes instead. Online schema-change forwarding is now ON by default and covers every unambiguous shape, so this ADD-COLUMN-only opt-in is subsumed. Setting it logs a deprecation warning and forwards (same as the default). To restore loud-refuse-on-DDL, set --schema-changes=refuse. Removed in a future release."`

	BackfillAddedColumn bool `help:"ADR-0058 §1c — opt-in source-side bounded backfill of already-shipped target rows after a forwarded ADD COLUMN lands. Default OFF (existing target rows carry the column's DEFAULT, NULL if none). When set, the streamer issues a bounded PK-cursor SELECT (pk, new_col) against the source and emits synthetic UPDATE events to populate the new column with per-row source values. Only consulted when --forward-schema-add-column is also set. Large tables: backfill cost is proportional to the table's row count on the source — operators must opt in knowingly."`

	ShardCoordinationLeaseDuration time.Duration `help:"ADR-0054 §2 / DP-A lease TTL. The lease-holder writes lease_expires_at = now + this value on every heartbeat. A stalled holder loses the lease after this window; the takeover stream runs probe-and-record. Default 30s (Kubernetes leader-election relaxed for sluice's stream-pause failure mode). Operators running ALTERs on tables >100GB may want 300s to absorb the longer ALTER window. Only consulted when --inject-shard-column is set and --no-coordinate-live-ddl is absent." default:"30s" placeholder:"DUR"`
	ShardCoordinationRenewDeadline time.Duration `help:"ADR-0054 §2 / DP-A lease renew deadline. A lease-holder considers itself failed if it can't write a heartbeat within this window and exits the apply path. Must be > --shard-coordination-retry-period and < --shard-coordination-lease-duration. Default 20s." default:"20s" placeholder:"DUR"`
	ShardCoordinationRetryPeriod   time.Duration `help:"ADR-0054 §2 / DP-A lease heartbeat cadence. The lease-holder writes lease_expires_at = now + LeaseDuration at this interval. Must be > 0 and < --shard-coordination-renew-deadline. Default 10s." default:"10s" placeholder:"DUR"`

	Redact       []string `help:"Redact a PII column (repeatable). Format: '[schema.]table.column=STRATEGY[:options]'. Strategies: null (NULLABLE columns only), static:<value>, hash:sha256, hash:hmac-sha256[:<keyname>] (requires --keyset-source), truncate:<n>, mask:inner:<m1>,<m2>[,<char>], mask:outer:<m1>,<m2>[,<char>], mask:ssn, mask:pan, mask:pan-relaxed, mask:email, mask:ca-sin, mask:uk-nin, mask:iban, mask:uuid (Phase 2.b country/format presets, v0.57.0+), randomize:int:<min>,<max>, randomize:email, randomize:us-phone, randomize:uuid (Phase 2.c first wave, v0.59.0+), randomize:ssn, randomize:pan[:<brand>], randomize:ca-sin, randomize:uk-nin, randomize:iban[:<country-code>] (Phase 2.c second wave, v0.60.0+; brand: visa|mastercard|amex; country: DE|GB|FR; all randomize:* require a PK on the source table), randomize:dict:<name>, tokenize:dict:<name>[:<keyname>] (Phase 3 v0.61.0+, keyset-sourced in Phase 4 v0.62.0+; dictionaries declared in YAML 'dictionaries:' block — CLI form REQUIRES YAML config to declare the dictionary content). Examples: --redact users.email=hash:sha256, --redact users.pan=mask:pan, --redact users.id=mask:uuid, --redact users.age=randomize:int:18,90, --redact users.first_name=tokenize:dict:first_names. Phase 1.5 (v0.54.0+): redaction covers BOTH cold-start bulk-copy AND mid-stream CDC events. Bare 'users.email' matches any source schema; schema-qualified 'public.users.email' takes precedence when both registered. See docs/dev/notes/prep-pii-redaction-phase-1.md." placeholder:"RULE" sep:"none"`
	KeysetSource string   `help:"Operator keyset source for keyset-using redaction strategies (hash:hmac-sha256, tokenize:dict). PII Phase 4 (ADR-0041). Forms: 'file:PATH' (keyset YAML on disk), 'env:VARNAME' (keyset YAML in an env var), 'db:DSN' (sluice_keysets table on the named DSN — shared across streams for cross-stream surrogate stability). Resolved ONCE at startup; rotation takes effect on next process restart only (no hot-reload). Required when any --redact / YAML rule uses hash:hmac-sha256 or tokenize:dict — the Phase 1 --redact-key-source flag and the built-in v0.61.0 tokenize key were removed." placeholder:"SRC"`

	CrashHookFlags
}

// Run implements `sluice sync start`.
// Retry-dial bounds from ADR-0038's Configuration table, pinned by
// the Operator-review sign-off (pin-down 3). Kept as named constants
// so the bound is greppable and a single source of truth shared by
// the validator and its test.
// Lo/Hi rather than Min/Max: revive's time-naming rule reads a
// "Min"/"Max" suffix on a time.Duration as a unit-specific name.
const (
	retryAttemptsLo    = 1
	retryAttemptsHi    = 64
	retryBackoffBaseLo = 10 * time.Millisecond
	retryBackoffBaseHi = 10 * time.Second
	retryBackoffCapLo  = 1 * time.Second
	retryBackoffCapHi  = 300 * time.Second
)

// resolveApplyBatchSize resolves the --apply-batch-size flag value
// (string form: a non-negative integer OR the sentinel "auto") to a
// pipeline.Streamer.ApplyBatchSize int. The "auto" sentinel maps to
// the engine-default ceiling per ADR-0052 (planetscale=100,
// mysql/postgres=1000). The numeric form is parsed verbatim. Returns
// a clear error on unparseable input so the operator gets a precise
// error rather than a kong parse-error generic.
func resolveApplyBatchSize(raw string, target ir.Engine) (int, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 1, nil
	}
	if strings.EqualFold(trimmed, "auto") {
		// ADR-0052 DP-1 engine-default ceiling.
		name := ""
		if target != nil {
			name = target.Name()
		}
		switch name {
		case "planetscale":
			return 100, nil
		default:
			return 1000, nil
		}
	}
	n, err := strconv.Atoi(trimmed)
	if err != nil {
		return 0, fmt.Errorf("expected a non-negative integer or 'auto'; got %q", raw)
	}
	if n < 0 {
		return 0, fmt.Errorf("expected a non-negative integer or 'auto'; got %d", n)
	}
	return n, nil
}

// buildTargetTelemetry constructs the OPTIONAL PlanetScale target-health
// telemetry provider (ADR-0107 Phase 2) from the operator's opt-in flags,
// or returns (nil, nil) when telemetry is OFF (no --planetscale-org). The
// opt-in is ALL-OR-NOTHING: setting --planetscale-org without a complete
// service-token pair is a LOUD refusal (the contain-PS-complexity tenet —
// a half-configured control-plane capability never half-runs silently).
//
// The provider is constructed here at the composition root (the sole place
// allowed to import the PS provider package) and threaded onto the streamer
// as the engine-neutral ir.TargetTelemetry. The provider starts its own
// background poll loop scoped to ctx; the caller defers Close.
//
// The token NEVER appears in any error or log line emitted here — only the
// org/database/branch identifiers, which are not secret.
// telemetryParams is the engine-neutral input to [buildTargetTelemetryProvider],
// gathered from a subcommand's --planetscale-* flags + the target DSN/driver.
// Sharing it lets both `sync start` and `diagnose` construct the same provider
// without duplicating the opt-in / all-or-nothing validation.
type telemetryParams struct {
	org       string
	tokenID   string
	token     string
	metricsDB string
	branch    string
	targetDSN string
	engine    string // target engine registry name (selects the metric-name table)
}

func buildTargetTelemetry(ctx context.Context, s *SyncStartCmd) (*pstelemetry.Provider, error) {
	return buildTargetTelemetryProvider(ctx, telemetryParams{
		org:       s.PlanetScaleOrg,
		tokenID:   s.PlanetScaleMetricsTokenID,
		token:     s.PlanetScaleMetricsToken,
		metricsDB: s.PlanetScaleMetricsDB,
		branch:    s.PlanetScaleMetricsBranch,
		targetDSN: s.Target,
		engine:    s.TargetDriver,
	})
}

// buildTargetTelemetryProvider constructs the optional PlanetScale telemetry
// provider (ADR-0107) from the gathered params, or returns (nil, nil) when
// telemetry is off (no --planetscale-org). Opt-in is all-or-nothing: an org
// without a complete token pair is a loud refusal.
func buildTargetTelemetryProvider(ctx context.Context, p telemetryParams) (*pstelemetry.Provider, error) {
	if p.org == "" {
		// Telemetry off (the default): no provider, no behaviour change.
		if p.tokenID != "" || p.token != "" {
			// Token supplied without --planetscale-org: nothing consumes it.
			// Warn rather than refuse — the operator may have set the env var
			// globally; refusing would block every non-PS sync on that shell.
			slog.WarnContext(ctx,
				"PlanetScale metrics service token is set but --planetscale-org is not; target-health telemetry is OFF (set --planetscale-org to enable)")
		}
		return nil, nil //nolint:nilnil // (nil, nil) == "telemetry off", a valid no-op result distinct from an error
	}
	if p.tokenID == "" || p.token == "" {
		return nil, errors.New(
			"--planetscale-org is set but the metrics service token is incomplete: supply BOTH --planetscale-metrics-token-id and --planetscale-metrics-token (env PLANETSCALE_METRICS_TOKEN_ID / PLANETSCALE_METRICS_TOKEN). Telemetry is opt-in and all-or-nothing — it never half-runs",
		)
	}

	database := p.metricsDB
	if database == "" {
		database = databaseFromDSN(p.targetDSN)
	}
	if database == "" {
		return nil, errors.New(
			"--planetscale-org telemetry: could not determine the target database name from the --target DSN; supply --planetscale-metrics-database explicitly",
		)
	}

	provider, err := pstelemetry.New(ctx, pstelemetry.Config{
		Org:      p.org,
		TokenID:  p.tokenID,
		Token:    p.token,
		Database: database,
		Branch:   p.branch,
		// Engine selects the per-engine metric-name table (ADR-0107 Phase 3):
		// a Postgres target reads `planetscale_volume_*` / `planetscale_postgres_*`
		// rather than the Vitess `vttablet_*` names. The raw driver name is the
		// registry key ("mysql"/"planetscale"/"vitess"/"postgres"/…); the
		// provider maps it.
		Engine: p.engine,
	})
	if err != nil {
		return nil, fmt.Errorf("--planetscale-org telemetry: %w", err)
	}
	slog.InfoContext(
		ctx,
		"PlanetScale target-health telemetry enabled (ADR-0107) — advisory only; apply correctness is unaffected",
		slog.String("org", p.org),
		slog.String("database", database),
		slog.String("branch", branchOrMainLabel(p.branch)),
	)
	return provider, nil
}

// telemetryProviderOrNil converts a possibly-nil *Provider into the
// streamer's ir.TargetTelemetry field WITHOUT the typed-nil interface trap:
// assigning a nil *Provider straight to an interface yields a NON-nil
// interface (concrete type, nil value) that the streamer's
// `TargetTelemetry != nil` guards would wrongly fire on, then nil-deref.
// Returning a true nil interface keeps "no provider ⇒ no telemetry" exact.
func telemetryProviderOrNil(p *pstelemetry.Provider) ir.TargetTelemetry {
	if p == nil {
		return nil
	}
	return p
}

// branchOrMainLabel is the log-label form of the telemetry branch: the
// configured value, or "main" when unset (matching the provider's default).
func branchOrMainLabel(branch string) string {
	if branch == "" {
		return "main"
	}
	return branch
}

// databaseFromDSN extracts the database name from a target DSN for the
// PlanetScale telemetry SD filter, best-effort across the two DSN shapes
// sluice's engines accept:
//
//   - Go-MySQL DSN: "user:pass@tcp(host:port)/dbname?params" — the path
//     segment after the last '/' (before any '?').
//   - URL DSN (postgres://…/dbname, mysql://…/dbname): the URL path.
//
// Returns "" when no database segment is present (the caller then refuses
// loudly and asks for --planetscale-metrics-database). The DSN may contain
// a password; this function returns ONLY the database segment, never echoes
// the DSN.
func databaseFromDSN(dsn string) string {
	if dsn == "" {
		return ""
	}
	// URL form (postgres://…/db, mysql://…/db): parse and take the path
	// segment, so the scheme's "//" and the host:port are never mistaken for
	// a database. A URL with no path (e.g. "postgres://host") yields "".
	if strings.Contains(dsn, "://") {
		if u, err := url.Parse(dsn); err == nil && u.Scheme != "" {
			return strings.Trim(u.Path, "/")
		}
		return ""
	}
	// Go-MySQL form ("user:pass@tcp(host:port)/db?params"): strip the query
	// string, then take the final path segment after the last '/'.
	if i := strings.IndexByte(dsn, '?'); i >= 0 {
		dsn = dsn[:i]
	}
	slash := strings.LastIndexByte(dsn, '/')
	if slash < 0 || slash == len(dsn)-1 {
		return ""
	}
	db := dsn[slash+1:]
	// Reject a segment that still looks like a host:port or carries '@' — a
	// malformed DSN must yield "" (loud refusal upstream), not a bogus name.
	if strings.ContainsAny(db, "@:") {
		return ""
	}
	return db
}

// parseIndexBuildMem turns the --index-build-mem flag value into a byte
// count for the PG index-build tuner. "auto" / "" → 0 (the auto
// sentinel: the writer derives maintenance_work_mem from a pg_settings
// probe). Otherwise a human size ("512MB", "2GB") or raw byte count is
// parsed via units.RAMInBytes (power-of-two units, case-insensitive,
// optional 'b'). Negative or unparseable input is a loud error — better
// than silently disabling the tuning.
func parseIndexBuildMem(raw string) (int64, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || strings.EqualFold(trimmed, "auto") {
		return 0, nil
	}
	n, err := units.RAMInBytes(trimmed)
	if err != nil {
		return 0, fmt.Errorf("--index-build-mem: expected a size ('512MB', '2GB') or 'auto'; got %q", raw)
	}
	if n < 0 {
		return 0, fmt.Errorf("--index-build-mem: expected a non-negative size; got %q", raw)
	}
	return n, nil
}

// parseRawCopyFormat maps the --raw-copy-format flag to the IR request
// constant (ADR-0078). kong's enum tag has already constrained raw to
// {text,binary,auto}, so this is a total map; "auto" requests binary as
// the intent and lets the orchestrator's version probe decide the actual
// wire format. Any unexpected value falls back to text — the always-safe
// default.
func parseRawCopyFormat(raw string) ir.RawCopyFormat {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "binary", "auto":
		return ir.RawCopyBinary
	default:
		return ir.RawCopyText
	}
}

// parseMaxMemory turns the --max-memory flag value into a byte count
// for runtime/debug.SetMemoryLimit. Empty / "off" → 0 (the OFF
// sentinel: SetMemoryLimit is not called, so Go honors the GOMEMLIMIT
// env var natively). Otherwise a human size ("2GiB", "512MiB", "2GB")
// or raw byte count is parsed via units.RAMInBytes (power-of-two
// units, case-insensitive, optional 'b'). A zero, negative, or
// unparseable size is a loud error rather than a silent no-op — the
// operator asked for a ceiling and a typo shouldn't drop it.
func parseMaxMemory(raw string) (int64, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || strings.EqualFold(trimmed, "off") {
		return 0, nil
	}
	n, err := units.RAMInBytes(trimmed)
	if err != nil {
		return 0, fmt.Errorf("--max-memory: expected a size ('2GiB', '512MiB', '2GB') or 'off'; got %q", raw)
	}
	if n <= 0 {
		return 0, fmt.Errorf("--max-memory: expected a positive size; got %q", raw)
	}
	return n, nil
}

// validateRetryFlags enforces the ADR-0038 pin-down-3 ranges on the
// three --apply-retry-* dials. Returns a precise error naming the
// flag, the offending value, and the allowed range so the operator
// can correct it without consulting the docs.
func validateRetryFlags(attempts int, base, capDur time.Duration) error {
	if attempts < retryAttemptsLo || attempts > retryAttemptsHi {
		return fmt.Errorf("--apply-retry-attempts=%d out of range; ADR-0038 allows %d–%d (1 = no retry)",
			attempts, retryAttemptsLo, retryAttemptsHi)
	}
	if base < retryBackoffBaseLo || base > retryBackoffBaseHi {
		return fmt.Errorf("--apply-retry-backoff-base=%s out of range; ADR-0038 allows %s–%s",
			base, retryBackoffBaseLo, retryBackoffBaseHi)
	}
	if capDur < retryBackoffCapLo || capDur > retryBackoffCapHi {
		return fmt.Errorf("--apply-retry-backoff-cap=%s out of range; ADR-0038 allows %s–%s",
			capDur, retryBackoffCapLo, retryBackoffCapHi)
	}
	return nil
}

// validateFlagCombos rejects mutually-exclusive sync-start flag combinations.
// It is intentionally pure (no I/O) and called from Run BEFORE the destructive
// --reset-target-data confirmation prompt, so an invalid combination fails loud
// up front rather than after asking the operator to authorize a target-table
// DROP the command would then refuse to perform.
func (s *SyncStartCmd) validateFlagCombos() error {
	if s.RestartFromScratch && s.ResetTargetData {
		return errors.New("--restart-from-scratch and --reset-target-data are mutually exclusive (--reset-target-data already forces a fresh cold-start, and additionally drops the target)")
	}
	if s.RestartFromScratch && s.PositionFromManifest != "" {
		return errors.New("--restart-from-scratch and --position-from-manifest are mutually exclusive (one discards the position, the other supplies one)")
	}
	if s.PositionFromManifest != "" && s.ResetTargetData {
		return errors.New("--position-from-manifest and --reset-target-data are mutually exclusive")
	}
	return nil
}

//nolint:funlen // ratchet: pre-existing 212-line accretion; split when next touched (hold-the-line note in .golangci.yml)
func (s *SyncStartCmd) Run(g *Globals) error {
	cfg, err := config.Load(g.Config)
	if err != nil {
		return err
	}

	source, err := resolveEngine(s.SourceDriver)
	if err != nil {
		return fmt.Errorf("--source-driver: %w", err)
	}
	target, err := resolveEngine(s.TargetDriver)
	if err != nil {
		return fmt.Errorf("--target-driver: %w", err)
	}

	// ADR-0118 finding 1(b): the FAST-cold-start parallelism flags are inert
	// on a MySQL/VStream source (serial cold-start). If the operator set one
	// EXPLICITLY against such a source, turn the silent no-op into a one-time
	// loud WARN (detection is by the literal argv spelling, not the resolved
	// value — see warnInertParallelismFlags).
	warnInertParallelismFlags(kongContext(), source)

	// ADR-0118 finding 4: thread the explicit read-axis CLI flags into the
	// mysql engine at the composition root (the same idiom as
	// --mysql-sql-mode / --zero-date). A value > 0 wins over the source DSN's
	// vstream_copy_table_parallelism / copy_table_parallelism param; 0 (the
	// default) leaves the DSN-then-default behaviour byte-identical.
	mysql.SetVStreamCopyTableParallelismOverride(s.VStreamCopyTableParallelism)
	mysql.SetNativeCopyTableParallelismOverride(s.CopyTableParallelism)

	// ADR-0120 (default flipped): thread the explicit --vstream-preserve-skew CLI
	// value into the mysql engine at the composition root. true wins over the
	// source DSN's vstream_preserve_skew param and restores the old MinimizeSkew=
	// on behaviour; false (the default) leaves the new relaxed MinimizeSkew=off.
	mysql.SetVStreamPreserveSkewOverride(s.VStreamPreserveSkew)

	if len(s.IncludeTable) > 0 && len(s.ExcludeTable) > 0 {
		return errors.New("--include-table and --exclude-table are mutually exclusive")
	}
	if len(s.IncludeView) > 0 && len(s.ExcludeView) > 0 {
		return errors.New("--include-view and --exclude-view are mutually exclusive")
	}
	// Multi-database fan-out (ADR-0074 Phase 1b.2) / multi-schema fan-out
	// (ADR-0075). The --*-schema and --*-database forms are synonyms; mixing
	// them is a loud error.
	includeNS, excludeNS, allNS, err := resolveNamespaceScopeArgs(
		s.IncludeDatabase, s.ExcludeDatabase, s.AllDatabases,
		s.IncludeSchema, s.ExcludeSchema, s.AllSchemas,
	)
	if err != nil {
		return err
	}
	if len(includeNS) > 0 && len(excludeNS) > 0 {
		return errors.New("--include-database/--include-schema and --exclude-database/--exclude-schema are mutually exclusive")
	}
	if allNS && (len(includeNS) > 0 || len(excludeNS) > 0) {
		return errors.New("--all-databases/--all-schemas is mutually exclusive with --include-* / --exclude-* namespace scope")
	}
	databaseFilter, err := pipeline.NewDatabaseFilter(includeNS, excludeNS)
	if err != nil {
		return err
	}

	// ADR-0038 pin-down 3: the three retry dials carry hard ranges.
	// Out-of-range values are rejected at startup (loud, before any
	// connection) rather than silently clamped — a clamp would let
	// an operator believe they configured a 5-minute envelope when
	// the policy quietly used 300s, masking the actual failure
	// behaviour the ADR is careful to keep computable.
	if err := validateRetryFlags(s.ApplyRetryAttempts, s.ApplyRetryBackoffBase, s.ApplyRetryBackoffCap); err != nil {
		return err
	}
	include, exclude := resolveTableFilterArgs(s.IncludeTable, s.ExcludeTable, cfg)
	filter, err := pipeline.NewTableFilter(include, exclude)
	if err != nil {
		return err
	}
	viewFilter, err := pipeline.NewViewFilter(s.IncludeView, s.ExcludeView)
	if err != nil {
		return err
	}

	mappings, err := resolveMappings(s.TypeOverride, cfg)
	if err != nil {
		return err
	}
	exprMappings, err := resolveExpressionMappings(s.ExprOverride, cfg)
	if err != nil {
		return err
	}

	// Validate mutually-exclusive flag combinations BEFORE the destructive
	// confirmation prompt below — an invalid combination must fail loud up
	// front, not after asking the operator to authorize a target-table DROP
	// the command will then refuse to perform.
	if err := s.validateFlagCombos(); err != nil {
		return err
	}

	if s.ResetTargetData && !s.Yes {
		ok, err := confirmTypedDestructive(os.Stdin, os.Stdout,
			"This will DROP tables on the target. Type 'reset' to confirm: ", "reset")
		if err != nil {
			return err
		}
		if !ok {
			fmt.Fprintln(os.Stdout, "aborted")
			return nil
		}
	}

	// --position-from-manifest: load the chain terminal position from
	// the supplied store. The streamer treats it as a warm-resume
	// position source, replacing the default sluice_cdc_state lookup.
	// Mutually exclusive with --reset-target-data (different recovery
	// shapes; both override the persisted position).
	var manifestStore irbackup.Store
	var manifestStoreCloser func() error
	if s.PositionFromManifest != "" {
		ctx := kongContext()
		store, _, closer, err := openBackupStore(ctx, "", s.PositionFromManifest, pipeline.BlobStoreOptions{
			Endpoint:  s.BackupEndpoint,
			Region:    s.BackupRegion,
			PathStyle: s.BackupPathStyle,
		})
		if err != nil {
			return fmt.Errorf("--position-from-manifest: %w", err)
		}
		manifestStore = store
		manifestStoreCloser = closer
	}
	if manifestStoreCloser != nil {
		defer func() { _ = manifestStoreCloser() }()
	}

	// ADR-0052: parse --apply-batch-size (string of "auto" or numeric).
	// AutoTune defaults to true; --no-auto-tune flips it off.
	applyBatchSize, err := resolveApplyBatchSize(s.ApplyBatchSize, target)
	if err != nil {
		return fmt.Errorf("--apply-batch-size: %w", err)
	}

	shardSpec, err := parseInjectShardColumn(s.InjectShardColumn)
	if err != nil {
		return err
	}

	indexBuildMem, err := parseIndexBuildMem(s.IndexBuildMem)
	if err != nil {
		return err
	}

	// ADR-0054 §2 / DP-A: validate the lease timing knobs eagerly so
	// an operator-misconfiguration refuses at startup rather than at
	// the first observed DDL boundary (loud-failure tenet).
	leaseCfg := pipeline.LeaseConfig{
		LeaseDuration: s.ShardCoordinationLeaseDuration,
		RenewDeadline: s.ShardCoordinationRenewDeadline,
		RetryPeriod:   s.ShardCoordinationRetryPeriod,
	}
	if shardSpec.Engaged() && !s.NoCoordinateLiveDDL {
		if err := leaseCfg.Validate(); err != nil {
			return err
		}
	}

	// connection-resilience (1): label every connection sluice opens
	// with the run's id (PG: application_name=sluice/<role>/<id>) so
	// the operator can find sluice's sessions in pg_stat_activity.
	// Applied once here, before any engine opens a connection; the
	// engine normalises an empty --stream-id to the "-" fallback.
	source = labelEngine(source, s.StreamID)
	target = labelEngine(target, s.StreamID)

	// ADR-0107 Phase 2: construct the OPTIONAL PlanetScale target-health
	// telemetry provider when the operator opts in. Wired ONLY here at the
	// composition root (the sole place allowed to import the PS provider);
	// the streamer holds it as the engine-neutral ir.TargetTelemetry. Nil
	// when the operator did not opt in ⇒ byte-identical default sync.
	telemetryProvider, err := buildTargetTelemetry(kongContext(), s)
	if err != nil {
		return err
	}
	if telemetryProvider != nil {
		defer func() { _ = telemetryProvider.Close() }()
	}

	streamer := &pipeline.Streamer{
		Source:             source,
		Target:             target,
		SourceDSN:          s.Source,
		TargetDSN:          s.Target,
		StreamID:           s.StreamID,
		SlotName:           s.SlotName,
		Mappings:           mappings,
		ExpressionMappings: exprMappings,
		DryRun:             s.DryRun,
		Filter:             filter,
		ViewFilter:         viewFilter,
		SkipViews:          s.SkipViews,
		DatabaseFilter:     databaseFilter,
		AllDatabases:       allNS,
		ForceColdStart:     s.ForceColdStart,
		ResetTargetData:    s.ResetTargetData,
		RestartFromScratch: s.RestartFromScratch,
		// ADR-0093: default auto re-snapshot on purged/invalid resume
		// position (parity with the binlog path; the zero-value default of
		// this opt-out field IS auto-recover); --no-auto-resnapshot flips it
		// to a loud terminal failure instead.
		SuppressAutoResnapshotOnInvalidPosition: s.NoAutoResnapshot,
		SchemaAlreadyApplied:                    s.SchemaAlreadyApplied,
		ApplyBatchSize:                          applyBatchSize,
		AutoTune:                                !s.NoAutoTune,
		ApplyTuneTargetLatency:                  s.ApplyTuneTargetLatency,
		MaxBufferBytes:                          s.MaxBufferBytes,
		IndexBuildMem:                           indexBuildMem,
		IndexBuildParallelism:                   s.IndexBuildParallelism,
		MaxTargetConnections:                    s.MaxTargetConnections,
		BulkParallelism:                         s.BulkParallelism,
		TableParallelism:                        s.TableParallelism,
		BulkParallelMinRows:                     s.BulkParallelMinRows,
		BulkBatchSize:                           s.BulkBatchSize,
		CopyFanoutDegree:                        s.CopyFanoutDegree,
		NoIntraTableStealing:                    s.NoIntraTableStealing,
		RawCopyFormat:                           parseRawCopyFormat(s.RawCopyFormat),
		ReapStaleBackends:                       s.ReapStaleBackends,
		ApplyExecTimeout:                        s.ApplyExecTimeout,
		ApplyConcurrency:                        s.ApplyConcurrency,
		ApplyRetryAttempts:                      s.ApplyRetryAttempts,
		ApplyRetryBackoffBase:                   s.ApplyRetryBackoffBase,
		ApplyRetryBackoffCap:                    s.ApplyRetryBackoffCap,
		MetricsListen:                           s.MetricsListen,
		BuildVersion:                            version,
		BuildCommit:                             commit,
		// ADR-0107: nil unless the operator opted into PlanetScale telemetry.
		// telemetryProviderOrNil returns a TRUE nil interface when off, so the
		// streamer's `TargetTelemetry != nil` guards stay exact (no typed-nil
		// trap from assigning a nil *Provider straight into the interface).
		TargetTelemetry: telemetryProviderOrNil(telemetryProvider),
		// ADR-0107 item 35: opt-out (zero value = record when telemetry wired).
		SuppressTargetMetricsHistory: s.SuppressTargetMetricsHistory,
		// ADR-0107 item 36: sync-scoped threshold alerter (opt-in; inert unless
		// a sink URL + a threshold are set AND telemetry is wired).
		NotifyWebhookURL:          s.NotifyWebhook,
		NotifySlackWebhookURL:     s.NotifySlack,
		NotifyStorageUtil:         s.NotifyStorageUtil,
		NotifyCPUUtil:             s.NotifyCPUUtil,
		NotifyMemUtil:             s.NotifyMemUtil,
		NotifyLagSeconds:          s.NotifyLagSeconds,
		NotifyStorageGrowthPerMin: s.NotifyStorageGrowthPerMin,
		NotifyCooldown:            s.NotifyCooldown,
		NotifySyncLagSeconds:      s.NotifySyncLagSeconds,
		HeartbeatInterval:         s.HeartbeatInterval,
		PollInterval:              s.PollInterval,

		SourceHeartbeatInterval:    s.SourceHeartbeatInterval,
		SourceHeartbeatPruneWindow: s.SourceHeartbeatPruneWindow,
		SourceHeartbeatTableName:   s.SourceHeartbeatTableName,
		NoSourceHeartbeat:          s.NoSourceHeartbeat,

		PositionFromManifestStore: manifestStore,
		StrictPreflight:           s.StrictPreflight,
		PatroniMode:               s.PatroniMode,
		TargetSchema:              s.TargetSchema,
		EnabledPGExtensions:       s.EnablePGExtension,
		InjectShardColumn:         shardSpec,
		AllowCrossShardMerge:      s.AllowCrossShardMerge,
		CoordinateLiveDDL:         !s.NoCoordinateLiveDDL,
		SchemaChanges:             s.SchemaChanges,
		ForwardSchemaAddColumn:    s.ForwardSchemaAddColumn,
		BackfillAddedColumn:       s.BackfillAddedColumn,
		ShardCoordinationLease: pipeline.LeaseConfig{
			LeaseDuration: s.ShardCoordinationLeaseDuration,
			RenewDeadline: s.ShardCoordinationRenewDeadline,
			RetryPeriod:   s.ShardCoordinationRetryPeriod,
		},
	}
	keysetSource := s.KeysetSource
	if keysetSource == "" {
		keysetSource = cfg.KeysetSource
	}
	keyset, err := redact.LoadKeyset(kongContext(), keysetSource)
	if err != nil {
		return err
	}
	dictionaries, err := redact.LoadDictionaries(cfg.Dictionaries)
	if err != nil {
		return err
	}
	redactor, err := parseRedactFlags(s.Redact, keyset, s.StreamID, dictionaries)
	if err != nil {
		return err
	}
	redactor, err = mergeYAMLRedactions(redactor, cfg.Redactions, keyset, s.StreamID, dictionaries)
	if err != nil {
		return fmt.Errorf("redactions (YAML): %w", err)
	}
	streamer.Redactor = redactor
	logKeysetLoaded(keyset)
	logRedactionConfig(redactor, "sync start")
	// ADR-0056 auto-on-crash hook (opt-in). When
	// --diagnose-on-crash-dir is set, the hook writes a bundle to the
	// directory if Run returns an error. The hook NEVER masks the
	// original error per the loud-failure tenet.
	crashWrap, err := installCrashHook(s.CrashHookFlags,
		crashHookRequestForStreamer(s.StreamID, source, target, s.Source, s.Target, s.SlotName))
	if err != nil {
		return err
	}
	return crashWrap(streamer.Run(kongContext()))
}

// SyncStatusCmd reports the state of every continuous-sync stream
// the target database has been the destination for. Reads the
// per-target sluice_cdc_state control table directly — no need for
// a running sync process.
//
// When `--stream-id` is supplied, output is filtered to that one
// stream (matches by exact stream_id). Without it, every row in
// the control table is printed.
//
// Output shape:
//   - --format=text  (default) — human-readable tab-aligned table.
//   - --format=json            — JSON array of stream rows; suitable
//     for scripted consumption / piping
//     to jq.
//
// Live mode: --watch[=DURATION] re-runs the query and re-renders
// every DURATION (default 2s) until interrupted. The terminal is
// cleared between renders so the output stays in place rather than
// scrolling. --summary prepends an aggregate header so a fleet of
// streams is summarisable at a glance even before scanning rows.
type SyncStatusCmd struct {
	TargetDriver string        `help:"Target engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"target"`
	Target       string        `help:"Target database DSN." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`
	StreamID     string        `help:"Filter to a specific stream id. When empty, every recorded stream is shown." placeholder:"ID"`
	Format       string        `help:"Output format: 'text' (human-readable table, default) or 'json' (machine-readable, suitable for jq pipes)." default:"text" enum:"text,json"`
	Watch        time.Duration `help:"Live-refresh mode: re-render every DURATION until interrupted. 0 (default) disables. Use --watch 2s for the typical operator polling cadence." placeholder:"DURATION"`
	Summary      bool          `help:"Prepend an aggregate-summary header (stream count, oldest/most-recent ages). Useful when a fleet of streams is hard to skim row-by-row."`
}

// Run implements `sluice sync status`.
func (s *SyncStatusCmd) Run(_ *Globals) error {
	target, err := resolveEngine(s.TargetDriver)
	if err != nil {
		return fmt.Errorf("--target-driver: %w", err)
	}

	ctx := kongContext()
	applier, err := target.OpenChangeApplier(ctx, s.Target)
	if err != nil {
		return fmt.Errorf("open target applier: %w", err)
	}
	defer func() {
		if c, ok := applier.(io.Closer); ok {
			_ = c.Close()
		}
	}()

	opts := statusRenderOpts{
		Format:   s.Format,
		Summary:  s.Summary,
		StreamID: s.StreamID,
	}

	// One-shot path (default).
	if s.Watch <= 0 {
		return runStatusOnce(ctx, applier, os.Stdout, opts)
	}

	// Live-refresh path.
	return runStatusWatch(ctx, applier, os.Stdout, opts, s.Watch)
}

// SyncStopCmd asks a running `sluice sync start` to drain in-flight
// changes, persist its final position, and exit cleanly. The signal
// is delivered via the per-target sluice_cdc_state control table:
// the column stop_requested_at is set to NOW(), and the running
// streamer's polling loop observes the flag on its next tick (every
// 5s by default).
//
// This is additive to the existing Ctrl-C / SIGTERM behavior; it
// exists so operators can stop streams from a different host (k8s
// lifecycle hooks, systemd, ad-hoc operator runbooks) without
// needing PID files or cross-process signal delivery. See
// internal/pipeline/stop_signal.go for the full design rationale.
type SyncStopCmd struct {
	TargetDriver string        `help:"Target engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"target"`
	Target       string        `help:"Target database DSN." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`
	StreamID     string        `help:"Stream identifier to stop." required:"" placeholder:"ID"`
	Wait         bool          `help:"Block until the running streamer drains and clears its stop signal. Use to coordinate ALTER windows or scripted teardowns." short:"w"`
	Timeout      time.Duration `help:"Maximum wait when --wait is set. On timeout the CLI exits non-zero; the stop request remains in place and the streamer will eventually drain." default:"5m"`
}

// Run implements `sluice sync stop`.
//
// Without --wait this is a fire-and-forget shape: the CLI writes
// stop_requested_at to the per-target control table and exits. The
// running streamer's polling loop observes the flag (5s tick by
// default) and drains gracefully on a timeline operators can read
// from `sluice sync status`.
//
// With --wait the CLI additionally polls ReadStopRequested until the
// flag clears (the streamer clears it at the end of a graceful drain
// — see ir.ChangeApplier.ClearStopRequested). Useful for ALTER
// coordination: `sync stop --wait && alter-source.sh && sync start`
// runs the ALTER only after the streamer has confirmed it drained.
// On --timeout the CLI exits non-zero and surfaces a clear message;
// the stop request itself stays written so the streamer continues
// draining in the background. Re-running `sync stop --wait` will
// keep watching the same flag.
func (s *SyncStopCmd) Run(_ *Globals) error {
	target, err := resolveEngine(s.TargetDriver)
	if err != nil {
		return fmt.Errorf("--target-driver: %w", err)
	}

	ctx := kongContext()
	applier, err := target.OpenChangeApplier(ctx, s.Target)
	if err != nil {
		return fmt.Errorf("open target applier: %w", err)
	}
	defer func() {
		if c, ok := applier.(io.Closer); ok {
			_ = c.Close()
		}
	}()

	if err := applier.RequestStop(ctx, s.StreamID); err != nil {
		// Mirrors `slot drop`'s shape: the CLI surfaces a friendly
		// "no stream X on target" rather than an engine-specific
		// stack trace when the operator typos a stream ID.
		if isStreamNotFoundErr(err) {
			fmt.Fprintf(os.Stdout, "no stream %q on target\n", s.StreamID)
			return nil
		}
		return fmt.Errorf("request stop: %w", err)
	}
	if !s.Wait {
		fmt.Fprintf(os.Stdout, "stop requested for stream %q on target; running process will drain and exit\n", s.StreamID)
		return nil
	}
	fmt.Fprintf(os.Stdout, "stop requested for stream %q on target; waiting for graceful drain (timeout %s)...\n", s.StreamID, s.Timeout)
	return waitForStopComplete(ctx, applier, s.StreamID, s.Timeout)
}

// stopFlagReader is the interface waitForStopComplete needs from the
// applier. Mirrors the unexported pipeline.stopFlagReader; declared
// here independently so cmd/sluice doesn't import internal/pipeline
// just for one method shape.
type stopFlagReader interface {
	ReadStopRequested(ctx context.Context, streamID string) (bool, error)
}

// stopWaitPollInterval is the cadence at which `sync stop --wait`
// polls for flag clearance. 1s is responsive without hammering the
// target; the streamer-side poll is the rate-limiting factor (5s
// default for graceful-drain trigger), so faster polling on this
// side gives no real win.
const stopWaitPollInterval = 1 * time.Second

// waitForStopComplete polls the control row until ReadStopRequested
// returns false (the streamer cleared the flag on graceful exit) or
// the timeout fires. Returns nil on success, an exitCode-2 error on
// timeout, and the underlying error on read failure.
func waitForStopComplete(ctx context.Context, applier ir.ChangeApplier, streamID string, timeout time.Duration) error {
	reader, ok := applier.(stopFlagReader)
	if !ok {
		// The applier's RequestStop succeeded (it implements that
		// part of the interface) but we can't poll. Fall back to
		// the fire-and-forget shape with a clear message.
		fmt.Fprintf(os.Stdout, "applier does not support polling for drain completion; stop signal sent — check `sluice sync status` to verify drain\n")
		return nil
	}

	deadline := time.Now().Add(timeout)
	t := time.NewTicker(stopWaitPollInterval)
	defer t.Stop()

	for {
		stopRequested, err := reader.ReadStopRequested(ctx, streamID)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("poll stop signal: %w", err)
		}
		if !stopRequested {
			fmt.Fprintf(os.Stdout, "stream %q drained and exited cleanly\n", streamID)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("stream %q did not complete drain within %s; the stop request remains in place and the streamer will continue draining — check `sluice sync status` to investigate", streamID, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// isStreamNotFoundErr returns true when err wraps an engine's stream-
// not-found sentinel. The CLI string-matches the wrapped engine
// error rather than importing the sentinel from a specific engine
// package — same shape `isSlotNotFoundErr` uses.
func isStreamNotFoundErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "stream not found")
}

// humanAgo returns a brief "5m ago" / "2h ago" / "3d ago" string
// for d. Operators glance at the column to spot stuck streams; a
// rough cadence is more useful than precise.
func humanAgo(d time.Duration) string {
	switch {
	case d < 0:
		return "in the future"
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}

// truncatePositionToken returns token if it's no longer than max,
// otherwise the head of the token followed by an ellipsis. Position
// tokens are JSON blobs that can run hundreds of bytes; the status
// table stays readable.
func truncatePositionToken(token string, maxLen int) string {
	if len(token) <= maxLen {
		return token
	}
	return token[:maxLen-1] + "…"
}

// kongContext returns a context.Context wired to OS signals so a
// long-running migration or sync stream cancels cleanly when the
// operator hits Ctrl-C. The context is cancelled on SIGINT or
// SIGTERM; the underlying pipeline goroutines unwind and the
// command exits with the cancellation propagated up.
func kongContext() context.Context {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// We deliberately don't capture the cancel func: the signal
	// notifier itself triggers cancellation, and we want the
	// context to stay live for the entire process lifetime
	// (kong dispatches one Run call per process). The spurious
	// context-leak the linter flags here is intentional; see the
	// nolint directive.
	return ctx //nolint:contextcheck // ctx is scoped to the process lifetime
}
