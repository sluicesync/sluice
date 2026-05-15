package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/alecthomas/kong"

	"github.com/orware/sluice/internal/config"
	"github.com/orware/sluice/internal/engines"
	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/pipeline"
	"github.com/orware/sluice/internal/redact"
)

// Globals are flags shared across every subcommand. Embedding into the
// top-level CLI makes them parse identically regardless of which
// subcommand the user runs; binding the value in main() makes it
// available to Run methods that declare a *Globals parameter.
type Globals struct {
	Config   string `help:"Path to a YAML config file." short:"c" type:"existingfile" placeholder:"PATH"`
	LogLevel string `help:"Log verbosity." short:"l" default:"info" enum:"debug,info,warn,error" placeholder:"LEVEL"`

	// PprofListen is the GitHub #23 Phase A operator-diagnostic hook.
	// When non-empty, starts net/http/pprof's debug endpoints at the
	// given address for the lifetime of the subcommand. Off by
	// default; opt-in. Useful for diagnosing silent stalls — the
	// operator hits /debug/pprof/goroutine?debug=2 to dump every
	// goroutine's stack, which is what's needed to localise a wedge.
	PprofListen string `help:"Bind net/http/pprof's debug endpoints at this address (e.g. ':6060', '127.0.0.1:6060') for the duration of the subcommand. Off by default. Useful for diagnosing silent stalls (GitHub #23 Phase A) — fetch /debug/pprof/goroutine?debug=2 to dump every goroutine's stack." placeholder:"ADDR"`
}

// CLI is the root of the sluice command tree. Kong populates this from
// argv and dispatches to the matched subcommand's Run method.
type CLI struct {
	Globals

	// --version prints the build identifier and exits. The value is
	// supplied via kong.Vars{"version": ...} in main().
	Version kong.VersionFlag `help:"Print version and exit." short:"V"`

	Engines EnginesCmd `cmd:"" help:"List registered database engines."`
	Migrate MigrateCmd `cmd:"" help:"Run a one-time schema + data migration (simple mode)."`
	Sync    SyncCmd    `cmd:"" help:"Manage continuous-sync streams."`
	Slot    SlotCmd    `cmd:"" help:"Manage source-side replication slots (Postgres)."`
	Schema  SchemaCmd  `cmd:"" help:"Inspect and describe schemas (preview translation, etc.)."`
	Verify  VerifyCmd  `cmd:"" help:"Verify data integrity between source and target (v0.12.0+ count mode)."`
	Backup  BackupCmd  `cmd:"" help:"Take and verify logical backups (Phase 1: full snapshot to local filesystem)."`
	Restore RestoreCmd `cmd:"" help:"Restore a logical backup into a target database."`
	Matview MatviewCmd `cmd:"" help:"Operate on PostgreSQL materialized views (refresh; PG-only)."`
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

	IncludeView []string `help:"Only migrate these views (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --exclude-view." sep:"," placeholder:"VIEW"`
	ExcludeView []string `help:"Migrate every view except these (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --include-view." sep:"," placeholder:"VIEW"`
	SkipViews   bool     `help:"Skip view processing entirely; views in the source schema are not created on the target. Useful when views are managed out-of-band (Atlas / sqitch / liquibase)."`

	TypeOverride []string `help:"Force a specific target type for a column (repeatable). Format: 'TABLE.COLUMN=TYPE', e.g. 'products.attrs=text'. CLI form of the YAML 'mappings:' config; for target-type options (e.g. 'jsonb' with binary=true), use the YAML form." placeholder:"TABLE.COLUMN=TYPE"`

	ExprOverride []string `help:"Replace a generated column's body with operator-supplied target-dialect text (repeatable). Format: 'TABLE.COLUMN=EXPRESSION'. The expression is emitted verbatim — sluice's cross-dialect translator (ADR-0016) does NOT run on overridden columns. Escape hatch for cases the translator's hand-coded rewrites don't recognise. CLI form of the YAML 'expression_mappings:' config." placeholder:"TABLE.COLUMN=EXPRESSION"`

	DryRun bool `help:"Read the source schema and print the migration plan without applying changes." short:"n"`

	Resume      bool   `help:"Resume a previously-failed migration. State is read from sluice_migrate_state on the target." short:"r"`
	MigrationID string `help:"Stable migration identifier; key in sluice_migrate_state. Auto-generated from source/target host info when empty." placeholder:"ID"`

	ForceColdStart bool `help:"Skip the cold-start pre-flight check that refuses to bulk-copy into a populated target. Use with caution — INSERT into a non-empty table will collide on PRIMARY KEY. Ignored when --resume is set."`

	ResetTargetData bool `help:"Destructive recovery: DELETE the migrate-state row, DROP every source-schema table on the target, then run a fresh cold-start. Use after a wedged-state recovery (e.g. slot-missing fall-through). Requires confirmation (type 'reset') unless --yes is set. Mutually exclusive with --resume. See ADR-0023."`

	Yes bool `help:"Skip the destructive-action confirmation prompt for --reset-target-data." short:"y"`

	BulkBatchSize int `help:"Bulk-copy batch size for resume-mid-table checkpointing. Each batch commits with an updated cursor in sluice_migrate_state.table_progress, so a crash mid-table resumes without re-copying the prefix. Tables without a PK fall back to truncate-and-redo regardless. Lower values shorten the replay window on crash; higher values amortise per-tx commit overhead. Only consulted on the resume path; cold-start migrations use the faster plain-INSERT / COPY path. Default 5000." default:"5000" placeholder:"N"`

	BulkParallelism int `help:"Number of parallel reader/writer pairs per table during bulk copy. Tables above --bulk-parallel-min-rows are split into this many PK ranges and copied concurrently. Tables without a single integer PK fall back to single-reader. 0 means use min(8, NumCPU); 1 disables parallelism. See ADR-0019." default:"0" placeholder:"N"`

	BulkParallelMinRows int64 `help:"Row-count threshold below which a table is copied with a single reader/writer pair regardless of --bulk-parallelism. Avoids per-chunk overhead on small tables. Default 80000 (v0.62.0+; pre-v0.62.0 default was 100000) — sits below 100k to absorb the typical information_schema row-count estimate undershoot on InnoDB, so 100k-actual tables don't miss the threshold by ~1%." default:"80000" placeholder:"N"`

	MaxBufferBytes int64 `help:"Soft cap on per-batch buffered memory in the bulk-copy writer. The writer flushes when accumulated row-value bytes reach the cap regardless of row count, so wide-row workloads (TEXT/BYTEA/JSON at MB scale) don't blow out heap. A single row larger than the cap still applies (soft target). Default 67108864 (64 MiB). See ADR-0028." default:"67108864" placeholder:"N"`

	TargetSchema string `help:"Per-source target schema namespace (Postgres-only). When set, every emitted CREATE TABLE / ALTER TABLE / CREATE INDEX / CREATE TYPE prefixes the table reference with this schema. Use to land multiple sluice streams on the same target without table-name collisions (Shape B microservices → analytics warehouse, ADR-0031). The schema is auto-created on the target if it doesn't exist. The control table sluice_cdc_state stays in the DSN's default schema regardless. MySQL operators use a different --target DSN database instead — schemas and databases collapse on MySQL." placeholder:"NAME"`

	EnablePGExtension []string `help:"Enable passthrough for a Postgres extension type (repeatable). Same-engine PG → PG passthrough preserves the source-native shape on the target. Cross-engine targets (MySQL) keep the loud-failure default except for hstore (→ JSON) and citext (→ VARCHAR with case-insensitive collation), which have built-in default translators. Each named extension must be installed on both source and target — sluice preflights via pg_extension before any data moves. Recognised: vector (pgvector), pg_trgm, hstore, citext. v1 shortlist per docs/research/pg-extensions-deployment-frequency.md. See ADR-0032." placeholder:"EXT"`

	Redact       []string `help:"Redact a PII column (repeatable). Format: '[schema.]table.column=STRATEGY[:options]'. Strategies: null (NULLABLE columns only), static:<value>, hash:sha256, hash:hmac-sha256[:<keyname>] (requires --keyset-source), truncate:<n>, mask:inner:<m1>,<m2>[,<char>], mask:outer:<m1>,<m2>[,<char>], mask:ssn, mask:pan, mask:pan-relaxed, mask:email, mask:ca-sin, mask:uk-nin, mask:iban, mask:uuid (Phase 2.b country/format presets, v0.57.0+), randomize:int:<min>,<max>, randomize:email, randomize:us-phone, randomize:uuid (Phase 2.c first wave, v0.59.0+), randomize:ssn, randomize:pan[:<brand>], randomize:ca-sin, randomize:uk-nin, randomize:iban[:<country-code>] (Phase 2.c second wave, v0.60.0+; brand: visa|mastercard|amex; country: DE|GB|FR; all randomize:* require a PK on the source table), randomize:dict:<name>, tokenize:dict:<name>[:<keyname>] (Phase 3 v0.61.0+, keyset-sourced in Phase 4 v0.62.0+; dictionaries declared in YAML 'dictionaries:' block — CLI form REQUIRES YAML config to declare the dictionary content). Examples: --redact users.email=hash:sha256, --redact users.pan=mask:pan, --redact users.id=mask:uuid, --redact users.age=randomize:int:18,90, --redact users.first_name=tokenize:dict:first_names. Bulk-copy + CDC paths both honour --redact. YAML form available under config 'redactions:' block. See docs/dev/notes/prep-pii-redaction-phase-1.md." placeholder:"RULE" sep:"none"`
	KeysetSource string   `help:"Operator keyset source for keyset-using redaction strategies (hash:hmac-sha256, tokenize:dict). PII Phase 4 (ADR-0041). Forms: 'file:PATH' (keyset YAML on disk), 'env:VARNAME' (keyset YAML in an env var), 'db:DSN' (sluice_keysets table on the named DSN — shared across streams for cross-stream surrogate stability). Resolved ONCE at startup; rotation takes effect on next process restart only (no hot-reload). Required when any --redact / YAML rule uses hash:hmac-sha256 or tokenize:dict — the Phase 1 --redact-key-source flag and the built-in v0.61.0 tokenize key were removed." placeholder:"SRC"`
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
	if len(m.IncludeView) > 0 && len(m.ExcludeView) > 0 {
		return errors.New("--include-view and --exclude-view are mutually exclusive")
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

	mig := &pipeline.Migrator{
		Source:              source,
		Target:              target,
		SourceDSN:           m.Source,
		TargetDSN:           m.Target,
		DryRun:              m.DryRun,
		Mappings:            mappings,
		ExpressionMappings:  exprMappings,
		Filter:              filter,
		ViewFilter:          viewFilter,
		SkipViews:           m.SkipViews,
		Resume:              m.Resume,
		MigrationID:         m.MigrationID,
		ForceColdStart:      m.ForceColdStart,
		ResetTargetData:     m.ResetTargetData,
		BulkBatchSize:       m.BulkBatchSize,
		BulkParallelism:     m.BulkParallelism,
		BulkParallelMinRows: m.BulkParallelMinRows,
		MaxBufferBytes:      m.MaxBufferBytes,
		TargetSchema:        m.TargetSchema,
		EnabledPGExtensions: m.EnablePGExtension,
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
	return mig.Run(kongContext())
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
		Target:          target,
		TargetDSN:       s.Target,
		Store:           store,
		ChainURL:        storeDesc,
		StreamID:        s.StreamID,
		PollInterval:    s.PollInterval,
		ApplyBatchSize:  s.ApplyBatchSize,
		MaxBufferBytes:  s.MaxBufferBytes,
		ResetTargetData: s.ResetTargetData,
		AtChainID:       s.AtChainID,
		SluiceVersion:   version,
		Envelope:        envelope,
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

	IncludeView []string `help:"Only create these views on the target during cold-start (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --exclude-view. Views are not replicated by CDC; this filter only affects the cold-start schema-apply phase." sep:"," placeholder:"VIEW"`
	ExcludeView []string `help:"Skip these views during cold-start schema-apply (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --include-view." sep:"," placeholder:"VIEW"`
	SkipViews   bool     `help:"Skip view creation entirely on cold-start. Views are not replicated by CDC, so this only affects the initial schema-apply step."`

	TypeOverride []string `help:"Force a specific target type for a column (repeatable). Format: 'TABLE.COLUMN=TYPE', e.g. 'products.attrs=text'. CLI form of the YAML 'mappings:' config; for target-type options, use the YAML form." placeholder:"TABLE.COLUMN=TYPE"`

	ExprOverride []string `help:"Replace a generated column's body with operator-supplied target-dialect text (repeatable). Format: 'TABLE.COLUMN=EXPRESSION'. Emitted verbatim; ADR-0016 translator skips overridden columns. CLI form of the YAML 'expression_mappings:' config." placeholder:"TABLE.COLUMN=EXPRESSION"`

	StreamID string `help:"Stream identifier; the key under which position is persisted on the target. Auto-generated from source/target host info when empty." placeholder:"ID"`
	SlotName string `help:"Replication-slot name suffix for engines that have a slot concept (Postgres). Default 'sluice_slot'. Sluice prepends 'sluice_' if the supplied name doesn't already start with it (so '--slot-name shard_a' creates 'sluice_shard_a'); the convention lets operators find every sluice slot with 'pg_replication_slots WHERE slot_name LIKE sluice\\_%'. Set per-instance to run multiple concurrent sluice instances against the same source — without distinct slot names they collide on the default. Engines without slots (MySQL: binlog stream is the slot) silently ignore this flag." placeholder:"NAME"`
	DryRun   bool   `short:"n" help:"Print what would happen — cold-start vs warm-resume, source schema summary or persisted position — without modifying the target or starting the stream."`

	ForceColdStart bool `help:"Skip the cold-start pre-flight check that refuses to bulk-copy into a populated target. Use with caution — INSERT into a non-empty table will collide on PRIMARY KEY. Ignored on the warm-resume path."`

	ResetTargetData bool `help:"Destructive recovery: DELETE the cdc-state row, DROP every source-schema table on the target, then run a fresh cold-start stream. Use after slot-missing fall-through or a similar wedged-state recovery. Requires confirmation (type 'reset') unless --yes is set. See ADR-0023."`

	SchemaAlreadyApplied bool `help:"Skip every DDL phase during cold-start (CREATE TABLE / CREATE INDEX / ADD FOREIGN KEY / CREATE VIEW / SyncIdentitySequences / EnsureControlTable). Operator promises the target's catalog matches the source's AND the sluice_cdc_state control table is pre-created. Use this on PlanetScale branches with Safe Migrations enabled (GitHub #17), or on Atlas/Liquibase-managed schemas where DDL goes through a separate pipeline. The cold-start preflight refusal is also skipped — bulk-copy runs into operator-prepared empty tables; sluice does NOT validate the schema match."`

	Yes bool `help:"Skip the destructive-action confirmation prompt for --reset-target-data." short:"y"`

	ApplyBatchSize int `help:"Batch up to N CDC changes per target transaction. Default 1 (one change per tx, conservative). Production tuning: 100-500 typically gives 50-100x throughput on bulk CDC traffic. Schema-change events (TRUNCATE) flush the in-progress batch; the cap is an upper bound on batch size, not a target. Idempotent applier semantics (ADR-0010) keep replay-on-crash safe; ADR-0017 covers the full design." default:"1" placeholder:"N"`

	MaxBufferBytes int64 `help:"Soft cap on per-batch buffered memory in the CDC applier (and, on the cold-start branch, the bulk-copy writer). The applier commits the in-flight target tx when accumulated row-value bytes reach the cap regardless of row count, so wide-row streams (TEXT/BYTEA/JSON at MB scale) don't blow out heap. A single change larger than the cap still applies (soft target). Default 67108864 (64 MiB). See ADR-0028." default:"67108864" placeholder:"N"`

	ApplyExecTimeout time.Duration `help:"Per-statement deadline applied to every tx.ExecContext on the apply path. GitHub #23 Phase B fix (v0.52.0): closes the silent-stall failure mode where a half-closed destination connection blocked the apply goroutine indefinitely inside the driver's TLS read path. On expiry the driver returns context.DeadlineExceeded, which is classified retriable so the runWithRetry loop reopens the applier and retries the batch on a fresh connection. 0 disables (the pre-v0.52.0 behaviour: unbounded). Tune up for legitimately slow batch upserts on slow targets; down for tighter stall detection." default:"60s" placeholder:"DUR"`

	ApplyRetryAttempts    int           `help:"Maximum consecutive retriable apply failures the streamer absorbs before exiting. ADR-0038. 1 = no retry (exit on first transient — pre-v0.42.0 behaviour). 8 = default for managed-Vitess / Vitess-flavoured MySQL where tx-killer transients are routine. Counter resets when persisted CDC position advances between attempts; a streamer surviving for hours doesn't carry retry debt." default:"8" placeholder:"N"`
	ApplyRetryBackoffBase time.Duration `help:"Base interval for the exponential backoff between retriable apply failures. ADR-0038. Doubles on each consecutive failure, capped at --apply-retry-backoff-cap. Only consulted when --apply-retry-attempts > 1." default:"100ms" placeholder:"DUR"`
	ApplyRetryBackoffCap  time.Duration `help:"Upper bound on each per-attempt backoff interval. ADR-0038. Defaults to 30s. With 8 attempts and default base, the per-attempt sequence is: 100ms → 200ms → 400ms → 800ms → 1.6s → 3.2s → 6.4s → 12.8s, capped at the cap when it grows past." default:"30s" placeholder:"DUR"`

	MetricsListen string `help:"Bind a Prometheus-format /metrics endpoint at this address (e.g. ':9090' for all interfaces port 9090, '127.0.0.1:9090' for localhost only) for the duration of the stream. Off by default — opt-in. Companion to 'sluice sync health' (which is the cron-friendly one-shot probe shape). Useful for operators running Prometheus / Grafana / alertmanager." placeholder:"ADDR"`

	HeartbeatInterval time.Duration `help:"Wall-clock cadence the per-stream heartbeat goroutine logs an INFO 'stream: heartbeat' line at. GitHub #23 Phase A: distinguishes silent-stall (process alive but no apply, no log) from wedge (process alive, no heartbeat either). 0 disables." default:"60s" placeholder:"DUR"`

	PositionFromManifest string `help:"URL of a backup chain (s3://bucket/prefix, gs://, azblob://, file:///path) whose terminal manifest's EndPosition is used as this stream's resume position. Use after 'sluice restore --from=<chain-url>' to resume CDC from the chain's tail without re-bulking. Mutually exclusive with the implicit 'resume from sluice_cdc_state' path: when set, the persisted position is bypassed and the chain's terminal becomes the source of truth. PG soft warnings (wal_keep_size, Patroni) fire as pre-flight checks; --strict-preflight promotes them to refusals. See docs/dev/design-logical-backups-phase-3.md." placeholder:"URL"`

	StrictPreflight bool `help:"Promote position-from-manifest soft warnings (wal_keep_size sufficiency, Patroni-managed source detection) to hard refusals. Default off: the warnings log but the run proceeds. Slot existence / wal_status='lost' is always a refusal regardless of this flag — the slot can't deliver what we need."`

	PatroniMode string `help:"Control the Patroni / HA-managed source detection. 'auto' (default) runs the engine heuristics + DSN hostname pattern check and warns if any signal fires; 'on' skips the heuristics and forces the warning (operator opts in regardless of detection — useful on tenant-isolated managed PG where the heuristics miss); 'off' skips the heuristics and suppresses the Patroni warning entirely (operator confirmed self-hosted single-node PG without HA). Combine with --strict-preflight=true and --patroni-mode=on to make the warning a hard refusal." default:"auto" enum:"auto,on,off" placeholder:"MODE"`

	BackupEndpoint  string `help:"Override the S3 endpoint for --position-from-manifest's S3-compatible providers. Only meaningful when --position-from-manifest is an s3:// URL." placeholder:"URL"`
	BackupRegion    string `help:"Override the S3 region for --position-from-manifest. Only meaningful when --position-from-manifest is an s3:// URL." placeholder:"REGION"`
	BackupPathStyle bool   `help:"Force path-style S3 addressing for --position-from-manifest. Only meaningful when --position-from-manifest is an s3:// URL."`

	TargetSchema string `help:"Per-source target schema namespace (Postgres-only). When set, every emitted CREATE TABLE / ALTER TABLE / CREATE INDEX / CREATE TYPE prefixes the table reference with this schema, and CDC events apply against the named schema. Use to land multiple concurrent sluice streams on the same target without table-name collisions (Shape B microservices → analytics warehouse, ADR-0031). The schema is auto-created on the target if it doesn't exist. The control table sluice_cdc_state stays in the DSN's default schema regardless — multiple target-schema streams share a single state table per target. MySQL operators use a different --target DSN database instead — schemas and databases collapse on MySQL." placeholder:"NAME"`

	EnablePGExtension []string `help:"Enable passthrough for a Postgres extension type (repeatable). Same-engine PG → PG preserves the source-native shape. Cross-engine targets (MySQL) keep the loud-failure default except for hstore (→ JSON) and citext (→ VARCHAR with case-insensitive collation). Sluice preflights extension presence on both source and target. Recognised: vector (pgvector), pg_trgm, hstore, citext. See ADR-0032." placeholder:"EXT"`

	Redact       []string `help:"Redact a PII column (repeatable). Format: '[schema.]table.column=STRATEGY[:options]'. Strategies: null (NULLABLE columns only), static:<value>, hash:sha256, hash:hmac-sha256[:<keyname>] (requires --keyset-source), truncate:<n>, mask:inner:<m1>,<m2>[,<char>], mask:outer:<m1>,<m2>[,<char>], mask:ssn, mask:pan, mask:pan-relaxed, mask:email, mask:ca-sin, mask:uk-nin, mask:iban, mask:uuid (Phase 2.b country/format presets, v0.57.0+), randomize:int:<min>,<max>, randomize:email, randomize:us-phone, randomize:uuid (Phase 2.c first wave, v0.59.0+), randomize:ssn, randomize:pan[:<brand>], randomize:ca-sin, randomize:uk-nin, randomize:iban[:<country-code>] (Phase 2.c second wave, v0.60.0+; brand: visa|mastercard|amex; country: DE|GB|FR; all randomize:* require a PK on the source table), randomize:dict:<name>, tokenize:dict:<name>[:<keyname>] (Phase 3 v0.61.0+, keyset-sourced in Phase 4 v0.62.0+; dictionaries declared in YAML 'dictionaries:' block — CLI form REQUIRES YAML config to declare the dictionary content). Examples: --redact users.email=hash:sha256, --redact users.pan=mask:pan, --redact users.id=mask:uuid, --redact users.age=randomize:int:18,90, --redact users.first_name=tokenize:dict:first_names. Phase 1.5 (v0.54.0+): redaction covers BOTH cold-start bulk-copy AND mid-stream CDC events. Bare 'users.email' matches any source schema; schema-qualified 'public.users.email' takes precedence when both registered. See docs/dev/notes/prep-pii-redaction-phase-1.md." placeholder:"RULE" sep:"none"`
	KeysetSource string   `help:"Operator keyset source for keyset-using redaction strategies (hash:hmac-sha256, tokenize:dict). PII Phase 4 (ADR-0041). Forms: 'file:PATH' (keyset YAML on disk), 'env:VARNAME' (keyset YAML in an env var), 'db:DSN' (sluice_keysets table on the named DSN — shared across streams for cross-stream surrogate stability). Resolved ONCE at startup; rotation takes effect on next process restart only (no hot-reload). Required when any --redact / YAML rule uses hash:hmac-sha256 or tokenize:dict — the Phase 1 --redact-key-source flag and the built-in v0.61.0 tokenize key were removed." placeholder:"SRC"`
}

// Run implements `sluice sync start`.
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

	if len(s.IncludeTable) > 0 && len(s.ExcludeTable) > 0 {
		return errors.New("--include-table and --exclude-table are mutually exclusive")
	}
	if len(s.IncludeView) > 0 && len(s.ExcludeView) > 0 {
		return errors.New("--include-view and --exclude-view are mutually exclusive")
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
	var manifestStore ir.BackupStore
	var manifestStoreCloser func() error
	if s.PositionFromManifest != "" {
		if s.ResetTargetData {
			return errors.New("--position-from-manifest and --reset-target-data are mutually exclusive")
		}
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

	streamer := &pipeline.Streamer{
		Source:                    source,
		Target:                    target,
		SourceDSN:                 s.Source,
		TargetDSN:                 s.Target,
		StreamID:                  s.StreamID,
		SlotName:                  s.SlotName,
		Mappings:                  mappings,
		ExpressionMappings:        exprMappings,
		DryRun:                    s.DryRun,
		Filter:                    filter,
		ViewFilter:                viewFilter,
		SkipViews:                 s.SkipViews,
		ForceColdStart:            s.ForceColdStart,
		ResetTargetData:           s.ResetTargetData,
		SchemaAlreadyApplied:      s.SchemaAlreadyApplied,
		ApplyBatchSize:            s.ApplyBatchSize,
		MaxBufferBytes:            s.MaxBufferBytes,
		ApplyExecTimeout:          s.ApplyExecTimeout,
		ApplyRetryAttempts:        s.ApplyRetryAttempts,
		ApplyRetryBackoffBase:     s.ApplyRetryBackoffBase,
		ApplyRetryBackoffCap:      s.ApplyRetryBackoffCap,
		MetricsListen:             s.MetricsListen,
		HeartbeatInterval:         s.HeartbeatInterval,
		PositionFromManifestStore: manifestStore,
		StrictPreflight:           s.StrictPreflight,
		PatroniMode:               s.PatroniMode,
		TargetSchema:              s.TargetSchema,
		EnabledPGExtensions:       s.EnablePGExtension,
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
	return streamer.Run(kongContext())
}

// SyncStatusCmd reports the state of every continuous-sync stream
// the target database has been the destination for. Reads the
// per-target sluice_cdc_state control table directly — no need for
// a running sync process.
//
// When `--stream-id` is supplied, output is filtered to that one
// stream (matches by exact stream_id). Without it, every row in
// the control table is printed.
type SyncStatusCmd struct {
	TargetDriver string `help:"Target engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"target"`
	Target       string `help:"Target database DSN." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`
	StreamID     string `help:"Filter to a specific stream id. When empty, every recorded stream is shown." placeholder:"ID"`
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

	streams, err := applier.ListStreams(ctx)
	if err != nil {
		return fmt.Errorf("list streams: %w", err)
	}

	if s.StreamID != "" {
		filtered := streams[:0]
		for _, st := range streams {
			if st.StreamID == s.StreamID {
				filtered = append(filtered, st)
			}
		}
		streams = filtered
	}

	if len(streams) == 0 {
		if s.StreamID != "" {
			fmt.Fprintf(os.Stdout, "no stream %q on target\n", s.StreamID)
			return nil
		}
		fmt.Fprintln(os.Stdout, "no streams recorded on target")
		return nil
	}

	// Sort for stable output across runs. Most-recently-updated
	// first matches the operator's interest: "what's been moving?"
	sort.Slice(streams, func(i, j int) bool {
		return streams[i].UpdatedAt.After(streams[j].UpdatedAt)
	})

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer func() { _ = tw.Flush() }()
	fmt.Fprintln(tw, "STREAM\tUPDATED\tAGE\tPOSITION")
	now := time.Now()
	for _, st := range streams {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
			st.StreamID,
			st.UpdatedAt.UTC().Format(time.RFC3339),
			humanAgo(now.Sub(st.UpdatedAt)),
			truncatePositionToken(st.Position.Token, 60),
		)
	}
	return nil
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
