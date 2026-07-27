// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// DumpParityAllowlist is the single reviewable answer to "what does
// sluice knowingly not carry on a same-engine PG → PG migration?"
// (roadmap item 51, gotcha 6). Every entry cites the doc/ADR/source
// that declares the degradation — or carries the
// DumpParityTriageCitation marker for a latent gap the oracle found
// that has NOT been triaged into a documented decision yet. TRIAGE
// entries are loudly banner-logged by the harness on every run;
// they are debt, not policy.
//
// Patterns match statement KEYS (see dumpStatementKey) under
// path.Match glob semantics. First match wins — keep entries
// most-specific-first. Every hit is logged in the test output so the
// active list is visible in CI logs.
var DumpParityAllowlist = []dumpParityAllowlistEntry{
	{
		Pattern:  "*sluice_migrate_*",
		Reason:   "sluice's own per-target migrate-state tables (resume machinery); operational metadata, not migrated schema",
		Citation: "internal/pipeline/resume.go",
	},
	{
		Pattern:  "*public.events*",
		Reason:   "PG declarative partitioning is refused loudly (Bug 100); the harness excludes the partitioned parent + children via migcore.TableFilter, so the whole partition family exists only on the oracle side",
		Citation: "internal/pipeline/partition_preflight.go (Bug 100, v0.92.0)",
	},
	// item-51 finding #1 CLOSED: the standalone order_number_seq (and
	// orders.order_number's nextval default) now round-trips at FULL
	// parity — the three TRIAGE entries that covered the collapse are
	// gone. What remains on the orders body is the typed-literal
	// default rendering, a documented cosmetic policy.
	{
		Pattern:  "CREATE TABLE public.orders",
		Reason:   "integer defaults re-emit as quoted casts (DEFAULT '0'::bigint vs source DEFAULT 0) — evaluates identically on every insert, catalog-text different; operator-approved cosmetic divergence (2026-07-03)",
		Citation: "docs/type-mapping.md \"Typed-literal default rendering\"",
	},
	// The v0.100-readiness C3 UNIQUE-attribute cell: the source
	// constraint is UNIQUE NULLS NOT DISTINCT; sluice lands it as a
	// plain UNIQUE (strictly weaker — admits duplicate NULLs), the PG
	// schema reader WARNs loudly per affected constraint at read time,
	// and the attribute is carried on ir.Index for the faithful-carry
	// follow-up. This entry is the oracle-side pin of that documented
	// weakening: if the follow-up ships, the mismatch disappears and
	// this entry goes stale-loud (allowlist hits are logged every run).
	{
		Pattern:  "ALTER TABLE public.customers ADD CONSTRAINT customers_extref_nnd",
		Reason:   "UNIQUE NULLS NOT DISTINCT lands as plain UNIQUE — the C3 read-time WARN names the weakening; faithful same-engine carry is the filed follow-up",
		Citation: "docs/dev/roadmap.md \"UNIQUE-constraint attribute fidelity\"",
	},
	// The PK-constraint-NAME cell (audit 2026-07-26 follow-up, roadmap item
	// 84). sluice emits PRIMARY KEY inline in CREATE TABLE, so Postgres
	// auto-names the constraint <table>_pkey and an explicitly-named source PK
	// loses its name. Harmless for enforcement — the key is identical — but it
	// breaks ON CONFLICT ON CONSTRAINT <name> and any DDL that names the
	// constraint. NOT fixed inline with the attribute work because the obvious
	// fix (emit CONSTRAINT <PrimaryKey.Name>) would emit CONSTRAINT PRIMARY for
	// a MySQL source, whose PK index is literally named PRIMARY — it needs a
	// source-engine-aware rule, not a one-liner. These entries keep the gap
	// VISIBLE in the ledger (allowlist hits are logged every run) instead of
	// letting the seed go quiet about it, and go stale-loud when item 84 ships.
	// NOTE on scope, learned the hard way (Bug 208): these entries match a
	// statement PREFIX, so an entry keyed on the constraint name swallows
	// everything that follows it on the line — including the DEFERRABLE
	// INITIALLY DEFERRED clause. The first cut of this pair masked a real
	// attribute defect: the oracle emitted the PK with its attributes and
	// sluice emitted it without them, and the ledger stayed green because the
	// allowlist had already excused the whole line on NAME grounds.
	//
	// So the attribute-bearing table (attr_defpk) is NOT allowlisted at all
	// any more — its PK is named to PG's own default so no name divergence
	// arises and the attribute axis is compared for real. The name gap keeps a
	// dedicated table of its own, carrying NO attributes, so excusing its line
	// cannot hide anything else.
	//
	// General rule for this file: an allowlist entry must be narrower than the
	// property it excuses. If the entry's prefix can absorb an unrelated
	// difference, it will eventually absorb a defect.
	{
		Pattern:  "ALTER TABLE public.attr_namedpk ADD CONSTRAINT attr_namedpk_pkey",
		Reason:   "an explicitly-named source PRIMARY KEY is re-emitted inline, so PG auto-names it <table>_pkey; this table carries NO constraint attributes, so the excused line cannot hide an attribute divergence",
		Citation: "docs/dev/roadmap.md item 84 (PK constraint-name fidelity)",
	},
	{
		Pattern:  "ALTER TABLE public.attr_namedpk ADD CONSTRAINT attr_namedpk_pk",
		Reason:   "oracle side of the same cell: pg_dump emits the source constraint name sluice did not preserve",
		Citation: "docs/dev/roadmap.md item 84 (PK constraint-name fidelity)",
	},
	// The serial → identity modernization trio (docs/type-mapping.md
	// "Sequences and serial columns"): pg_dump restores a classic
	// serial column as CREATE SEQUENCE + OWNED BY + SET DEFAULT
	// nextval(...), while sluice deliberately emits an identity
	// column. Insert behaviour is identical; the catalog text is not.
	{
		Pattern:  "CREATE SEQUENCE public.legacy_counters_id_seq",
		Reason:   "oracle side only: the backing sequence pg_dump re-creates for the classic serial column sluice modernizes to identity",
		Citation: "docs/type-mapping.md \"Sequences and serial columns\"",
	},
	{
		Pattern:  "ALTER SEQUENCE public.legacy_counters_id_seq OWNED",
		Reason:   "oracle side only: the OWNED BY binding of the serial backing sequence (counterpart of the CREATE SEQUENCE legacy_counters_id_seq entry)",
		Citation: "docs/type-mapping.md \"Sequences and serial columns\"",
	},
	{
		Pattern:  "ALTER TABLE public.legacy_counters ALTER COLUMN id",
		Reason:   "the serial column's default: oracle side SET DEFAULT nextval('legacy_counters_id_seq'), sluice side ADD GENERATED BY DEFAULT AS IDENTITY — the modernization's catalog-visible face",
		Citation: "docs/type-mapping.md \"Sequences and serial columns\"",
	},
	// TRIAGE #3 (bare-timestamptz precision materialization: the
	// customers + shipments entries) and TRIAGE #4 (UNIQUE constraints
	// demoted to unique indexes: the *_unique entry pair) are CLOSED —
	// ir temporal PrecisionUnspecified + ir.Index.ConstraintBacked
	// carry both shapes at FULL parity. Zero TRIAGE entries remain.
}
