# ADR-0127: Per-sync MySQL zero-date (and sql_mode) via DSN params

## Status

**Accepted (2026-06-26).** Roadmap item 47 deferred-polish — the second of the two
process-global gaps ADR-0122 §Status deferred (the sibling per-sync PlanetScale
telemetry shipped as ADR-0126). Makes the MySQL **zero-date policy** configurable
**per sync** (per source) instead of process-global, and **documents that `sql_mode`
is already per-sync** via the DSN.

## Context

Two MySQL knobs are process-global today, set once at `cmd/sluice/main.go` from the
top-level CLI flags:

- `--mysql-sql-mode` → `mysql.SetSessionSQLMode` → the package global
  `sessionSQLMode` (`connect.go`), forced on every MySQL session.
- `--zero-date={error,null,epoch}` → `mysql.SetZeroDateMode` → the package global
  `zeroDatePolicy` (`value_decode.go`), read on the temporal-decode path
  (`applyZeroDatePolicy`, called from `row_reader.go`, `cdc_reader.go`, and
  `cdc_vstream.go`).

In a `sync run` fleet every MySQL-source sync shares both — two syncs that need
different zero-date handling (e.g. one legacy source with `0000-00-00` sentinels to
map to NULL, another to refuse) cannot coexist. The MySQL `Engine` is looked up
generically and `OpenRowReader(ctx, dsn)` carries no per-instance config channel
**except the DSN** — which is exactly how the existing per-connection knobs already
travel (`copy_table_parallelism`, the `vstream_*` family) via the `nativeSluiceParams`
allowlist + `stripVStreamParams` strip.

Crucially, **`sql_mode` is already per-sync today**: `connect.go` only applies the
global `sessionSQLMode` when the DSN does not specify `sql_mode`, so a per-sync source
DSN `?sql_mode='...'` already wins. The real gap is **zero-date**, which has no
per-connection escape hatch — and it lives on the value-decode path, so it is the
value-fidelity-sensitive part (Bug-74 territory).

## Decision

1. **`zero_date` DSN param on the MySQL source** (values `error` | `null` | `epoch`),
   the per-sync mechanism — mirroring `sql_mode`'s DSN path and the
   `copy_table_parallelism` precedent. It is a sluice-specific param: add it to
   `nativeSluiceParams` so it is **stripped before the MySQL session** (never emitted
   as a bogus `SET`). Each reader (vanilla row-reader, native CDC reader, VStream
   reader) parses it at construction into a **per-reader `zeroDateMode`**, and the
   temporal-decode call sites pass that instance mode to `applyZeroDatePolicy`.

2. **The process-global `--zero-date` becomes the default, not the only setting.** A
   reader whose source DSN omits `zero_date` uses the global `zeroDatePolicy` (set from
   `--zero-date`) exactly as today — so the change is backward-compatible and the
   default path is byte-identical. The DSN param overrides the global per reader.

3. **`applyZeroDatePolicy` takes the mode as a parameter** instead of reading the
   global. This is the value-path change; it is mechanical (thread the per-reader mode
   to the 4 call sites) and must be pinned across the full family (below).

4. **Fleet ergonomics: a `zero-date` `SyncSpec` key** (and documenting `mysql-sql-mode`
   via the source DSN) so a `sync run` operator sets it per sync in `syncs.yaml`
   without hand-editing DSN query strings — the key resolves to the source DSN's
   `zero_date` param. (The DSN param is the foundational mechanism; the key is sugar
   over it, validated to one of the three values at config load.)

5. **`sql_mode` stays DSN-only, documented.** No code change for sql_mode — the DSN
   `?sql_mode=` path already makes it per-sync. The fleet docs + the `--mysql-sql-mode`
   flag help state that per-sync sql_mode is set via the source/target DSN.

## Consequences

- A MySQL-source sync (standalone or in a fleet) can set its own zero-date policy via
  `?zero_date=null` on the source DSN (or the `zero-date:` fleet key), independent of
  other syncs in the same process — closing the process-global gap. The default
  (`--zero-date`, default `error`) is unchanged for any sync that doesn't set it.
- It is a **value-path change** (the temporal-decode policy), so it ships only with the
  full value-fidelity pin matrix and a value-fidelity review (see below). No silent
  behavior change: an absent param means today's behavior exactly.
- No engine-interface change, no new global; the per-reader mode is parsed from the DSN
  the reader already receives, stripped before the MySQL session like the other
  sluice-specific params.

## Value-fidelity requirement (load-bearing — the Bug-74 lesson)

`applyZeroDatePolicy` is on the temporal value path and is reached from THREE reader
paths (vanilla `row_reader.go`, native `cdc_reader.go`, VStream `cdc_vstream.go`). The
pins MUST cover the **class, not a representative**: every temporal family that can
carry a zero/partial date — `DATE`, `DATETIME`, `DATETIME(6)`, `TIMESTAMP`,
`TIMESTAMP(6)` — × every zero/partial **shape** (`0000-00-00`, `YYYY-00-00`,
`YYYY-MM-00`, and the datetime analogues) × each **policy** (`error` refuses loudly /
`null` → SQL NULL, refused on NOT NULL / `epoch` → the representable floor) — on **both**
the vanilla row-decode path **and** the VStream path. Plus: the DSN param overrides the
global; an absent param falls back to the global default; and **two readers with
different modes in one process do not interfere** (the per-sync isolation this ADR
adds). A `value-fidelity-reviewer` pass is required before this lands.

## Alternatives considered

- **Thread a per-instance config struct through the `ir.Engine` interface.** Rejected:
  a sprawling interface change across every engine for one MySQL knob, when the DSN is
  the established per-connection config channel and already carries sql_mode.
- **Leave zero-date process-global, document the limitation.** Rejected: it's a real
  per-sync gap for heterogeneous legacy-MySQL fleets, and the DSN-param fix is contained.
- **A fleet-only `zero-date` key with no DSN param.** Rejected: the DSN param also
  benefits `sync start` and `migrate`, and is the uniform mechanism; the fleet key is
  sugar over it.
