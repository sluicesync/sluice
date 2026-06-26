# sluice v0.99.140

**New: per-sync MySQL zero-date policy via a `zero_date` DSN param — the zero/partial-date handling (`error`/`null`/`epoch`) is no longer process-global, so a `sync run` fleet can give each MySQL source its own policy (ADR-0127, roadmap item 47). This closes the last of the two process-global gaps ADR-0122 deferred. Opt-in and backward-compatible; fully drop-in over v0.99.139.**

## Features

**Per-sync MySQL zero-date policy (`zero_date` DSN param, ADR-0127).** The MySQL zero/partial-date policy — how sluice handles legacy `0000-00-00` / `YYYY-00-00` / `YYYY-MM-00` values (`error` refuse / `null` / `epoch`, from the v0.99.19 loud-failure work) — was set once process-wide from `--zero-date`, so a `sync run` fleet couldn't give two MySQL sources different handling. It is now configurable **per source** via a sluice-specific `zero_date` DSN param on the MySQL source (e.g. `?zero_date=null`), mirroring how `sql_mode` already travels on the DSN and the `copy_table_parallelism` precedent. The param is stripped before the MySQL session (it never becomes a bogus `SET`), parsed once per reader, and threaded to the temporal-decode policy. A `sync run` fleet also gains a per-sync `zero-date` config key (validated to `error|null|epoch` at config load, refused on a non-MySQL source) that folds into the source DSN.

`--zero-date` remains the process-wide **default**: a reader whose DSN omits the param behaves byte-identically to before. A `zeroDateInherit` zero-value sentinel means "use the global default" at every reader construction site, so no path can silently flip the policy (the v0.99.51 zero-value-trap guard), and an invalid `?zero_date=bogus` is refused loudly at reader construction.

**Documented (no code change): per-sync `sql_mode`** is set the same way — via the source/target DSN `?sql_mode=` — which has always overridden the process-global `--mysql-sql-mode` per connection.

## Compatibility

Opt-in and backward-compatible: with no `zero_date` DSN param (the default), behavior is byte-identical to v0.99.139 — the process-global `--zero-date` still applies. This is a value-path change (the temporal-decode policy), shipped under the full Bug-74 family matrix — DATE / DATETIME / DATETIME(6) / TIMESTAMP / TIMESTAMP(6) × every zero/partial shape × each policy, on **both** the vanilla row-decode and the VStream paths — plus per-reader isolation pins (two readers, different modes, no interference), a real-MySQL integration pin, and an independent value-fidelity review. No flag defaults move. Fully drop-in over v0.99.139.

## Who needs this

Operators running a `sync run` fleet (or multiple `sync start` processes) against more than one MySQL source that need *different* zero-date handling — e.g. one legacy source whose `0000-00-00` sentinels should map to NULL, another that should refuse — can now set `?zero_date=...` per source (or the per-sync `zero-date` key) instead of a single process-wide `--zero-date`. Everyone else is unaffected; the global default is unchanged.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.140 · **Container:** ghcr.io/sluicesync/sluice:0.99.140
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
