# sluice v0.99.174

**Two SQLite/D1 improvements: sluice now translates the portable subset of carried SQLite generated-column / CHECK / index expressions to Postgres/MySQL (instead of loud-failing them), and trigger-CDC syncs can bound their own change-log growth automatically with `--auto-prune-change-log`.**

## Added

**SQLite/D1 → canonical expression translator (ADR-0133 follow-up).** ADR-0133 carries SQLite generated columns, CHECK constraints, and partial/expression indexes into the schema, but emitted their bodies verbatim — so any non-portable SQLite construct made the migration loud-fail at CREATE-TABLE time. sluice now translates the **provably-safe subset** to the target dialect: arithmetic/comparison/logical operators, `||` (→`CONCAT` on MySQL), PG-only integer `/`, `abs`, `coalesce`, `ifnull`, `nullif`, `length` (→`CHAR_LENGTH` on MySQL), 1-arg `trim` family, `substr` (literal start ≥ 1), `min`/`max` (MySQL only), `cast AS text/real`, `cast AS numeric` (PG only), and current-instant keywords. Anything outside the allowlist in a data-bearing generated-column or CHECK body is **refused loudly** (named), because a syntactically-valid-but-divergent operator is silently accepted by the target with a wrong result; a non-portable partial/expression index WARN-skips.

The allowlist is deliberately conservative. An independent value-fidelity review caught six silent-corruption vectors in the first cut — `cast AS numeric` landing as MySQL `DECIMAL(10,0)` (2.5 → 3), `min`/`max` → PG `LEAST`/`GREATEST` skipping NULLs, `cast AS integer` truncate-vs-round, `%` diverging on non-integers, `substr` negative-start, and MySQL `/`·`%` being accepted verbatim with the wrong result — all excluded or made loud, and pinned by **src==dst value-ground-truth integration tests** on real PG + MySQL (the target-recomputed stored generated column must equal SQLite's own computed value) plus loud-refuse pins for every excluded construct. `strftime`/`julianday`/date-time-with-args translation remains a documented follow-up.

**Automatic change-log pruning for trigger-CDC (`--auto-prune-change-log`, ADR-0137 Phase B).** Phase A shipped the operator-run `sluice trigger prune`; Phase B makes it automatic. With `--auto-prune-change-log` (opt-in, default off), a streamer sidecar prunes the source `sluice_change_log` on a cadence (`--auto-prune-interval`, default 5 min) up to the **target's durably-applied frontier minus `--auto-prune-keep`** — so a continuous `sqlite-trigger` / `d1-trigger` / `pgtrigger` sync bounds its own change-log growth (and, on D1, its billable rows-written/storage) without scheduling a cron. The prune cut is `appliedLastID - keep`, never above the durable frontier (pruning higher would delete not-yet-applied rows → silent loss), and it is fully failure-isolated: a read or prune error is logged and swallowed, never stalling the sync.

## Compatibility

Both features are opt-in or additive. `--auto-prune-change-log` defaults off (no behavior change; a no-op for non-trigger sources). The expression translator only affects SQLite/D1-source migrations that carry generated-column/CHECK/index expressions: previously-loud-failing portable bodies now succeed; the non-portable tail is still loud (now refused at schema-emit rather than at target DDL). No flag removed, no default flipped.

## Who needs this

Anyone migrating a SQLite or Cloudflare D1 database whose schema uses generated columns / CHECK constraints / partial or expression indexes (the translator lands the portable ones instead of aborting), and anyone running a continuous trigger-CDC sync who wants the change-log to bound itself automatically.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.174 · **Container:** ghcr.io/sluicesync/sluice:0.99.174
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
