# ADR-0174: Row-level filtering Phase 3 — continuous filtered sync on MySQL & Vitess/VStream

## Status

**Accepted (2026-07-17). Both pieces implemented** — Piece 1 (faithful collation-aware eval, commit `8655b27c`) and Piece 2 (VStream server-side filtered sync, commit `9d89cd7c`). Extends ADR-0173 (row-level `--where`). Closes the two gaps that live testing of the region-split use case surfaced: continuous `sync --where` refused on MySQL-family sources for two distinct reasons, and this ADR makes it *work* — faithfully — rather than refuse.

**Implementation note — the VStream COPY opens eagerly, so the predicate is threaded at OPEN, not via a post-open setter.** The design anticipated `RowFilterSetter` carrying the predicate into the copy phase; in fact the VStream COPY sends its filter rules to vtgate in the constructor, *before* the pipeline's post-open `ApplyRowFilters` runs — so a post-open setter would leave the first table's COPY unfiltered (a silent leak). Fixed by threading the predicate through two new `ir` surfaces — `FilteredSnapshotOpener` / `FilteredSnapshotResumer` (`OpenSnapshotStreamForTablesFiltered` / `…FromPositionFiltered`) — dispatched when `--where` is set; `RowFilterSetter` on the VStream snapshot reader is a documented no-op that only satisfies the capability gate, with compile-time asserts guarding the method set. PG/vanilla-MySQL are unaffected (their snapshot readers filter lazily per read, so the post-open gate still carries their predicate).

**Move-out proven safe on a real Vitess-24 cluster** (new `vitesscluster`-tagged suite): the filtered COPY excludes out-of-scope rows server-side; a move-IN UPDATE arrives with both images → target INSERT; a move-OUT UPDATE arrives with **both** images (before in-scope / after out-of-scope) → target DELETE. The premise-falsifying case (move-out dropped/reshaped by VStream) did not trigger — the vendored-Vitess `processRowEvent` both-images behavior held. The universal silent-loss floor is the pipeline `route()` before-image guard: any filtered UPDATE/DELETE whose before-image omits a predicate-referenced column refuses (`SLUICE-E-WHERE-CDC-BEFORE-IMAGE`), so a partial before-image (source not `binlog_row_image=FULL`) can never silently mis-classify a move-out.

**Concurrency + value-fidelity chunk.** Touches the CDC apply path, both CDC readers, and a value-comparison codec. It MUST pass CI's `-race` Integration job before any release tag, and the collation core gets the Bug-74 family-matrix treatment (ground-truthed against a real MySQL), plus real-Vitess-cluster validation for the VStream path.

## Context

ADR-0173 Phase 2 shipped continuous filtered sync for **Postgres** and vanilla-MySQL **binlog** sources. Live validation (2026-07-17, PlanetScale Postgres + PlanetScale MySQL) confirmed Postgres works end-to-end — cold-start, move-in→INSERT, move-out→DELETE, sustained CDC — but surfaced **two** independent barriers on the MySQL side, both currently handled by a loud refusal:

1. **Case-insensitive string collations (all MySQL, incl. self-hosted).** The client-side CDC evaluator refuses a string `=`/`IN`/ordering when the column's collation is case- or accent-insensitive (MySQL's default), because a byte-exact client compare would diverge from the source's collation-aware `=` and silently leak or drop a row (`SLUICE-E-WHERE-CDC-UNSUPPORTED-PREDICATE`). The motivating `region = 'EU'` filter hits this on every default-collation MySQL column. Postgres is unaffected because its default collation is *deterministic* (byte-equality).

2. **VStream (`planetscale` driver) has no client-side row filter at all.** The Vitess VStream cold-start reader does not implement `ir.RowFilterSetter`, so `sync --where` refuses at the capability gate (`internal/pipeline/migcore/row_filter.go`) — regardless of predicate. (`migrate --where` works on `planetscale` because its migrate reader *does* implement the setter.)

Both refusals are *correct* under the loud-failure tenet — better to refuse than to silently corrupt a filtered target. This ADR upgrades "refuse" to "handle correctly," preserving the no-silent-leak guarantee.

**Two ground-truth findings make this tractable (both verified against vendored Vitess v0.24.2):**

- **VStream move-out is safe.** `vstreamer.processRowEvent` (vstreamer.go:1199) evaluates the filter on *both* the before- and after-image. For a **non-vindex** filter (a plain `--where`, no sharding-key term), if *either* image passes it forces `beforeOK = afterOK = true` and emits the RowChange with **both** images (lines 1224–1234). So a move-out (before matches, after doesn't) is **never dropped and never silently reshaped** — VStream delivers the full UPDATE with both images, and sluice applies its existing before×after row-move truth table → DELETE. This removes the silent-leak hazard that would otherwise gate the VStream design.

- **Faithful collation is reusable, not reimplemented.** Vitess compares with `evalengine.NullsafeCompare(v1, v2, collationEnv, collationID, nil)` (`go/vt/vtgate/evalengine/api_compare.go:151`) over its `collations.Environment` — and both packages are already transitive deps (`vitess.io/vitess`). sluice can evaluate a string comparison under the column's declared collation using the **source engine's own comparison code**, so its client-side `=` is byte-for-byte what MySQL/Vitess would compute. No collation reimplementation, no fidelity guesswork.

## Decision

Two composable pieces. Piece 1 is the foundation (both CDC paths re-evaluate the predicate client-side to classify move-in/out); Piece 2 adds the VStream path on top.

### Piece 1 — Faithful collation-aware client-side evaluation

Replace the blanket ci/ai-collation refusal in `internal/rowpredicate` with real collation-aware comparison:

- **MySQL-family sources** (mysql, planetscale, vitess, mariadb): compare strings via Vitess's `collations.Environment` + `evalengine.NullsafeCompare`, keyed on the column's declared collation (already carried on the IR string type). The result is identical to the source's `=`/`IN`/ordering — including case- and accent-insensitivity — so a `region = 'EU'` filter matching `Eu`, `EU`, `eu` is classified exactly as the source would. The evaluator's SQL three-valued logic is preserved (NULL → UNKNOWN → not-matching, never widening scope).
- **Postgres / SQLite sources:** the deterministic-default `=` is byte-equality (unchanged, already faithful). A **non-deterministic** collation (PG ICU `… (deterministic = false)`) that sluice cannot reproduce still **refuses loudly** — the loud-failure floor is preserved for the genuinely-unreproducible case.
- **Still-refused cases stay refused:** an unknown/unmappable collation, a function/subquery, a tz-aware temporal comparison — anything sluice cannot prove it evaluates identically to the source — refuses at sync-start as before.

**Operator control — the "stricter behavior" opt-out.** Default is faithful CI when the collation is reproducible (the common, useful case). A `--where-strict-collation` flag forces the pre-0174 behavior: treat any non-deterministic-collation string comparison as unreproducible and refuse, for operators who want the strict byte-exact guarantee regardless. Zero-value-safe: the field defaults to *off* (faithful mode on), so every construction path (tests, broker, chain) gets the common default (the v0.99.51 trap).

### Piece 2 — VStream server-side filter + client-side row-move

- **Push the predicate into the VStream filter rule.** Extend `vstreamCopyFilterRules` so a filtered table's rule becomes `select * from <t> where <predicate>` (today it is `select * from <t>`). This filters the **copy phase** natively and reduces the streaming phase server-side. The restricted grammar (column op literal, `IN`, `IS [NOT] NULL`, `AND`/`OR`/`NOT`, parens) maps 1:1 to a Vitess `WHERE`; the predicate is rendered to Vitess SQL from the parsed `rowpredicate` AST (not string-concatenated from operator input).
- **Implement `ir.RowFilterSetter` on the VStream snapshot path** so the capability gate passes and the copy-phase rule carries the `WHERE`.
- **Expose the before-image.** VStream already delivers `RowChange.Before` for updates; implement `SetFullBeforeImageTables` on `vstreamCDCReader` to surface the un-narrowed before-image for filtered tables (the CDC intercept re-narrows to the key before the applier builds its WHERE, exactly as the binlog/PG path does).
- **Classify client-side via Piece 1.** sluice re-evaluates the predicate (faithful collation) on the before- and after-image to drive the SAME row-move truth table as Postgres/binlog — move-in→INSERT, move-out→DELETE, in-scope→apply, else drop. The server-side filter is an *efficiency* layer (less stream, filtered copy); correctness rests on the client-side classification, which Piece 1 makes faithful to what VStream itself filtered on.

**Why not trust VStream's filter alone for classification?** VStream emits both images whenever *either* matches but does not report `beforeOK`/`afterOK` separately, so sluice cannot read the move direction off the event — it must re-evaluate. Piece 1 guarantees that re-evaluation agrees with VStream's server-side decision, so the two never diverge.

### Snapshot ↔ CDC single-sourcing

Unchanged from ADR-0173: the same predicate scopes both legs. For VStream the snapshot (copy phase) and the streaming phase share the one filter rule, so they cannot drift.

## Consequences

- **`sync --where` works on the full engine matrix**: Postgres/PG-managed (shipped), self-hosted MySQL (Piece 1 unblocks string filters), and PlanetScale MySQL/Vitess (Piece 2). `migrate --where` is unchanged (already universal).
- **The collation core becomes a value-fidelity surface** — it decides row membership, so a wrong comparison is a silent leak/drop. It gets the Bug-74 family matrix (below) and reuses Vitess's own comparator to make divergence structurally impossible for MySQL-family sources.
- **New dependency surface (already in the module graph):** `vitess.io/vitess/go/mysql/collations` + `go/vt/vtgate/evalengine`, previously only reached through the VStream reader, are now imported by `internal/rowpredicate`. The tags-vet matrix must keep the non-integration build green.
- **The `SLUICE-E-WHERE-CDC-UNSUPPORTED-PREDICATE` surface shrinks** (fewer refusals) but never to zero — genuinely unreproducible predicates still refuse. The error message gains the `--where-strict-collation` context.
- **Message-clarity fix (bundled):** `row_filter.go`'s refusal says "ADR-0173 Phase 1 covers mysql and postgres sources," which misleads on `planetscale` (where `migrate --where` works). Reword to name the sync-vs-migrate distinction.

## Alternatives considered

- **Reimplement collation folding (`strings.ToLower` / `x/text/collate`).** Rejected as the primary path: `ToLower` is wrong for accent-insensitivity and locale tailoring (Turkish dotless-i, ß, …); `x/text/collate` reproduces the Unicode UCA but not necessarily MySQL's exact per-collation implementation. Reusing Vitess's own `evalengine`/`collations` is faithful *by construction* for MySQL-family sources — the only way to guarantee no divergence.
- **Trust VStream's server-side filter for the target op (no client re-eval).** Rejected: VStream reports "either image matched," not the direction, so sluice cannot distinguish move-in from move-out from in-scope-update off the event alone. Re-evaluation is required; Piece 1 makes it faithful.
- **Constrain VStream filters to the sharding key** (where Vitess's vindex path handles move-out via `hasVindex`). Rejected as too narrow — the use case (region/tenant/country) is rarely the vindex; the non-vindex both-images behavior is exactly what we need and is safe.
- **Leave the refusal; document "use `migrate --where`."** Rejected — that is the current state, and the operator's use case is *continuous* region-scoped replication, not a one-shot extract.

## Testing

- **Collation-equivalence family matrix (Bug-74 discipline), ground-truthed on a real MySQL.** For each supported collation (`utf8mb4_0900_ai_ci`, `utf8mb4_general_ci`, `utf8mb4_bin`, `utf8_general_ci`, `latin1_swedish_ci`, …) × comparison (`=`, `!=`, `IN`, ordering) × a corpus of hard pairs (ASCII case, Turkish dotless-i, ß/ss, accented vs base, emoji, trailing space), assert sluice's classification equals `SELECT … WHERE <pred>` run on a real MySQL of that collation. The pin covers **every family, not a representative** — the class, not the instance.
- **VStream row-move on a real Vitess cluster** (`vitesscluster` / gated build tags, via the `vitess-cluster-validator`): cold-start filtered copy lands only in-scope rows; a move-in UPDATE arrives and becomes a target INSERT; a move-out UPDATE arrives (both images) and becomes a target DELETE; an out-of-scope change never reaches the target. Ground-truthed against the source. This is the independent gate that the server-side filter + client classification compose correctly.
- **Strict-mode flag:** `--where-strict-collation` forces refusal on a non-deterministic-collation string comparison that faithful mode would accept; pinned through the real CLI parser (the Bug-180 CLI-layer lesson).
- **No-silent-leak regression:** a filtered sync over a ci-collation column that would previously refuse now runs and is byte/row-exact against the source's own `WHERE` — the pin that would have caught a naive-folding divergence.
- **Zero-value default:** every non-CLI construction path gets faithful mode (flag off), asserted so the v0.99.51 inversion can't recur.
