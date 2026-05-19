# ADR-0049 — implementation-readiness brief (pre-checkpoint)

> **Status: ADR-0049 is design-complete and implement-ready; this brief
> exists so the owner's interactive checkpoint is "answer 5 go/no-go
> questions", not "derisk from scratch".** Produced 2026-05-19 by the
> architect agent, **every load-bearing code claim re-verified against
> the tree in the main session** (the "trust code, not prose / pin the
> class" discipline). No code-vs-prose disagreement found; the ADR's
> Phase-1c "already faithful at the loud floor — efficiency upgrade,
> not a correctness emergency" reframe is accurate against the code.
> ADR-0049 is **not** demand-gated and has standalone resume-after-DDL
> value; it stays separate from but **hard-sequenced before** ADR-0050.

## 1. Design-completeness verdict

All three DPs are genuinely RESOLVED + owner-signed-off (2026-05-18).
Verified against code, not the doc:

- **DP-1 triggers are real and already detected today** (the ADR adds
  *anchoring*, not detection): MySQL binlog generic-DDL does
  `clear(r.schemaCache)` (`internal/engines/mysql/cdc_reader.go:552`);
  VStream FIELD populates `r.fields` / `dispatchDDL`→`clear(r.fields)`
  and the loud floor is the literal `"row event for %q without
  preceding FIELD event"` (`cdc_vstream.go:708`); PG pgoutput rebuilds
  `relations[…]` on `RelationMessage` (`postgres/cdc_reader.go` +
  `cdc_relations.go`).
- **Phase-1c test confirmed present and asserting what the ADR claims**:
  `internal/engines/mysql/cdc_vstream_schema_evolution_integration_test.go`,
  `//go:build integration && vstream`, "silent corruption == FAIL",
  FAITHFUL-or-LOUD for ADD/DROP/MODIFY. Not stale-doc.
- **DP-2 / DP-3** fully specified and recorded symmetrically in
  ADR-0050; backup-envelope seam exists (`internal/ir/backup.go`
  tagged-union manifest, additive `BackupFormatVersion`).

### Three latent IMPLEMENTATION ambiguities (not reopening design DPs)

1. **History payload serialization.** The IR's sealed `Type` /
   `DefaultValue` can't survive plain `encoding/json` — the only
   proven serializer is the backup tagged-union codec
   (`internal/ir/backup.go:21-27`). *Recommendation: reuse it verbatim
   as the `sluice_cdc_schema_history` payload format* (DP-2 already
   mandates history be part of that same envelope).
2. **The reader does not carry a post-DDL `ir.Table` at the boundary
   today — only the cache-clear signal.** Binlog reconstructs lazily
   on next row; **VStream holds `[]*query.Field` (Vitess proto), NOT
   an `ir.Table`** → a new `[]*query.Field → ir.Table` projector is
   load-bearing new code (the single largest new surface); PG's
   `relationCacheEntry` is already IR-typed (least new code). The
   snapshot MUST be built from in-stream position-anchored metadata,
   never re-introspection (the ADR explicitly rejects re-introspection).
3. **Position ordering for `resolve()` / retention.** `ir.Position`
   is engine-opaque; `resolve(pos)` and the DP-2 floor need an
   *ordering*, not a string compare. `ir.ErrPositionInvalid`
   (`internal/ir/change.go:34`) is the existing loud-floor sentinel to
   reuse. *Recommendation: an engine-supplied "is P ≤ anchor A"
   predicate, a new optional engine interface mirroring the existing
   `verifyPositionResumable` / `verifyGTIDSetReachable` pattern.*

## 2. Phased chunk breakdown (dependency order)

`A → (B1 ∥ B2 ∥ B3 after A) → C → D → E`. Each B is independently
shippable behind the unchanged loud floor (correctness never depends
on a later chunk — validate-end-to-end-before-building tenet).

| Chunk | Scope | Key files | Concurrency? |
|---|---|---|---|
| **A** | additive `sluice_cdc_schema_history` table + IR-schema serialization + `resolveSchemaVersion(...)` with below-floor → `ErrPositionInvalid` loud refuse | `engines/{mysql,postgres}/control_table.go`, `internal/ir/backup.go` codec | No |
| **B1** | MySQL binlog QUERY-event → true-delta snapshot keyed by the **DDL event's own GTID** | `mysql/cdc_reader.go`, `change_applier.go` | Borderline → treat as concurrency |
| **B2** | VStream FIELD-delta → snapshot; **new `[]*query.Field → ir.Table` projector**; uniform regardless of Vitess schema-tracking | `mysql/cdc_vstream.go` + new `field_to_ir.go` | Yes |
| **B3** | PG pgoutput Relation-delta → snapshot (relationCacheEntry already IR-typed) | `postgres/cdc_reader.go`, `cdc_relations.go` | Yes |
| **C** | hot-path active-version cache + boundary swap, **O(1) amortised** (resolve called O(#boundaries), not O(#rows)) | `internal/pipeline/streamer.go`, per-engine `change_applier.go` | Yes — strict `-race`-before-tag |
| **D** | backup-envelope inclusion (append-only, no format bump) + retention floor = `min(ADR-0007 safe-point, oldest retained backup resume pos)` | `internal/ir/backup.go`, `internal/pipeline/backup.go`+restore | No |
| **E** | full cross-engine + regression-pin consolidation | (test-only) | gate before tag |

## 3. Hot-path checkpoint decisions (concrete, options+tradeoffs)

- **HP-1 — where the active version lives.** (a) reader-side cache
  (local swap, but widens the `ir.Change` contract to carry resolved
  schema) vs **(b, recommended)** applier-side cache keyed by event
  `Pos()` vs next anchor (one query per DDL, amortised O(1), no IR
  contract change, matches applier-owns-control-table pattern).
- **HP-2 — history-write failure is fatal/loud, not logged-and-
  continued.** A lost history version silently degrades future resume
  → per the loud-failure/zero-users tenet, **hard-fail the stream**.
  Owner ratifies.
- **HP-3 — anchor = the DDL/FIELD/Relation event's OWN position,
  captured at detection**, not the first subsequent row's position.
  Binlog subtlety: `clear(r.schemaCache)` is eager but the `ir.Table`
  rebuilds lazily on the next `tableFor` — key the version with the
  QUERY-event GTID captured at clear time, else replay between DDL and
  first post-DDL row silently resolves to the *old* schema (the exact
  bug class this ADR kills).
- **HP-4 — the history-version write MUST be in the same target tx as
  the ADR-0007 position write** (`writePositionTx` call sites). A
  cross-tx crash persists a position whose schema version isn't
  durable → unwanted cold-start. This makes B1/B2/B3/C concurrency
  chunks under the `-race`-before-tag rule.

## 4. Test matrix (pin the class, not the representative)

Family = **{binlog, VStream, pgoutput} × {ADD, DROP, MODIFY, RENAME} ×
{steady-state, resume/replay-across-boundary, restore-from-backup-
across-boundary, compaction-floor refuse}.**

- **Unit (`-race` on CI):** serialization round-trip per `ir.Type`
  family (reuse the backup-codec matrix); `resolve()` ordering
  (before/exact/between/after/below-floor→`errors.Is ErrPositionInvalid`);
  true-delta (no-op ALTER → zero versions); O(1) assertion (resolve
  call-count == boundary-count).
- **Integration (`integration` tag, `-race` on CI Linux):** per
  engine × DDL kind for all four scenarios; the replay-across-boundary
  correctness pin (events between DDL-anchor and first post-DDL row
  decode against the *post*-DDL version); compaction-floor → loud
  `ErrPositionInvalid` → ADR-0022 cold-start executes.
- **VStream:** extend `cdc_vstream_schema_evolution_integration_test.go`
  (`integration && vstream`) — add the history-version assertion
  alongside the existing FAITHFUL/LOUD verdicts; add RENAME (not
  currently covered).
- **Cross-engine headline pin:** MySQL→PG mid-stream ALTER on source →
  resume-after-DDL no longer forces a whole-stream re-snapshot.

**Regression pins that MUST stay green:** the Phase-1c VStream test
(loud-floor + FAITHFUL); `cdc_reader` GTID-position-loss + node-replace
`verifySourceInstanceIdentity` (DP-2 refuse must compose with, not
bypass, the existing floor); ADR-0007 `writePositionTx` atomicity
(HP-4 extends that tx); ADR-0034 `live_added_tables` control-table
migration (new additive table must not perturb `ensureControlTable`).

## 5. ADR-0050 sequencing gate

**ADR-0050 implementation MUST NOT start until ADR-0049 Chunks A–D are
landed + green** (ADR-0049 DP-3 / ADR-0050 DP-3+Status gate-2; 0050
DP-3 correctness is contingent on 0049 DP-1 per-engine boundary
detection being live in code, especially VStream-tracking-OFF via
FIELD-delta; D is needed for 0050's restore-then-reconcile
consistency). Encode as a tracked blocker + a one-line note in
ADR-0050 Status/next referencing the chunk IDs (docs-only, on a `main`
worktree if a subagent is active).

## 6. Sizing & release shape

~1,400–1,900 LOC prod + ~1,500–2,000 LOC tests. **Several releases,
not one** — A (minor: new durable control table + engine surface);
B1/B3 patch-or-grouped-minor; B2 likely its own (new VStream projector
+ `vstream`-tagged test cost); C minor (O(1), Consequences-mandated);
D minor (additive backup format). Own feature branch, independent of
#37; one PR per chunk, squash-merge per chunk (linear history).
Concurrency chunks (B1/B2/B3/C) follow `-race`-integration-gate
**before** the tag (push, wait for CI Integration green, then tag — not
tag-then-watch; the v0.67.0 retag-trap class).

## Owner checkpoint ask (the 5 go/no-go decisions)

1. History payload format = the existing `internal/ir/backup.go`
   tagged-union JSON codec, reused verbatim? (ambiguity #1)
2. Acknowledge the VStream `[]*query.Field → ir.Table` projector is
   in-scope new load-bearing code; snapshot is built from in-stream
   position-anchored metadata, never re-introspection? (#2 / HP-3)
3. Ratify position ordering as an engine-supplied predicate (new
   optional engine interface, mirroring `verifyPositionResumable`),
   not a generic token compare? (#3)
4. Ratify same-target-tx for the history write (HP-4) + history-write
   failure is fatal/loud (HP-2) + anchor = the boundary event's own
   position captured at detection (HP-3)?
5. Approve the chunk sequencing + the explicit ADR-0050 A–D blocker?

With these five answered, **Chunk A can begin immediately** — design
is complete, all code seams verified present and behaving as the ADR
describes.

## References

- `docs/adr/adr-0049-cdc-schema-history.md` (design, all DPs resolved)
- `docs/adr/adr-0050-reconciling-resnapshot.md` (hard-sequenced after)
- `docs/adr/adr-0007-position-persistence.md`, `adr-0030`, `adr-0034`
  (control-table additive pattern + position atomicity this builds on)
- `docs/dev/notes/prep-planetscale-vitess-readiness.md` §"Phase 1c"
  (the empirical evidence DP-1 rests on)
