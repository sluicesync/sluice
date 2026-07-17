# ADR-0172: VStream partial-row-image belt (NOBLOB/MINIMAL through the Vitess door)

## Status

**Accepted (2026-07-17).** Roadmap item 74, filed from the Bug-193 review (N4). Extends the vanilla-binlog partial-row-image discipline (Bug 193 — `SLUICE-E-CDC-ROW-IMAGE-PARTIAL`, `internal/engines/mysql/cdc_row_image_preflight.go`) to the VStream reader. Pinned by unit tests across the row-image-posture matrix, both directly on the belt and through the real `dispatch` path. The vendored Vitess proto/tablet semantics this ADR depends on were ground-truthed against `vitess.io/vitess@v0.24.2` (the module version in `go.mod`).

**Concurrency note:** this touches the VStream CDC dispatch path (the streaming goroutine that turns `VEvent`s into `ir.Change`s). The `-race` Integration job is CI-only (the dev box is CGO=0), so it **must pass `-race` before any tag**.

## Context

The vanilla MySQL binlog reader refuses partial binlog row images two ways (Bug 193): a stream-start preflight of `@@GLOBAL.binlog_row_image` (`preflightBinlogRowImage`) and a defense-in-depth belt on the rows-event dispatch (`refusePartialRowImage`, reading go-mysql's per-image `SkippedColumns`). Under `binlog_row_image=MINIMAL`/`NOBLOB` an UPDATE image omits columns; sluice's applier writes every column it decodes, so a partial image silently loses or corrupts UPDATEs while the stream stays green and row counts stay equal. See ADR context in `cdc_row_image_preflight.go` for the full rationale.

The VStream reader (`cdc_vstream.go`, used by the PlanetScale/Vitess flavors) is a **separate** reader that never touched either belt. It reaches the identical silent-loss class through a different door:

- Vitess 16+ supports NOBLOB on the underlying mysqlds via the experimental `VReplicationExperimentalFlagAllowNoBlobBinlogRowImage` flag. When set, a tablet's `vstreamer` emits a partial UPDATE after-image and marks which columns are present in the `binlogdata.RowChange.DataColumns` bitmap.
- **PlanetScale (the managed flavor) pins `binlog_row_image=FULL`**, so `DataColumns` is never populated there — only **self-hosted Vitess** reaches this cell.

**The ground-truth mechanism of the loss** (traced through `vitess.io/vitess@v0.24.2`):

1. `binlogdata.RowChange.DataColumns` is a `RowChange_Bitmap{Count, Cols}`. Its proto comment: *"a bitmap of all columns: bit is set if column is present in the after image."* `Count` is the table's full column count; a **set** bit ⇒ present, an **unset** bit ⇒ omitted.
2. Bit order is vttablet's `isBitSet` (`table_plan_partial.go`): `byte = index/8`, `mask = 1 << (index % 8)` — little-endian within each byte.
3. Vitess populates `DataColumns` **only** when the after image is genuinely partial (the NOBLOB flag is on *and* a column was dropped) or when there are partial-JSON values. On a FULL stream the field is `nil`. (Without the flag, a partial image makes the tablet's `getValues` abort the stream loudly — *"partial row image encountered: ensure binlog_row_image is set to 'full'"* — so that case is already loud on the Vitess side.)
4. Critically, for an omitted column vttablet leaves the zero `sqltypes.Value{}` in the row, which `RowToProto3` encodes with **length −1** (the NULL-cell wire encoding). sluice's `decodeVStreamRow` has no bitmap and reads a −1 length as SQL NULL — so an omitted, still-present column would be emitted as though it changed to NULL. On UPDATE apply that writes NULL over the real value: **silent corruption**, exactly Bug 193, one door over.

A companion signal, `binlogdata.RowChange.JsonPartialValues`, is the same class one variable over: under `binlog_row_value_options=PARTIAL_JSON` a JSON column's after value is a `JSON_INSERT/REPLACE/REMOVE` diff expression, not the value — applying it verbatim corrupts the document. (This is the VStream analogue of the vanilla path's `partialJSONUpdatesError`.)

## Decision

Add a decode-time belt, **not a preflight.** A preflight is structurally impossible for VStream: sluice connects to a **vtgate**, not to the underlying mysqlds. A self-hosted Vitess is a fleet of tablets, each with its own mysqld and its own `binlog_row_image`, and vtgate exposes no aggregate row-image posture to probe. The faithful, always-available signal is the one Vitess already carries on the wire — the `DataColumns`/`JsonPartialValues` bitmaps.

`refuseVStreamPartialRowImage(rc, fields, schema, table)` (new file `cdc_vstream_partial_row_image.go`) runs **per `RowChange` in `dispatchRow`, before decode**:

- `DataColumns.Count > 0` and any bit in `[0, Count)` is **unset** ⇒ a column was omitted from the after image (NOBLOB) ⇒ refuse with `SLUICE-E-CDC-ROW-IMAGE-PARTIAL`, naming the first omitted column.
- `JsonPartialValues.Count > 0` and any bit is **set** ⇒ a partial-JSON diff value ⇒ refuse (same code), naming the column.
- Otherwise (the common FULL case: both bitmaps `nil`/empty) ⇒ `nil`, the cheap fast path.

The two arms are checked independently and are cleanly disjoint: Vitess sets `DataColumns` to a **full** (all-bits-set) bitmap alongside `JsonPartialValues` on a partial-JSON row, so a partial-JSON row has no unset `DataColumns` bit, and a NOBLOB row has no set `JsonPartialValues` bit.

### Refuse, not carry-forward

For an omitted column the faithful alternatives are (a) carry the prior value forward or (b) refuse. Carry-forward is impossible from the event alone: NOBLOB omits the unchanged BLOB/TEXT from the **before** image too, so the prior value is not in the `RowChange` — recovering it would mean reading the target, i.e. replica-apply semantics sluice deliberately does not attempt (the same reasoning that made the vanilla binlog UPDATE arm a refusal rather than a Bug-88-style narrowing). So the belt refuses loudly. NOBLOB omits columns only from **UPDATE** after-images (INSERT and DELETE log every column — there is no "unchanged" for them), so refusing on the after image covers the entire NOBLOB class; the paired partial before-image — for which Vitess hands sluice **no** bitmap — is moot once the UPDATE is refused outright.

### Reuse the existing code, not a Vitess-specific one

The belt reuses `sluicecode.CodeCDCRowImagePartial` rather than minting a new code. It is the same silent-loss class (a partial row image the target can't faithfully hold), and an operator grepping the code finds one entry covering both the binlog and VStream doors. The registry description and `docs/operator/error-codes.md` row were broadened to name the VStream/`DataColumns` path and the self-hosted-Vitess remedy (`binlog_row_image=FULL` on the cluster's mysqld tablets); the bidirectional doc-sync test (`TestRegistryDocSync`) stays green.

## Consequences

- **A self-hosted Vitess running NOBLOB now stops loudly** at the first partial UPDATE with a coded, column-naming refusal, instead of silently NULL-ing an unchanged blob. The stream is fatal (the error propagates out of `dispatchRow` — warm resume — **and its hand-mirrored cold-start twin `dispatchCDCRow`, the default first sync**; the belt was wired into both by v0.99.273 after the 2026-07-17 confirming audit found it missing from the cold-start path, audit A1); recovery is `binlog_row_image=FULL` on the source tablets, then restart (a fresh cold start when the partial-image window's UPDATEs matter).
- **No behavior change for PlanetScale or any FULL stream.** `DataColumns` is `nil` on a FULL image, so the belt's fast path returns `nil` and the decode path is byte-identical to before. Pinned by the full-bitmap and no-bitmap pass-through cases.
- **`PARTIAL_JSON` through VStream is now refused too**, matching the vanilla path's coverage.
- The belt is a per-`RowChange` bitmap scan (`O(columns)` only when a bitmap is present, which is never on the common path) — negligible cost.

## Alternatives considered

- **Probe `binlog_row_image` via vtgate at stream start.** Rejected: vtgate routes such a query to a single arbitrary tablet, does not represent the fleet, and gives a false sense of a global that does not exist for a self-hosted cluster. The wire bitmap is authoritative per-row and needs no probe.
- **Carry the omitted value forward from the before image.** Rejected: NOBLOB omits it from both images (§ "Refuse, not carry-forward").
- **A Vitess-specific error code.** Rejected: identical silent-loss class; one code across both doors is better operator ergonomics (§ "Reuse the existing code").
- **Trust vttablet's own "ensure binlog_row_image is set to 'full'" abort.** That abort fires only when the NOBLOB experimental flag is **off**. With the flag on — the exact configuration this ADR guards — vttablet does *not* abort; it emits the partial image with `DataColumns`, and only sluice's belt catches it.

## Testing

Unit pins (`cdc_vstream_partial_row_image_test.go`), the row-image-posture matrix (the "pin the class, not the representative" discipline applied to posture rather than type family, since the code path is posture-dispatched):

- **Bit helpers** ground-truthed against vttablet `isBitSet` semantics (`bitmapBitSet`, `firstUnsetBit`, `firstSetBit`), incl. out-of-range/`nil` reading UNSET (safe/loud direction).
- **Belt directly** across: no bitmap (FULL) → pass; full bitmap → pass; leading-/middle-/trailing-column omitted → refuse, naming the first omitted column; partial-JSON with a full `DataColumns` bitmap → refuse.
- **Through the real dispatcher** (`dispatch` → `dispatchRow`): a partial UPDATE stops loudly with the coded error and **emits nothing** (no half-applied change on the channel); a FULL INSERT/UPDATE/DELETE sequence flows unchanged, with the unchanged BLOB carrying its real value (proof the belt does not over-fire).

**Residual risk / follow-up (SHIPPED):** the unit pins construct the exact wire shape Vitess produces (NULL-cell after image + packed bitmap); the full cluster-level validation — a self-hosted Vitess-24 mysqld set to `binlog_row_image=NOBLOB` with the `AllowNoBlobBinlogRowImage` experimental flag, streamed end-to-end — landed as the gated `vitesscluster` NOBLOB suite (`cdc_vstream_cluster_noblob_integration_test.go`, commit `7d680dec`), confirmed green in the extended-suites CI run: the belt fires loudly on BOTH dispatch paths (`dispatchRow` warm-resume + `dispatchCDCRow` cold-start) and does not over-fire on FULL images, with a `@@global.binlog_row_image=NOBLOB` provisioning guard so a regression can't pass vacuously. PlanetScale/FULL streams are unaffected and already covered by the existing VStream integration suite.
