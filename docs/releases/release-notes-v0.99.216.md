# sluice v0.99.216

**Two `timestamptz` fixes from a fresh full-codebase audit — the last two silent-loss HIGH findings in the value core, both ground-truthed against a live Postgres 16. Continuous sync from a Postgres source on a fractional-offset server timezone (Asia/Kolkata +05:30, Kathmandu +05:45, St Johns -03:30, …) no longer aborts on the first `timestamptz` change (PROM-P1), and a slot-less `postgres-trigger` `timestamptz` applied to MySQL now stores the correct UTC instant instead of the source session's wall clock — closing a silent hours-off divergence from bulk-copied rows under a non-UTC writer session (PROM-M1). No behavior change for UTC sources or already-working paths.**

## Fixed

- **pgoutput CDC decodes fractional-offset `timestamptz` (PROM-P1).** Postgres renders a `timestamptz` zone offset as `±HH`, `±HH:MM`, or `±HH:MM:SS` depending on the session/server TimeZone. The pgoutput CDC decoder (`parsePGTimeText`) carried only the whole-hour `±HH` layout, so under any non-whole-hour server timezone the logical-replication pump failed to decode the FIRST timestamptz value and the stream aborted — continuous sync was unusable for a whole class of mainstream server configurations (any half/quarter-hour zone). The decoder now handles all three offset widths — every form observed from a live Postgres 16, including the second-level historical LMT offset `+00:19:32`. The non-finite / pre-Gregorian values `infinity` / `-infinity` / `… BC` (no representable fixed-width target instant) now refuse by NAME rather than an opaque parse error (PROM-P2).

- **`postgres-trigger` → MySQL `timestamptz` stores the UTC instant, not the source wall clock (PROM-M1).** The trigger capture renders a `timestamptz` via `to_jsonb` — an ISO string carrying the source session's zone offset, e.g. `2026-02-02T07:32:02.020202+05:30`. The MySQL writer previously STRIPPED the offset and stored `07:32:02` (the Kolkata wall clock), while the bulk-copy path (which binds a pgx `time.Time`) correctly stored the UTC instant `02:02:02`: the SAME source instant landing 5h30m apart depending on which path delivered it, silently. The writer now parses the offset to a UTC `time.Time` so the driver serializes the same instant the bulk path does. It also refuses `infinity` / `-infinity` / `… BC` by name (stripping a `… BC` would silently drop the era — 44 BC → 44 AD). Both fixes are pinned end to end by a new congruence test that runs a Postgres source on `Asia/Kolkata` through BOTH CDC paths into MySQL and asserts the identical stored instant.

## Compatibility

- **No behavior change for a UTC (or whole-hour) source, or for any already-working path.** A whole-hour offset (`+00`, `-08`) always parsed and is unchanged; the pgoutput/bulk path already produced a correct `time.Time` instant and is untouched; plain `timestamp` (timezone-naive), `time`, and `timetz` values are unchanged. The only behavior changes are: (1) a fractional-offset source now works instead of aborting the CDC pump, and (2) a `postgres-trigger` timestamptz under a **non-UTC** writer session now stores the correct instant instead of the source wall clock. If you have been running a `postgres-trigger` sync from a non-UTC source into MySQL, timestamptz columns applied by CDC will now match the bulk-copied rows (and the source instant); re-sync affected tables if you need the previously-wall-clock values corrected.
- **Loud, not silent:** `infinity` / `-infinity` / BC timestamptz values are now refused with a named error on both the Postgres-read and MySQL-write paths, instead of an opaque parse failure or a silent era-drop.

## Who needs this — action required

- **If you run continuous sync from a Postgres source whose server timezone is not a whole hour** (India, Nepal, Newfoundland, South Australia, Iran, and other +HH:MM zones): upgrade — pgoutput CDC no longer aborts on timestamptz.
- **If you run a `postgres-trigger` (slot-less) sync into MySQL from a source with non-UTC writer sessions:** upgrade to get correct timestamptz instants; consider re-syncing tables whose timestamptz values were applied by CDC under the old wall-clock behavior.
- **Everyone else (UTC sources, Postgres targets, plaintext timestamp columns): no action.**

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.216 · **Container:** ghcr.io/sluicesync/sluice:0.99.216
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
