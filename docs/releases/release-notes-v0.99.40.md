# sluice v0.99.40

**Two backup-correctness fixes — a single float `NaN`/`±Infinity` row no longer makes a database un-backupable (`backup full` refused the whole table loudly with `json: unsupported value: NaN`), and `backup compact --strategy=smart` no longer leaks one open store handle per compacted chunk (silent FD lingering on Linux, fatal "Access is denied" on Windows). Both bugs are pre-existing and both always failed LOUDLY — there is no silent loss anywhere in this range. Drop-in from v0.99.39 — no flag, default, or invocation changes.**

## Fixed

- **`backup full` no longer refuses tables containing float `NaN` / `±Infinity` (Bug 138).** PG `float4`/`float8` columns legally hold IEEE specials and `migrate` carries them exactly — but the chunk codec rendered floats as JSON numbers, which cannot represent them, so one NaN row made the whole database un-backupable (loud refusal: `json: unsupported value: NaN`; `numeric`-typed NaN always backed up fine — only the float path refused). Non-finite floats now ride a new additive tagged envelope (`{"_t":"f64s","v":"NaN"|"+Inf"|"-Inf"}`) on BOTH codec paths — the fast path emits it byte-identically to the legacy marshal, and the fast decoder accepts exactly the three canonical sentinels, bailing to the legacy path (the loud error oracle) on anything else. The same envelope covers the CDC change-chunk path (live during `sync` streaming and `backup incremental`). Restores are `float8send`-BIT-IDENTICAL to a `pg_dump` round trip: ±Inf sign-exact, every NaN canonicalized to the IEEE quiet NaN (`0x7ff8…0`) exactly as PG's own text format does (NaN payload bits are not representable in either format). Affected releases: every release with `backup` — v0.15.0 through v0.99.39 (the float-as-JSON-number passthrough dates to the original Phase-1 chunk codec). Pinned by a full family × shape matrix ({NaN, +Inf, −Inf, −0, finite} × {f64, f32} × {scalar, list, map} with bit-level assertions through the real writer/reader), a strict-decode ladder (alien payloads fail loudly), the same matrix on the change-chunk path, a real-PG integration round trip asserting `float8send` bits at the target, and the differential sweep/fuzzers now comparing NaN bit patterns.
- **`backup compact --strategy=smart` leaked one open store handle per compacted change chunk (task #9).** The decode pass wrapped its byte-counting reader in `io.NopCloser`, so the handle opened by `store.Get` was released only on the constructor-error path. On Linux the leaked descriptor lingered silently until process exit (which is why CI never saw it); on Windows it was fatal — the rewrite step renames over the very path the leaked handle still holds open, failing loudly with "Access is denied". The counting reader now owns the store handle (its `Close` closes through), so the chunk reader releases it on every path. Affected releases: v0.85.0 (where smart compaction shipped) through v0.99.39. Pinned by a handle-tracking store wrapper (revert-verified — the old code leaks exactly one handle per chunk) and ground-truthed on the real Windows repro: the previously-deterministic smart-compaction integration failure now passes repeatedly.

## Compatibility

- **No breaking changes.** Drop-in from v0.99.39 — no flag, default, or invocation changes. `migrate`, `sync`, and CDC behavior are unchanged apart from change chunks now being able to carry non-finite floats.
- **Chunk format: additive tag only.** Chunks WITHOUT non-finite floats are byte-unchanged — fully interoperable with older binaries in both directions. A chunk that DOES contain a non-finite float is refused LOUDLY ("unknown value tag") by v0.99.39-and-older binaries, never read silently wrong — this is the format's designed additive-tag forward-compatibility mechanism.

## Who needs this — action required

- **Anyone whose `backup full` failed with `json: unsupported value: NaN`** — upgrade and re-run the backup. The old behavior was a loud refusal before any artifact was finalized; no prior backup silently dropped or mangled the values.
- **Anyone running `backup compact --strategy=smart` on Windows** — previously failed deterministically with "Access is denied"; upgrade and re-run. Linux users get the FD hygiene fix for free (the leak never corrupted output there — handles just lingered until exit).
- **Mixed-version fleets** — backups taken by v0.99.40 from data containing float NaN/±Inf cannot be restored by older binaries (loud "unknown value tag" refusal by design). Restore such backups with v0.99.40+.
- **Nobody needs to re-verify prior migrations or backups.** Both bugs were loud failures — neither could complete with wrong or missing data.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.40`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.40`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
