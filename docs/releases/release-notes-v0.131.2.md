# sluice v0.131.2

**A correctness patch — a second silent data-loss the adversarial value-fidelity corpus caught, this time on the SQLite / Cloudflare D1 floating-point path.** The corpus that shipped in v0.131.1 was extended to the SQLite and D1 engine families, and it surfaced a silent rounding of `REAL` values on the trigger-CDC capture path. Fixed. **Drop-in from v0.131.1 — no schema, format, or flag change. If you run continuous sync with a SQLite or Cloudflare D1 database that has floating-point (`REAL`) columns, upgrade.**

## Fixed

**Silent data-loss — SQLite / D1 floating-point values were silently rounded on the trigger-CDC capture path (SQLite 3.43+).** sluice captured SQLite and Cloudflare D1 `REAL` values by rendering them to text with `format('%.17g', …)`. SQLite 3.43 rewrote `format()`'s float rendering so `%.Ng` no longer honours the precision spec at all — it falls back to a lossy shortest-value algorithm, and **2295 of 5007 sampled doubles fail to round-trip** (measured on both the bundled driver and real Cloudflare D1): `0.30000000000000004` renders as `"0.3"`. Every `REAL` flowing through the affected paths — the D1 reader, `--stage-local` D1 staging, and both the SQLite and D1 trigger-CDC capture lanes — was silently altered at exit 0, and a `REAL` at `float64` max rendered out-of-range and tore the CDC stream down. Fixed by rendering with SQLite's lossless alternate-form flag `format('%!.20g', …)` — **0 of 5007 misses, confirmed live on D1.** The local SQLite reader that scans `REAL` directly as a `float64` was never affected, and Go-side float rendering already used shortest-exact formatting.

**A stale or missing SQLite / D1 capture trigger now refuses loudly instead of capturing wrong (or nothing).** Continuous sync on SQLite/D1 installs capture triggers whose body encodes that value-render. A trigger left over from an older sluice (rendering with the now-lossy `%.17g`), or a capture trigger that was never installed, is now caught at CDC-open by a capture-shape check that byte-compares the installed trigger against what this binary renders, and refuses with a re-setup remedy — previously a stale trigger silently captured lossy values, and a missing one was an unguarded silent gap.

## Changed

**`backup` chain-readability verification now stats every chunk the surviving manifests reference.** The "a maintenance operation can never report success over a chain it just made unreadable" guarantee previously rested on the delete-set geometry rather than on checking the result, so a survivor-chunk-eating sweep bug would have exited 0. `prune` / `compact` now stat every referenced chunk file (one existence probe each) and refuse with `SLUICE-E-BACKUP-CHAIN-UNREADABLE` if any is missing.

## Compatibility

Drop-in from v0.131.1 — no schema, format, error-code, flag, or command change. The corpus fix is pure correctness on the SQLite/D1 value-render path: a sync not hitting a lossy `REAL` is byte-identical after the upgrade, and one that was now carries the value faithfully. The new capture-shape refusal only fires on a trigger that predates this fix or was never installed — the remedy is a one-command re-setup. The backup readability check adds O(chunks) existence probes to `prune`/`compact` with no behaviour change on a healthy chain. Internally, the adversarial value-fidelity corpus now covers all four engine families (SQLite and D1 added), and several audit gates were hardened — no user-facing effect.

**Who needs this:** anyone running **continuous sync with a SQLite or Cloudflare D1 database that has floating-point (`REAL`) columns** — this is a genuine silent-data-loss repair on that path. Everyone else: no action.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.131.2
```

Container images: `ghcr.io/sluicesync/sluice:0.131.2` (multi-arch; the image tag carries no `v` prefix).
