# sluice v0.123.1

**Patch: v0.123.0's new XA refusal now honours the sync's table filter, and its printed remedies actually run (Bug 246).** Found by the v0.123.0 post-release regression cycle within hours of publish: the refusal fired for XA transactions on tables the sync *excludes* via `--include-table`/`--exclude-table` — a configuration that worked before the refusal existed — and a tripped stream re-refused forever on the historical XA body, so both remedies the message named were no-ops. Loud, zero silent loss, but it broke working filtered syncs sharing a server with an XA-using application. Upgrade if v0.123.0's XA refusal fired on a filtered sync; everyone else is unaffected.

## Fixed

**The XA refusal consults the sync's effective table scope — the operator filter merged with live-added tables — instead of schema scope alone.** The table filter is applied one stage downstream of the binlog reader (the pipeline's dispatch-filter goroutine), so the reader's in-scope check could only see database granularity: an XA transaction touching a filtered-out table in the replicated database refused, and because excluding the table changed nothing the reader could see, the tripped stream re-refused on every resume. The pipeline now hands the reader the same name-based scope core its own dispatch filter uses (a new optional `ir.CDCScopePredicateSetter` surface, wired at all four reader-open sites: cold start, warm resume, and both multi-database paths), so the two can never disagree — including live-added tables, whose XA traffic still refuses exactly as the dispatch filter would deliver it. A filter-exempted XA row still emits and the downstream filter drops it like any other excluded-table event; delivery semantics are unchanged. The refusal's remedies now run as printed: `--exclude-table` works because the refusal honours it, and the historical-body case (a stream that keeps refusing on resume) names `sync start --restart-from-scratch`, since no filter change can move a position past an in-scope XA body already in the stream's past. Pinned at both layers (the reader-side exempt/refuse/emit pins and the pipeline closure-agreement pin, plus the compile-time capability pin), mutation-run in both halves, and the real-server XA suite re-verified.

## Compatibility

No error code, flag, or on-disk format changed — the `SLUICE-E-CDC-XA-UNSUPPORTED` message's remedy text is updated to name the runnable recoveries. **Drop-in upgrade from v0.123.0.** One behaviour note: an XA transaction on a table the sync's filter excludes now streams past the refusal (the excluded table's events are dropped by the dispatch filter exactly as before v0.123.0); XA on replicated tables refuses unchanged.

## Who needs this — action required

**Anyone whose v0.123.0 filtered sync refused with `SLUICE-E-CDC-XA-UNSUPPORTED` on a table the filter excludes**: upgrade and restart the stream — it resumes from its held position and streams past the historical XA body (the excluded table's events were never applied, so nothing needs repair). **Anyone whose stream refuses on an XA body for a genuinely replicated table**: the remedies now printed are the real ones — exclude the table, or `sync start --restart-from-scratch`. If v0.123.0's refusal never fired for you, this patch is a no-op.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.123.1
```

Container images: `ghcr.io/sluicesync/sluice:0.123.1` (multi-arch; the image tag carries no `v` prefix).
