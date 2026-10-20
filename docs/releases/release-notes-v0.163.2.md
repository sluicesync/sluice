# sluice v0.163.2

**MEDIUM fix, silent for one run — a failed re-read of a copy's table ids now refuses instead of passing (ANCHOR-ID-REREAD-FAILURE-PASSES, Postgres, present since v0.163.1).** v0.163.1 made every cold start's CDC anchor, and `schema add-table` before it records an add complete, re-read each table's `pg_class.oid` and refuse `TABLE-IDENTITY-CHANGED` on one that moved since the copy. A re-read that *failed* returned no id, and the check treated that the same as a table the source no longer holds, so it passed. Such a failure now refuses with `TABLE-IDENTITY-UNREAD`, naming the cause, and writes nothing. No flag change and no backup format change.

## Fixed

- **A failed id re-read refuses `TABLE-IDENTITY-UNREAD` (ANCHOR-ID-REREAD-FAILURE-PASSES, MEDIUM, silent for one run).** On a Postgres source, the anchor's re-read of the copy's table ids could fail: a connection error, or a namespace that could not be resolved. It then returned no id for the affected tables, and the anchor recorded the copy-time ids as verified. If a table had also been dropped and re-created under its name in the same window, CDC applied the new table's changes onto the old table's rows for the rest of that run, at exit 0. The resumed cold start's anchor runs after the index and constraint phases, possibly hours after the copy. The record kept the copy-time ids, so the next start's identity check refused `TABLE-IDENTITY-CHANGED`; the harm was bounded to one run. The read now reports its failure (`readEstablishedIDs`), and the anchor refuses before writing anything. A table the source genuinely no longer holds still passes, as before. `schema add-table`'s check of its one table had the same pass-on-error and refuses the same way. Pinned by new cells in `TestAnchorTableIDs` and `TestAddTableVerifiedTableIdentity`, mutation-run in both directions.
- **What to do on the refusal.** Once the source can be queried, re-copy as for `TABLE-IDENTITY-CHANGED`: `sluice slot drop` if the stream's slot is left, then the same `sync start` with `--restart-from-scratch`. For `schema add-table`, empty the target table and run the add again. Both are the existing remedies of the same check's `TABLE-IDENTITY-CHANGED` refusal, not measured end to end for this one.
- **Unchanged:** MySQL-family sources, whose table ids do not survive a rebuild and so are not verified, keep their best-effort read.

## Not closed, stated

- **Not run for this release: `d1verify` and `psverify`.** `scripts/prerelease-triggers.sh` raised its unscheduled-pin advisory because the delta touches `internal/pipeline/`. The change is confined to Postgres sources; neither suite exercises it. Coverage is the CI integration matrix on real MySQL, MariaDB and Postgres containers, including the `-race` Integration job.

## Compatibility

- **No flag change and no backup format change** (`BackupFormatVersion` stays 15).

## Who needs this

- **Anyone on v0.163.1 with a Postgres source whose cold start or `schema add-table` logged `could not read the source's table ids`, `could not resolve the source namespaces to read table ids` or `could not read the table's id`.** If a table was also re-created in that window, the next start refused `TABLE-IDENTITY-CHANGED` and named it. Follow that refusal's remedy.
- **Everyone else:** upgrade; nothing to do.

---

**Install:** `brew install sluicesync/tap/sluice` · `go install sluicesync.dev/sluice/cmd/sluice@v0.163.2` · **Container:** `ghcr.io/sluicesync/sluice:0.163.2`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
