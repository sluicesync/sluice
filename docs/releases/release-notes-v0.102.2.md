# sluice v0.102.2

One more way a continuous sync could die on a routine connection blip, found by auditing the class rather than waiting for it to be reported. Drop-in upgrade, no breaking changes, no flag changes.

### Fixed

**A source connection blip during a binlog stream's schema re-read no longer kills the stream.** When a MySQL-family binlog stream meets a table it has not cached, it performs a LIVE `information_schema` read at the TABLE_MAP boundary to learn the column list. That read is ordinary client traffic on the source, so it can fail for all the ordinary reasons — and its failure was stored raw, which the streamer treats as terminal however routine the cause. A blip there killed a sync that should have reconnected and carried on. The error is now classified like every other transport failure the reader raises, so it rides out on the existing retry path.

This is the second instance of the defect corrected in v0.102.1, and it was found by auditing every error-parking site in the engine rather than by a field report — the first instance shipped believed-fixed and inert, so the audit assumed there were more. There is no report of this one firing in practice; a stream would have had to meet an uncached table at the same moment as a transport fault.

### Internal

**Two gates now make this class fail loudly at commit time rather than in the field.** The first walks every error-parking site in the MySQL engine and requires the value be classified, or be a typed error the streamer already matches structurally; its exception list is small and every entry states why classification would be wrong or redundant there. The second reflects over the target-health snapshot and fails if a documentation-sync fixture leaves any capability flag false, because a false flag silently makes a whole exporter family unemittable and the doc gate blind to it — which happened twice in one day, in two exporters, for the same series.

Both were verified by reintroducing the exact defects they exist to catch. Both carry anti-vacuity floors, so a rename or refactor that stops them matching fails them instead of silently passing forever.

**The claim underneath the cold-copy read retry is now tested against a real server.** ADR-0109's per-table reconnect-and-resume rests on a mid-table source drop surfacing as the rows-iteration error rather than at the row-scan exit, and that had never been exercised — the existing test only pinned that the session timeouts get raised. Killing a real connection mid-stream confirms it: the drop arrives at the classified exit and is retriable. The probe is kept as a permanent pin, since a driver change that reroutes it would otherwise surface as a long migration dying in the field. This closes the question raised by the new gate about whether the scan exit needed classifying too — it does not, and the reasoning is recorded where the exception lives.

### Compatibility

No breaking changes, no flag changes, drop-in upgrade. The fix only converts a previously-fatal condition into a retry, so a stream that never met it behaves identically.

## Who needs this

- **Anyone running a continuous MySQL-family sync that adds tables over time** — this is the case that could meet an uncached table and a transport blip at the same moment. There is no report of it firing; upgrade at your convenience.
- Everyone else: no action needed beyond upgrading.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.102.2
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:0.102.2`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
