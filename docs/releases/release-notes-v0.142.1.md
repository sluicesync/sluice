# sluice v0.142.1

**If you run `--exclude-table` against a Postgres source on v0.142.0, upgrade.** The warning that release added fired on *every* exclude pattern, including ones that worked — and the remedy it offered pointed at the input that actually breaks. One release, one day, one flag; nothing was lost or altered, but the advice was backwards.

## Fixed

**The unmatched-pattern warning stops firing on working exclusions (Bug 273).** v0.142.0 added `TABLE-FILTER-PATTERN-UNMATCHED` so that an `--exclude-table` pattern matching nothing — which fails **open**, copying the table you meant to keep out — would no longer be silent. On a **Postgres** source it fired on every exclude pattern instead, including correct ones. Under an exclude filter no pattern can ever match a surviving table, so it was unconditional rather than occasional: on the exact flag and engine the original bug was filed against, the new warning carried no information at all.

That is worse than the silence it replaced, because the remedy branch keys on the *shape* of the pattern rather than on what happened. An operator running a perfectly correct `--exclude-table=pii` was told their PII table was being copied and handed a suggestion to write `public.pii` — which is precisely the input that copies it for real. The warning pointed from a working configuration at the broken one.

The cause is an interaction rather than a mistake in the check. Postgres implements a scope **push-down** (catalog Bug 76): the reader is handed the filter and skips excluded tables at read time, so they never reach the pipeline. That has always been fine, and the note at the push-down still says the post-read filter "remains the authoritative prune" — which is true, for pruning. v0.142.0 asked that same post-read view a *different* question ("did this pattern match anything?"), and for that question the view is missing exactly the rows the answer depends on. The push-down predicate now records what it was asked about, and the census consults the full source table set. MySQL never had the defect: it has no push-down.

## Compatibility

Drop-in from v0.142.0. No flag, format or behaviour change beyond the warning firing correctly; a genuinely dead pattern still warns, with the same text and the same remedy.

**Only v0.142.0 was affected.** Earlier releases had no such warning.

## Who needs this

- **Anyone on v0.142.0 using `--include-table` / `--exclude-table` against Postgres.** The upgrade is worth taking promptly if the warning has already sent someone chasing a correct configuration.
- Everyone else can take it at leisure — the fix touches one warning and nothing else.

## Development notes

Two things about how this was found are worth recording, because neither is flattering and both are the point of the process.

**The v0.142.0 pin for this warning passed while the bug shipped.** It built its schema by hand, so the excluded table was still present when the filter door ran — a shape no Postgres run ever produces. It graded the function and not the wiring. The replacement models the push-down with a reader that behaves like `postgres.readTables`: asks the predicate about every candidate, then omits what it rejects.

**The regression cycle's own no-false-fire cell is what caught it**, and that cell had been flagged in advance as the load-bearing one — a warning that always fires is worse than the bug it reports. It failed exactly there.

This release also corrects a v0.142.0 claim about which paths report an unmatched pattern. `restore` does report it (it shares the same door as `migrate`); only `cutover` does not. The original sentence is left standing in the v0.142.0 notes under a correction banner rather than rewritten.
