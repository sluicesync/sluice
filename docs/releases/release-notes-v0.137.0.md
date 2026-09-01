# sluice v0.137.0

**The trigger-CDC capture door stops taking a trigger's word for it.** It verified that each capture trigger existed and pointed at the expected function, then trusted that function's contents entirely — so anyone able to replace a function body could leave every name and binding intact and silently stop recording changes. The door now grades what the triggers actually execute, and tells an old install from a tampered one using provenance rather than guesswork. Plus a database-wide echo-loop probe, a `trigger setup` that covers the whole install, and a statement-DML refusal that no longer prints operator data. **Every existing pgtrigger install warns until `sluice trigger setup` is re-run once — see Compatibility.**

## Changed

**The capture-shape door grades what the triggers execute, not just what they are named.** Its own comment claimed to mirror the SQLite door, which does check bodies; it did not. The door now compares the whole definition — and a PostgreSQL function definition is three things, not one: the body lives in `prosrc`, the `SET` clauses in **`proconfig`**, and `SECURITY DEFINER` in `prosecdef`. That distinction is load-bearing rather than pedantic: v0.134.1's SEC-1 fix lives entirely in `proconfig`, so a body-only check would have been blind to its own headline case.

The door resolves each function by name *and argument count*, which matters more than it sounds: PostgreSQL permits overloading, so a name alone does not identify a function. The pre-release review of this very release found that a name-only read could be defeated by gutting the real capture function and planting a same-named overload carrying a healthy body — the door would grade the decoy and pass while the trigger executed the gutted one, silently, at exit 0. That was caught and closed before the release went out, with the attack itself pinned against real PostgreSQL.

Telling an *old* install from a *tampered* one cannot be done from the body, so it is done from provenance. `trigger setup` now records a digest of the definitions it installed. A definition that no longer matches what setup recorded is a refusal; one that matches the record but not this binary's render is a `STALE-CAPTURE-FUNCTION` warning rather than an outage, because this door runs at every warm resume and refusing there would turn a routine binary upgrade into downtime. Independent of provenance, a capture function that no longer writes into the change log at all is refused at **any** vintage — no released sluice ever rendered such a function — and that arm is what protects every install predating this release, including yours before you re-run setup.

**`sluice trigger setup` writes the replicated-capture posture for the whole install.** The posture was recorded once per install but applied per table, and setup only touched the tables named in that invocation. So `trigger setup --tables=shipments` against an install created with `--capture-replicated-writes --tables=orders,shipments` recorded "not replicated", left `orders` still capturing replicated writes, and mentioned `orders` in no statement of its plan. Every combination did land on a loud refusal, so nothing was lost silently — the problem was that the refusal blamed a hand-flipped trigger and prescribed a `trigger setup` re-run, which is the command that produced the state and provably cannot clear it. Setup now reads the installed triggers through the door's own reader and **widens** to cover tables it was not asked about, while **refusing** to narrow implicitly, since quietly taking a table off replicated capture is a capture gap arriving as a side effect of naming a different table. The refusal messages now print the full table list, which repairs installs that already diverged.

## Fixed

**The echo-loop refusal probes the database, not one schema.** `--capture-replicated-writes` refuses when a sluice sync is applying into the same database, because capturing those writes would loop them back. The probe looked for the applier's control table in a single schema — but `--target-schema` moves the user data and deliberately leaves the control table in the target DSN's schema, so the two are *designed* to diverge, and the hazard is database-wide. It now searches every non-system schema and names where the bookkeeping was found. A stream count that could not be read now says so, rather than printing a `0` indistinguishable from an empty control table.

**The statement-format-DML refusal withholds every literal.** The refusal quotes the offending statement's leading fragment to identify it, and cut that fragment at a token list that did not include comparison operators — so `UPDATE accounts SET … WHERE ssn = '123-45-6789'` put the value into a log line. The cut is now an allowlist: it keeps identifier material and stops at the first token that is not, so every literal form falls outside it by construction rather than by enumeration, and the next unlisted operator cannot reopen the hole. The `sha256` prefix that accompanied it is **removed** rather than lengthened or salted — its only use was letting an operator recompute the digest against a candidate statement, and that recomputation *is* the oracle for a low-entropy value. It is replaced by the binlog file and position, the commit timestamp, and the GTID, which identify the event exactly, feed `mysqlbinlog`, and are a function of the stream rather than of the data.

**A comment-prefixed statement keeps its diagnostic.** The redaction cut did not skip leading comments, and a comment's first character is not identifier material — so `/*vt+ … */ UPDATE …` had its entire lead cut away and the refusal said nothing about which statement it was. That is the safe direction, and it was also useless for the normal traffic shape from Vitess and PlanetScale, ProxySQL, and tracing-annotated clients. The verb, table and columns now survive; the values still do not.

**`SLUICE-E-CDC-STATEMENT-DML` names its fifth verb** in all four operator-facing homes; v0.134.0 added `WITH`-prefixed CTE-DML to the detector and not to its description.

## Internal

The MySQL CDC-open preflight roster and the probe-timeout roster now derive their own universes structurally instead of hand-listing them, each verified with an addition-shaped mutation the pre-fix gate passes and the fixed gate catches. The Postgres scalar-registry parity gate compares the whole projected type rather than its Go type, with the fields `pgoutput` genuinely cannot carry handled as named asymmetries and a staleness guard so an excuse cannot outlive the divergence it excused. v0.133.0's notes and CHANGELOG carry a correction banner for the claim Bug 257 falsified, with a gate for the class.

The LOAD DATA segmentation cost test was rewritten and its premise withdrawn. It asserted a wall-clock budget scaled to the target's commit cost, on the belief that an extra segment costs exactly one extra commit — which held only on the slow-fsync box where it was written. It now counts what segmentation changes and asserts that segmenting adds statements and nothing else. The withdrawn claim also lived in the segment-budget documentation and the performance-parity matrix; both are corrected rather than left standing.

## Compatibility

**One `sluice trigger setup` re-run per pgtrigger install.** The install's meta table gains a `capture_fn_digest` column (schema version 4 → 5, added tolerantly; older readers are unaffected). Until setup is re-run, an existing install has no recorded provenance and opens with a `STALE-CAPTURE-FUNCTION` warning at every resume — a warning, never a refusal, and the same single re-run that v0.134.1's `INSECURE-CAPTURE-FUNCTION` warning already asks for. Streams keep running throughout.

**One behavior change that can newly refuse:** a `trigger setup` naming a subset of an opt-in install's tables now refuses rather than succeeding and wedging the stream. The refusal prints the full table list to pass back.

No format, flag, or on-disk change otherwise. Drop-in from v0.136.0 for everyone not using `postgres-trigger`.

## Who needs this

- **Anyone running `postgres-trigger` CDC**: re-run `sluice trigger setup` once at your convenience to clear the warning and record provenance. If you also saw v0.134.1's `INSECURE-CAPTURE-FUNCTION` warning, that same re-run clears both.
- **Anyone using `--capture-replicated-writes`**: the posture is now install-wide, and a subset `trigger setup` refuses instead of wedging. If your install already diverged, the refusal tells you the exact command to run.
- **MySQL sync operators who have hit `SLUICE-E-CDC-STATEMENT-DML`**: the refusal no longer prints statement literals, and identifies the event by binlog position instead of a digest. Prior log lines may contain data — treat them accordingly.
- Everyone else: upgrade normally; no action.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.137.0
```

Container images: `ghcr.io/sluicesync/sluice:0.137.0` (multi-arch; the image tag carries no `v` prefix).

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
