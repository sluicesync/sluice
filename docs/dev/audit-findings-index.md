# Audit findings index

The **tracked** list of finding IDs each blind audit produced, and whether
each is expected to appear in `docs/dev/audit-backlog.md`.

`TestAuditFindingsAreFiled` (`internal/docsync`) reads this file and fails
the build when an ID listed here as `filed` is absent from the backlog.

## Why this file exists at all

Four consecutive reconcilers (2026-08-27, 08-31, 09-01, 09-06) reported
the same process defect: findings reach a worker report and never reach
`docs/dev/audit-backlog.md`, so the next audit re-derives them from
scratch — or, worse, re-litigates a decision that was already made and
recorded only in a workspace file. The 09-06 pass measured **20 of the
09-01 section's own IDs with zero backlog hits**, in a section whose
opening sentence claims to file every finding.

Each of those reconcilers proposed the same gate: *a docsync test
asserting every OPEN/PARTIAL finding ID in the newest
`workspace/repo-audit-*.md` scorecard appears in the backlog.* It was
never built, and it is worth recording why, because the reason is not
neglect:

**`workspace/` is gitignored, and nothing under it is tracked.** A test
that reads `workspace/repo-audit-*.md` finds those files on the machine
that ran the audit and finds **nothing at all in CI** — where it would
parse zero reports, extract zero IDs, and pass. That is a vacuous green
of exactly the shape this project keeps writing rules about: a gate whose
existence implies coverage it does not have. Three passes asked for a
gate that could not have worked as specified.

So the artifact moved rather than the gate: an audit's finding IDs are
committed HERE, and the gate grades a tracked file against a tracked
file. That also fixes the underlying complaint on its own — a decision
recorded only in `workspace/` is a decision the next pass cannot see.

## How to update this file

When an audit completes, append a section for it listing every finding ID
and its disposition:

- `filed` — the ID must appear in `docs/dev/audit-backlog.md`. This is
  the default and what the gate enforces.
- `prose <reason>` — filed in the backlog by content but without its ID.
  Allowed, and deliberately awkward to write: prefer giving it the ID.
- `withdrawn <reason>` — the finding was retracted (not merely fixed). A
  fixed finding is still `filed`; the backlog records the fix.

The gate demands the ID rather than accepting prose because two of the
09-01 leaks *were* present in prose (`wholeRecordParsed`,
`ScopesByArity`) and a gate that accepted prose would have passed on the
other eighteen.

---

## 2026-09-09 — Tier-3 blind audit (post-v0.148.0; 6 blind workers + reconciler)

**Consolidated 2026-09-09.** The five blind workers' IDs are now entered
alongside the reconciler's and the fix session's, so this list is the
whole pass rather than the half of it that happened to get worked.

The consolidation measured a **~70% leak**: of the 65 worker IDs, 11
were in the backlog by ID, 6 in prose, and roughly 48 in neither. The
absences were then filed in one batch under
`docs/dev/audit-backlog.md` → "The five blind workers' own findings",
each as the worker filed it and explicitly NOT re-verified, so every ID
below is `filed`.

**One of the 48 was a HIGH nobody had routed.** The perf worker found
`--target-schema` broken on every parallel copy lane, flagged it as
outside its own dimension, and asked the reconciler to hand it to the
silent-loss owners. The reconciler did not, and no backlog entry named
it. Re-measured during the consolidation it was also WIDER than filed —
it reaches the cross-table pool at any row count, not only chunked
tables — and it is now fixed (`A0909-PDD-P1`). That is what this index
is for: the gate is cheap, and the thing it catches is a HIGH that fell
between two workers.

**Worker prefixes.** Three of the five schemes collide with another
worker's — `silent-loss-mysql` and `silent-loss-pg` both number
`HIGH-1`/`MEDIUM-1`/`MEDIUM-2`/`LOW-1`, and `arch-quality` and
`testing-ci` both use `H-n`/`M-n`/`L-n` for different findings — so a
bare worker ID is not citable. IDs are qualified here: `SLM`
silent-loss-mysql, `SLP` silent-loss-pg, `AQ` arch-quality, `TCI`
testing-ci, `PDD` perf-deps-devex-docs.

### Reconciler and fix-session IDs

- A0909-HIGH-1 filed
- A0909-H1-ESCAPE filed
- A0909-P2 filed
- A0909-P3 filed
- A0909-P2b filed
- RC-1 filed
- RC-1b filed
- RC-2 filed
- RC-5 filed
- VF0909-1 filed
- VF0909-2 filed
- VF0909-3 filed
- VF0909-4 filed
- VF0909-5 filed
- VF0909-6 filed
- A0909-STOP-1 filed
- A0909-MYSQL-HIGH-1 filed
- A0909-MYSQL-HIGH-1-POLICY filed
- A0909-MYSQL-MEDIUM-2 filed
- A0909-PG-MEDIUM-1 filed
- A0909-MYSQL-MEDIUM-2b filed
- VF0909B-6 filed
- VF0909B-5 filed
- VF0909B-4 filed
- VF0909B-3 filed
- VF0909B-2 filed
- VF0909B-1 filed

### silent-loss-mysql

- A0909-SLM-HIGH-1 prose filed as A0909-MYSQL-HIGH-1, whose entry names this worker's HIGH-1 as one of its two sources
- A0909-SLM-HIGH-2 prose the ESCAPE parse failure is filed as A0909-H1-ESCAPE, which records that two workers found it independently
- A0909-SLM-MEDIUM-1 filed
- A0909-SLM-MEDIUM-2 prose filed as A0909-MYSQL-MEDIUM-2, with its residual as A0909-MYSQL-MEDIUM-2b
- A0909-SLM-LOW-1 filed
- A0909-SLM-LOW-2 filed

### silent-loss-pg

- A0909-SLP-HIGH-1 prose the parent-DDL finding is filed as A0909-HIGH-1, which is the PG worker's; cite it as A0909-HIGH-1 (PG)
- A0909-SLP-MEDIUM-1 prose filed as A0909-PG-MEDIUM-1
- A0909-SLP-MEDIUM-2 filed
- A0909-SLP-LOW-1 filed

### arch-quality

- A0909-AQ-H-1 prose the same ESCAPE parse failure as A0909-SLM-HIGH-2, filed as A0909-H1-ESCAPE
- A0909-AQ-H-2 prose the lineage-routing finding is filed as A0909-MYSQL-HIGH-1, which credits the arch worker's row
- A0909-AQ-H-3 filed
- A0909-AQ-M-1 filed
- A0909-SLOTCONST-1 filed
- A0909-AQ-M-2 filed
- A0909-AQ-M-3 filed
- A0909-AQ-M-4 filed
- A0909-AQ-L-1 filed
- A0909-AQ-L-2 filed
- A0909-AQ-L-3 filed
- A0909-AQ-L-4 filed
- A0909-AQ-L-5 filed
- A0909-AQ-L-6 filed
- A0909-AQ-L-7 filed

### testing-ci

- A0909-TCI-H-1 filed
- A0909-TCI-H-2 filed
- A0909-TCI-H-3 filed
- A0909-TCI-M-1 filed
- A0909-TCI-M-2 filed
- A0909-TCI-M-3 filed
- A0909-TCI-M-4 filed
- A0909-TCI-M-5 filed
- A0909-TCI-M-6 filed
- A0909-TCI-M-7 filed
- A0909-TCI-M-8 filed
- A0909-TCI-M-9 filed
- A0909-TCI-L-1 filed
- A0909-TCI-L-2 filed
- A0909-TCI-L-3 filed
- A0909-TCI-L-4 filed
- A0909-TCI-L-5 filed
- A0909-TCI-L-6 filed
- A0909-TCI-L-7 filed
- A0909-TCI-L-8 filed
- A0909-TCI-L-9 filed
- A0909-TCI-L-10 filed
- A0909-TCI-L-11 filed
- A0909-TCI-L-12 filed
- A0909-TCI-L-13 filed
- A0909-TCI-L-14 filed

### perf-deps-devex-docs

- A0909-PDD-P1 filed
- A0909-PDD-P2 prose filed as A0909-P2
- A0909-PDD-P2b prose filed as A0909-P2b
- A0909-PDD-P3 prose filed as A0909-P3, regraded MEDIUM against the worker's HIGH
- A0909-PDD-P4 filed
- A0909-PDD-P5 filed
- A0909-PDD-P6 filed
- A0909-PDD-D1 prose folded into A0909-P2; the operator-facing "no marker, no hint" half is recorded in the consolidated list
- A0909-PDD-D2 filed
- A0909-PDD-D3 filed
- A0909-PDD-DOC1 prose the cross-region-migration.md home is filed under A0909-P2b; the other two claim homes are named in the consolidated list
- A0909-PDD-DOC2 filed
- A0909-PDD-DOC3 filed
- A0909-DOCFLAG-1 filed
- A0909-PDD-DOC4 filed
- A0909-PDD-DOC5 filed

The perf worker's three LOW bullets carry no ID of their own and are
filed by content at the end of the consolidated list. They are named
here so the count reconciles: 65 IDs plus 3 unnumbered findings.

## 2026-09-07 — v0.145.0 pre-tag review

- PRE-TAG-1 filed
- PRE-TAG-2 filed
- PRE-TAG-3 filed
- PRE-TAG-4 filed
- PRE-TAG-5 filed
- H1 filed
- H2 filed
- H5 filed

## 2026-09-06 — reconciler pass (post-v0.144.0)

- RCN-1 filed
- S-1 filed
- S-2 filed
- S-3 filed
- VF-ARRAY-WITNESS filed
- PERF-MATRIX-ARREARS filed

## 2026-09-01 — post-v0.137.3 blind audit on Fable 5.1

The 20 below were the leak the 09-06 reconciler measured; all are now in
the backlog under the 2026-09-06 section.

- SLM-6 filed
- SLM-7 filed
- SLP-3 filed
- SLP-4 filed
- SLP-5 filed
- A2-6 filed
- A2-7 filed
- LA-5 filed
- LA-7 filed
- TCI-5 filed
- TCI-7 filed
- AQP-1 filed
- AQP-3 filed
- AQP-4 filed
- DDD-6 filed
- DDD-8 filed
- DDD-9 filed
- SEC-LOW-1 filed
- NEW-2 filed
- C-5 filed

Filed in prose without their IDs, and left that way:

- TCI-3 prose `wholeRecordParsed` appears in the 09-01 section
- TCI-4 prose `ScopesByArity` appears in the 09-01 section
