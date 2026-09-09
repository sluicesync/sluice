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

The reconciler's and the fix-session's IDs. The five blind workers' own
IDs (silent-loss-mysql, silent-loss-pg, arch-quality, testing-ci,
perf-deps-devex-docs) are NOT yet entered — that consolidation is the
open item the backlog section names, and until it lands this list is
narrower than the pass.

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
