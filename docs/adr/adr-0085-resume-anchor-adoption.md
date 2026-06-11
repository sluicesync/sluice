# ADR-0085: Chain semantics under crash-resume — EndPosition is the adopted minimum anchor

- **Status:** Accepted (implemented; task #42)
- **Date:** 2026-06-11
- **Relates to:** ADR-0010 (idempotent applier semantics — now
  load-bearing for backup chains, not just live sync), ADR-0083
  (chain provisioning; this ADR reverses its crash-recovery message),
  ADR-0084 §3 (crash/resume contract under concurrency — amended
  here), Bug 135 (the re-stream-from-scratch resume rule this builds
  on)

## Context

A resumed `backup full` (re-run against an `in_progress` manifest)
keeps the prior attempt's fully-completed tables **verbatim** — their
chunks are exact as-of the *first* attempt's snapshot anchor `A1`. But
the resume always opened a **fresh** snapshot and recorded the new
anchor `A2` as the final `EndPosition`, because the in-progress
manifest never carried an anchor at all (`EndPosition` was only set at
step 4.5, after the sweep — a crashed run's anchor was simply lost).

That combination is a **silent chain gap with exit 0**: writes landing
on a kept table in `(A1, A2]` are in *neither* the kept row chunks
(read at `A1`) *nor* the next incremental's window (opened at the
recorded `A2`). The chain restores cleanly and is missing data.

The `--chain-slot` shape (ADR-0083) made it worse: a crashed
`--chain-slot` run leaves the persistent chain slot standing at `A1`
— which is exactly the WAL-retention guarantee a sound resume needs —
but the re-run hit the slot-already-exists refusal, whose message
advised `sluice slot drop` + retry as crash recovery. Following that
advice **releases the gap-covering WAL** and funnels the operator
straight into the silent gap.

### The soundness pivot

Why is "kept tables at `A1`, re-streamed tables at `A2`, chain replay
from `A1`" sound at all? Because the chain-restore appliers are the
**idempotent** ADR-0010 appliers: INSERT upserts on a key
(PG `ON CONFLICT DO UPDATE` / MySQL ODKU), UPDATE/DELETE carry full
row images and tolerate zero affected rows. Replaying a contiguous
window `(P, stop]` onto table data as-of any `Q` with `P ≤ Q ≤ stop`
converges to state-at-`stop` — the overlap `(P, Q]` re-applies values
the data already has. So:

- **kept tables** (exact at `A1`): replay from `A1` covers everything
  after — no overlap, no gap;
- **re-streamed tables** (exact at `A2 ≥ A1`): replay from `A1`
  overlaps `(A1, A2]`, which converges under the idempotent appliers.

**The exception is the load-bearing edge:** a *truly keyless* table
(no PK, no all-NOT-NULL plain-column UNIQUE index) has nothing for the
upsert to collide on; ADR-0010's documented fallback is plain INSERT,
so overlap replay **duplicates** rows. Adoption is therefore refused
when a keyless table must be (re-)streamed on an anchored resume.

The second input to soundness is WAL availability back to `A1` — the
existing chain preflight's `confirmed_flush ≤ A1` check (ADR-0083),
which the standing chain slot satisfies *as long as nobody drops it*.

## Decision

**`EndPosition` under crash-resume is the adopted minimum anchor**:
the first attempt's anchor wins, for every attempt that follows.

1. **Early anchor stamp.** The in-progress manifest carries
   `EndPosition` from its very first write (the snapshot position on
   the snapshot-anchored path; the v0.17.x non-snapshot fallback still
   captures post-sweep and stamps nothing early). A pre-sweep manifest
   commit makes the anchor durable *before* any table streams, so
   every post-fix crash leaves a resumable, anchor-carrying record.

2. **Resume re-anchoring (adoption).** When the prior in-progress
   manifest carries an anchor, the resume adopts it: the final
   `EndPosition` is the prior anchor, the step-4.5 overwrite is
   skipped, and the adoption is WARN-logged with both position tokens.
   The resumed run's fresh snapshot serves **only** read consistency
   for tables streamed this run.

3. **Pre-fix manifests (no anchor) re-stream everything.** Kept tables
   would be exact as-of an *unknown* earlier anchor; pairing them with
   this run's anchor is the gap shape. Scope: only when this run is
   snapshot-anchored — the v0.17.x fallback records no snapshot anchor
   (its during-backup gap is the documented v0.17.2 caveat) and keeps
   table-level resume.

4. **Keyless re-stream guard.** `ir.TableReplayIdempotent` (the
   Bug-125 keyed-ness derivation: PK, else an all-NOT-NULL
   plain-column UNIQUE index) gates every table that must be
   (re-)streamed on an anchored resume; a keyless one is refused
   loudly, naming `--force-overwrite`. Kept tables may be keyless —
   they are exact at the adopted anchor.

5. **Schema-stability guard.** The resume re-reads the schema and
   refuses on fingerprint drift vs `prior.Schema` — DDL in the gap
   would corrupt the replay claim. Implementation wart, named: the
   fingerprints are compared in the manifest's JSON-round-tripped
   domain (`manifestSchemaFingerprint`), because the IR's decode hooks
   materialize concrete zero values (e.g. nil `Column.Default` →
   `kind=None`) that make `ir.ComputeSchemaHash` unstable across the
   store round-trip.

6. **`--chain-slot` resume is an adoption, not a creation.** The
   resume preflights the adopted anchor against the standing slot
   (`ir.ChainResumePreflighter` — missing/advanced slot → loud refusal
   naming `--force-overwrite`), then opens its snapshot with
   `PersistChainSlot=false` (temporary-anchor shape): the chain slot
   already exists at the right position, so there is nothing to create
   or commit, and the adopted slot is **never** dropped by this run's
   failure path.

7. **Commit timing change (amends ADR-0083).** `BackupSnapshot.Commit`
   now fires the moment the anchor-stamped in-progress manifest is
   durable — *not* after the final manifest write. Once a durable
   manifest references the anchor, the chain slot must survive a later
   failure of the same run (it is the WAL-retention guarantee the
   resume adopts); a failure *before* the manifest is durable still
   drops the slot via the uncommitted Close. Consequence, named
   loudly: an interrupted `--chain-slot` run now intentionally leaves
   a standing slot retaining WAL — resume it or `--force-overwrite` +
   `slot drop` it; the refusal/log text says so.

8. **Recovery-message reversal (ADR-0083).** The slot-already-exists
   refusal's crashed-run clause now says *re-run the same `backup
   full` command — resume adopts the slot* — and explicitly warns that
   dropping the slot releases the WAL the resume needs. Drop +
   `--force-overwrite` is reserved for a deliberate fresh start.
   `--force-overwrite` itself now also discards an *in-progress* prior
   (previously it only applied to complete ones), so the escape hatch
   the guards name is actually actionable.

9. **`resolveParent` hardening.** Incremental/stream parents with
   `partial_state == in_progress` are refused loudly ("a chain cannot
   extend from an incomplete link"). Required by (1): in-progress
   fulls now carry an anchor, which would otherwise let an incremental
   silently chain off a crashed full whose row chunks are incomplete.

### ADR-0084 §3 amendment

The crash/resume contract gains one clause: *the crashed manifest
carries the chain anchor from its first write, and the resume adopts
it as the final `EndPosition`* (this ADR). Everything else in §3
(table-granular re-stream per Bug 135, pre-staged entries, the
committer) is unchanged; the pre-sweep manifest commit adds one
manifest Put per run.

## Consequences

- The chain-gap class is closed end-to-end and pinned by
  `TestBackup_ResumeAnchorAdoption_NoChainGap` (interrupt → gap writes
  to a kept AND a re-streamed table across the value families → resume
  → incremental → chain restore → multiset-exact content equality),
  plus the operator-pre-created-slot mirror and the keyless refusal.
- ADR-0010's idempotent-applier semantics are now **load-bearing for
  backup chains**: the overlap-replay convergence argument above is
  the reason a resumed backup may mix two snapshots' table states at
  all. Any future change weakening applier idempotency must revisit
  this ADR.
- An interrupted-then-abandoned `--chain-slot` backup retains WAL
  until the operator acts (resume, or `--force-overwrite` + drop).
  That is deliberate: the alternative (auto-drop on failure) was
  exactly the silent-gap funnel. The refusal text and the
  slot-persisted log line both name the retention cost.
- A graceful failure that never wrote the in-progress manifest leaves
  no slot and no manifest — re-runs start clean, as before.
- Older binaries resuming a post-fix crashed manifest still exhibit
  the pre-fix behaviour (they overwrite the anchor); nothing can be
  done about that from this side. Post-fix binaries resuming pre-fix
  manifests take the conservative re-stream-everything path.

## Alternatives rejected

- **(b) Refuse to resume entirely when an anchor exists.** Loud and
  trivially sound, but it discards the resume feature exactly when it
  matters (large corpora, where crashes are most likely) and makes
  `--chain-slot` crash recovery a manual slot-drop dance — the funnel
  this ADR removes.
- **(c) Re-stream everything on every resume.** Sound and simple, but
  pays the full redo cost on every crash; kept-table reuse is the
  point of resume. Retained only as the fallback for pre-fix
  manifests, where the anchor is unrecoverable.
- **Auto-advance: record `A2` and re-stream kept tables whose rows
  changed in `(A1, A2]`.** Requires change detection the backup path
  doesn't have (and a CDC read of the gap window before the new
  anchor exists is exactly a chain replay — circular).
