# sluice v0.112.1

**CRITICAL fix — a transient connection-slot shortage that then CLEARS could stop a `migrate` or `sync` cold copy permanently, with no log line, no error and no exit code. This is a regression sluice shipped in v0.112.0 earlier today; v0.112.0 is the only affected release. Drop-in upgrade — no flags, error codes or on-disk formats changed.**

v0.112.0's fix made the copy's retry budget per-chunk rather than per-table. That was correct and it fixed a real defect: at the shipped default parallelism, sixteen workers meeting a shortage at nearly the same moment consumed the whole run's budget between them and the seventh aborted the run before the first six had retried anything. What that fix also did — and this was not seen at the time — was remove the run-wide give-up bound, and that bound turned out to be the only thing aborting the run before the token pool could drain. A loud failure became a silent hang. That is the worse of the two, and it is what this release exists for.

## Fixed

**A transient SQLSTATE 53300 (`too many connections`) that then CLEARS deadlocks the parallel bulk copy.** The run stops, and it stops the way nothing else in sluice is allowed to: no log line, no error, no exit code at all. The process sits there. Measured on the regression cycle's reproduction — a six-million-row migrate with a shortage injected a few seconds in and lifted a couple of seconds later — sluice landed 1,593,750 of 6,000,000 rows, left 48 of its 67 goroutines blocked in the copy gate's `Acquire`, and made zero further progress. v0.111.1 aborted loudly in about four seconds on the identical injection. Reproduced 2 of 2 on v0.112.0 against 0 of 2 on v0.111.1.

**Two plausible explanations were investigated and killed by measurement before the real one was found**, which is worth saying because both were reported confidently. The first was that the shrink's token retirement was unbounded; it is not — the retirement telescopes, any monotone descent from N sums to exactly N−1, and a deterministic reproduction driving the real gate through the shipped sixteen-worker ladder does not deadlock. The second was that the copy merely crawled because nothing ever raised the parallelism back up; that is real and it is fixed here, but it is not the root cause — the forensic dump measured zero live tokens in the pool, not one, and zero chunks completing over the following second.

**The root cause is a token-class confusion, and the guarding comment was arithmetically true while being operationally false.** One connection-budget gate is shared by the whole run, and two very different kinds of worker draw from it. A chunk worker takes a token, opens its connections, copies its key range and gives the token back — it can always finish on its own. A table-pool worker takes a token for the table's primary connection and holds it for the *whole* table copy, which means it is holding that token while blocked waiting on that table's chunk workers — it can never finish until chunk tokens are available. On the shipped `--bulk-parallelism 16 --table-parallelism 1` shape, the budget's 16 tokens are therefore **one base token plus fifteen chunk tokens**. A shrink retires at most fifteen, so "at least one token always survives to make forward progress" is a true statement about the arithmetic — but the survivor is the base token, held by the goroutine that is itself waiting on the chunks whose tokens were just retired. Circular wait, and the whole pool at zero. The invariant was about token *count* when the property actually needed was "one token that can make progress."

The run-wide gate is necessary to the failure rather than merely aggravating it: the same harness against a per-table gate, which has no base token, finishes every chunk.

**The fix replaces the mechanism rather than patching the arithmetic.** The token channel and its retirement counter are gone, replaced by a resizable counting semaphore whose admission rule is evaluated under the lock — which makes the entire accounting-underflow class inexpressible rather than merely absent, and has the side benefit that a shrink now takes effect immediately for new acquires instead of lazily as workers happen to finish. Base and chunk tokens are distinct classes at the API, and the shrink floor is `max(aimdTarget, baseHeld+1)` so a shrink can never leave a long-lived base holder owning the last slot. That floor is the honest number rather than a fudge: a base connection is already open and a shrink cannot close it, so the only thing a shrink can actually govern is how many *more* connections get opened. With it, a shortage degrades the copy to one chunk at a time — which is what "a transient slot shortage degrades to slower-but-correct" always claimed.

**The additive-increase half that was missing is now there, and it is gated on evidence rather than on a timer.** The original design was multiplicative-decrease only, on the argument that a bulk copy is a bounded one-shot phase and re-probing upward would just re-trigger the same shortage. Bug 228 priced that argument properly: the decrease is permanent and the copy is not short, so a two-second blip six seconds into a six-million-row table pins the remaining hours of it at the floor. Recovery now fires on an open that actually succeeded, so a still-saturated target — whose opens are still failing — grows its cap exactly zero times, while any concurrent shortage halves it. Decrease dominates increase by construction, which is the point of AIMD.

**A second deadlock, reachable with no connection shortage at all, is closed by the same floor.** The copy budget is table parallelism × within-table parallelism, and a fresh run cannot reach the chunked path at within-table parallelism 1 because the chunking preflight refuses it — but a `--resume` whose *recorded* chunk plan re-engages chunking bypasses that check, so a resumed run gets a budget equal to the table parallelism, which the base holders alone can consume entirely. This one predates v0.112.0; it has been reachable since the run-wide budget gate shipped in v0.99.134. It is pinned separately.

**The sibling was swept rather than left for an audit.** `backup`'s table pool has the identical topology — a base token held across a range fan-out — and it now declares the base class too. It is documented in place as currently unreachable, because nothing on the backup read path classifies a slot shortage or calls the shrink, so the gate never shrinks there. Fixed as a class, not as the one reachable instance.

**What pins it, including the part that nearly got away.** The gate's own tests pin the floor, the recovery after a transient clears, a full-topology storm that must complete, and the resume-shaped budget. They cannot, however, see whether the production *call sites* tag their token class — reverting one call site from the base-class acquire back to the untagged one restores the deadlock in full and leaves every one of those tests green, which was confirmed by mutation rather than assumed. So there is also an AST roster over both table pools and both chunk fan-outs, stating each one fixed-or-exempt with its reason and failing if the package's set of gate callers grows beyond the roster, with anti-vacuity floors in both directions.

## Compatibility

No flags were added, renamed or removed. No error codes changed. Nothing about the on-disk formats — backup chunks, chain manifests, resume state — is touched. This is a drop-in upgrade from v0.112.0 and from anything older.

**One behaviour change is worth naming: copy parallelism now recovers after a transient shortage clears, where previously it only ever decreased.** A run that degrades under pressure and then finds the pressure gone returns to its measured budget instead of crawling at the floor for the rest of the copy. The recovery never exceeds the budget the preflight measured, and it only advances on a connection the target actually seated, so it cannot re-trigger the storm the decrease just quelled.

**A known limitation is recorded rather than bundled in.** The per-chunk retry budget is keyed by chunk index alone, and under the run-wide gate that index is not unique across tables — table A's chunk 3 and table B's chunk 3 share one budget, so a give-up can fire earlier than the per-chunk bound advertises. It is bounded and it is loud: the failure mode is an early abort, never a stall, which is why it is documented in place and tracked as roadmap item 137 instead of being folded into a fix for a silent hang.

## Who needs this — action required

- **Anyone running `migrate` or `sync` cold-start on v0.112.0 against a target that can transiently run out of connection slots — a busy or managed Postgres with a modest `max_connections` is the shape. Upgrade.** v0.112.0 is the only release carrying this, and the practical workaround before upgrading is the unsatisfying one: notice that the copy has stopped making progress and restart it. **If a copy has ever stopped with no error and no exit code, this is why.**
- **There is nothing to re-verify, and that is a real distinction worth stating plainly.** This is a stall, not silent data loss. The copy never claims success — it produces no exit code at all — so no run has reported completion over a partial target. The rows that landed before the stall are correct; the copy simply never finished. Re-run the migration (or resume it) and check that it now reports completion.
- **Anyone resuming a migration with `--resume` against a recorded chunk plan**, where the copy budget can equal the table parallelism. That second deadlock needs no connection shortage at all and has been reachable since v0.99.134, so it is not confined to v0.112.0. The symptom is identical — a resumed copy that parks and never progresses — and the same restart-and-upgrade advice applies.
- **Backup users need do nothing.** The sibling fix in `backup`'s table pool closes the same topology, but nothing on the backup read path could trigger a shrink, so the deadlock was never reachable there.

## Install

```
brew install sluicesync/tap/sluice
scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket && scoop install sluice
```

Binaries for Linux, macOS and Windows are attached below; container images are published to `ghcr.io/sluicesync/sluice`. Every artifact carries a build attestation:

```
gh attestation verify sluice_0.112.1_Linux_x86_64.tar.gz --repo sluicesync/sluice
```
