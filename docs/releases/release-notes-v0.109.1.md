# sluice v0.109.1

**If you migrate from PlanetScale or Vitess and any table holds a wide row — a large `mediumtext`, `longtext`, or `json` column — your cold copy failed, and the error blamed the wrong end. Upgrade; nothing about your schema or data needs changing.**

## Fixed

**A cold copy from a PlanetScale or Vitess source failed outright on a table containing a wide row.** The copy aborted with:

```
rpc error: code = ResourceExhausted desc = grpc: received message
after decompression larger than max 4194304
```

That reads like the source rejecting an oversized row. It is not. **4194304 is 4 MiB — gRPC's stock client default — and sluice was the end refusing to receive.** sluice builds its VStream client directly instead of through Vitess's own dial helper, so it inherited that 4 MiB ceiling while every Vitess-native client gets 16 MiB.

VStream sends whole rows and cannot split one across messages, so a single wide row is enough to trip it: a `mediumtext` column is 16 MiB on its own, and a couple of `json` columns alongside it clear 4 MiB comfortably. Nothing about the source schema or the data was at fault, and there was no workaround needed on that side — which is worth saying plainly, because the message pointed away from the actual cause.

The receive ceiling is now **128 MiB**, overridable with a `vstream_max_recv_bytes=<bytes>` source-DSN parameter.

The size follows a rule rather than a guess. Two independent ceilings bound a VStream message — vtgate's `--grpc_max_message_size` governs what the server will **send**, and this one governs what sluice will **receive** — and only the second is ours. **sluice should never be the stricter of the two.** Vitess defaults to 16 MiB and PlanetScale runs a larger ceiling, so 128 MiB clears both; matching a server ceiling exactly would leave sluice sitting on the boundary, where a message the server sends at its own limit can still exceed ours once protocol framing is counted. It is a limit rather than an allocation, so the headroom costs nothing on ordinary traffic, where VStream packs to a far smaller packet size.

If raising it does not clear your failure, the row also exceeds vtgate's own ceiling and only the server side can raise that — the error now says so rather than leaving you to guess.

**That failure was also retried as though it were transient, so it took far longer to surface than it should have.** The error text carries `code = ResourceExhausted`, which sluice classifies as a vttablet throttler or connection-pool transient. That is correct for those and wrong for this one: the same row is re-sent on every reconnect, so the outcome can never change. A run burned its stream-reconnect budget and then its copy-retry budget before failing, producing a long and misleading trail.

It is now terminal on first occurrence, with a message that names the DSN override, names vtgate's ceiling as the remaining server-side limit, and states that it is not retriable. The carve-out matches gRPC's specific size wording rather than `ResourceExhausted` broadly, so a genuine PlanetScale throttle still retries as before.

**Postgres batched writes ignored the coordinated storage-grow pause.** That pause exists so every cold-copy lane quiesces together while a target grows its volume, instead of each lane independently hammering a struggling server. It reached only the COPY path — both batched write cores were invisible to it, while MySQL had wired the equivalent into both of its cores.

This was not a corner case. A PlanetScale or Vitess source requires an idempotent writer, so that cold-start lands on one of the two ungated cores and nowhere else — precisely the auto-growing-target shape the pause exists for. Both now participate. The upsert core additionally rides a transient by replaying its batch, which is safe there because a replayed upsert converges; the plain-insert core waits and signals but does not replay, because a failed insert batch on a dropped connection is ambiguous and has no conflict target to absorb a duplicate.

**MySQL's `LOAD DATA` cold copy ignored the same pause, and a transient on it cannot be resumed.** It now waits before starting and signals siblings when it hits one. The absent resume point is inherent rather than an oversight: that path streams a whole table through a single statement and consumes its source rows as it goes, so when the statement fails there is nothing left to replay. The error now says that explicitly instead of surfacing as a bare driver failure. Chunking it into resumable segments is tracked as follow-up work.

**A relabelled signature scheme told operators to supply key material they had already supplied.** Editing a backup chain's recorded scheme to one the operator holds no key for moves the chain from *verified* to *unverifiable*, and unverifiable warns and proceeds. The warning advised passing the chain's key material — which had already been passed — so the one signal worth acting on read as a forgotten step.

It now distinguishes "no key material supplied at all" from "material supplied that cannot verify the claimed scheme", names the claimed scheme against what was actually provided, and states that an edited scheme produces exactly this shape. This is deliberately not a refusal: sluice cannot distinguish a relabel from an operator who legitimately holds only the encryption key for an asymmetrically-signed chain. `--require-signature` continues to refuse outright and remains the way to make it fail.

## Changed

**Two scope claims in the v0.109.0 release notes were corrected.** Both were wrong in the direction that tells operators they were affected when they were not: the `--restart-from-scratch` clearing change does **not** affect Postgres sources (which already cleared their target and are unchanged), and the resume-position fix applies to the **serial** apply path rather than the default. The v0.109.0 notes now carry a dated correction, and the engine list behind the first claim is held to the code by a doc-sync gate so that class of error fails the build rather than reaching a release.

## Compatibility

**No format, schema, or behaviour changes.** Every item above either raises a ceiling that was too low, adds coordination that was missing, or corrects a message. Nothing that worked before behaves differently.

**One new source-DSN parameter**, `vstream_max_recv_bytes`, optional and defaulted.

## Who needs this

- **Anyone migrating from PlanetScale or Vitess** whose tables hold large `mediumtext`, `longtext`, or `json` values — this is the release that unblocks that copy.
- **Anyone whose PlanetScale or Vitess cold-start writes into Postgres**, which is the path that was missing the storage-grow coordination.
- **Anyone who read the v0.109.0 notes** and concluded they were affected by the `--restart-from-scratch` change on a Postgres source — see Changed.
