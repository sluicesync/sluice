# sluice v0.140.0

**Five defects that only a real server could show, three of them in code v0.139.0 had just shipped.** The trigger-CDC engine stops halting your stream over somebody else's table, a Cloudflare D1 read stops silently copying text the API rewrote in transit, and the Postgres standby refusal starts reaching the replicas people actually run. If you use `postgres-trigger`, back up from a Postgres replica, or migrate D1 tables holding text that is not valid UTF-8, one of the entries below names you.

## Fixed

**Trigger-CDC no longer halts the stream over DDL on a table it does not capture.** A Postgres event trigger is database-wide and cannot be attached to a schema, and this one filtered by command tag alone. So a `CREATE TABLE` in a schema sluice never touches recorded a DDL marker and stopped the stream with the restart-from-scratch remedy: somebody else's unrelated table broke your sync. The tier now asks the question the drop arm has asked since v0.136.0, whether the command's relation carries this install's capture trigger. The resolution was measured rather than assumed, because it differs by shape and one shape could have failed quietly: an `ALTER` reports the table directly, including `ADD CONSTRAINT`, while a `CREATE INDEX` reports the index and resolves through its table. A brand-new table in a captured schema is exempt for the same reason it was always harmless, since without a capture trigger it produces no change rows at all, and `sync add-table` is how it joins. An install created by an earlier release warns `STALE-CAPTURE-FUNCTION` until `sluice trigger setup` is re-run.

**A `trigger setup --allow-polled-fingerprint` re-run no longer records its own DDL as operator DDL.** That flag is what an operator reaches for when the role can no longer create event triggers, after a demotion or on a managed provider. The plan then rendered no capture functions at all, so an event trigger created by an earlier privileged install kept firing an older function body, one that predates the current suppression protocol and did not recognise setup's own transaction. Measured on Postgres 16.15, the re-run recorded markers for sluice's own statements, including on the change log and the meta table themselves, and the next resume would refuse them as operator DDL. Setup now refreshes those bodies whenever one of its event triggers is live. No new failure mode comes with it, because the plan already replaces the row capture function, so any role that can complete a re-run must already own these.

**A Cloudflare D1 read refuses when the API mangles invalid UTF-8, instead of copying the mangled value.** D1 stores invalid-UTF-8 text intact on disk and replaces every invalid byte with the Unicode replacement character in its query response, three bytes for one, on the server. The cell therefore arrives as perfectly valid UTF-8, which is why sluice's own encoding guard could never fire for it. The independent evidence is on the server: the summed byte length of the table's text-storage cells, which the reader now reads in the same round trip as its closing row count and compares against the bytes it actually received. A quiescent table whose totals disagree refuses with the new `SLUICE-E-D1-TEXT-MANGLED`, naming both numbers and pointing at `hex(col)`, which still returns the true bytes. Measured on live D1: a three-byte cell arrives as seven.

**The Postgres standby refusal now reaches a standby at the default `wal_level`, which is what a read replica actually runs.** On a hot standby both the standby check and the `wal_level` check hold, and whichever ran first decided the error. So v0.139.0's refusal only fired on a standby someone had set to logical; on the default setting `backup full` still copied every row and died at the position capture, exactly the waste that fix existed to prevent. It also told the operator to change a setting a standby inherits from its primary and cannot change. The standby check now runs first everywhere both run, so `SLUICE-E-CDC-STANDBY-SOURCE` is what an operator sees. Found by the v0.139.0 regression cycle on a real streaming standby, restarted under each setting.

**The MySQL snapshot handoff log line reports the position arm actually taken.** A GTID-mode source that has executed nothing falls back to a file-and-offset anchor, but the handoff line still announced the GTID arm with an empty set, moments before the token it described was persisted in the other shape.

## Compatibility

Drop-in from v0.139.0. No flag change, no format change. One new error code, `SLUICE-E-D1-TEXT-MANGLED`.

Two behaviour changes worth knowing. A Cloudflare D1 table holding invalid UTF-8 in a text column now refuses where it previously copied a rewritten value; the source is intact and readable as `hex(col)`, so the remedy is to repair or exclude those rows. And a `postgres-trigger` install created by an earlier release will warn `STALE-CAPTURE-FUNCTION` until `sluice trigger setup` is re-run once, which is the existing posture for any change to those bodies.

The D1 refusal is the one addition; everything else in this release removes a refusal or a wasted copy.

## Who needs this

Anyone running the `postgres-trigger` engine, especially alongside schemas sluice does not sync; anyone taking backups from a Postgres replica; and anyone migrating Cloudflare D1 tables whose text columns may hold bytes that are not valid UTF-8.

## Install

```
brew install sluicesync/tap/sluice
scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket && scoop install sluice
go install sluicesync.dev/sluice/cmd/sluice@v0.140.0
docker pull ghcr.io/sluicesync/sluice:0.140.0
```
