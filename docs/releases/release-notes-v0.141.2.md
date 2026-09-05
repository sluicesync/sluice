# sluice v0.141.2

> **Correction (2026-09-05).** One sentence below is wrong, and it is wrong about a safety property. Fixed in **v0.141.3**.
>
> **"the stream also has to be stopped first, which `sluice sync decommission` already enforces."** It enforces that only when the control row records a slot name. A stream started **without `--slot-name`** — the default — records an *empty* one, and the empty case was handled by the first arm of a switch that sat ahead of the active-stream refusal. So on the default configuration the command did not refuse a running stream: it deleted that stream's control row underneath it, dropped neither the slot nor the publication, reported the fresh row as "a legacy row from an older sluice", printed `stream "…" decommissioned` and exited 0. The orphaned slot then blocks the next cold start with `replication slot "sluice_slot" already exists`.
>
> This behaviour is **older than v0.141.2** — v0.141.1 is identical, and it is not a regression. What this release did was advertise the command at three warning doors alongside a statement of what it enforces, which is what turned a latent gap into printed advice. **If you ran `sluice sync decommission` without `--slot-name` and it reported no slot dropped, check `sluice slot list` for a leftover.**
>
> v0.141.3 recovers the slot name from the position the stream itself recorded, so the refusal and the drop both reach the default configuration. It does not adopt the engine default as a guess — that caution was correct and is kept for rows where no name can be recovered.


**Two Postgres warnings were telling operators things that were not true — one of them in the direction that hides harm.** Both were found by v0.141.1's own regression cycle, and both were claims v0.141.1's release notes repeated. No code path that moves data changed; if you do not run Postgres CDC or `backup full --chain-slot`, there is nothing here for you.

## Fixed

**A plain `sync start` was warning you about a backup flag you never passed.** The publication-exposure warning has two doors — `backup full --chain-slot`, and *any* stream recreating a publication that has gone missing, warm resume included — and until now they shared one message, written for the first. So an operator whose stream was recreating its publication read that "a chain slot needs a database-wide publication" and that "`--chain-slot` keeps the publication after the run", with no backup anywhere in the picture. Worse, the shared message named no stream remedy at all — not even `sluice sync decommission`, which is the only thing that retires a stream's slot and its *own* publication together — so the one door whose operator could act on that remedy was the one door not told about it. Each door now has its own wording, and a new AST roster (`TestPublicationExposureSiteRoster`) fails the build if a call site does not name which door it is, with a companion gate grading the wording itself.

**The advice about dropping a publication described the loud outcome as if it were the only one.** Both the warning and v0.141.1's notes said that dropping the publication out from under a live stream pins the slot's restart position behind the drop and the stream then fails to resume. That holds only when the stream has to decode something written after the drop. When it does not, the resume **succeeds**: the publication silently widens to `FOR ALL TABLES`, the stream replicates the next change and reports nothing wrong, and every keyless table in the database begins refusing `UPDATE` from that moment. The quiet branch is the dangerous one, and describing only the loud branch is what kept it out of sight. All three doors now state it as a fork, and the advice not to drop the publication stands — it is stronger for being accurate about what happens if you do.

**The remedy the multi-schema warning pointed at did not do what it said.** It told you `sluice sync decommission` "drops the slot and the publication together". It drops the slot, and the publication only when the stream had one of its own: the shared default `sluice_pub` is deliberately never dropped, because other streams may read through it. That is the dominant configuration at these doors, so the common case was an operator running the suggested command, getting exit 0, and keeping the `FOR ALL TABLES` publication that was the entire subject of the warning. (The decommission report itself was honest about this — it says the shared default was not removed. What was wrong is the warning that sent you there.) Every message that names decommission now says what it actually retires and that a shared `sluice_pub` has to be dropped by hand once nothing reads it; the stream also has to be stopped first, which `sluice sync decommission` already enforces. This claim is specific to v0.141.1 and to that one warning — v0.141.0's version named no command — and it was found by the pre-tag review of *this* release, which caught it being copied onto a second door.

## Compatibility

Drop-in from v0.141.1. No flag change, no format change, no new error codes, and no change to which tables either warning reports — only the text and the remedy each door prints.

There is nothing to re-run and no data to re-check. If you followed the old advice and dropped a publication, the outcome you got is one of the two described above: a stream that will not resume, or a stream that came back green with a database-wide publication. The second is worth checking for, because it looks like success — `SELECT pubname, puballtables FROM pg_publication` will show `t` on a publication you had scoped.

The same query is the check if you ran `sluice sync decommission` expecting it to remove the publication: a surviving `sluice_pub` with `puballtables = t` is still refusing `UPDATE` on every keyless table in that database, whatever the decommission report said.

## Who needs this

Anyone running Postgres CDC who has seen `UNSELECTED-NAMESPACE-EXPOSURE`, and anyone who acted on what it or v0.141.1's notes said about dropping a publication. The v0.141.1 archive carries a correction banner naming both claims.

## Install

```
brew install sluicesync/tap/sluice
scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket && scoop install sluice
go install sluicesync.dev/sluice/cmd/sluice@v0.141.2
docker pull ghcr.io/sluicesync/sluice:0.141.2
```
