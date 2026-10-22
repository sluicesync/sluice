# sluice v0.163.5

<!-- notes-claims-exempt: FORCE-COLD-START-HELP-STALE -->
<!-- notes-claims-exempt: VERIFY-HINT-SAMPLE-UNRUNNABLE -->

**Two operator-guidance corrections and a SQLite driver update.** The `VSTREAM-COPY-COMPLETION-UNPROVEN` refusal that v0.163.4 added, and the help of its acknowledgement flag `--assert-copy-finished`, told you to compare row counts before passing the flag. That cannot work on a source that kept taking writes while the stream was stopped, so the advice would have sent you to re-copy healthy streams. Both now give the order that was measured to work. The `sync start --force-cold-start` help no longer says a populated target always collides on PRIMARY KEY; on a PlanetScale/Vitess source it collides on nothing and silently keeps rows the source has deleted. No behaviour change in the copy, the refusal or any guard; no flag change; no backup format change.

## Fixed

- **The unproven-copy remedy now checks after the flag, not before it (sluice-testing Bug 328, loud).** A stream refused `VSTREAM-COPY-COMPLETION-UNPROVEN` has applied none of the source's writes since it stopped, so its row counts differ from the source's even when its copy finished: measured 4,992 on the target against 4,989 on the source for a complete stream. The refusal's message (`vstreamCopyUnprovenError`) and the `--assert-copy-finished` help now give this order: pass the flag once, let the stream catch up, pause writes on the source, and count each table on both sides with `sluice verify --depth count` (`--depth sample` refuses a PlanetScale/Vitess source); a table short on the target was never fully copied and needs `--restart-from-scratch`. They also say the check is mandatory, because the flag records the tables at once and nothing refuses the stream afterwards. Measured on a local Vitess 24 cluster with Postgres 18.6 and MySQL 8.4 targets: an incomplete copy reported `t2_big MISMATCH source=299500 target=149000` (and `199500` on MySQL) and matched after the re-copy; complete copies, including the one whose stopped counts read 4,992 against 4,989, reported every table `OK`. The operator docs, `AGENTS.md` and the `cdc-sync-operator` skill carry the same order, and they now list one more kind of stream refused once though healthy: a PlanetScale/Vitess stream handed off with `--position-from-manifest` on v0.163.3 or earlier, which has neither a record nor a progress row. The v0.163.4 notes carry a Correction banner saying the same, gated by `TestDeclaredNotesAmendmentsAreBannered`.
- **`sync start --force-cold-start` help states what the flag does on each kind of source (FORCE-COLD-START-HELP-STALE).** v0.163.3 corrected the refusal's hint and the `SLUICE-E-COLDSTART-TARGET-NOT-EMPTY` row but left the flag's own help saying an INSERT into a non-empty table collides on PRIMARY KEY. On a PlanetScale/Vitess source the cold-start copy writes by upsert, so it collides on nothing and keeps every target row the source has since deleted, silently. The help now says so and names `--restart-from-scratch` for re-copying a populated target. `sluice migrate --force-cold-start` is unchanged and its help was already right: `migrate` copies by INSERT from every source.

## Changed

- **modernc.org/sqlite 1.58.0 → 1.60.1** (with modernc.org/libc 1.75.6 → 1.77.1), the pure-Go SQLite driver sluice uses for local SQLite databases. It merged to `main` while v0.163.3 was being cut and was held out of v0.163.3 and v0.163.4 so that each guard change shipped alone.

## Not closed, stated

- **`sluice verify` still suggests `--depth=sample` after a count mismatch** even where that depth cannot run (every cross-engine pair, every PlanetScale/Vitess source). Filed as VERIFY-HINT-SAMPLE-UNRUNNABLE (LOW); `--depth count` is the check to use.
- **Not run for this release: `psverify` and `d1verify`.** `scripts/prerelease-triggers.sh` raised its unscheduled-pin advisory for the touched paths; this release changes message and help text and a driver version, not the paths those suites exercise.

## Compatibility

- **No flag change, no behaviour change in any copy, refusal or guard, and no backup format change** (`BackupFormatVersion` stays 15). Drop-in from v0.163.4.

## Who needs this

- **Anyone who has met, or expects to meet, the `VSTREAM-COPY-COMPLETION-UNPROVEN` refusal:** follow the order above instead of comparing counts while the stream is stopped. You do not need this binary to do so; the corrected order works on v0.163.4.
- **SQLite and D1 users:** the driver update.
- **Everyone else:** nothing to do.

---

**Install:** `brew install sluicesync/tap/sluice` · `go install sluicesync.dev/sluice/cmd/sluice@v0.163.5` · **Container:** `ghcr.io/sluicesync/sluice:0.163.5`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
