# sluice v0.103.2

One warning that was missing, and the gates that stop three recurring mistakes from recurring. Drop-in upgrade, no flag changes, no behaviour changes to any data path.

### Fixed

**A `DEFERRABLE PRIMARY KEY` is no longer dropped silently against a MySQL-family target.** InnoDB has no deferred-constraint concept, so the attribute genuinely cannot be carried — but it vanished without a word, while a `DEFERRABLE UNIQUE` and a `DEFERRABLE FOREIGN KEY` in the same schema and the same migrate run both warned. The consequence is the one the attribute exists for: a bulk key shift like `UPDATE t SET id = id + 1` commits on the source and fails partway through on the target with a duplicate-key error. It now warns, naming the table, the constraint and that consequence. A Postgres target carries the attribute properly, so this affects MySQL-family targets only.

### Internal

**Three gates, each built for a mistake that had already happened more than once.**

The first holds the operator documentation's filtered-sync engine list to the code. Three consecutive releases described *which engines* a filtered-sync behaviour applied to, and got it wrong each time; the worst of them told SQLite and trigger-CDC operators their targets had been diverging silently, for a mode those engines refuse at preflight and could never have entered. A filtered continuous sync requires the source's change stream to deliver full row before-images, which is declared by a compile-time capability pin — so the gate derives the engine set from those pins and fails if the doc disagrees. The documentation also gained the sentence a reader actually needed and never had: which sources can run a filtered continuous sync, why the others cannot, and that `migrate --where` is unaffected.

The second requires every optional capability discovered at runtime to have a compile-time pin. That discovery idiom fails silently by construction — if an implementing method's receiver or signature drifts, the check simply stops matching, and the build, the vet pass and every test stay green while the feature quietly stops existing. Two capabilities had already fallen through, one of which would have left every dead-tuple alert threshold permanently inert with no signal at all.

The third holds the telemetry sink's two outputs to the byte-identity its documentation promises. That promise was false for `<`, `>` and `&` — and a pin for exactly this existed and could not see it, because its fixture used a character set the encoder never re-escapes. The replacement drives the same nine-shape corpus through both sinks, organised around what could make the two paths differ rather than around what looks like a realistic value.

**Smaller repairs in the same pass.** The telemetry sink now narrows an over-permissive existing file to owner-only rather than promising that in documentation and only enforcing it at creation; the local race-detector gate script, which is the pre-tag gate for concurrency work, no longer fails to parse under Windows PowerShell and no longer defaults to a toolchain older than the module requires; and a load-bearing comment that justified an error carve-out with a mechanism replaced a release earlier now states the reason that is currently true.

### Compatibility

No breaking changes and no flag changes. The only behaviour difference is one additional warning during schema creation against a MySQL-family target, in a case that previously produced silence.

## Who needs this

- **Anyone migrating a Postgres schema with a `DEFERRABLE PRIMARY KEY` to a MySQL-family target** — the attribute was being dropped without a word. You now get a warning naming the table and what breaks. If you have already migrated such a schema, check any bulk key-shift workload: it commits on the source and fails on the target.
- **Anyone using `--sink-file` with a path created by something other than sluice** — the file's permissions are now narrowed to owner-only on open, instead of silently keeping whatever mode it was created with.
- Everyone else: no action needed beyond upgrading. Nothing on any data path changed.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.103.2
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:0.103.2`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
