# sluice v0.147.0

A small release with one thing in it that matters: a backup chain that a quiet hour made unusable is usable again. Plus a refusal that finally has a name you can look up, and a correction to two comments that were wrong about PostgreSQL.

Take this one if you run scheduled `backup incremental` against a source that is sometimes idle.

## Fixed

**A `backup incremental` over a quiet window made its own chain unusable for the CDC handoff.** An incremental whose window captured nothing exits 0 and writes a terminal manifest with no `EndPosition` recorded. `sync start --position-from-manifest` then refused that chain as a *"pre-Phase-3.3 full backup or malformed chain"* — about a chain written seconds earlier by the same binary, whose correct resume point was sitting in that same manifest's `StartPosition`, byte-identical to the parent full's end. Restore off the chain worked the whole time; what was lost was the no-re-bulk handoff, which is exactly the path you reach for when you are trying to *avoid* a full re-copy. The undocumented workaround was to take one more incremental that happened to catch a change.

The two cases were distinguishable all along and the refusal conflated them. A pre-Phase-3.3 **full** has no `EndPosition` and no `StartPosition` either, so it genuinely has nowhere to start, and it still refuses. An **incremental** has a `StartPosition` by construction. The resume now uses it, and refuses only when both are empty. This reaches chains **already on disk** — a fix that only corrected new writes would have left every existing one permanently unusable. Nothing rewrites a manifest, so every restore-side guard still reads exactly what it reads today; only the position handed to the resuming stream changes. Re-reading a window that produced nothing is the safe direction — a resume that re-observes a span cannot skip one.

**The REAL render-fidelity refusal now has an error code: `SLUICE-E-REAL-RENDER-LOSSY`.** v0.146.0 put this refusal on the plain `migrate` path, and it had neither a code nor a grep-stable marker — while `sluice diagnose` and the error-triage workflow resolve entirely through `SLUICE-E-*` against the error-code table. A terminal refusal on a mainstream path was unroutable by both. The code sits at the shared verifier, so all four lanes that share the encoding raise the same one: the `sqlite-trigger` capture lane, `d1-trigger`, the `--source-driver d1` bulk copy, and `migrate --stage-local`. It is named for the engine behaviour rather than for D1, because an engine ignoring the `!` alternate-form-2 precision flag is not a D1 fact.

**Two comments were wrong about `encode(bytea, 'escape')`, and both are corrected.** They said it octal-escapes every byte `>= 0x7F`; measured on real PostgreSQL 16, `0x7F` passes through raw and the threshold is `>= 0x80`. More interestingly, they implied the encoding is lossy. It is not: `decode(encode(x,'escape'),'escape') = x` holds. What makes it a trap is that its *text* is a lie — feed the output back through `decode` and you get your bytes; read it as text, as any JSON parser must, and `\000` is four characters. The v0.146.0 defect was never "information was lost", it was "the text says something other than what the bytes say", and calling it lossy would send the next reader hunting the wrong kind of bug.

## Compatibility

No behaviour change to anything that worked before. The `--position-from-manifest` change only *accepts* chains it previously refused; a chain it accepted before resolves to the same position. One new error code, which is additive. No flag added, renamed or removed; backup format version unchanged at 10.

If you have a chain that `--position-from-manifest` refused as malformed and you worked around it with an extra incremental, that workaround is no longer needed — but nothing about the chain you built that way is wrong.

## Who needs this

Upgrade if you run scheduled `backup incremental` and your source is ever idle for a whole window, since that is the shape that produced an unusable chain. Everyone else can take this at their leisure.

## Install

```sh
# Homebrew
brew install sluicesync/tap/sluice

# Scoop (Windows)
scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket
scoop install sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.147.0
```

Container image: `ghcr.io/sluicesync/sluice:v0.147.0`
