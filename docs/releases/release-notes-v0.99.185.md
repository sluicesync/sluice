# sluice v0.99.185

**A single-fix release that completes v0.99.184's Bug 179 fix. That release made a mode-mismatched encrypted chain refuse loudly at build (closing the un-restorable-backup class), but its "omit `--encrypt-mode` to inherit the chain's mode" convenience was unreachable from the CLI — omitting the flag still arrived as an explicit `per-chain`, so extending a `per-chunk` encrypted chain was refused with a hint that couldn't be satisfied. This release makes omission actually inherit. No data was ever at risk (the failure was always a loud build-time refusal), but the release's headline behavior and printed remediation now work as documented.**

## Fixed

**Bug 180 — `--encrypt-mode` omission now inherits an existing chain's mode from the CLI.** The mode was defaulted to `per-chain` in three places before the inherit logic could see it: the `--encrypt-mode` flag's kong default, the CLI encryption-builder, and (for a resumed full) the chunk-writer's own fallback. The net effect from the command line was that an omitted `--encrypt-mode` was indistinguishable from an explicit `per-chain`, so `backup incremental` / `backup stream` / a resumed `backup full` extending a `per-chunk` chain was refused (`conflicts with the chain's encryption mode`) even though the operator expressed no preference — and the tool's own hint, "omit `--encrypt-mode` to inherit it," was impossible to follow.

The fix makes an omitted flag resolve to an empty value that flows all the way to the backup orchestrator, which owns the decision: **inherit the chain's mode when the flag is omitted, default a fresh full to `per-chain`, and refuse loudly only when an *explicit* `--encrypt-mode` conflicts with the chain.** The resumed-full path additionally writes the resolved mode back so the chunk-writer and the manifest agree (the same one-mode-per-chain invariant the v0.99.184 fix enforces for incrementals and streams). Behavior is pinned *through* the CLI parser — the earlier fix's pin exercised the internal function directly and so passed a code path no command-line user could actually reach.

## Compatibility

**No breaking changes; strictly more permissive.** A fresh `backup full` with an omitted `--encrypt-mode` still defaults to `per-chain` exactly as before. Explicit `--encrypt-mode=per-chain`/`per-chunk` are unchanged. The only behavior change is that omitting `--encrypt-mode` when extending an encrypted chain now *succeeds by inheriting* the chain's mode instead of being refused — the intended v0.99.184 behavior. Chains built by v0.99.184 restore identically. No format-version change.

## Who needs this — action required

- **Anyone extending an encrypted backup chain (`backup incremental` / `backup stream` / a resumed `backup full`) whose full backup used `--encrypt-mode=per-chunk`.** Before this release you had to pass `--encrypt-mode=per-chunk` explicitly on every extension; now you can omit it and it inherits. If you were already passing it explicitly, nothing changes.
- **Everyone else: no action.** Plaintext backups, `per-chain` chains (the default), and all migrate / sync paths are unaffected.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.185 · **Container:** ghcr.io/sluicesync/sluice:0.99.185
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
