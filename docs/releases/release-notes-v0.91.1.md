# sluice v0.91.1

# sluice v0.91.1 — CRITICAL PII silent-loss hotfix (Bug 99)

**Headline:** A typo in a `--redact='TABLE.COLUMN=STRATEGY'` selector (e.g. `users.emial` instead of `users.email`) silently no-op'd the rule, leaving plaintext PII to land at the destination. v0.91.1 closes the silent-loss surface: typo'd selectors now refuse loudly at preflight. **If you use `--redact` in production, upgrade.**

## Fixed

- **`fix(pipeline): refuse loudly when a redaction rule's selector doesn't resolve (Bug 99)`** — the existing per-strategy preflights (`mask:uuid` type, `randomize:*` PK, `hash:hmac-sha256` keyset) all silently `continue`d on a missed schema lookup; strategies with no per-strategy guard (**`hash:sha256`, `static`, `truncate`, `null`** — the workhorses) hit no check at all, so a typo on those strategies passed preflight silently and applied to zero rows at the apply step. The fix adds a selector-resolution check at the top of `preflightRedactTypes` (`internal/pipeline/redact_preflight.go`): every rule's `(Table, Column)` must resolve to a real column in the post-mappings schema, otherwise the rule is refused with the new `errRedactSelectorUnresolved` sentinel ("redaction rule's TABLE.COLUMN selector does not resolve to any column in the source schema (typo class — would silently leak PII)") naming the unresolved selector. Found by a deep bug-finding sweep against v0.91.0.

## Compatibility

- **Patch bump (v0.91.1) — hotfix.** Drop-in from v0.91.0 except for the one documented behavior change below.
- **One behavior change:** typo'd / dead redaction rules now refuse loudly at preflight instead of silently leaking. An existing pipeline whose `--redact` rule didn't actually match any column will start refusing at the next `migrate` / `sync start`. The actionable fix is to correct the selector or remove the dead rule. A rule that applies to nothing is not a no-op, it's a silent compliance failure — that's exactly the loud-failure tenet's job to surface.

## Who needs this

- **Anyone using `--redact` on `sluice migrate` / `sluice sync start` / `sluice backup`** — especially with `hash:sha256`, `static`, `truncate`, or `null` strategies (no upstream guard before this fix). Upgrade ASAP; the typo-class leak fires silently against any current rule that doesn't resolve, and there is no other indication you're affected besides comparing source and destination row-by-row.
- **Everyone else:** no action needed. Patch-level drop-in from v0.91.0 with no other surface changes.

## How to confirm the fix on your deployment

After upgrading to v0.91.1, re-run a representative `sluice schema preview --redact='your.rule.spec=...'` against your source. A previously-silent typo will now surface as `pipeline: redaction rule's TABLE.COLUMN selector does not resolve ...` naming the offending entry. A rule with a correct selector continues to apply silently as before.
