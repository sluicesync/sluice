# Release-notes template

Sluice's GitHub release notes follow a stable structure so operators can scan a release for the parts they care about — what's new, what's fixed, what changes for them, and whether they need to upgrade urgently. The template below is the canonical shape; copy it into the release body when cutting `vX.Y.Z` and fill in.

The CHANGELOG entry is the source of truth for *what changed*; the GitHub release notes are the source of truth for *how to talk about it to operators*. Both stay in sync — the template's sections map directly to CHANGELOG sections (Added / Fixed) plus the operator-framing additions (Compatibility, Who-needs-this).

---

## Template

````markdown
# vX.Y.Z — <one-line headline>

<One-paragraph summary. What's the shape of this release — feature wave,
patch, security fix? Anchor to a real-world driver (testing surfaced X,
v0.7.0 testing closed Y) when applicable.>

## Highlights

- **<Feature name>.** <One paragraph on what it does, why it matters,
  what enables it. Cross-reference the relevant ADR by number when one
  exists.>
- **<Feature name>.** <…>

## Fixed

- **Bug N — <one-line symptom>.** <One paragraph: cause, fix, impact.
  When the bug was a silent-corruption class, lead with that — it's
  the load-bearing reason to upgrade.>

## Compatibility

- IR change: <interface or struct change>. <Migration guidance for
  out-of-tree implementers when applicable; "no expected impact" when
  not.>
- Behavioural change: <observed change>. <When operators may need to
  adjust configuration; "no operator action required" when not.>
- No CLI flag changes; no migrate-state schema changes.

## Who needs this

- **<Operator audience 1>** — <one-line reason>.
- **<Operator audience 2>** — <one-line reason>.
- Everyone else is on the same code paths as v(X.Y.Z-1); upgrading is
  optional but recommended.
````

---

## Section-by-section guidance

**Title and headline.** `# vX.Y.Z — <headline>`. Keep the headline ≤ 70 characters; it's the GitHub release-list rendering. For a feature-driven minor release, name the headline feature (`v0.8.0 — schema diff + seven real-world bug fixes`). For a patch, name the audience (`v0.8.1 — PlanetScale auto-detect + CI fix`).

**Opening paragraph.** Anchor the release to context the operator already has — what testing surfaced this batch, what previous release this is a follow-up to, what user-visible problem this solves. Avoid changelog-restatement; the CHANGELOG is one click away.

**Highlights.** Reserved for net-new capability. Each bullet is one paragraph: what the feature does, why it matters (the operator-visible payoff), and the ADR number when one exists. Order by importance, not alphabetically.

**Fixed.** Each fixed-bug bullet starts with `**Bug N —**` (the canonical bug catalog reference) and the one-line symptom. The body is a one-paragraph cause/fix/impact. **Lead silent-corruption fixes with the silent-corruption framing** — those are the load-bearing reasons to upgrade and operators decide priority on that signal alone. Examples:

> **Bug 19 — silent TIMESTAMP corruption in MySQL→PG CDC on non-UTC hosts.** […] **If you are running CDC against a MySQL source from a non-UTC host or with a server `default_time_zone` other than UTC, upgrade to vX.Y.Z — prior versions silently corrupt TIMESTAMP values.**

**Compatibility.** Three sub-themes worth covering when relevant; omit any that don't apply:

- **IR changes** — interface signature changes, new optional surfaces, struct field additions. Note migration guidance for any out-of-tree engine implementations.
- **Behavioural changes** — runtime behaviour that an unchanged config produces differently. The Bug 19 fix is the canonical example: every MySQL connection now runs `SET time_zone='+00:00'` after handshake, which is a behaviour change visible to applications relying on session TZ.
- **CLI / state-schema changes** — flag removals, control-table column additions, etc. Most releases have neither; explicitly say so when both are stable.

**Who needs this.** The operator-routing section. List the audiences that have a load-bearing reason to upgrade, then close with "Everyone else …". Keep each audience to one line. Examples of well-shaped audience lines:

- "Anyone running CDC against MySQL from a non-UTC host" (Bug 19's audience)
- "Anyone running cross-engine resumes (PS↔PG, MySQL↔PG, etc.)" (Bug 20's audience)
- "Anyone migrating from PlanetScale" (Bug 22's audience)

The shape is "operator persona → reason in one short clause". If you can't write the reason in one clause, the audience is too broad — split it.

---

## When to deviate

Patch releases (`vX.Y.Z` with non-zero Z) often don't have all four sections — a single CI test fix might be just `## Fixed` plus `## Who needs this` saying "CI users running `-tags=integration`". That's fine; the template is a checklist, not a contract. Drop sections that would be empty.

Minor releases that bundle a headline ADR with several bug fixes (the v0.8.0 shape) keep all four sections. Major releases (v1.0+) gain a `## Migration` section above Compatibility describing the upgrade path from the previous major.

## See also

- `CHANGELOG.md` — source of truth for what changed.
- `docs/dev/roadmap.md` — what's coming next.
- Past GitHub releases — examples of the template applied. v0.7.0 and v0.8.0 are the closest to canonical shape; older ones predate the formalisation.
