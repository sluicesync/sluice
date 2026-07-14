# sluice v0.99.242

**The cross-engine collation WARN now leads with "column data is preserved" — the same advisory, reworded so it can't be misread as data loss.**

## Changed

- **Cross-engine collation WARN reworded to reassure first.** On a cross-engine migrate (most visibly MySQL→Postgres), a source column's collation frequently has no equivalent on the target, so sluice drops the collation attribute and the target column falls back to its database/table default collation. The column *values* are fully preserved — only text sort/comparison order can change. Because MySQL tags nearly every string column with a collation, this WARN appears on almost every cross-engine migrate, and the previous wording ("dropping cross-engine column collations…") could read as though data were being discarded. The message now opens with **"column data is preserved"** and names the actual effect precisely — *"text sort/comparison order may differ."* This applies to all target engines (Postgres, MySQL, SQLite) and both emit paths (the per-column live-ALTER path and the per-table CREATE path). **Behavior is unchanged — this is wording only;** the collation was always dropped with values intact, and the WARN still fires (aggregated one-per-table) so the degradation stays visible.

## Compatibility

- No behavior or format change. If you match this WARN in log tooling, the message text changed (it no longer contains "dropping cross-engine column collations"; it now contains "column data is preserved" and "source collations have no <engine> equivalent"). The structured fields (`table`, `column`, `collation`) are unchanged.

## Who needs this

Anyone running cross-engine migrations who saw the old WARN and wondered whether their data was affected — it never was, and now the message says so plainly.

---

Install / upgrade: see the [README](https://github.com/sluicesync/sluice#install). Verify downloads against `checksums.txt`.
