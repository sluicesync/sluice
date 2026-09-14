# sluice v0.152.2

**A small release with one shipped change: the index and constraint phases now name their failure the way the copy phase always has.** Everything else in this tag is test infrastructure and measurement — the weekly live-platform suite that found this, and four more findings it is still working through.

## Fixed

**A missing relation during the index or constraint phase now carries a code and a hint.** Point a copy at a target whose table is absent and the copy phase answers in milliseconds with `SLUICE-E-BULKCOPY-TARGET-TABLE-MISSING` and the question worth asking. Hit the *identical* condition one phase later — the table dropped between the copy and the index build — and it answered with a bare `pipeline: create indexes: … (SQLSTATE 42P01)`: no code to search for, no hint, nothing to act on. Correct and loud, and one phase less useful than its neighbour. Two new codes, `SLUICE-E-INDEX-TARGET-MISSING` and `SLUICE-E-CONSTRAINT-TARGET-MISSING`, following the existing per-phase convention.

**The hint is deliberately not the copy phase's, and that is the whole content of the change.** At copy time a missing relation indicts schema-apply — the copy is the first thing to touch the table after it is created, so its absence points at the phase before. By the index phase the table demonstrably existed and *took rows*, so the same SQLSTATE means something later and rarer: something dropped or altered it between the copy and the DDL, or the index names a column schema-apply never created. Handing that operator "did the schema-apply phase fail?" would send them to audit a phase that provably worked, which is worse than saying nothing. The constraint arm names the likeliest real cause: a `FOREIGN KEY` whose **parent** table sits outside a `--tables` selection.

Found by the v0.152.1 regression cycle, which graded it and correctly declined to file it as a defect — the refusal was already loud, immediate, and named the SQLSTATE, relation and phase. It is the kind of gap that is invisible until the two messages sit side by side.

## Compatibility

No flags, no config, no state-table or on-disk format changes. Two error codes are added; none change meaning, and no existing message is reworded. A run that failed before fails identically, with more to go on.

## Who needs this

- **Anyone whose target schema can change under a running migration** — concurrent DDL, a shared staging target, a target reset mid-run. That is the condition these codes name.
- **Anyone running `migrate` with `--tables`** against a schema with foreign keys: the constraint code now tells you when the missing side is a parent you did not select, rather than leaving you to infer it from a SQLSTATE.
