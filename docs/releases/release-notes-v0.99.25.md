# sluice v0.99.25

**A string with an embedded NUL byte going to a Postgres text column now fails clearly, early, and by name** — instead of a cryptic mid-COPY driver error. PostgreSQL text types can't store a NUL (`0x00`); MySQL text types can, so a cross-engine MySQL → Postgres copy could hit this. **Drop-in from v0.99.24.**

## Fixed

- **Embedded NUL (`0x00`) into a PG text column: loud, early, actionable refusal (Vector C).** PostgreSQL `text`/`varchar`/`char` cannot store a NUL byte — PG rejects it with SQLSTATE 22021, which over the COPY protocol surfaces as an opaque stream error disconnected from the offending row. A MySQL `CHAR`/`VARCHAR`/`TEXT` *can* hold embedded NULs, so a cross-engine MySQL → Postgres migration of such data could trip this. sluice now detects the NUL at the value layer and refuses with a message that names the column and the data-preserving remedy: map the column to `bytea` with `--type-override <col>=bytea` (bytea holds arbitrary bytes, NUL included), or clean the source data. No value is silently altered — stripping the NUL would itself be silent corruption, so sluice refuses rather than guesses. Pinned by a unit test covering `text`/`varchar`/`char` and a DOMAIN-over-text column, with `bytea` and NUL-free text confirmed unaffected.

## Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.24. The only behavior change is that a previously-cryptic mid-stream failure on NUL-bearing text is now a clear, column-named refusal up front; data that migrated fine before is unaffected (PG could never store a NUL in text, so no previously-working value is newly refused).

## Who needs this

- **Anyone migrating MySQL `CHAR`/`VARCHAR`/`TEXT` columns that may contain embedded NUL bytes to a Postgres target.** You now get a clear refusal naming the column and the `bytea` override, instead of a generic COPY-stream error. Use `--type-override <col>=bytea` to carry the bytes faithfully, or clean the source.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.25`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.25`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
