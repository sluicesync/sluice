# Case study — migrating a GitLab-shape Postgres schema

This case study walks through what sluice does when pointed at a
real-world Postgres schema with the features that catch most CDC
tooling off-guard: DOMAIN-typed columns with CHECK constraints, range
types, EXCLUDE constraints, partial / functional / covering indexes,
and operator-specified opclasses.

The schema is shaped like GitLab's `db/structure.sql` (a popular,
publicly-available reference for "what does a complex production PG
schema look like"). The recipe is "load this on a PG source, point
sluice at it, observe what sluice does." The point is not to migrate
GitLab specifically — it's to ground-truth that sluice handles a
schema of *that complexity class*.

## Why this matters

Most CDC tooling falls down on real schemas for one of three reasons:

1. **Silent type loss** — DOMAINs, range types, custom collations,
   composite types arrive on the target as the underlying base type
   with no warning. The CHECK constraint enforcement disappears.
   This is a Bug 113-shaped silent-loss class.
2. **Silent constraint loss** — EXCLUDE constraints, partial-index
   `WHERE` clauses, covering-index `INCLUDE` columns, NULLS
   FIRST/LAST ordering get dropped silently. The target accepts data
   the source would reject.
3. **Loud failure at apply time** — the tool didn't refuse upfront
   on a feature it can't carry, so the operator only finds out after
   the bulk-copy has partially run. Recovery requires manual cleanup.

sluice's design tenet is **loud failure**: if a feature can't be
carried, refuse before any data moves. The features below are the
specific ones sluice has work to demonstrate on — each one was a
bug we caught during validation cycles, each one has a closed
BUG-CATALOG entry, and each one has an integration test pinning the
fix.

## What this case study shows

| Schema feature | Pre-sluice silent class | Current behavior |
|---|---|---|
| DOMAIN-typed columns with CHECK (e.g. `email_address`) | Silent unwrap to base type; CHECK lost (Bug 113) | PG → PG round-trips DOMAIN + CHECK exactly. PG → MySQL emits a structured WARN naming the column AND emits a MySQL CHECK on 8.0.16+ when the CHECK shape is translatable (regex / range). |
| Range types (`int4range`, `tstzrange`, `daterange`) | Loud refusal on PG → PG (no IR shape) | PG → PG carries verbatim via the ADR-0051 allowlist; PG → MySQL loud-refuses cleanly (no MySQL equivalent). |
| EXCLUDE constraints (`EXCLUDE USING gist (... WITH &&)`) | Silently dropped from the IR (Bug 53-shape silent loss) | PG → PG round-trips via ADR-0053 verbatim text; cross-engine refuses loudly at preflight. |
| Partial indexes (`WHERE (status = 'published')`) | Silently dropped | Preserved end-to-end; integration test covers the assertion. |
| Covering indexes (`INCLUDE (a, b, c)`) | Silently dropped or order-mangled | Preserved end-to-end. |
| NULLS FIRST / LAST ordering | Silently dropped (PG cosmetic normalization confused the comparison) | Preserved end-to-end; sluice's emitter only emits the clause when it's operator-significant (non-default). |
| Functional indexes (`btree (LOWER(email))`) | Silent expression-text divergence | Verbatim through with same-engine same-dialect requoting; cross-engine emits a clear refusal when the target's expression grammar diverges. |
| Operator-class indexes (`btree (col text_pattern_ops)`) | Silently dropped, falling back to default opclass (Bug 115) | Preserved; the non-default core opclasses are explicitly carried. |
| `bigserial` PK + `setval` priming | Silent PK collisions post-cutover if sequence not primed | `sluice cutover` primes the target's sequences past the source's `MAX(id)` + margin. |

The rest of this doc walks through actually pointing sluice at a
GitLab-shape schema and showing what each phase does.

## Setup

Spin up a PG-16 source and an empty PG-16 target on the same host.
Stock `postgres:16` containers are sufficient; no extensions needed
for the core shape (PostGIS, pg_trgm, etc. are demonstrated in their
own recipes).

```sh
docker run -d --rm --name pg-src -p 5442:5432 \
    -e POSTGRES_PASSWORD=pgpw postgres:16

docker run -d --rm --name pg-dst -p 5443:5432 \
    -e POSTGRES_PASSWORD=pgpw postgres:16
```

Load a representative schema chunk on the source. Here's a compact
fixture that exercises every feature in the table above:

```sql
-- Connect as: psql 'postgres://postgres:pgpw@localhost:5442/postgres'

CREATE DATABASE gitlab_shape;
\c gitlab_shape

-- DOMAIN with regex CHECK (Bug 113 representative)
CREATE DOMAIN email_address AS text
    CHECK (VALUE ~ '^[^@]+@[^@]+\.[^@]+$');

-- DOMAIN with range CHECK
CREATE DOMAIN percentage AS numeric
    CHECK (VALUE >= 0 AND VALUE <= 100);

-- Range column + EXCLUDE constraint (ADR-0053 representative)
CREATE TABLE reservations (
    id bigserial PRIMARY KEY,
    room_id bigint NOT NULL,
    period tstzrange NOT NULL,
    notes text,
    EXCLUDE USING gist (room_id WITH =, period WITH &&)
);

-- DOMAIN-typed columns + covering / partial / functional indexes
CREATE TABLE users (
    id bigserial PRIMARY KEY,
    email email_address NOT NULL,
    username varchar(255) NOT NULL,
    completion_pct percentage,
    state text DEFAULT 'active',
    created_at timestamptz DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX users_email_idx ON users (email);

-- Covering index
CREATE INDEX users_state_covering ON users (state) INCLUDE (created_at);

-- Partial index
CREATE INDEX users_active_idx ON users (id)
    WHERE (state = 'active');

-- Functional index
CREATE INDEX users_email_lower ON users (LOWER(email));

-- Operator-class index (Bug 115 representative)
CREATE INDEX users_username_prefix ON users
    USING btree (username text_pattern_ops);

-- Seed some data
INSERT INTO users (email, username, completion_pct)
VALUES ('alice@example.com', 'alice', 75.5),
       ('bob@example.com',   'bob',   42.0);

INSERT INTO reservations (room_id, period, notes)
VALUES (1, tstzrange('2026-06-01 09:00:00+00', '2026-06-01 10:00:00+00'), 'team standup'),
       (2, tstzrange('2026-06-01 09:30:00+00', '2026-06-01 11:00:00+00'), 'planning');
```

## Step 1: preview before migrating

```sh
sluice schema preview \
    --source-driver postgres \
    --source 'postgres://postgres:pgpw@localhost:5442/gitlab_shape?sslmode=disable' \
    --target-driver postgres \
    --target 'postgres://postgres:pgpw@localhost:5443/gitlab_shape?sslmode=disable'
```

Expected output (truncated to the interesting bits):

```sql
-- Phase 1a: enum types
-- (none in this fixture)

-- Phase 1a': domains (v0.95.2+ Bug 113 round-trip carry)
CREATE DOMAIN email_address AS text
    CHECK ((VALUE ~ '^[^@]+@[^@]+\.[^@]+$'::text));
CREATE DOMAIN percentage AS numeric
    CHECK (((VALUE >= 0::numeric) AND (VALUE <= 100::numeric)));

-- Phase 1b: tables
CREATE TABLE reservations (
    id bigint NOT NULL,
    room_id bigint NOT NULL,
    period tstzrange NOT NULL,                  -- ADR-0051 verbatim
    notes text
);

CREATE TABLE users (
    id bigint NOT NULL,
    email email_address NOT NULL,               -- DOMAIN preserved
    username varchar(255) NOT NULL,
    completion_pct percentage,
    state text DEFAULT 'active',
    created_at timestamptz DEFAULT CURRENT_TIMESTAMP
);

-- Phase 2: indexes
CREATE UNIQUE INDEX users_email_idx ON users (email);
CREATE INDEX users_state_covering ON users (state) INCLUDE (created_at);
CREATE INDEX users_active_idx ON users (id)
    WHERE (state = 'active');
CREATE INDEX users_email_lower ON users (lower(email));
CREATE INDEX users_username_prefix ON users
    USING btree (username text_pattern_ops);   -- Bug 115 carry

-- Phase 3: constraints
ALTER TABLE reservations
    ADD CONSTRAINT reservations_room_id_period_excl
        EXCLUDE USING gist (room_id WITH =, period WITH &&);  -- ADR-0053
```

Every feature from the table at the top of this doc appears in the
preview. Nothing is silently dropped.

## Step 2: run the migrate

```sh
sluice migrate \
    --source-driver postgres \
    --source 'postgres://postgres:pgpw@localhost:5442/gitlab_shape?sslmode=disable' \
    --target-driver postgres \
    --target 'postgres://postgres:pgpw@localhost:5443/gitlab_shape?sslmode=disable'
```

Expected: exit 0. Every table populated.

## Step 3: verify the constraints actually enforce

This is the load-bearing assertion. The target should reject the
same inputs the source rejects:

```sql
-- Connect: psql 'postgres://postgres:pgpw@localhost:5443/gitlab_shape'

-- DOMAIN CHECK enforcement
INSERT INTO users (email, username) VALUES ('not-an-email', 'mallory');
-- Expected: ERROR: value for domain email_address violates check constraint "email_address_check"

-- Range DOMAIN enforcement
INSERT INTO users (email, username, completion_pct)
VALUES ('eve@example.com', 'eve', 150);
-- Expected: ERROR: value for domain percentage violates check constraint "percentage_check"

-- EXCLUDE constraint enforcement
INSERT INTO reservations (room_id, period, notes)
VALUES (1, tstzrange('2026-06-01 09:15:00+00', '2026-06-01 09:45:00+00'), 'collision');
-- Expected: ERROR: conflicting key value violates exclusion constraint "reservations_room_id_period_excl"
```

All three rejections should fire. The source's invariants are now
the target's invariants — that's the point.

### A v0.97.0 → v0.97.1 fidelity follow-up — strict regex-dot fidelity

Initial v0.97.0 release emitted the regex pattern into MySQL's SQL
string literal without escaping the source backslashes. MySQL's
string-literal parser treats `\` as an escape character by default,
so the literal `'\.'` arrived at the regex engine as `.` (any
character) rather than `\.` (literal dot). The constraint stayed
functionally correct for the email case — the `@` and negated
character classes carried the rejection — but the stored expression
diverged from the source semantics.

v0.97.1 closes the gap: backslashes in the source pattern are doubled
when emitting the MySQL SQL literal, so `'\.'` arrives as `\.` at the
regex engine regardless of the operator's `SQL_MODE` setting. The
email regex now stores byte-faithfully on the MySQL target, and shared
PG regex shorthands (`\d`, `\s`, `\w`, `\b`) translate the same way.

This is the kind of carefully-bounded translation gap sluice's
design tenet expects — observed during the v0.97.0 validation cycle,
filed honestly, closed in the next patch. Operators who hit a more
exotic PG-vs-MySQL regex divergence (Unicode property escapes,
lookbehind, possessive quantifiers) still need to hand-translate the
CHECK against MySQL's ICU syntax; the loud-failure path covers those
shapes by leaving the v0.96.2 WARN to fire.

## What happens cross-engine (PG → MySQL)?

Repoint the target at a MySQL 8.0.16+ container:

```sh
sluice migrate \
    --source-driver postgres --source ... \
    --target-driver mysql    --target 'root:rootpw@tcp(localhost:3316)/gitlab_shape'
```

Expected behavior:

- **DOMAIN columns** → downgraded to the base type (MySQL has no
  DOMAIN). The MySQL writer emits a `slog.WARN` naming every affected
  column. On 8.0.16+, the regex / range CHECKs are translated into
  MySQL table-level CHECK constraints (`REGEXP_LIKE` for the email
  regex; verbatim `pct >= 0 AND pct <= 100` for the range). The WARN
  is suppressed for columns whose CHECKs were translated.
- **Range columns** → loud refusal at the read-schema phase. MySQL
  has no range type and the silent-loss class would be CRITICAL.
  Operator-actionable error.
- **EXCLUDE constraint** → loud refusal at preflight (ADR-0053).
- **Partial / covering / functional / opclass indexes** → loud refusal
  at preflight when the syntax doesn't translate; verbatim carry for
  the shapes that do.

The cross-engine path is **intentionally restrictive** — sluice's
position is that a CHECK present on the target with different
enforcement semantics than the source is *more* dangerous than no
CHECK (the operator sees it in `SHOW CREATE TABLE` and assumes
parity). Loud refusal sends the operator to add the carry manually
or to choose a different target.

## What this validates

The features in this case study aren't aspirational. Each one has:

1. A bug-catalog entry documenting the original silent-loss class.
2. A closed BUG-CATALOG status with the fix release tagged.
3. An integration test in the sluice repo pinning the round-trip.

The case study is reproducible against the v0.97.0 binary published
at <https://github.com/orware/sluice/releases/latest>. Every command
above was run during validation; the expected outputs are the
post-fix observed behavior.

## See also

- [`docs/comparison.md`](../comparison.md) — broader comparison of
  sluice against managed / FOSS / enterprise CDC tools.
- [`docs/type-mapping.md`](../type-mapping.md) — the per-type
  translation policy.
- [`docs/dev/adr/`](../dev/adr/) — the ADRs covering DOMAIN
  round-trip (ADR-0051 family), EXCLUDE carry (ADR-0053), and the
  verbatim-extension passthrough tier (ADR-0047 / ADR-0070).
