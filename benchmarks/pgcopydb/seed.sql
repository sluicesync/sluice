-- benchmarks/pgcopydb seed generator (Tier A: local sluice-vs-pgcopydb throughput).
--
-- Produces a FAIR mixed PG corpus that exercises BOTH pgcopydb strengths the
-- comparison must cover:
--   * a few HUGE tables  -> within-table PK-range COPY splitting
--                           (sluice --bulk-parallelism / pgcopydb --split-tables-larger-than)
--   * many MEDIUM tables -> cross-table concurrency
--                           (sluice --table-parallelism / pgcopydb --table-jobs, item 3 / ADR-0076)
-- plus realistic column types + secondary indexes (parallel index builds:
-- sluice index-build-parallelism / pgcopydb --index-jobs).
--
-- Parameterized via psql -v so the SAME script drives the small CALIBRATION
-- pass (measure local MB/s) and the sized full pass:
--   psql -v huge_tables=3  -v huge_rows=2000000  \
--        -v medium_tables=40 -v medium_rows=120000 -f seed.sql
--
-- Measure the realized size with the final SELECT and scale the full pass from
-- that (don't trust a bytes/row estimate).
--
-- psql NOTE: :var interpolation does NOT happen inside a DO $$...$$ dollar-quoted
-- block, so the params are pushed into session GUCs at the psql level (where
-- :var works) and read back via current_setting() inside the block.

\set ON_ERROR_STOP on

\if :{?huge_tables}
\else
  \set huge_tables 3
\endif
\if :{?huge_rows}
\else
  \set huge_rows 2000000
\endif
\if :{?medium_tables}
\else
  \set medium_tables 40
\endif
\if :{?medium_rows}
\else
  \set medium_rows 120000
\endif

-- Push the psql -v params into session GUCs (placeholder custom GUCs). This is
-- the psql lexer level, where :var substitution works; the DO block below reads
-- them via current_setting() because :var is NOT substituted inside $$...$$.
SELECT set_config('bench.huge_tables',   :'huge_tables',   false),
       set_config('bench.huge_rows',     :'huge_rows',     false),
       set_config('bench.medium_tables', :'medium_tables', false),
       set_config('bench.medium_rows',   :'medium_rows',   false);

-- gen_table(name, rows): one table with the standard mixed shape + indexes,
-- populated server-side from generate_series (streams at disk-write speed; no
-- client round-trips). Values are pseudo-random but deterministic in g so
-- re-seeding is reproducible. The jsonb payload + filler give a realistic row
-- width rather than a trivial int-only table.
CREATE OR REPLACE FUNCTION gen_table(tname text, nrows bigint) RETURNS void AS $$
BEGIN
  EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', tname);
  EXECUTE format($f$
    CREATE TABLE %I (
      id          bigint        PRIMARY KEY,
      user_id     bigint        NOT NULL,
      amount      numeric(12,2) NOT NULL,
      event_type  varchar(32)   NOT NULL,
      payload     jsonb         NOT NULL,
      created_at  timestamptz   NOT NULL,
      is_active   boolean       NOT NULL,
      filler      text          NOT NULL
    )$f$, tname);
  EXECUTE format($f$
    INSERT INTO %I (id, user_id, amount, event_type, payload, created_at, is_active, filler)
    SELECT g,
           (g %% 5000000) + 1,
           round((random() * 100000)::numeric, 2),
           (ARRAY['click','view','purchase','signup','logout','error','refund','search'])[1 + (g %% 8)],
           jsonb_build_object('k', md5(g::text), 'n', g %% 1000, 'tags', to_jsonb(ARRAY[g %% 7, g %% 13])),
           timestamptz '2020-01-01' + ((g %% 126230400) || ' seconds')::interval,
           (g %% 3) = 0,
           repeat('x', 80)
    FROM generate_series(1, %s) g
  $f$, tname, nrows);
  -- Secondary indexes on the source so the copy tools must recreate them on the
  -- target -> exercises the parallel index-build axis.
  EXECUTE format('CREATE INDEX ON %I (user_id)', tname);
  EXECUTE format('CREATE INDEX ON %I (created_at)', tname);
  EXECUTE format('CREATE INDEX ON %I (event_type)', tname);
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
  ht int    := current_setting('bench.huge_tables')::int;
  hr bigint := current_setting('bench.huge_rows')::bigint;
  mt int    := current_setting('bench.medium_tables')::int;
  mr bigint := current_setting('bench.medium_rows')::bigint;
  i  int;
BEGIN
  FOR i IN 1..ht LOOP
    PERFORM gen_table(format('huge_%s', i), hr);
    RAISE NOTICE 'seeded huge_% (% rows)', i, hr;
  END LOOP;
  FOR i IN 1..mt LOOP
    PERFORM gen_table(format('medium_%s', i), mr);
  END LOOP;
  RAISE NOTICE 'seeded % medium tables (% rows each)', mt, mr;
END $$;

DROP FUNCTION gen_table(text, bigint);

-- Realized size + per-table breakdown (read this off the calibration pass to
-- size the full run).
SELECT pg_size_pretty(sum(pg_total_relation_size(c.oid))) AS total_with_indexes,
       pg_size_pretty(sum(pg_relation_size(c.oid)))       AS heap_only,
       sum(pg_relation_size(c.oid))                       AS heap_bytes,
       count(*)                                           AS tables
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r' AND n.nspname = 'public'
  AND (c.relname LIKE 'huge\_%' OR c.relname LIKE 'medium\_%');
