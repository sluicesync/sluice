-- gen_table(name, rows): one bench table with the standard mixed shape +
-- secondary indexes, populated server-side from generate_series. Factored out
-- of seed.sql so the harness (bench.sh) can call it per-table across several
-- parallel psql connections — the serial DO-loop in seed.sql is fine for the
-- small calibration pass but too slow for the ~110 GB full corpus.
--
-- Tables are LOGGED on purpose: both copy tools can carry an UNLOGGED source
-- attribute to the target, and an unlogged target writes without WAL, which
-- would unfairly skew the throughput comparison. Realistic WAL on both sides.
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
  EXECUTE format('CREATE INDEX ON %I (user_id)', tname);
  EXECUTE format('CREATE INDEX ON %I (created_at)', tname);
  EXECUTE format('CREATE INDEX ON %I (event_type)', tname);
END;
$$ LANGUAGE plpgsql;
