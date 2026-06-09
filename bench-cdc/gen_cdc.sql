-- gen_cdc_table(name, rows): one CDC-bench table, server-side populated.
--
-- Shape mirrors bench-pgcopydb/gen_fn.sql (mixed columns + 3 secondary
-- indexes) so the two harnesses are comparable, with two CDC-specific
-- requirements:
--   * a real PRIMARY KEY (id) — the CDC applier needs it to route
--     UPDATE/DELETE events; a no-PK table can't be continuously synced.
--   * REPLICA IDENTITY FULL — so UPDATE/DELETE WAL carries the full OLD
--     row, which the cross-checksum verification relies on (and which the
--     writer's UPDATEs/DELETEs exercise end-to-end).
--
-- This is the LOWER-SCALE sibling of the 110 GB migrate corpus: good-sized
-- but copies in a few minutes, so the harness can spend its time proving the
-- continuous-sync path (writes during cold-copy + steady-state CDC) is
-- zero-loss, not just measuring throughput.
CREATE OR REPLACE FUNCTION gen_cdc_table(tname text, nrows bigint) RETURNS void AS $$
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
  -- REPLICA IDENTITY FULL: UPDATE/DELETE WAL carries the full OLD row so the
  -- continuous-sync applier (and any downstream verification) sees complete
  -- before-images, matching how the test suites configure CDC source tables.
  EXECUTE format('ALTER TABLE %I REPLICA IDENTITY FULL', tname);
END;
$$ LANGUAGE plpgsql;
