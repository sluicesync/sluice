-- CDC-bench zero-loss checksum. Same aggregate shape as
-- benchmarks/pgcopydb/verify.sql, scoped to the cdc_% tables: md5 over per-table
-- count + sum(id) + sum(amount) + sum(length(event_type)) + true-count.
--
-- A matching value on source and target — taken AFTER the writer has stopped
-- and CDC has fully drained — proves the continuous-sync path delivered every
-- INSERT/UPDATE/DELETE exactly once (no loss, no dup, no value corruption):
--   * count        catches lost/duplicated INSERTs and lost/extra DELETEs
--   * sum(id)      catches wrong-row DELETEs/INSERTs
--   * sum(amount)  catches lost/misapplied UPDATEs (the writer mutates amount)
--   * true-count   catches lost/misapplied is_active UPDATEs
-- so the checksum is sensitive to all three mutation kinds the writer issues.
CREATE OR REPLACE FUNCTION cdc_checksum() RETURNS text AS $$
DECLARE
  r       record;
  acc     text := '';
  cnt     bigint;
  sid     numeric;
  samt    numeric;
  setlen  bigint;
  strue   bigint;
BEGIN
  FOR r IN
    SELECT c.relname
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r' AND n.nspname = 'public'
      AND c.relname LIKE 'cdc\_%'
    ORDER BY c.relname
  LOOP
    EXECUTE format(
      'SELECT count(*), coalesce(sum(id),0), coalesce(sum(amount),0),
              coalesce(sum(length(event_type)),0), count(*) FILTER (WHERE is_active)
       FROM %I', r.relname)
    INTO cnt, sid, samt, setlen, strue;
    acc := acc || format('%s:%s:%s:%s:%s:%s|', r.relname, cnt, sid, samt, setlen, strue);
  END LOOP;
  RETURN md5(acc) || ' (' || length(acc) || 'b over tables)';
END;
$$ LANGUAGE plpgsql;
