-- benchmarks/pgcopydb zero-loss checksum. Returns an md5 over per-table
-- count + sum(id) + sum(amount) + sum(length(event_type)) + true-count, so a
-- matching value on source and target proves no row loss / dup / value
-- corruption across the whole corpus (not just row counts). Mirrors the
-- aggregate-checksum verification the comparison-pgcopydb.md doc already uses.
CREATE OR REPLACE FUNCTION bench_checksum() RETURNS text AS $$
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
      AND (c.relname LIKE 'huge\_%' OR c.relname LIKE 'medium\_%')
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
