-- gen_mysql.sql — bench-mysql corpus generator (MySQL→MySQL index-overlap bench).
--
-- Produces a realistic many-indexed MySQL corpus to measure ADR-0080's
-- index-build overlap (3c, v0.99.30) vs the pre-3c serial post-copy index
-- phase. Each table:
--   * bigint PRIMARY KEY (so sluice can chunk + keyset-paginate the read)
--   * a mix of column types + a fat filler so rows are a realistic width
--   * 4 SECONDARY indexes — the whole point: per-table index-build work must be
--     a meaningful fraction of the copy, or the overlap signal is unmeasurable.
--
-- MySQL has no generate_series, so rows come from a TALLY (numbers) table built
-- by cross-joining a 0–9 digit table seven times (10^7 = 10M candidate rows),
-- LIMIT nrows. The tally is built once; gen_table() is then a fast
-- INSERT … SELECT that streams server-side (no client round-trips).
--
-- Tables are normal InnoDB (WAL/redo on) so the index-build timing is realistic.
--
-- Usage (driven by bench-mysql-up.sh; see there for the parallel seed loop):
--   SET @ntables = 30; SET @nrows = 1500000;  -- then CALL gen_table per table
-- This file defines the tally + the gen_table procedure; the harness loops it.

-- ---------------------------------------------------------------------------
-- Tally / numbers table: 10M rows of g = 1..10000000, built from a digits CTE
-- cross-join. ~80 MB; built once, reused by every gen_table() call. Kept as a
-- persistent table (not a temp) so the parallel per-table seed connections in
-- bench-mysql-up.sh all share it.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS _tally;
CREATE TABLE _tally (g BIGINT PRIMARY KEY);

-- 10 single digits, cross-joined 7 times -> 10,000,000 rows. Insert in one shot;
-- InnoDB streams it server-side.
INSERT INTO _tally (g)
SELECT (d0.d
      + d1.d*10
      + d2.d*100
      + d3.d*1000
      + d4.d*10000
      + d5.d*100000
      + d6.d*1000000) + 1 AS g
FROM   (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d0
CROSS JOIN (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d1
CROSS JOIN (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d2
CROSS JOIN (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d3
CROSS JOIN (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d4
CROSS JOIN (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d5
CROSS JOIN (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d6;

-- ---------------------------------------------------------------------------
-- gen_table(tname, nrows): create one bench table with the standard mixed shape
-- + 4 secondary indexes, populated from the tally. The CREATE carries the
-- secondary indexes inline so the source heap is built WITH them (sluice's
-- reader then sees them and recreates them on the target — exercising the
-- index-build axis we're measuring).
-- ---------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS gen_table;
DELIMITER //
CREATE PROCEDURE gen_table(IN tname VARCHAR(64), IN nrows BIGINT)
BEGIN
  SET @drop := CONCAT('DROP TABLE IF EXISTS `', tname, '`');
  PREPARE s FROM @drop; EXECUTE s; DEALLOCATE PREPARE s;

  SET @ddl := CONCAT(
    'CREATE TABLE `', tname, '` (',
    '  id          BIGINT        NOT NULL,',
    '  user_id     BIGINT        NOT NULL,',
    '  amount      DECIMAL(12,2) NOT NULL,',
    '  event_type  VARCHAR(32)   NOT NULL,',
    '  payload     JSON          NOT NULL,',
    '  created_at  DATETIME      NOT NULL,',
    '  is_active   TINYINT(1)    NOT NULL,',
    '  filler      TEXT          NOT NULL,',
    '  PRIMARY KEY (id),',
    '  KEY idx_user_id    (user_id),',
    '  KEY idx_created_at (created_at),',
    '  KEY idx_event_type (event_type),',
    '  KEY idx_active_amt (is_active, amount)',
    ') ENGINE=InnoDB');
  PREPARE s FROM @ddl; EXECUTE s; DEALLOCATE PREPARE s;

  -- INSERT … SELECT from the tally. Values are deterministic in g so re-seeding
  -- is reproducible. The JSON payload + 80-char filler give a realistic width.
  SET @ins := CONCAT(
    'INSERT INTO `', tname, '` ',
    '(id, user_id, amount, event_type, payload, created_at, is_active, filler) ',
    'SELECT g, ',
    '       (g % 5000000) + 1, ',
    '       ROUND(RAND(g) * 100000, 2), ',
    '       ELT(1 + (g % 8), ''click'',''view'',''purchase'',''signup'',''logout'',''error'',''refund'',''search''), ',
    '       JSON_OBJECT(''k'', MD5(g), ''n'', g % 1000, ''tags'', JSON_ARRAY(g % 7, g % 13)), ',
    '       TIMESTAMPADD(SECOND, g % 126230400, TIMESTAMP''2020-01-01 00:00:00''), ',
    '       (g % 3) = 0, ',
    '       REPEAT(''x'', 80) ',
    'FROM _tally WHERE g <= ', nrows);
  PREPARE s FROM @ins; EXECUTE s; DEALLOCATE PREPARE s;
END //
DELIMITER ;
