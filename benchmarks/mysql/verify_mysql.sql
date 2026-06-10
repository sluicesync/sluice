-- verify_mysql.sql — bench-mysql zero-loss checksum.
--
-- Defines bench_checksum(): a procedure that loops over every bench table
-- (bench_%) and accumulates, per table, count + sum(id) + sum(amount) +
-- sum(length(event_type)) + true-count, then emits an md5 over the whole
-- accumulator. A matching value on source and target proves no row loss / dup /
-- value corruption across the corpus — not just row counts. Mirrors the
-- bench-pgcopydb verify.sql contract.
--
-- Called transiently by bench-mysql.sh (CREATE -> CALL -> DROP) so it is never
-- a persistent object a full copy would carry to the target and collide on.

DROP PROCEDURE IF EXISTS bench_checksum;
DELIMITER //
CREATE PROCEDURE bench_checksum(OUT result TEXT)
BEGIN
  DECLARE done   INT DEFAULT 0;
  DECLARE tname  VARCHAR(64);
  DECLARE acc    TEXT DEFAULT '';
  DECLARE cur CURSOR FOR
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = DATABASE() AND table_name LIKE 'bench\_%'
    ORDER BY table_name;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  OPEN cur;
  read_loop: LOOP
    FETCH cur INTO tname;
    IF done THEN LEAVE read_loop; END IF;
    SET @q := CONCAT(
      'SELECT CONCAT_WS('':'', ''', tname, ''', ',
      ' COUNT(*), COALESCE(SUM(id),0), COALESCE(SUM(amount),0),',
      ' COALESCE(SUM(LENGTH(event_type)),0),',
      ' SUM(is_active = 1)) INTO @row FROM `', tname, '`');
    PREPARE s FROM @q; EXECUTE s; DEALLOCATE PREPARE s;
    SET acc := CONCAT(acc, @row, '|');
  END LOOP;
  CLOSE cur;

  SET result := CONCAT(MD5(acc), ' (', LENGTH(acc), 'b over tables)');
END //
DELIMITER ;
