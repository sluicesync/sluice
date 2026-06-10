#!/usr/bin/env bash
# writer.sh — drive concurrent INSERT/UPDATE/DELETE load against the CDC-bench
# SOURCE while `sluice sync start` cold-copies and then follows. The point is
# to exercise the snapshot/CDC boundary (writes during the cold-copy must
# arrive exactly once via CDC, never lost or duplicated) AND steady-state CDC.
#
# Correctness model: the workload is NOT deterministic and does not need to be.
# cdc-bench.sh stops this writer, lets CDC drain, then compares cdc_checksum()
# on source vs target — they must MATCH whatever this writer did. So every
# mutation just has to be a legitimate, replicable change.
#
# Usage: writer.sh <src-container> <n_tables> <seed_rows> <duration_secs>
#   INSERTs use ids strictly ABOVE seed_rows (monotonic, no PK collision).
#   UPDATEs/DELETEs target ids within the live range.
set -uo pipefail
export MSYS_NO_PATHCONV=1

C="${1:?src container}"; NT="${2:?n_tables}"; SEED="${3:?seed_rows}"; DUR="${4:?duration_secs}"
PSQL=(docker exec -i "$C" psql -U postgres -d benchdb -qtA)

# Per-table next-insert id, starting just past the seed high-water mark.
declare -a NEXT
for ((t=0; t<NT; t++)); do NEXT[$t]=$((SEED + 1)); done

start=$(date +%s)
iters=0
ins=0; upd=0; del=0
while [ $(( $(date +%s) - start )) -lt "$DUR" ]; do
  t=$(( iters % NT ))
  tbl=$(printf 'cdc_%02d' "$t")
  lo=${NEXT[$t]}
  hi=$((lo + 199))                 # INSERT 200 fresh rows per iteration
  NEXT[$t]=$((hi + 1))
  # Pick an existing live id window for UPDATE/DELETE (within the seed range,
  # rotating so we don't keep hitting the same rows).
  uwin_lo=$(( (iters * 137) % (SEED>500 ? SEED-500 : 1) + 1 ))
  uwin_hi=$(( uwin_lo + 99 ))      # UPDATE ~100 rows
  dwin_lo=$(( (iters * 911) % (SEED>50 ? SEED-50 : 1) + 1 ))
  dwin_hi=$(( dwin_lo + 9 ))       # DELETE ~10 rows

  "${PSQL[@]}" >/dev/null 2>&1 <<SQL
INSERT INTO ${tbl} (id, user_id, amount, event_type, payload, created_at, is_active, filler)
SELECT g, (g % 5000000)+1, round((random()*100000)::numeric,2),
       (ARRAY['click','view','purchase','signup','logout','error','refund','search'])[1+(g%8)],
       jsonb_build_object('k', md5(g::text), 'n', g%1000),
       timestamptz '2020-01-01' + ((g%126230400)||' seconds')::interval,
       (g%3)=0, repeat('x',80)
FROM generate_series(${lo}, ${hi}) g
ON CONFLICT (id) DO NOTHING;
UPDATE ${tbl} SET amount = amount + 1.00, is_active = NOT is_active
 WHERE id BETWEEN ${uwin_lo} AND ${uwin_hi};
DELETE FROM ${tbl} WHERE id BETWEEN ${dwin_lo} AND ${dwin_hi};
SQL
  rc=$?
  if [ $rc -eq 0 ]; then
    ins=$((ins+1)); upd=$((upd+1)); del=$((del+1))
  fi
  iters=$((iters+1))
done
echo "writer done: ${iters} iterations over ${DUR}s (~${ins} insert / ${upd} update / ${del} delete batches)"
