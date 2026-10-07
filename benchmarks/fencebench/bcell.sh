#!/usr/bin/env bash
# bcell.sh PAIR WORKLOAD ARM N REPS — the ADR-0191 brokerbench arm: the cost
# of `sync from-backup` replay with change identities, frontier tokens and
# apply marks, against the binary before them.
#
#   PAIR      my2pg | pg2pg | my2my
#   WORKLOAD  A | B | D | K   (README.md; K = keyless, 90% inserts, 10% deletes)
#   ARM       the sluice binary under test is FB_BIN_<ARM> (as in cell.sh)
#   N         source transactions captured in the incremental
#   REPS      replays per apply mode (serial, lanes) of the one captured chain
#
# One chain per (pair, workload, arm): reset the source, seed it, take a full
# with `backup full --chain-slot`, generate N transactions, capture them with
# `backup incremental` (a fixed window, long enough for the backlog). The
# chain's change-chunk bytes (compressed, as stored) are recorded. Then each
# replay resets the target, restores the full (the only link at that point
# would be the full, so the restore is done BEFORE the incremental is
# captured and copied per replay as a database template), and times
# `sync from-backup run --at-chain-id <full>` until the target's per-table
# checksum equals the source's (`fb wait`), then `fb verify` (a full-row
# SHA-256 of both sides, read by the harness, independent of sluice). A
# binary that refuses the chain (the base binary refuses a keyless table)
# records REFUSED with its error code.
#
# Result lines are appended to $FB_OUT/broker-results.txt:
#   <pair>_<W>_<arm>_<cfg>_r<rep> N=<n> chunks=<bytes> | <wait RESULT> | txps=<n/s> | <verify>
set -uo pipefail
export MSYS_NO_PATHCONV=1
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PAIR=$1 W=$2 ARM=$3 N=$4 REPS=$5
FB="${FB:-$HERE/fb/fb}"
[ -x "$FB" ] || [ -x "$FB.exe" ] || { echo "build the helper first: (cd $HERE/fb && go build -o fb .)"; exit 1; }
FB_OUT="${FB_OUT:-$HERE/out}"
binvar="FB_BIN_$ARM"
BIN="${!binvar:-}"
[ -n "$BIN" ] || { echo "set $binvar to the sluice binary for arm '$ARM'"; exit 1; }
CAPWIN="${CAPWIN:-25s}"

MYSRC="root:pw@tcp(127.0.0.1:13306)/bench"
MYDST="root:pw@tcp(127.0.0.1:13307)/bench"
PGSRC="postgres://postgres:pw@127.0.0.1:15432/bench?sslmode=disable"
PGDST="postgres://postgres:pw@127.0.0.1:15433/bench?sslmode=disable"
case $PAIR in
  my2pg) SK=mysql SD=$MYSRC TK=postgres TD=$PGDST ;;
  pg2pg) SK=postgres SD=$PGSRC TK=postgres TD=$PGDST ;;
  my2my) SK=mysql SD=$MYSRC TK=mysql TD=$MYDST ;;
  *) echo "unknown pair $PAIR"; exit 1 ;;
esac
LOG="$FB_OUT/logs"
CHAIN="$FB_OUT/chains/${PAIR}_${W}_${ARM}"
mkdir -p "$LOG"
rm -rf "$CHAIN"
psq() { docker exec "$1" psql -U postgres -d "$2" -qtAc "$3" >/dev/null 2>&1; }
myq() { docker exec "$1" mysql -uroot -ppw -e "$2" >/dev/null 2>&1; }

reset_target() {
  if [ $TK = mysql ]; then
    myq fb-mydst "DROP DATABASE IF EXISTS bench; CREATE DATABASE bench;"
  else
    psq fb-pgdst postgres "DROP DATABASE IF EXISTS bench WITH (FORCE)"
    psq fb-pgdst postgres "CREATE DATABASE bench"
  fi
}

# The source and the chain.
if [ $SK = mysql ]; then
  myq fb-mysrc "DROP DATABASE IF EXISTS bench; CREATE DATABASE bench;"
else
  psq fb-pgsrc postgres "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots"
  psq fb-pgsrc postgres "DROP DATABASE IF EXISTS bench WITH (FORCE)"
  psq fb-pgsrc postgres "CREATE DATABASE bench"
  psq fb-pgsrc bench "CREATE PUBLICATION sluice_pub FOR ALL TABLES"
fi
"$FB" setup -sk $SK -sd "$SD" -w $W || { echo "${PAIR}_${W}_${ARM} SETUP-FAIL"; exit 1; }
"$BIN" backup full --source-driver=$SK --source="$SD" --output-dir="$CHAIN" --chain-slot --no-progress \
  >"$LOG/${PAIR}_${W}_${ARM}.full.log" 2>&1 || { echo "${PAIR}_${W}_${ARM} FULL-FAIL"; tail -3 "$LOG/${PAIR}_${W}_${ARM}.full.log"; exit 1; }
FULLID=$(sed -n 's/.*"backup_id": *"\([0-9a-f]*\)".*/\1/p' "$CHAIN/manifest.json" | head -1)
# The full, restored once into a template the replays copy (Postgres), or
# re-restored per replay (MySQL) — before the incremental exists, so the
# restore is the full alone.
reset_target
"$BIN" restore --from-dir="$CHAIN" --target-driver=$TK --target="$TD" --no-progress \
  >"$LOG/${PAIR}_${W}_${ARM}.restore.log" 2>&1 || { echo "${PAIR}_${W}_${ARM} RESTORE-FAIL"; exit 1; }
if [ $TK = postgres ]; then
  psq fb-pgdst postgres "DROP DATABASE IF EXISTS bench_full WITH (FORCE)"
  psq fb-pgdst postgres "CREATE DATABASE bench_full TEMPLATE bench"
else
  docker exec fb-mydst sh -c 'mysqldump -uroot -ppw --databases bench > /tmp/bench_full.sql' >/dev/null 2>&1
fi
"$FB" gen -sk $SK -sd "$SD" -w $W -n "$N" -workers 8 >"$LOG/${PAIR}_${W}_${ARM}.gen.log" 2>&1 || { echo "${PAIR}_${W}_${ARM} GEN-FAIL"; exit 1; }
"$BIN" backup incremental --source-driver=$SK --source="$SD" --output-dir="$CHAIN" --window="$CAPWIN" --no-progress \
  >"$LOG/${PAIR}_${W}_${ARM}.incr.log" 2>&1 || { echo "${PAIR}_${W}_${ARM} INCR-FAIL"; tail -3 "$LOG/${PAIR}_${W}_${ARM}.incr.log"; exit 1; }
CHUNKS=$(find "$CHAIN/chunks/_changes" -type f -exec cat {} + | wc -c | tr -d ' ')

for rep in $(seq 1 "$REPS"); do
  for cfg in serial lanes; do
    FLAGS=""
    [ $cfg = serial ] && FLAGS="--apply-concurrency=1"
    TAG="${PAIR}_${W}_${ARM}_${cfg}_r${rep}"
    reset_target
    if [ $TK = postgres ]; then
      psq fb-pgdst postgres "DROP DATABASE IF EXISTS bench WITH (FORCE)"
      psq fb-pgdst postgres "CREATE DATABASE bench TEMPLATE bench_full"
    else
      docker exec fb-mydst sh -c 'mysql -uroot -ppw < /tmp/bench_full.sql' >/dev/null 2>&1
    fi
    STREAM="bb$RANDOM"
    # shellcheck disable=SC2086 # FLAGS is a word list by design
    "$BIN" sync from-backup run --backup-dir="$CHAIN" --target-driver=$TK --target="$TD" --stream-id=$STREAM \
      --at-chain-id="$FULLID" --poll-interval=1s --no-progress $FLAGS >"$LOG/$TAG.broker.log" 2>&1 &
    PID=$!
    "$FB" wait -sk $SK -sd "$SD" -tk $TK -td "$TD" -w $W -timeout "${TIMEOUT:-600s}" >"$LOG/$TAG.wait.log" 2>&1 &
    WPID=$!
    refused=""
    while kill -0 "$WPID" 2>/dev/null; do
      if ! kill -0 "$PID" 2>/dev/null; then
        refused=1
        kill "$WPID" 2>/dev/null
        break
      fi
      sleep 0.2
    done
    wait "$WPID" 2>/dev/null
    if [ -n "$refused" ]; then
      code=$(grep -o 'SLUICE-E-[A-Z0-9-]*' "$LOG/$TAG.broker.log" | head -1)
      echo "$TAG N=$N chunks=$CHUNKS | REFUSED ${code:-exit} | txps=NA | VERIFY NA" | tee -a "$FB_OUT/broker-results.txt"
      continue
    fi
    RES=$(grep RESULT "$LOG/$TAG.wait.log")
    VER=$("$FB" verify -sk $SK -sd "$SD" -tk $TK -td "$TD" -w $W 2>&1 | head -1)
    kill "$PID"
    wait "$PID" 2>/dev/null
    first=$(echo "$RES" | sed -n 's/.*first=\([0-9.-]*\).*/\1/p')
    done_=$(echo "$RES" | sed -n 's/.*done=\([0-9.]*\).*/\1/p')
    rate=$(awk -v n="$N" -v f="$first" -v d="$done_" 'BEGIN{ if (d=="" || d-f<=0) {print "NA"} else printf "%.1f", n/(d-f) }')
    echo "$TAG N=$N chunks=$CHUNKS | $RES | txps=$rate | $VER" | tee -a "$FB_OUT/broker-results.txt"
  done
done
