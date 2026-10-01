#!/usr/bin/env bash
# cell.sh PAIR WORKLOAD ARM CFG LAT_MS N REP — one fencebench cell.
#
#   PAIR      my2pg | pg2pg | my2my
#   WORKLOAD  A | B | C | D   (see README.md)
#   ARM       a label naming the sluice binary under test: the binary is the
#             value of the environment variable FB_BIN_<ARM>, e.g.
#             FB_BIN_head=/path/to/sluice.exe
#   CFG       lanes | serial | eol | lanes-b1 | serial-b1
#   LAT_MS    netem delay added on the target (0 = none)
#   N         source transactions in the backlog
#   REP       repetition label
#
# The cell resets the source and target `bench` databases, seeds the
# workload, cold-starts a sync and stops it once in CDC mode, generates an
# N-transaction backlog on the source, restarts the sync and times the
# catch-up (`fb wait`), then verifies the target against the source with a
# full-row hash (`fb verify`, independent of sluice). One result line is
# appended to $FB_OUT/results.txt.
set -uo pipefail
export MSYS_NO_PATHCONV=1
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PAIR=$1 W=$2 ARM=$3 CFG=$4 LAT=$5 N=$6 REP=$7
FB="${FB:-$HERE/fb/fb}"
[ -x "$FB" ] || [ -x "$FB.exe" ] || { echo "build the helper first: (cd $HERE/fb && go build -o fb .)"; exit 1; }
FB_OUT="${FB_OUT:-$HERE/out}"
binvar="FB_BIN_$ARM"
BIN="${!binvar:-}"
[ -n "$BIN" ] || { echo "set $binvar to the sluice binary for arm '$ARM'"; exit 1; }

MYSRC="root:pw@tcp(127.0.0.1:13306)/bench"
MYDST="root:pw@tcp(127.0.0.1:13307)/bench"
PGSRC="postgres://postgres:pw@127.0.0.1:15432/bench?sslmode=disable"
PGDST="postgres://postgres:pw@127.0.0.1:15433/bench?sslmode=disable"
case $PAIR in
  my2pg) SK=mysql SD=$MYSRC TK=postgres TD=$PGDST TC=fb-tc-pgdst ;;
  pg2pg) SK=postgres SD=$PGSRC TK=postgres TD=$PGDST TC=fb-tc-pgdst ;;
  my2my) SK=mysql SD=$MYSRC TK=mysql TD=$MYDST TC=fb-tc-mydst ;;
  *) echo "unknown pair $PAIR"; exit 1 ;;
esac
case $CFG in
  lanes) FLAGS="" ;;
  serial) FLAGS="--apply-concurrency=1" ;;
  eol) FLAGS="--exactly-once-lanes" ;;
  lanes-b1) FLAGS="--apply-batch-size=1" ;;
  serial-b1) FLAGS="--apply-concurrency=1 --apply-batch-size=1" ;;
  *) echo "unknown cfg $CFG"; exit 1 ;;
esac
TAG="${PAIR}_${W}_${ARM}_${CFG}_${LAT}ms_r${REP}"
LOG="$FB_OUT/logs"
mkdir -p "$LOG"
psq() { docker exec "$1" psql -U postgres -d "$2" -qtAc "$3" >/dev/null 2>&1; }
myq() { docker exec "$1" mysql -uroot -ppw -e "$2" >/dev/null 2>&1; }

# Reset latency, source and target.
docker exec fb-tc-pgdst tc qdisc del dev eth0 root >/dev/null 2>&1
docker exec fb-tc-mydst tc qdisc del dev eth0 root >/dev/null 2>&1
if [ $SK = mysql ]; then
  myq fb-mysrc "DROP DATABASE IF EXISTS bench; CREATE DATABASE bench;"
else
  psq fb-pgsrc postgres "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots"
  psq fb-pgsrc postgres "DROP DATABASE IF EXISTS bench WITH (FORCE)"
  psq fb-pgsrc postgres "CREATE DATABASE bench"
fi
if [ $TK = mysql ]; then
  myq fb-mydst "DROP DATABASE IF EXISTS bench; CREATE DATABASE bench;"
else
  psq fb-pgdst postgres "DROP DATABASE IF EXISTS bench WITH (FORCE)"
  psq fb-pgdst postgres "CREATE DATABASE bench"
fi
"$FB" setup -sk $SK -sd "$SD" -w $W || { echo "$TAG SETUP-FAIL"; exit 1; }

STREAM="fb$RANDOM"
run_sync() {
  # shellcheck disable=SC2086 # FLAGS is a word list by design
  "$BIN" sync start --source-driver=$SK --source="$SD" --target-driver=$TK --target="$TD" --stream-id=$STREAM $FLAGS >"$1" 2>&1 &
  echo $!
}
PID=$(run_sync "$LOG/$TAG.cold.log")
for _ in $(seq 1 240); do
  grep -q "entering CDC mode" "$LOG/$TAG.cold.log" && break
  kill -0 "$PID" 2>/dev/null || { echo "$TAG COLD-EXIT"; tail -5 "$LOG/$TAG.cold.log"; exit 1; }
  sleep 0.5
done
sleep 4
kill "$PID"
wait "$PID" 2>/dev/null
sleep 1
if [ "$LAT" != 0 ]; then docker exec $TC tc qdisc add dev eth0 root netem delay "${LAT}ms"; fi
"$FB" gen -sk $SK -sd "$SD" -w $W -n "$N" -workers 8 >"$LOG/$TAG.gen.log" 2>&1 || { echo "$TAG GEN-FAIL"; cat "$LOG/$TAG.gen.log"; exit 1; }
RTT=$("$FB" ping -tk $TK -td "$TD")
PID=$(run_sync "$LOG/$TAG.cdc.log")
RES=$("$FB" wait -sk $SK -sd "$SD" -tk $TK -td "$TD" -w $W -timeout "${TIMEOUT:-900s}" | tee "$LOG/$TAG.wait.log" | grep RESULT)
VER=$("$FB" verify -sk $SK -sd "$SD" -tk $TK -td "$TD" -w $W 2>&1 | head -1)
kill "$PID"
wait "$PID" 2>/dev/null
docker exec $TC tc qdisc del dev eth0 root >/dev/null 2>&1
first=$(echo "$RES" | sed -n 's/.*first=\([0-9.-]*\).*/\1/p')
done_=$(echo "$RES" | sed -n 's/.*done=\([0-9.]*\).*/\1/p')
rate=$(awk -v n="$N" -v f="$first" -v d="$done_" 'BEGIN{ if (d=="") {print "NA"} else printf "%.1f", n/(d-f) }')
echo "$TAG N=$N $RTT | $RES | txps=$rate | $VER" | tee -a "$FB_OUT/results.txt"
