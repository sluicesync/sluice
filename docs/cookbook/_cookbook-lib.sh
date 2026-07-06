# shellcheck shell=bash
# Shared helpers for the cookbook sidecar scripts (roadmap item 53:
# "cookbook-as-executable-tests"). Sourced by recipe-*.sh + quickstart.sh.
#
# These scripts run the *released* sluice binary through each documented
# recipe verbatim-in-intent and ASSERT the documented outcome, so a rotted
# recipe (renamed flag, changed default, wrong syntax) fails a test instead
# of a user's session.
#
# Contract with the caller (the sluice-testing runner, or a human):
#   - $SLUICE            path to the sluice binary under test (required)
#   - rig connection env (all have local-rig defaults, override to retarget):
#       PG_SRC_CONTAINER PG_DST_CONTAINER MY_SRC_CONTAINER MY_DST_CONTAINER
#       PG_HOST PG_SRC_PORT PG_DST_PORT PG_USER PG_PW
#       MY_HOST MY_SRC_PORT MY_DST_PORT MY_USER MY_PW
#   - docker on PATH (SQL setup/seed/assert runs via `docker exec`)
#
# The scripts create + drop their OWN throwaway databases and backup dirs;
# they never touch the standing rig's baseline data.

set -uo pipefail

# ---- rig defaults (mirror sluice-testing local-rig; override via env) --------
PG_SRC_CONTAINER="${PG_SRC_CONTAINER:-sluice-localrig-pg-src}"
PG_DST_CONTAINER="${PG_DST_CONTAINER:-sluice-localrig-pg-dst}"
MY_SRC_CONTAINER="${MY_SRC_CONTAINER:-sluice-localrig-mysql-src}"
MY_DST_CONTAINER="${MY_DST_CONTAINER:-sluice-localrig-mysql-dst}"
PG_HOST="${PG_HOST:-localhost}"; PG_SRC_PORT="${PG_SRC_PORT:-5442}"; PG_DST_PORT="${PG_DST_PORT:-5443}"
PG_USER="${PG_USER:-postgres}"; PG_PW="${PG_PW:-pgpw}"
MY_HOST="${MY_HOST:-localhost}"; MY_SRC_PORT="${MY_SRC_PORT:-3316}"; MY_DST_PORT="${MY_DST_PORT:-3317}"
MY_USER="${MY_USER:-root}"; MY_PW="${MY_PW:-rootpw}"

# RECIPE_PAGE is set by each sidecar; assertions cite it so triage starts at
# the right file (roadmap item 53 gotcha #3).
RECIPE_PAGE="${RECIPE_PAGE:-<unset>}"

# ---- DSN builders (what the recipe's `sluice` commands connect with) --------
pg_src_dsn(){ echo "postgresql://${PG_USER}:${PG_PW}@${PG_HOST}:${PG_SRC_PORT}/$1?sslmode=disable"; }
pg_dst_dsn(){ echo "postgresql://${PG_USER}:${PG_PW}@${PG_HOST}:${PG_DST_PORT}/$1?sslmode=disable"; }
my_src_dsn(){ echo "${MY_USER}:${MY_PW}@tcp(${MY_HOST}:${MY_SRC_PORT})/$1"; }
my_dst_dsn(){ echo "${MY_USER}:${MY_PW}@tcp(${MY_HOST}:${MY_DST_PORT})/$1"; }

# ---- SQL exec helpers (test scaffolding: create/seed/assert) -----------------
# $1 = database, $2 = SQL. Postgres via psql -tA; MySQL via mysql -N -B.
pg_src_sql(){ docker exec -i "$PG_SRC_CONTAINER" psql -U "$PG_USER" -d "$1" -tA -c "$2"; }
pg_dst_sql(){ docker exec -i "$PG_DST_CONTAINER" psql -U "$PG_USER" -d "$1" -tA -c "$2"; }
pg_src_admin(){ docker exec -i "$PG_SRC_CONTAINER" psql -U "$PG_USER" -d postgres -tA -c "$1"; }
pg_dst_admin(){ docker exec -i "$PG_DST_CONTAINER" psql -U "$PG_USER" -d postgres -tA -c "$1"; }
my_src_sql(){ docker exec -i "$MY_SRC_CONTAINER" mysql -u"$MY_USER" -p"$MY_PW" --default-character-set=utf8mb4 "$1" -N -B -e "$2" 2>/dev/null; }
my_src_admin(){ docker exec -i "$MY_SRC_CONTAINER" mysql -u"$MY_USER" -p"$MY_PW" -N -B -e "$1" 2>/dev/null; }

# ---- assertions (exit-code + named-artifact, never just "ran") ---------------
# Every failure names the recipe page + the step, so triage starts there.
_recipe_fail(){ # step, message...
  local step="$1"; shift
  echo "" >&2
  echo "RECIPE FAIL" >&2
  echo "  page: $RECIPE_PAGE" >&2
  echo "  step: $step" >&2
  echo "  why : $*" >&2
  _run_cleanup
  exit 1
}
pass_note(){ echo "  ok: $*"; }

assert_rc0(){ # rc, step, desc
  [ "$1" = "0" ] || _recipe_fail "$2" "$3 — expected exit 0, got exit $1"
  pass_note "$3 (exit 0)"
}
assert_rc_nonzero(){ # rc, step, desc
  [ "$1" != "0" ] || _recipe_fail "$2" "$3 — expected NON-zero exit (loud refusal), got exit 0"
  pass_note "$3 (loud refusal, exit $1)"
}
assert_eq(){ # actual, expected, step, desc
  [ "$1" = "$2" ] || _recipe_fail "$3" "$4 — expected '$2', got '$1'"
  pass_note "$4 ($1)"
}
assert_contains(){ # haystack, needle, step, desc
  case "$1" in *"$2"*) pass_note "$4 (found '$2')";; *) _recipe_fail "$3" "$4 — output did not contain '$2'";; esac
}
assert_file_glob(){ # dir, glob, step, desc  — at least one match
  local n; n=$(find "$1" -name "$2" 2>/dev/null | wc -l | tr -d '[:space:]')
  [ "$n" -ge 1 ] || _recipe_fail "$3" "$4 — no file matching '$2' under $1"
  pass_note "$4 ($n match under $1)"
}

# ---- cleanup registration ----------------------------------------------------
# A sidecar defines cleanup() (idempotent); _run_cleanup calls it once.
_CLEANED=0
_run_cleanup(){ [ "$_CLEANED" = "1" ] && return 0; _CLEANED=1; type cleanup >/dev/null 2>&1 && cleanup || true; }

require_sluice(){
  [ -n "${SLUICE:-}" ] || { echo "RECIPE SKIP: \$SLUICE not set" >&2; exit 2; }
  "$SLUICE" --version >/dev/null 2>&1 || { echo "RECIPE FAIL: \$SLUICE ($SLUICE) does not run --version" >&2; exit 1; }
}

# poll until `$1` (a command printing a value) equals $2, or timeout $3 sec
wait_until(){ local q="$1" want="$2" to="${3:-60}" i=0 got=""; while [ "$i" -lt "$to" ]; do got=$(eval "$q" 2>/dev/null | tr -d '[:space:]'); [ "$got" = "$want" ] && return 0; sleep 1; i=$((i+1)); done; echo "  wait_until TIMEOUT: want=$want got=$got after ${to}s" >&2; return 1; }

# launch a detached sluice child on Windows (Start-Process), echo its PID.
# Bash `&` doesn't detach on this Windows rig; PowerShell Start-Process does.
# IMPORTANT: PowerShell's stdout must go to a FILE we then read — NOT a pipe
# captured via $(). The launched sluice child inherits PowerShell's stdout
# pipe handle, so a `powershell ... | tr` in a command substitution never
# sees EOF and blocks forever. Redirecting to a pidfile (as resume.sh does)
# lets PowerShell exit cleanly and bash read the PID after.
sluice_detached(){ # logfile, args...
  local log="$1"; shift
  local pidfile="${log}.pid"
  # PowerShell is a native-Windows process: its -Redirect* args need a Windows
  # path, not an MSYS one (e.g. a /tmp/... log silently writes to the wrong
  # place, so a bash-side grep of the log never sees it). Convert for PS; the
  # converted Windows path resolves to the SAME physical file bash reads back.
  local wlog="$log"; command -v cygpath >/dev/null 2>&1 && wlog="$(cygpath -w "$log")"
  local ps_args=""
  for a in "$@"; do ps_args="$ps_args,'$a'"; done
  ps_args="${ps_args#,}"
  powershell -NoProfile -Command "Start-Process -FilePath '$SLUICE' -ArgumentList $ps_args -RedirectStandardError '$wlog' -RedirectStandardOutput '$wlog.out' -PassThru | Select-Object -ExpandProperty Id" > "$pidfile" 2>/dev/null
  tr -dc '0-9' < "$pidfile"
}
kill_pid(){ [ -n "${1:-}" ] && powershell -NoProfile -Command "Stop-Process -Id $1 -Force -ErrorAction SilentlyContinue" 2>/dev/null; return 0; }
