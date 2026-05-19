#!/bin/sh
# Fetch-on-demand for the real-world schema corpus (schema-only DDL).
# Provenance + rationale: ./MANIFEST.md and
# docs/dev/notes/prep-new-test-surfaces.md § "Idea 3".
#
# The fetched *.sql + FETCHED.txt are gitignored; this script and
# MANIFEST.md are the only tracked files in this dir. Re-run any time
# to refresh; outputs are deterministic given upstream state.
set -eu

here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$here"

# curl resolution: PATH first (Win10+ has curl.exe), then the bundled
# Rancher Desktop curl (often not on PATH).
if command -v curl >/dev/null 2>&1; then
  CURL=curl
elif [ -x "/c/Program Files/Rancher Desktop/resources/resources/win32/bin/curl.exe" ]; then
  CURL="/c/Program Files/Rancher Desktop/resources/resources/win32/bin/curl.exe"
else
  echo "fetch.sh: no curl on PATH and no Rancher curl found" >&2
  exit 1
fi

get() { # url outfile
  echo "  fetch $2"
  "$CURL" -fsSL -m 120 -o "$2" "$1"
}

# Strip data so the corpus is schema-only (no data-licensing concern):
# drop INSERT statements and PG COPY..stdin blocks; keep DDL.
strip_data() { # infile outfile
  # Chinook uses multi-row INSERTs: an "INSERT INTO t (...) VALUES"
  # opener then many "(...)," continuation lines until a line ending
  # ";". Skip the whole statement, not just the opener. Also handles
  # single-line INSERTs and PG COPY..stdin blocks. The ";-at-EOL"
  # terminator is a heuristic safe for this corpus (data values never
  # end a line with ";"); documented in MANIFEST.md.
  awk '
    ininsert { if ($0 ~ /;[[:space:]]*$/) ininsert=0; next }
    /^[[:space:]]*INSERT[[:space:]]+INTO/ {
      if ($0 ~ /;[[:space:]]*$/) next      # single-line INSERT
      ininsert=1; next                     # multi-line INSERT begins
    }
    /^[[:space:]]*COPY[[:space:]].*FROM[[:space:]]+stdin;/ { incopy=1; next }
    incopy && /^\\\.[[:space:]]*$/ { incopy=0; next }
    incopy { next }
    { print }
  ' "$1" > "$2"
  if ! grep -qiE 'CREATE TABLE' "$2"; then
    echo "fetch.sh: WARNING $2 has no CREATE TABLE after strip" >&2
  fi
}

# best-effort upstream-ref capture (never fails the fetch)
gh_sha=$("$CURL" -fsSL -m 30 \
  "https://api.github.com/repos/lerocha/chinook-database/commits/master" 2>/dev/null \
  | grep -m1 '"sha"' | sed 's/.*"sha":[[:space:]]*"\([0-9a-f]*\)".*/\1/' || true)
gl_sha=$("$CURL" -fsSL -m 30 \
  "https://gitlab.com/api/v4/projects/gitlab-org%2Fgitlab/repository/commits?path=db/structure.sql&per_page=1" 2>/dev/null \
  | grep -o '"id":"[0-9a-f]*"' | head -1 | sed 's/.*"id":"\([0-9a-f]*\)".*/\1/' || true)

echo "real-world-corpus fetch:"

get "https://gitlab.com/gitlab-org/gitlab/-/raw/master/db/structure.sql" \
    "gitlab_structure.pg.sql"   # schema-only by design

get "https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_MySql.sql" \
    ".chinook_mysql.raw"
strip_data ".chinook_mysql.raw" "chinook_mysql.ddl.sql"; rm -f ".chinook_mysql.raw"

get "https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_PostgreSql.sql" \
    ".chinook_postgres.raw"
strip_data ".chinook_postgres.raw" "chinook_postgres.ddl.sql"; rm -f ".chinook_postgres.raw"

{
  echo "fetched_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "gitlab_structure.pg.sql  <- gitlab-org/gitlab@master  commit=${gl_sha:-unresolved}"
  echo "chinook_*.ddl.sql        <- lerocha/chinook-database@master  commit=${gh_sha:-unresolved}  (data stripped)"
} > FETCHED.txt

echo "done. outputs:"
ls -1 *.sql FETCHED.txt 2>/dev/null | sed 's/^/  /'
