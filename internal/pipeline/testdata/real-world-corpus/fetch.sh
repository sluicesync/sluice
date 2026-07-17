#!/bin/sh
# Fetch-on-demand for the real-world schema corpus (schema-only DDL).
# Provenance + rationale: ./MANIFEST.md and
# docs/dev/notes/prep-new-test-surfaces.md § "Idea 3".
#
# The fetched *.sql + FETCHED.txt are gitignored; this script and
# MANIFEST.md are the only tracked files in this dir. Re-run any time
# to refresh; outputs are DETERMINISTIC because every source is fetched
# by a PINNED upstream commit SHA (see the PINS block below), not a
# moving ref (master/trunk/main/5.4-dev).
#
# WHY PINNED (the drift trap): the congruence oracle
# (migrate_realworld_corpus_congruence_integration_test.go) compares
# sluice's cross-engine emit against these fetched authored pairs and
# tolerates a hand-maintained benign allowlist. Fetching from moving
# refs meant every upstream schema edit silently invalidated the
# allowlist and turned the corpus leg red on drift that has nothing to
# do with sluice. Pinning freezes the corpus so the allowlist stays
# valid until a DELIBERATE bump.
#
# TO BUMP (intentional, reviewed): run `./fetch.sh --resolve-latest` to
# print each repo's current upstream HEAD SHA, update the one line in
# the PINS block below, re-run `./fetch.sh`, then refresh the congruence
# allowlist/characterizations against the new schemas in the SAME PR.
set -eu

here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$here"

# ---------------------------------------------------------------------
# PINS — the committed pin manifest (single source of truth). Each line
# is one repo's frozen upstream commit SHA. Bumping any source is a
# one-line change here (+ an allowlist refresh in the same PR). Resolve
# fresh SHAs with `./fetch.sh --resolve-latest`.
#
#   repo@ref (at pin time)                        pinned commit
CHINOOK_SHA=7f67772503d71ba90f19283c38e93923addb43fa   # lerocha/chinook-database@master
MEDIAWIKI_SHA=c3c99d51534f59511e5497f4593b21e6ce45183b # wikimedia/mediawiki@master
TESTDB_SHA=e324b56193ca506ab7cc1ab143a9153d8c4535d7    # datacharmer/test_db@master
JOOMLA_SHA=32122f5d747dcf485e6c4e944a426f18b44e1fc9    # joomla/joomla-cms@5.4-dev
WORDPRESS_SHA=ace9192af868524bdc49cf4fcb91f4c12c73ee5f # WordPress/wordpress-develop@trunk
VITESS_SHA=704634c7eeb1bb92bcb6d25d04ccf89e8f258b2f    # vitessio/vitess@main
GITLAB_SHA=19eed6d0c7c1d36797308ca3d0f4c1d34f4cafb1    # gitlab-org/gitlab@master (gitlab.com)
# ---------------------------------------------------------------------

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

# --resolve-latest: best-effort helper for the deliberate-bump workflow.
# Prints each repo's CURRENT upstream HEAD SHA so an operator can copy
# the new value into the PINS block above. Never fails (|| true on every
# probe) — it is a convenience, not part of the deterministic fetch.
if [ "${1:-}" = "--resolve-latest" ]; then
  gh_head() { # owner/repo ref
    "$CURL" -fsSL -m 30 "https://api.github.com/repos/$1/commits/$2" 2>/dev/null \
      | grep -m1 '"sha"' | sed 's/.*"sha":[[:space:]]*"\([0-9a-f]*\)".*/\1/' || true
  }
  gl_head() { # project-path-encoded ref
    "$CURL" -fsSL -m 30 \
      "https://gitlab.com/api/v4/projects/$1/repository/commits?ref_name=$2&per_page=1" 2>/dev/null \
      | grep -o '"id":"[0-9a-f]*"' | head -1 | sed 's/.*"id":"\([0-9a-f]*\)".*/\1/' || true
  }
  echo "current upstream HEADs (copy into the PINS block, then re-run fetch.sh):"
  printf '  CHINOOK_SHA=%s\n'   "$(gh_head lerocha/chinook-database master)"
  printf '  MEDIAWIKI_SHA=%s\n' "$(gh_head wikimedia/mediawiki master)"
  printf '  TESTDB_SHA=%s\n'    "$(gh_head datacharmer/test_db master)"
  printf '  JOOMLA_SHA=%s\n'    "$(gh_head joomla/joomla-cms 5.4-dev)"
  printf '  WORDPRESS_SHA=%s\n' "$(gh_head WordPress/wordpress-develop trunk)"
  printf '  VITESS_SHA=%s\n'    "$(gh_head vitessio/vitess main)"
  printf '  GITLAB_SHA=%s\n'    "$(gl_head gitlab-org%2Fgitlab master)"
  exit 0
fi

# Raw-file base URLs, pinned by SHA.
GH_RAW=https://raw.githubusercontent.com
GL_RAW=https://gitlab.com/gitlab-org/gitlab/-/raw

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
    # Chinook PG is a psql script: drop psql backslash meta-commands
    # (\connect, \encoding, \set, ...) — db.ExecContext is not psql and
    # errors "syntax error at or near \". Reached only OUTSIDE a COPY
    # block (incopy handled above), so the COPY terminator is safe.
    /^[[:space:]]*\\/ { next }
    # Drop ALL DB-switching so every CREATE TABLE lands in the DB the
    # sluice DSN/connection reads (else tables go to a side DB and the
    # reader sees 0 → a VACUOUS green; the harness now also guards this
    # with a table-count assertion). Covers DATABASE + SCHEMA
    # create/drop and the MySQL `USE db;` session switch (the MySQL
    # analog of psql \connect, already dropped above). Unqualified
    # CREATE TABLEs then land in the current connection DB.
    /^[[:space:]]*(DROP|CREATE)[[:space:]]+(DATABASE|SCHEMA)[[:space:]]/ { next }
    /^[[:space:]]*USE[[:space:]]/ { next }
    # mysql-client "source <file>;" include directives (datacharmer
    # test_db sources its data .dump files) are not SQL — drop.
    /^[[:space:]]*source[[:space:]]/ { next }
    { print }
  ' "$1" > "$2"
  if ! grep -qiE 'CREATE TABLE' "$2"; then
    echo "fetch.sh: WARNING $2 has no CREATE TABLE after strip" >&2
  fi
}

# extract_wp_schema: WordPress core schema lives in PHP
# (wp-admin/includes/schema.php, wp_get_db_schema()) as `"CREATE TABLE
# $wpdb->NAME ( ... ) $charset_collate;"` string blocks. Pull just the
# CREATE TABLEs, substituting the PHP placeholders so the result is
# plain MySQL DDL: $wpdb->NAME -> wp_NAME, $max_index_length -> 191,
# the `) $charset_collate[ ...];` terminator -> `);`. Per-statement
# terminator so PHP block boundaries between tables don't matter.
# DEDUP: wp_get_db_schema() defines wp_users/wp_usermeta TWICE
# (mutually-exclusive $users_single_table vs $users_multi_table
# scopes); keep the FIRST occurrence per table name = the canonical
# single-site schema, skip later duplicates (else "table already
# exists" on apply).
extract_wp_schema() { # infile outfile
  awk '
    /CREATE TABLE \$wpdb->/ {
      name=$0
      sub(/^.*CREATE TABLE \$wpdb->/, "", name)
      sub(/[^a-zA-Z0-9_].*$/, "", name)          # bare table name
      if (seen[name]++) { skip=1; inct=0; next } # dup → skip block
      l=$0
      sub(/^.*CREATE TABLE \$wpdb->/, "CREATE TABLE wp_", l)
      gsub(/\$max_index_length/, "191", l)
      print l; inct=1; skip=0; next
    }
    skip {
      if ($0 ~ /\$charset_collate.*;/) { skip=0 }
      next
    }
    inct {
      l=$0
      if (l ~ /\$charset_collate.*;/) {
        sub(/\)[[:space:]]*\$charset_collate.*;.*/, ");", l)
        gsub(/\$wpdb->/, "wp_", l); gsub(/\$max_index_length/, "191", l)
        print l; inct=0; next
      }
      gsub(/\$wpdb->/, "wp_", l); gsub(/\$max_index_length/, "191", l)
      print l; next
    }
  ' "$1" > "$2"
  if ! grep -qiE 'CREATE TABLE wp_' "$2"; then
    echo "fetch.sh: WARNING $2 has no CREATE TABLE after WP extract" >&2
  fi
}

echo "real-world-corpus fetch (pinned):"

get "$GL_RAW/$GITLAB_SHA/db/structure.sql" \
    "gitlab_structure.pg.sql"   # schema-only by design

get "$GH_RAW/lerocha/chinook-database/$CHINOOK_SHA/ChinookDatabase/DataSources/Chinook_MySql.sql" \
    ".chinook_mysql.raw"
strip_data ".chinook_mysql.raw" "chinook_mysql.ddl.sql"; rm -f ".chinook_mysql.raw"

get "$GH_RAW/lerocha/chinook-database/$CHINOOK_SHA/ChinookDatabase/DataSources/Chinook_PostgreSql.sql" \
    ".chinook_postgres.raw"
strip_data ".chinook_postgres.raw" "chinook_postgres.ddl.sql"; rm -f ".chinook_postgres.raw"

# --- iteration 2 ---
# MediaWiki tables-generated.sql: both dialects generated from ONE
# abstract schema (sql/tables.json). NOTE: "generated from one source"
# does NOT make the two files column-for-column congruent — MediaWiki's
# PG adapter renders the abstract `binary`/`blob` types as TEXT and the
# MW-timestamp type as TIMESTAMPTZ, while the MySQL side uses
# VARBINARY(n). sluice faithfully carries MySQL VARBINARY -> PG bytea,
# so those columns DIVERGE from the authored PG side by upstream design,
# not by a sluice defect (see the congruence test's classification).
# Schema-only by design.
get "$GH_RAW/wikimedia/mediawiki/$MEDIAWIKI_SHA/sql/mysql/tables-generated.sql" \
    "mediawiki_mysql.ddl.sql"
get "$GH_RAW/wikimedia/mediawiki/$MEDIAWIKI_SHA/sql/postgres/tables-generated.sql" \
    "mediawiki_postgres.ddl.sql"

# datacharmer test_db employees (partitioned): real MySQL with
# PARTITION BY (a feature Chinook lacks). Sources its data from .dump
# files (stripped: the "source ...;" directives are dropped).
get "$GH_RAW/datacharmer/test_db/$TESTDB_SHA/employees_partitioned.sql" \
    ".employees.raw"
strip_data ".employees.raw" "employees_mysql_partitioned.ddl.sql"; rm -f ".employees.raw"

# --- iteration 3 ---
# Joomla ships raw install SQL for BOTH dialects → a real-CMS matched
# cross-engine pair (independently authored per dialect, like Chinook;
# not generated-from-one-source like MediaWiki). base.sql = core
# schema (+ seed INSERTs, stripped). joomla-cms default branch 5.4-dev.
get "$GH_RAW/joomla/joomla-cms/$JOOMLA_SHA/installation/sql/mysql/base.sql" \
    ".joomla_mysql.raw"
strip_data ".joomla_mysql.raw" "joomla_mysql.ddl.sql"; rm -f ".joomla_mysql.raw"
get "$GH_RAW/joomla/joomla-cms/$JOOMLA_SHA/installation/sql/postgresql/base.sql" \
    ".joomla_postgres.raw"
strip_data ".joomla_postgres.raw" "joomla_postgres.ddl.sql"; rm -f ".joomla_postgres.raw"

# WordPress core schema is PHP (wp_get_db_schema()); extract the
# CREATE TABLEs to plain MySQL DDL. Canonical operator-brought MySQL.
get "$GH_RAW/WordPress/wordpress-develop/$WORDPRESS_SHA/src/wp-admin/includes/schema.php" \
    ".wordpress.raw"
extract_wp_schema ".wordpress.raw" "wordpress_mysql.ddl.sql"; rm -f ".wordpress.raw"

# --- iteration 4 ---
# Vitess own example schema (vitessio/vitess examples/common). Apache-2.0
# (vitessio/vitess LICENSE) — permissively licensed, but kept gitignored
# / fetch-on-demand for consistency with the rest of the corpus. The
# `commerce` keyspace schema is the canonical Vitess example: no FKs
# (Vitess discourages cross-shard FKs), a reference/sequence-table idiom
# — characterizes Vitess DDL idioms through sluice's MySQL reader. Run
# through strip_data for discipline (it is schema-only upstream; the
# strip is a no-op safety pass + drops any `USE`/`source` if present).
# NOTE: upstream relocated this from examples/local → examples/common
# (2026-07); the old path 404s. Keep on examples/common.
get "$GH_RAW/vitessio/vitess/$VITESS_SHA/examples/common/create_commerce_schema.sql" \
    ".vitess_commerce.raw"
strip_data ".vitess_commerce.raw" "vitess_commerce_mysql.ddl.sql"; rm -f ".vitess_commerce.raw"

# NOTE — evaluated, NOT fetched (do not "fix"): pgloader's test corpus
# is `.load` orchestration against live MySQL DSNs (no standalone
# schema .sql); Drupal core schema is PHP hook_schema() in *.install
# (no raw .sql). Neither fits the fetch-on-demand schema-corpus shape.
# See MANIFEST.md / real-world-corpus-findings.md for the rationale +
# the alternative (pgloader's cast ruleset is a translator-catalog
# reference, not a corpus).

{
  echo "fetched_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "# All sources fetched by PINNED commit SHA (see the PINS block in fetch.sh)."
  echo "gitlab_structure.pg.sql             <- gitlab-org/gitlab@$GITLAB_SHA  (schema-only by design)"
  echo "chinook_*.ddl.sql                   <- lerocha/chinook-database@$CHINOOK_SHA  (data stripped)"
  echo "mediawiki_*.ddl.sql                 <- wikimedia/mediawiki@$MEDIAWIKI_SHA  (schema-only upstream)"
  echo "employees_mysql_partitioned.ddl.sql <- datacharmer/test_db@$TESTDB_SHA  (source-directives stripped)"
  echo "joomla_*.ddl.sql                    <- joomla/joomla-cms@$JOOMLA_SHA  (seed data stripped)"
  echo "wordpress_mysql.ddl.sql             <- WordPress/wordpress-develop@$WORDPRESS_SHA  (extracted from PHP wp_get_db_schema())"
  echo "vitess_commerce_mysql.ddl.sql       <- vitessio/vitess@$VITESS_SHA  (examples/common commerce keyspace; data-strip pass)"
} > FETCHED.txt

echo "done. outputs:"
ls -1 *.sql FETCHED.txt 2>/dev/null | sed 's/^/  /'
