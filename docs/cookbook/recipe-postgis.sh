#!/usr/bin/env bash
# SKIP stub for docs/cookbook/recipe-postgis.md (roadmap item 53).
#
# EXCLUDED from the automated cookbook run — operator-only. Reason:
# the recipe requires the PostGIS extension installed on BOTH the source and
# target Postgres (`CREATE EXTENSION postgis;`) plus a MySQL 8.x target with
# SRID-aware geometry types. The standing local rig runs the STOCK `postgres:16`
# image, which does NOT ship PostGIS — verified: `pg_available_extensions` has
# no `postgis` row, so `CREATE EXTENSION postgis` fails with "could not open
# extension control file". Standing up a PostGIS-enabled image (e.g.
# postgis/postgis) is rig infrastructure this pin deliberately does NOT invent.
#
# The recipe's geometry/SRID round-trip IS covered by the in-repo integration
# suite (the PostGIS shard: `Integration (PostGIS)` in CI, ADR-0035 / Bug 26 /
# Bug 27 pins) — that's the authoritative geospatial coverage. This cookbook
# sidecar stays a documented SKIP until/unless the local rig gains a PostGIS
# image; to run it then, swap the rig PG image for postgis/postgis and add a
# `CREATE EXTENSION postgis` step on src+dst before migrate.
set -uo pipefail
echo "RECIPE SKIP: recipe-postgis.md — operator-only (rig postgres:16 has no PostGIS extension; covered by the CI 'Integration (PostGIS)' shard instead)"
exit 2
