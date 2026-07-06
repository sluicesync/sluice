#!/usr/bin/env bash
# SKIP stub for docs/cookbook/recipe-heroku-migration.md (roadmap item 53).
#
# EXCLUDED from the automated cookbook run — operator-only / cloud. Reason:
# the recipe is defined ENTIRELY by a managed cloud provider's restrictions —
# Heroku Postgres with `rolreplication = false`, no `CREATE EVENT TRIGGER`, and
# no `CREATE EXTENSION` outside its allowlist. Its whole point is the slot-less,
# non-superuser path those restrictions force. The local rig runs a full-
# privilege `postgres:16` superuser role, so it CANNOT reproduce the constraint
# the recipe exists to document — a rig run would either exercise the wrong
# (privileged) path or require a live Heroku account, which this pin does NOT
# invent (roadmap item 53 gotcha #1: no cloud deps in the automated path).
#
# What a green here would need: a real Heroku Postgres instance (or a role
# deliberately stripped of REPLICATION + CREATE EXTENSION + event-trigger
# grants). That's an operator-run validation, out of the local-rig scope.
set -uo pipefail
echo "RECIPE SKIP: recipe-heroku-migration.md — operator-only (needs a live Heroku Postgres / a restriction-stripped role; the full-privilege local rig can't reproduce the slot-less non-superuser constraint the recipe is about)"
exit 2
