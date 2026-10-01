#!/usr/bin/env bash
# down.sh — remove every fencebench container (they hold no volumes).
docker rm -f fb-tc-mydst fb-tc-pgdst fb-mysrc fb-mydst fb-pgsrc fb-pgdst >/dev/null 2>&1 || true
left="$(docker ps -a --format '{{.Names}}' | grep -c '^fb-' || true)"
echo "down: ${left} fencebench containers remain"
