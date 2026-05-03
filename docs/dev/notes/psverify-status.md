# PlanetScale Postgres verification — status

This is a working note tracking the PS-PG verification chunk. It lives
in `docs/dev/notes/` because it's intentionally ephemeral — once
verification completes, the findings move to
`docs/managed-services.md` and this file goes away.

## What's set up

- `PLANETSCALE_CREDENTIALS.env` at the repo root holds DSNs for two
  PS-MySQL and two PS-Postgres databases. The file is gitignored
  (`*.env` rule in `.gitignore`).
- `internal/engines/postgres/planetscale_verify_test.go` is gated
  behind the `psverify` build tag and reads credentials from the env
  file (or from real env vars if exported). With the tag absent the
  file is excluded from the build, so the regular test/lint gate
  doesn't touch it.

## How to run the verification (when ready)

From the repo root:

```bash
go test -tags=psverify -v -count=1 -timeout=10m \
  -run 'TestPSPG' ./internal/engines/postgres/...
```

The tests skip cleanly when the credentials file isn't present, so a
checkout without the file just no-ops on this set.

Each phase that creates objects on PS-PG drops them at the end so the
same database can host repeated runs.

## Sandbox-permission caveat (encountered 2026-05-02)

When run from the agent's sandbox, both `go test -tags=psverify` and
`go vet -tags=psverify` were denied with the rationale that they
would connect to external PlanetScale databases. The verification
work therefore needs the user to either:

1. Run the tests directly from a non-sandboxed shell, or
2. Approve the bash command(s) for the agent's sandbox.

The tests are written to be the same in either case. They have NOT
been compile-checked under the `psverify` tag inside this session
because of the denial — please run `go vet -tags=psverify ./internal/...`
yourself first to confirm they build before running the actual tests.

## Verification phases

Listed in dependency order — each phase assumes the prior one
passed.

1. **Connectivity** (`TestPSPG_Connectivity`): pgx connects, ping
   succeeds, `SELECT 1` returns 1. Logs `version()`, `wal_level`,
   `max_wal_senders`, `max_replication_slots`, current user's
   REPLICATION attribute, and PostGIS-installed flag.
2. **Schema reader** (`TestPSPG_SchemaReaderRoundTrip`): seed a
   small schema (`users`, `posts` with FK CASCADE), run sluice's
   `SchemaReader.ReadSchema`, assert IR shape matches.
3. **Simple-mode migration**: PS-PG → PS-PG via `pipeline.Migrator`,
   small dataset, verify rows round-trip.
4. **CDC reader**: `CDCReader.StreamChanges` against PS-PG with
   `wal_level=logical` and the REPLICATION role attribute. Phase 1
   surfaces both prerequisites.
5. **Continuous-sync streamer**: full snapshot+CDC handoff. Mirrors
   the §4/§5 same-engine PG streamer test.
6. **Cross-engine MySQL → PS-PG**: bonus, MySQL source via the
   PlanetScale-MySQL flavor → PS-PG destination.

## Outstanding decisions

- **Flavor split**: declare `FlavorPlanetScalePostgres` only if 3+
  vendor-specific quirks cluster (per the prep doc's rule).
  Decision deferred until the verification phases reveal what the
  quirks actually are.
- **External-service CI tag**: `psverify` is set up the same way
  `integration` is. The CI workflow does not run it. Promoting it to
  CI would require credential storage in GitHub secrets and a
  decision about which datasets it should keep as fixtures — both
  out of scope for v1.
