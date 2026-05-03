# Prep: wire sluice.yaml mappings through translation

Roadmap reference: not in the original roadmap. Surfaces from the post-walkthrough conversation about handling SET / GEOMETRY / other policy-edge types via config rather than code changes — and prerequisite for the SET and GEOMETRY chunks that follow.

## Goal

The `mappings:` section in `sluice.yaml` already exists in the config struct (`internal/config/config.go` parses `Mappings []Mapping`) and is documented in `docs/examples/sluice.yaml`. It is *not* yet consumed by anything: the orchestrator reads source schema → writes target schema with no per-column override step in between.

This chunk adds that step. After it lands, an operator can write:

```yaml
mappings:
  - table: film
    column: special_features
    target_type: text_array
  - table: address
    column: location
    target_type: bytea           # opt-out of GEOMETRY translation
```

…and the named columns get their `ir.Type` rewritten to the requested target before the schema reaches the writer.

This is a load-bearing prerequisite for two follow-up chunks:

- **SET as `TEXT[]` (default) with mappings overrides** ([prep-set-translation-policy.md](prep-set-translation-policy.md)) — needs the override mechanism to be real.
- **PostGIS-aware GEOMETRY translation** ([prep-postgis-geometry.md](prep-postgis-geometry.md)) — uses mappings to declare PostGIS availability per-column when auto-detection isn't sufficient.

Out of scope:

- **Adding new mapping shapes** beyond the simple `target_type: <name>` override. Per-table renames, schema renames, default-value rewrites — all roadmap §10 territory.
- **Type-override validation against the target engine's capabilities.** A `mappings:` entry that asks for `target_type: jsonb` on a MySQL target should fail loudly. v1 of this chunk implements the override; capability-aware validation can be a small follow-up.

## The translation pass

The cleanest place is a new `internal/translate` package with a single function:

```go
// ApplyMappings rewrites column types in s according to the
// per-column rules in m. The returned schema is a deep copy with
// only the named columns changed; the input schema is not mutated.
//
// Unknown target_type values produce a clear error naming the
// table+column. Mappings that reference tables/columns not in the
// source schema are also errors — silent passthrough would mask
// typos.
func ApplyMappings(s *ir.Schema, m []config.Mapping) (*ir.Schema, error)
```

The function is pure (no I/O), package-private to `translate`, and exhaustively unit-testable.

The mapping → IR-type mapping is a small table:

```go
var targetTypeRegistry = map[string]ir.Type{
    "text":         ir.Text{Size: ir.TextLong},
    "text_array":   ir.Array{Element: ir.Text{Size: ir.TextLong}},
    "jsonb":        ir.JSON{Binary: true},
    "json":         ir.JSON{Binary: false},
    "bytea":        ir.Blob{Size: ir.BlobLong},
    "varchar":      ir.Varchar{Length: 255},  // default length; override via target_type_options
    // GEOMETRY entries land here once that chunk ships.
    // SET-specific aliases (`text_array`, `boolean_per_member`) land too.
}
```

`target_type_options` (already in `config.Mapping`) carries the parameters that don't fit a single string — VARCHAR length, NUMERIC precision/scale, etc. v1 supports the small set above and grows the registry as new types surface real demand.

## Orchestrator wiring

Both `Migrator.Run` and `Streamer.Run` (cold-start path) currently do:

```
schema, err := sr.ReadSchema(ctx)
// ... straight to sw.CreateTablesWithoutConstraints(ctx, schema)
```

After this chunk:

```
schema, err := sr.ReadSchema(ctx)
schema, err = translate.ApplyMappings(schema, cfg.Mappings)
// ... then sw.CreateTablesWithoutConstraints
```

`cfg` is the `*config.Config` already loaded from the YAML+env pair via koanf. The CLI's `Run()` methods need to thread it through to the orchestrator types as a new `Mappings []config.Mapping` field on `Migrator` and `Streamer`.

The CDC path on `Streamer.Run` (the warm-resume branch and the streaming applier) doesn't need ApplyMappings — by the time CDC runs, the target schema is already shaped, and changes flow as IR Row values keyed by column name. Only the schema-shape phase consumes mappings.

## Files to add / touch

- `internal/translate/mappings.go` — new package, `ApplyMappings` plus the target-type registry. ~150 lines.
- `internal/translate/mappings_test.go` — table-driven tests covering: simple overrides, nested types (Array element override), unknown target_type errors, missing table/column errors. ~150 lines.
- `internal/pipeline/migrate.go` — `Migrator.Mappings []config.Mapping` field; `Run` calls `translate.ApplyMappings` after `ReadSchema`. ~10 lines.
- `internal/pipeline/streamer.go` — same on `Streamer.Mappings`; `coldStart` calls ApplyMappings. Warm-resume path doesn't need it. ~10 lines.
- `cmd/sluice/cli.go` — `MigrateCmd.Run` and `SyncStartCmd.Run` thread `cfg.Mappings` to the orchestrator type. ~10 lines.

~330 lines net.

## Anticipated rough edges

- **Deep copy semantics.** `ApplyMappings` returns a copy because callers may rely on the source schema being unchanged after the translation pass. The IR types are mostly value types (`ir.Integer`, `ir.Varchar`, etc.); the table/column structures are pointers. A field-by-field copy of the affected columns is sufficient — full schema clone isn't needed.
- **Registry extensibility.** `targetTypeRegistry` lives in the `translate` package today. As more target_type aliases land (PostGIS shapes, SET strategies, etc.), the registry grows. A registration-style API (`translate.RegisterTargetType("postgis_point", ...)`) is a natural evolution but not necessary for v1.
- **Validation against capabilities.** A mapping requesting `target_type: jsonb` against a MySQL target works (MySQL has JSON). Against a SQLite target (future) it might not. v1 emits the IR type and trusts the writer to error if it can't emit the type — same shape as today's "loud failure beats silent corruption" policy. Capability-aware pre-validation is a follow-up.
- **Per-engine vs. universal mappings.** A mapping that makes sense for a MySQL→PG migration may not make sense for the reverse. Today's config has no per-direction key; the same `mappings:` block applies to both Migrate and Streamer. Probably fine for v1 (operators tend to define mappings for one specific migration), but worth flagging.
- **Existing `target_type_options.binary` example.** The current `docs/examples/sluice.yaml` shows `target_type: jsonb` with `target_type_options: {binary: true}` — but for `jsonb` the binary flag is redundant (jsonb IS binary). Clean up the example to match what the registry actually consumes.

## Open questions for the user

1. **Strict vs lenient on missing tables/columns.** A mapping referencing `table: orders` when the source schema has no `orders` table — error or warning? *Recommendation:* error. Silent passthrough masks typos and the operator who wrote the mapping clearly intended *something*. Confirm?
2. **Initial target_type registry.** Above lists `text`, `text_array`, `jsonb`, `json`, `bytea`, `varchar`. Is that the right v1 set, or do you want to start tighter (just `text` + `text_array` + `jsonb`)? *Recommendation:* the wider set — adding entries reactively is annoying when each requires touching a registry constant. Confirm?
3. **Mappings on the warm-resume path.** Above I'm only running ApplyMappings during cold-start (the schema-apply phase). Warm-resume skips schema apply entirely. *Recommendation:* same — there's no schema to apply, no overrides to honor. Confirm?
4. **Per-engine direction filtering.** Above, mappings apply uniformly. *Recommendation:* defer per-direction filtering until a real use case appears. Confirm?

## Suggested first-cut prompt

> "Read CLAUDE.md, docs/dev/notes/prep-mappings-config-wiring.md, and internal/config/config.go's Mapping shape. Propose the design before writing: (1) the exact translate.ApplyMappings function signature and the table-driven unit-test shape, (2) the orchestrator-side wiring (Migrator.Mappings + Streamer.Mappings), (3) the initial target_type registry contents, (4) how docs/examples/sluice.yaml's example block should change. Note any deviation from the prep doc with a why. Stop after the design for review."
