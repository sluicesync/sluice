# ADR-0003: kong + koanf for CLI and config

## Status

Accepted.

## Context

PlanetScale's `pscale` CLI uses cobra + pflag + viper, the de-facto Go CLI stack. Sluice could have followed that template, both for familiarity to contributors crossing over from `pscale` and for ecosystem reach (the cobra docs and recipes are extensive).

Two concerns argued against the default. First, the cobra/viper stack has accumulated incidental complexity over years: command construction is imperative, configuration binding involves several layers (pflag → viper → struct), and edge cases around precedence and key normalization are easy to get wrong. Second, the project tenet of clean, elegant code (CLAUDE.md) puts a premium on small surface areas and a story-readable codebase. Reading a `pscale`-shaped CLI definition vs. a kong-shaped one is a meaningful difference for a small project.

## Decision

CLI parsing is handled by `github.com/alecthomas/kong`. The command tree is declared as a Go struct, with subcommands as fields and flags as struct tags (`cmd/sluice/cli.go`). Each subcommand has a `Run` method bound by kong; globals are an embedded struct so they parse identically across every subcommand.

Configuration loading is handled by `github.com/knadh/koanf/v2`. A YAML file (loaded via `koanf/providers/file`) is overlaid with environment variables prefixed `SLUICE_` (loaded via `koanf/providers/env`). The resulting key namespace is unmarshalled into the typed `Config` struct in `internal/config`. CLI flags override config — kong owns flag parsing and runs first; koanf is consulted by the subcommand's `Run` method.

## Consequences

The CLI definition reads top-down as a single Go struct, with commands and flags collocated with their behavior. Adding a new subcommand is a struct field plus a `Run` method, no command-builder boilerplate. Configuration precedence is explicit in one function (`config.Load`) rather than spread across pflag/viper bindings.

The cost is unfamiliarity for contributors used to cobra. Mitigation: `cmd/sluice/cli.go` and `internal/config/config.go` are heavily commented, and CLAUDE.md flags the choice. If a maintainer ever needs to swap stacks, both libraries are scoped to small, well-isolated packages.
