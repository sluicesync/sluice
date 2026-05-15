// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

// PII Phase 4 — keyset loader (ADR-0041 §"CLI surface").
//
// `--keyset-source=<scheme>:<value>` is the ONLY key path (the
// Phase 1 `--redact-key-source` flag was deleted — clean break, no
// shim). Supported schemes:
//
//   - file:<path>  — keyset YAML on disk
//   - env:<var>    — keyset YAML in an environment variable
//   - db:<dsn>     — keyset rows in a sluice-managed `sluice_keysets`
//     table on the named DSN (cross-stream-stability primitive: two
//     streams pointing at the same db: source share the keyset)
//
// All loading happens ONCE at startup (D1 — startup-snapshot, no
// hot-reload). Every failure mode is loud and actionable.

// KeysetStore is the engine-neutral contract for the `db:` scheme.
// Engine packages (internal/engines/postgres, .../mysql) implement
// it; the redact package depends only on this interface and never
// imports an engine package (IR-first tenet). The store is opened
// by a registered opener (see RegisterKeysetStoreOpener).
type KeysetStore interface {
	// EnsureKeysetTable creates the sluice_keysets table if it does
	// not exist. Idempotent (CREATE TABLE IF NOT EXISTS shape,
	// mirroring the engines' control-table pattern).
	EnsureKeysetTable(ctx context.Context) error

	// LoadKeyset reads every row of sluice_keysets into the resolved
	// in-memory shape. Returns a loud, actionable error when the
	// table is absent or empty (the operator must populate it via
	// manual SQL — the rotate/list CLI is out of v1 scope).
	LoadKeyset(ctx context.Context) (*Keyset, error)

	// Close releases the underlying connection.
	Close() error
}

// KeysetStoreOpener constructs a [KeysetStore] from a DSN. Engine
// packages register one in init(); the db: loader picks an opener by
// DSN shape.
type KeysetStoreOpener func(ctx context.Context, dsn string) (KeysetStore, error)

var (
	keysetOpenerMu sync.RWMutex
	keysetOpeners  = map[string]KeysetStoreOpener{}
)

// RegisterKeysetStoreOpener registers an engine's db: keyset-store
// opener under a name ("postgres" / "mysql"). Called from engine
// package init() functions so the redact package never imports an
// engine package. Re-registering a name panics — a duplicate
// registration is a build-time wiring bug, not a runtime condition.
func RegisterKeysetStoreOpener(name string, opener KeysetStoreOpener) {
	keysetOpenerMu.Lock()
	defer keysetOpenerMu.Unlock()
	if _, dup := keysetOpeners[name]; dup {
		panic("redact: duplicate keyset-store opener registration for " + name)
	}
	keysetOpeners[name] = opener
}

// keysetStoreOpenerFor returns the opener for engineName, or an
// error naming the registered set when none matches.
func keysetStoreOpenerFor(engineName string) (KeysetStoreOpener, error) {
	keysetOpenerMu.RLock()
	defer keysetOpenerMu.RUnlock()
	o, ok := keysetOpeners[engineName]
	if !ok {
		names := make([]string, 0, len(keysetOpeners))
		for n := range keysetOpeners {
			names = append(names, n)
		}
		return nil, fmt.Errorf("redact: no keyset-store opener registered for engine %q (registered: %v)", engineName, names)
	}
	return o, nil
}

// engineNameForDSN classifies a db: DSN to an engine opener name.
// Mirrors how the rest of sluice's CLI distinguishes drivers: a
// postgres:// / postgresql:// URI is Postgres; anything else is the
// MySQL go-sql-driver DSN shape.
func engineNameForDSN(dsn string) string {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		return "postgres"
	}
	return "mysql"
}

// LoadKeyset resolves the operator's --keyset-source into the
// immutable startup [Keyset] snapshot (D1). source is the raw
// `<scheme>:<value>` string; an empty source returns (nil, nil) so
// callers can detect "no keyset configured" and apply the D2 loud
// preflight refusal only when a rule actually needs a key.
//
// Every failure mode is loud and actionable (loud-failure tenet):
// unknown scheme, missing file, empty env var, db unreachable,
// table absent, no keys.
func LoadKeyset(ctx context.Context, source string) (*Keyset, error) {
	source = strings.TrimSpace(source)
	if source == "" {
		return nil, nil
	}
	scheme, value, ok := strings.Cut(source, ":")
	if !ok {
		return nil, fmt.Errorf("--keyset-source %q: expected '<scheme>:<value>' (scheme is one of file, env, db)", source)
	}
	switch scheme {
	case "file":
		return loadKeysetFromFile(value)
	case "env":
		return loadKeysetFromEnv(value)
	case "db":
		return loadKeysetFromDB(ctx, value)
	default:
		return nil, fmt.Errorf("--keyset-source %q: unknown scheme %q (supported: file, env, db)", source, scheme)
	}
}

func loadKeysetFromFile(path string) (*Keyset, error) {
	if path == "" {
		return nil, errors.New("--keyset-source file: path is empty (use 'file:/path/to/keyset.yaml')")
	}
	data, err := os.ReadFile(path) //nolint:gosec // operator-supplied keyset path
	if err != nil {
		return nil, fmt.Errorf("--keyset-source file:%s: %w", path, err)
	}
	if strings.TrimSpace(string(data)) == "" {
		return nil, fmt.Errorf("--keyset-source file:%s: file is empty", path)
	}
	var y keysetYAML
	if err := yaml.Unmarshal(data, &y); err != nil {
		return nil, fmt.Errorf("--keyset-source file:%s: invalid keyset YAML: %w", path, err)
	}
	ks, err := keysetFromYAML(&y, "file:"+path)
	if err != nil {
		return nil, fmt.Errorf("--keyset-source file:%s: %w", path, err)
	}
	return ks, nil
}

func loadKeysetFromEnv(varName string) (*Keyset, error) {
	if varName == "" {
		return nil, errors.New("--keyset-source env: variable name is empty (use 'env:VARNAME')")
	}
	raw := os.Getenv(varName)
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("--keyset-source env:%s: environment variable is empty or unset", varName)
	}
	var y keysetYAML
	if err := yaml.Unmarshal([]byte(raw), &y); err != nil {
		return nil, fmt.Errorf("--keyset-source env:%s: invalid keyset YAML: %w", varName, err)
	}
	ks, err := keysetFromYAML(&y, "env:"+varName)
	if err != nil {
		return nil, fmt.Errorf("--keyset-source env:%s: %w", varName, err)
	}
	return ks, nil
}

func loadKeysetFromDB(ctx context.Context, dsn string) (*Keyset, error) {
	if dsn == "" {
		return nil, errors.New("--keyset-source db: DSN is empty (use 'db:<dsn>')")
	}
	opener, err := keysetStoreOpenerFor(engineNameForDSN(dsn))
	if err != nil {
		return nil, fmt.Errorf("--keyset-source db: %w", err)
	}
	store, err := opener(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("--keyset-source db: open store: %w", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.EnsureKeysetTable(ctx); err != nil {
		return nil, fmt.Errorf("--keyset-source db: ensure sluice_keysets table: %w", err)
	}
	ks, err := store.LoadKeyset(ctx)
	if err != nil {
		return nil, fmt.Errorf("--keyset-source db: load keyset: %w", err)
	}
	// Preserve the scheme for the audit line but never echo the raw
	// DSN (it may carry credentials) — redact everything after the
	// host so the audit line stays safe to ship to operators' logs.
	ks.Source = "db:" + redactDSNForAudit(dsn)
	return ks, nil
}

// redactDSNForAudit returns a credential-safe locator for the audit
// line: scheme + host, with userinfo and query string stripped.
// Best-effort — an unparseable DSN collapses to "<dsn>".
func redactDSNForAudit(dsn string) string {
	// URI form: strip the scheme prefix, the userinfo before '@',
	// and any query string, leaving just host[:port]/db.
	if i := strings.Index(dsn, "://"); i >= 0 {
		rest := dsn[i+3:]
		if at := strings.LastIndex(rest, "@"); at >= 0 {
			rest = rest[at+1:]
		}
		if q := strings.IndexAny(rest, "?"); q >= 0 {
			rest = rest[:q]
		}
		return rest
	}
	// go-sql-driver DSN form: take everything after the last '@'
	// (drops user:pw), then drop any query string.
	if at := strings.LastIndex(dsn, "@"); at >= 0 {
		rest := dsn[at+1:]
		if q := strings.IndexAny(rest, "?"); q >= 0 {
			rest = rest[:q]
		}
		return rest
	}
	return "<dsn>"
}

// KeysetRow is one persisted sluice_keysets row, exposed so engine
// store implementations build the resolved [Keyset] via a shared
// helper ([KeysetFromRows]) rather than each re-deriving the
// name-resolution + validation logic.
type KeysetRow struct {
	Name       string
	Generation int
	Bytes      []byte
	Active     bool
}

// KeysetFromRows assembles a resolved [Keyset] from the raw
// sluice_keysets rows. Shared by the PG and MySQL store
// implementations so the db: path produces byte-identical resolution
// to the file:/env: path. Refuses loudly on an empty row set or a
// name with no active=true row.
//
// defaultName is the operator's chosen unnamed-rule fallback. It is
// out of band of the rows (the table has no "default" column per
// ADR-0041's schema); engine stores pass "" — an operator using the
// db: scheme with multiple keys must reference them by explicit
// `key:` name. (Single-key db: keysets still resolve an unnamed
// rule; the sole-entry rule in [Keyset.ResolveKey] applies.)
func KeysetFromRows(rows []KeysetRow, source, defaultName string) (*Keyset, error) {
	if len(rows) == 0 {
		return nil, errors.New("sluice_keysets is empty; populate it with at least one key row (the rotate/list CLI is out of v1 scope — insert rows via SQL per ADR-0041)")
	}
	type acc struct {
		gens   map[int]KeysetGeneration
		active int
		hasAct bool
	}
	byName := map[string]*acc{}
	for _, r := range rows {
		if r.Name == "" {
			return nil, errors.New("sluice_keysets has a row with an empty name")
		}
		if len(r.Bytes) == 0 {
			return nil, fmt.Errorf("sluice_keysets row name=%q generation=%d has empty bytes", r.Name, r.Generation)
		}
		a := byName[r.Name]
		if a == nil {
			a = &acc{gens: map[int]KeysetGeneration{}}
			byName[r.Name] = a
		}
		if _, dup := a.gens[r.Generation]; dup {
			return nil, fmt.Errorf("sluice_keysets has duplicate row name=%q generation=%d", r.Name, r.Generation)
		}
		a.gens[r.Generation] = KeysetGeneration{Generation: r.Generation, Bytes: r.Bytes}
		if r.Active {
			if a.hasAct {
				return nil, fmt.Errorf("sluice_keysets key %q has more than one active=true row (exactly one generation per name may be active)", r.Name)
			}
			a.active = r.Generation
			a.hasAct = true
		}
	}
	ks := &Keyset{
		Default: defaultName,
		Keys:    make(map[string]KeysetKey, len(byName)),
		Source:  source,
	}
	for name, a := range byName {
		if !a.hasAct {
			return nil, fmt.Errorf("sluice_keysets key %q has no active=true row; exactly one generation per name must be marked active", name)
		}
		ks.Keys[name] = KeysetKey{Name: name, Active: a.active, Generations: a.gens}
	}
	if ks.Default != "" {
		if _, ok := ks.Keys[ks.Default]; !ok {
			return nil, fmt.Errorf("redact: keyset 'default' names %q but no such key is in sluice_keysets", ks.Default)
		}
	}
	return ks, nil
}
