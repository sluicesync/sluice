// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"time"
)

// PII Phase 4 — operator-keyset persistence (ADR-0041).
//
// A Keyset is a durable, versioned, operator-controlled set of named
// keys that the HMAC-using strategies (`hash:hmac-sha256` and
// `tokenize:dict`) reference by name. It replaces the Phase 1
// `--redact-key-source` flag (deleted in Phase 4 — clean break, no
// back-compat shim; sluice is pre-users) and the hardcoded
// `tokenize:dict` constant.
//
// # Startup-snapshot only (ADR-0041 decision D1)
//
// The keyset is resolved ONCE at process startup and is immutable
// for the run. There is NO file-watch and NO db-poll hot-reload.
// Rationale: a mid-run active-key change would give some rows gen-N
// and others gen-N+1 surrogates within the same run, breaking
// within-run referential integrity for hash/tokenize. Startup-
// snapshot keeps each run internally consistent and removes the
// fsnotify dependency / poll goroutine. Live-watch is deferred to a
// future Phase 4.5.
//
// # Determinism contract (ADR-0041 §"Determinism contract")
//
//   - A rule with an explicit `key: <name>` pins to that named
//     entry's bytes regardless of `active`. Rotation never drifts
//     its surrogates.
//   - A rule with no `key:` resolves to the `default` entry (if
//     declared) or the sole entry (if exactly one). With multiple
//     entries and no `default`, omitting `key:` is refused loudly
//     (ADR-0041 open-question #2).
//
// The `active` generation governs which generation a NAMED key
// resolves to when the operator references a name (the active
// generation of that name). See [Keyset.ResolveKey].

// KeysetKey is one named key with its full generation history. Each
// generation carries the raw secret bytes and a created-at stamp.
type KeysetKey struct {
	// Name is the operator-chosen key identifier referenced by a
	// rule's `key:` option (e.g. "customer_pii_v2"). Required;
	// non-empty.
	Name string

	// Active is the generation number used for NEW surrogates when
	// this key is referenced by name. Existing surrogates produced
	// by an older generation still map to the same output as long as
	// that generation's bytes remain in Generations.
	Active int

	// Generations is the full generation history, keyed by
	// generation number. Must contain an entry for Active.
	Generations map[int]KeysetGeneration
}

// KeysetGeneration is one generation's secret material plus
// provenance.
type KeysetGeneration struct {
	// Generation is the generation number (monotonic; higher is
	// newer).
	Generation int

	// CreatedAt is when this generation's key was minted. Audit
	// provenance only; not load-bearing for surrogate determinism.
	CreatedAt time.Time

	// Bytes is the raw HMAC secret material. Operators are
	// responsible for storage-layer encryption (ADR-0041
	// open-question #1).
	Bytes []byte
}

// Keyset is the resolved, immutable startup snapshot of every named
// key. Constructed by the loaders in keyset_source.go; consumed by
// strategy construction (Hash / TokenizeDict) at startup.
type Keyset struct {
	// Default names the key entry an unnamed rule resolves to when
	// the keyset has more than one entry. Empty means "no default
	// declared" — an unnamed rule with multiple entries is refused.
	Default string

	// Keys is the resolved set of named keys, keyed by name.
	Keys map[string]KeysetKey

	// Source is the resolved scheme:value the keyset was loaded
	// from, used only for the startup audit-log line (secrets are
	// never logged — only the scheme + a redacted locator).
	Source string
}

// keysetYAML is the on-disk / in-env YAML shape (ADR-0041
// §"Keyset shape"). One named key per top-level list entry, each
// with its generation history. Bytes are base64-encoded.
//
//	keyset:
//	  default: customer_pii          # optional
//	  keys:
//	    - name: customer_pii
//	      active: 3
//	      generations:
//	        - generation: 3
//	          created_at: 2026-05-15T00:00:00Z
//	          bytes: "<base64 32-byte secret>"
//	        - generation: 2
//	          created_at: 2026-03-01T00:00:00Z
//	          bytes: "<base64 32-byte secret>"
type keysetYAML struct {
	Keyset struct {
		Default string `yaml:"default"`
		Keys    []struct {
			Name        string `yaml:"name"`
			Active      int    `yaml:"active"`
			Generations []struct {
				Generation int    `yaml:"generation"`
				CreatedAt  string `yaml:"created_at"`
				Bytes      string `yaml:"bytes"`
			} `yaml:"generations"`
		} `yaml:"keys"`
	} `yaml:"keyset"`
}

// keysetFromYAML validates and converts the wire shape into a
// resolved [Keyset]. Every failure mode is loud and actionable —
// an operator misconfiguring a keyset must learn at startup, before
// any data moves, exactly which entry is wrong.
func keysetFromYAML(y *keysetYAML, source string) (*Keyset, error) {
	if len(y.Keyset.Keys) == 0 {
		return nil, errors.New("redact: keyset has no keys (the 'keyset.keys' list is empty); declare at least one named key with a generation")
	}
	ks := &Keyset{
		Default: y.Keyset.Default,
		Keys:    make(map[string]KeysetKey, len(y.Keyset.Keys)),
		Source:  source,
	}
	for i, k := range y.Keyset.Keys {
		if k.Name == "" {
			return nil, fmt.Errorf("redact: keyset.keys[%d] has an empty 'name'; every key must be named", i)
		}
		if _, dup := ks.Keys[k.Name]; dup {
			return nil, fmt.Errorf("redact: keyset key %q is declared more than once", k.Name)
		}
		if len(k.Generations) == 0 {
			return nil, fmt.Errorf("redact: keyset key %q has no generations; declare at least one", k.Name)
		}
		gens := make(map[int]KeysetGeneration, len(k.Generations))
		for j, g := range k.Generations {
			if _, dup := gens[g.Generation]; dup {
				return nil, fmt.Errorf("redact: keyset key %q generation %d is declared more than once", k.Name, g.Generation)
			}
			raw, err := base64.StdEncoding.DecodeString(g.Bytes)
			if err != nil {
				return nil, fmt.Errorf("redact: keyset key %q generation %d: bytes is not valid base64: %w", k.Name, g.Generation, err)
			}
			if len(raw) == 0 {
				return nil, fmt.Errorf("redact: keyset key %q generation %d: decoded secret is empty", k.Name, g.Generation)
			}
			var created time.Time
			if g.CreatedAt != "" {
				created, err = time.Parse(time.RFC3339, g.CreatedAt)
				if err != nil {
					return nil, fmt.Errorf("redact: keyset key %q generation %d: created_at %q is not RFC3339: %w", k.Name, g.Generation, g.CreatedAt, err)
				}
			}
			gens[g.Generation] = KeysetGeneration{
				Generation: g.Generation,
				CreatedAt:  created,
				Bytes:      raw,
			}
			_ = j
		}
		if _, ok := gens[k.Active]; !ok {
			return nil, fmt.Errorf("redact: keyset key %q declares active generation %d but no such generation exists", k.Name, k.Active)
		}
		ks.Keys[k.Name] = KeysetKey{
			Name:        k.Name,
			Active:      k.Active,
			Generations: gens,
		}
	}
	if ks.Default != "" {
		if _, ok := ks.Keys[ks.Default]; !ok {
			return nil, fmt.Errorf("redact: keyset 'default' names %q but no such key is declared", ks.Default)
		}
	}
	return ks, nil
}

// ResolveKey returns the HMAC secret bytes a strategy should use
// given its optional `key:` name.
//
//   - name != "": resolves the named key's ACTIVE generation bytes.
//     Pinned — rotation of `active` for that name takes effect only
//     on the next process restart (startup-snapshot, D1); within a
//     run the active generation is fixed.
//   - name == "" with exactly one key: that key's active bytes.
//   - name == "" with multiple keys + a declared default: the
//     default key's active bytes.
//   - name == "" with multiple keys + no default: refused loudly
//     (ADR-0041 open-question #2).
//
// Also returns the resolved (keyName, generation) for the audit
// line. Never logs the bytes.
func (k *Keyset) ResolveKey(name string) (secret []byte, keyName string, generation int, err error) {
	if k == nil || len(k.Keys) == 0 {
		return nil, "", 0, errors.New("redact: no keyset is loaded; supply --keyset-source")
	}
	if name == "" {
		switch {
		case len(k.Keys) == 1:
			for n := range k.Keys {
				name = n
			}
		case k.Default != "":
			name = k.Default
		default:
			avail := k.keyNames()
			return nil, "", 0, fmt.Errorf("redact: rule has no 'key:' but the keyset has %d keys and no 'default' is declared; name a key explicitly (available: %v) or declare a 'default'", len(k.Keys), avail)
		}
	}
	entry, ok := k.Keys[name]
	if !ok {
		return nil, "", 0, fmt.Errorf("redact: rule references key %q which is not in the keyset (available: %v)", name, k.keyNames())
	}
	gen, ok := entry.Generations[entry.Active]
	if !ok {
		// keysetFromYAML guarantees this; defensive.
		return nil, "", 0, fmt.Errorf("redact: keyset key %q has no bytes for its active generation %d", name, entry.Active)
	}
	return gen.Bytes, name, entry.Active, nil
}

// keyNames returns the sorted list of declared key names for
// operator-facing error messages.
func (k *Keyset) keyNames() []string {
	out := make([]string, 0, len(k.Keys))
	for n := range k.Keys {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// AuditSummary returns the per-key generation list + active
// generation for the single startup INFO line (ADR-0041
// §"Audit log entry"). Deterministic ordering. Never includes
// secret bytes.
func (k *Keyset) AuditSummary() []KeysetAuditEntry {
	if k == nil {
		return nil
	}
	out := make([]KeysetAuditEntry, 0, len(k.Keys))
	for _, name := range k.keyNames() {
		entry := k.Keys[name]
		gens := make([]int, 0, len(entry.Generations))
		for g := range entry.Generations {
			gens = append(gens, g)
		}
		sort.Ints(gens)
		out = append(out, KeysetAuditEntry{
			Name:        name,
			Active:      entry.Active,
			Generations: gens,
		})
	}
	return out
}

// KeysetAuditEntry is one key's audit-log summary (name + generation
// list + active generation). No secret material.
type KeysetAuditEntry struct {
	Name        string
	Active      int
	Generations []int
}

// StrategyNeedsKeyButMissing reports whether s is a keyset-using
// strategy (`hash:hmac-sha256` or `tokenize:dict:*`) whose HMAC key
// is empty. Used by the pipeline preflight as defense-in-depth for
// the ADR-0041 decision-D2 loud refusal: although the CLI/YAML
// parsers already refuse a keyless rule at construction (the keyset
// is required up front), the preflight re-asserts it so a
// programmatically-built Registry can't slip a keyless rule into a
// data path.
func StrategyNeedsKeyButMissing(s Strategy) bool {
	switch v := s.(type) {
	case Hash:
		return v.Algo == "hmac-sha256" && len(v.Key) == 0
	case TokenizeDict:
		return len(v.Key) == 0
	default:
		return false
	}
}
