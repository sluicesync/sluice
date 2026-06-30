// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"fmt"
	"sort"
	"strings"
)

// NamespaceRenameMap is the per-namespace source → target rename for a
// multi-namespace fan-out (ADR-0142). The zero value is the identity map:
// every source namespace routes to a same-named target namespace, exactly
// as ADR-0074 / ADR-0075 fan-out did before the rename existed. A non-empty
// map renames the listed source namespaces; any source NOT in the map keeps
// its name (identity within the selection).
//
// The map is pure routing metadata: it changes ONLY the TARGET namespace
// identifier (the PG schema via --target-schema, or the MySQL target
// database via the derived DSN, or the per-change CDC route). Reads,
// Table.Schema / View.Schema stamping, the cross-namespace FK carve-out,
// the deferred-FK pass, --redact rule matching, and the per-source
// migration-/stream-id all stay keyed on the SOURCE name.
//
// Constructed once in the CLI from the resolved --map-database / --map-schema
// pairs (or the YAML namespace_map block) and carried on
// [Migrator.NamespaceMap] / [Streamer.NamespaceMap].
type NamespaceRenameMap struct {
	// m maps source namespace → target namespace. nil/empty is identity.
	m map[string]string
}

// NewNamespaceRenameMap builds a [NamespaceRenameMap] from a list of
// "OLD=NEW" pairs (the --map-database / --map-schema flag form, or the YAML
// namespace_map block rendered as pairs). Each pair is trimmed; an empty
// list yields the identity map.
//
// Refused LOUDLY at construction (loud-failure-first — a bad map can't move
// any data):
//
//   - a malformed entry (no '=' separator, or an empty source / target);
//   - a duplicate source key (the same OLD twice — ambiguous intent);
//   - a many-to-one mapping (two distinct sources → the same target), which
//     would silently merge two source namespaces into one target. sluice
//     never merges namespaces.
//
// A self-map (OLD=OLD) is permitted — it is a redundant identity and harms
// nothing. The orchestrator runs a second many-to-one guard over the
// RESOLVED selection (a mapped source colliding with an unmapped selected
// source) because that collision isn't visible from the pairs alone.
func NewNamespaceRenameMap(pairs []string) (NamespaceRenameMap, error) {
	if len(pairs) == 0 {
		return NamespaceRenameMap{}, nil
	}
	m := make(map[string]string, len(pairs))
	// target → first source that mapped to it, for many-to-one detection.
	targets := make(map[string]string, len(pairs))
	for _, raw := range pairs {
		eq := strings.IndexByte(raw, '=')
		if eq < 0 {
			return NamespaceRenameMap{}, fmt.Errorf(
				"pipeline: --map-database/--map-schema entry %q is malformed; expected OLD=NEW", raw,
			)
		}
		src := strings.TrimSpace(raw[:eq])
		dst := strings.TrimSpace(raw[eq+1:])
		if src == "" || dst == "" {
			return NamespaceRenameMap{}, fmt.Errorf(
				"pipeline: --map-database/--map-schema entry %q has an empty source or target; expected OLD=NEW", raw,
			)
		}
		if strings.ContainsRune(dst, '=') {
			return NamespaceRenameMap{}, fmt.Errorf(
				"pipeline: --map-database/--map-schema entry %q has multiple '=' separators; expected a single OLD=NEW", raw,
			)
		}
		if _, dup := m[src]; dup {
			return NamespaceRenameMap{}, fmt.Errorf(
				"pipeline: --map-database/--map-schema lists the source namespace %q twice", src,
			)
		}
		if prev, dup := targets[dst]; dup {
			return NamespaceRenameMap{}, fmt.Errorf(
				"pipeline: --map-database/--map-schema maps both %q and %q to the same target namespace %q "+
					"(many-to-one is refused; sluice never merges two source namespaces into one target)",
				prev, src, dst,
			)
		}
		m[src] = dst
		targets[dst] = src
	}
	return NamespaceRenameMap{m: m}, nil
}

// IsEmpty reports whether the map is the identity map (no rename rules).
func (r NamespaceRenameMap) IsEmpty() bool {
	return len(r.m) == 0
}

// Apply returns the target namespace for a source namespace: the mapped
// name when src is a key, src itself otherwise (identity default).
func (r NamespaceRenameMap) Apply(src string) string {
	if dst, ok := r.m[src]; ok {
		return dst
	}
	return src
}

// Keys returns the source namespaces the map renames, sorted for
// deterministic logs and error messages. Used by the orchestrator both to
// derive the map-only selection and to cross-check that every key is in the
// resolved selection.
func (r NamespaceRenameMap) Keys() []string {
	keys := make([]string, 0, len(r.m))
	for k := range r.m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// selectNamespaces resolves the selected source-namespace set for a
// multi-namespace fan-out (ADR-0142). Two modes:
//
//   - Selection given (--all-* or --include-*/--exclude-*): the filter
//     decides; the rename map renames WITHIN that selection.
//   - Map-only (a non-empty rename map and NO selection flag): the map keys
//     ARE the selection — `--map-database app=app_prod` migrates exactly
//     `app`. (`--map-*` engages multi-namespace mode the same way
//     `--include-*` does.)
//
// `all` is the engine-enumerated source-namespace set (already system-db
// filtered). The result is sorted for deterministic logs.
func selectNamespaces(all []string, filter DatabaseFilter, allDatabases bool, nsMap NamespaceRenameMap) []string {
	mapOnly := !allDatabases && filter.IsEmpty() && !nsMap.IsEmpty()
	var keySet map[string]struct{}
	if mapOnly {
		keys := nsMap.Keys()
		keySet = make(map[string]struct{}, len(keys))
		for _, k := range keys {
			keySet[k] = struct{}{}
		}
	}
	selected := make([]string, 0, len(all))
	for _, ns := range all {
		if mapOnly {
			if _, ok := keySet[ns]; ok {
				selected = append(selected, ns)
			}
			continue
		}
		if filter.Allows(ns) {
			selected = append(selected, ns)
		}
	}
	sort.Strings(selected)
	return selected
}

// crossCheckMapSelection refuses LOUDLY when a rename-map key is NOT in the
// resolved selection (ADR-0142 typo guard): a `--map-database typo=x` whose
// source namespace was never selected (or doesn't exist on the source) is
// almost certainly a mistake, so sluice names the offending key and refuses
// before any data moves rather than silently renaming nothing. No-op for the
// identity map.
func crossCheckMapSelection(selected []string, nsMap NamespaceRenameMap) error {
	if nsMap.IsEmpty() {
		return nil
	}
	sel := make(map[string]struct{}, len(selected))
	for _, s := range selected {
		sel[s] = struct{}{}
	}
	for _, key := range nsMap.Keys() {
		if _, ok := sel[key]; !ok {
			return fmt.Errorf(
				"pipeline: --map-database/--map-schema key %q is not in the resolved namespace selection "+
					"(it was not selected by --all-* / --include-* / --exclude-*, or it does not exist on the "+
					"source); rename keys must name a selected source namespace",
				key,
			)
		}
	}
	return nil
}

// resolveTargetNamespaces maps each selected SOURCE namespace to its TARGET
// namespace via nsMap (identity for unmapped sources) and refuses LOUDLY
// when two distinct sources resolve to the SAME target — the many-to-one
// merge hazard that NewNamespaceRenameMap can't see on its own (a mapped
// source colliding with an unmapped selected source, e.g. selecting `app`
// and `billing` while mapping `app=billing`). The returned slice is aligned
// with `selected` (targets[i] is the target for selected[i]). Engine-
// agnostic — it catches exact collisions even on a case-sensitive target
// where the [ir.NamespaceFolder] fold preflight is a no-op.
func resolveTargetNamespaces(selected []string, nsMap NamespaceRenameMap) ([]string, error) {
	targets := make([]string, len(selected))
	seen := make(map[string]string, len(selected))
	for i, src := range selected {
		dst := nsMap.Apply(src)
		if prev, dup := seen[dst]; dup {
			return nil, fmt.Errorf(
				"pipeline: source namespaces %q and %q both resolve to the target namespace %q after applying "+
					"--map-database/--map-schema (many-to-one is refused; sluice never merges two source "+
					"namespaces into one target)",
				prev, src, dst,
			)
		}
		seen[dst] = src
		targets[i] = dst
	}
	return targets, nil
}
