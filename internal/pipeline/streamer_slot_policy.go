// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// sluiceSlotPrefix is prepended to operator-supplied slot names
// that don't already start with it. Convention: every sluice-created
// replication slot starts with `sluice_` so operators can find them
// all with `pg_replication_slots WHERE slot_name LIKE 'sluice\_%'`
// for cleanup, audits, and disambiguation from other tools' slots
// (Debezium, native logical replication subscribers, etc.). The
// default slot name is `sluice_slot` (already prefixed); custom
// names like `--slot-name shard_a` become `sluice_shard_a`.
const sluiceSlotPrefix = "sluice_"

// ResolveSlotName is the exported counterpart of [resolveSlotName].
// CLI commands outside the pipeline package (today: `sluice backup
// full --slot-name`) call through to apply the sluice-prefix
// convention without re-implementing it.
func ResolveSlotName(operatorSupplied string) string {
	return resolveSlotName(operatorSupplied)
}

// resolveSlotName applies the sluice-prefix convention to an
// operator-supplied slot name. Empty input passes through unchanged
// — the empty signal means "use the engine's default" (which is
// already `sluice_slot`). Names already starting with `sluice_`
// pass through verbatim. Anything else gets the prefix prepended.
//
// Examples:
//
//	""                → ""              (engine default)
//	"shard_a"         → "sluice_shard_a"
//	"sluice_shard_a"  → "sluice_shard_a" (idempotent)
//	"sluice_slot"     → "sluice_slot"
//
// Centralised here so the prefix policy applies uniformly to both
// the CDC-reader and snapshot-stream open paths, and any future
// CLI / YAML / env entry points.
func resolveSlotName(operatorSupplied string) string {
	if operatorSupplied == "" {
		return ""
	}
	if strings.HasPrefix(operatorSupplied, sluiceSlotPrefix) {
		return operatorSupplied
	}
	return sluiceSlotPrefix + operatorSupplied
}

// resolvePublicationName applies the SAME sluice-prefix convention to
// an operator-supplied publication name (ADR-0175), so both of a
// stream's per-instance names read alike and every sluice-managed
// publication stays findable with
// `pg_publication WHERE pubname LIKE 'sluice\_%'` — the mirror of the
// slot convention's `pg_replication_slots` lookup.
//
//	""                → ""              (engine default: sluice_pub)
//	"wave1"           → "sluice_wave1"
//	"sluice_wave1"    → "sluice_wave1"  (idempotent)
//
// Deliberately shares sluiceSlotPrefix rather than minting a second
// constant: the convention is one prefix for "objects sluice owns on
// the source", not one per object type.
func resolvePublicationName(operatorSupplied string) string {
	return resolveSlotName(operatorSupplied)
}

// validatePublicationName refuses a resolved --publication-name that is
// not a SAFE Postgres replication identifier: lowercase [a-z0-9_] only,
// at most [pgMaxIdentifierBytes] (63) bytes — the mirror of PG's own
// server-side slot-name charset enforcement, applied at resolve time
// (audit 2026-07-23 D0-9).
//
// Why refusal, not quoting: sluice's CREATE PUBLICATION quotes the
// identifier, PRESERVING case — but START_REPLICATION's
// publication_names argument is DOWNCASED by the server, so a
// mixed-case name creates one publication ("sluice_MyPub") and streams
// from another ("sluice_mypub"): the stream runs green through the
// entire bulk copy, then 42704s at the first change — or stays green
// forever on an idle source, silently replicating nothing. Over-length
// names have the sibling hazard the derived-name path already guards
// against: CREATE silently TRUNCATES past NAMEDATALEN-1 (a NOTICE, not
// an error) while publication_names matches verbatim. Neither failure
// names the flag, so both are refused up front instead. Empty passes —
// it means "engine default", which is already safe.
func validatePublicationName(resolved string) error {
	if resolved == "" {
		return nil
	}
	bad := len(resolved) > pgMaxIdentifierBytes
	if !bad {
		for _, r := range resolved {
			if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' {
				bad = true
				break
			}
		}
	}
	if !bad {
		return nil
	}
	return sluicecode.Wrap(
		sluicecode.CodeCDCPublicationNameInvalid,
		"use only lowercase letters, digits, and underscores ([a-z0-9_]), at most 63 bytes",
		fmt.Errorf(
			"pipeline: --publication-name %q is not a safe Postgres replication identifier (allowed: [a-z0-9_], "+
				"max 63 bytes). CREATE PUBLICATION quotes the name and preserves its exact spelling, but "+
				"START_REPLICATION's publication_names argument is DOWNCASED by the server (and matched verbatim "+
				"against a name CREATE would silently truncate past 63 bytes) — so an unsafe name creates one "+
				"publication and streams from another: green through the whole bulk copy, then a 'publication does "+
				"not exist' (42704) at the first change, or a silently idle stream on a quiet source. This mirrors "+
				"the charset Postgres itself enforces on replication slot names (audit 2026-07-23 D0-9)",
			resolved,
		),
	)
}
