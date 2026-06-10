// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"strings"
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
