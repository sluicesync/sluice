// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/orware/sluice/internal/ir"
)

// positionMode identifies which of MySQL's two equivalent "where am I in
// the binlog?" representations a [binlogPos] is using. The mode is fixed
// for the lifetime of a [CDCReader] stream; switching between modes
// mid-stream would require coordinating the two representations and
// isn't worth the complexity for the gain.
type positionMode string

const (
	// positionModeGTID represents the position as a GTID executed set.
	// Preferred when the source has gtid_mode = ON because it survives
	// failover cleanly: a replica promoted to primary keeps the same
	// GTID identifiers, while binlog filenames reset.
	positionModeGTID positionMode = "gtid"

	// positionModeFilePos represents the position as a (binlog file,
	// byte offset) pair — the classic MySQL replication position type.
	// Used when GTIDs aren't enabled on the source.
	positionModeFilePos positionMode = "file_pos"
)

// binlogPos is the engine-side representation of a CDC position. It is
// JSON-serialised into [ir.Position.Token] when surfaced to the IR; the
// IR contract treats Token as opaque.
//
// Two valid shapes:
//
//	{"mode":"gtid","gtid_set":"uuid:1-100,uuid2:1-50"}
//	{"mode":"file_pos","file":"mysql-bin.000123","pos":4567}
//
// Mode is always present; the other fields are populated according to
// the mode. Decoding rejects unknown or missing modes so silent
// misinterpretation isn't possible.
type binlogPos struct {
	// Mode is the discriminator. Required.
	Mode positionMode `json:"mode"`

	// GTIDSet is populated when Mode == positionModeGTID. The string
	// form is the same one the source accepts in @@gtid_executed —
	// e.g. "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-1000".
	GTIDSet string `json:"gtid_set,omitempty"`

	// File and Pos are populated when Mode == positionModeFilePos.
	File string `json:"file,omitempty"`
	Pos  uint32 `json:"pos,omitempty"`

	// ServerUUID binds a file/pos position to the source server
	// instance it was captured on (@@server_uuid). It is the
	// loud-failure floor for the PlanetScale "node replaced /
	// restored from backup" position-loss class (Track 1c): binlog
	// FILE NAMES are instance-local and a fresh instance frequently
	// reuses the same names (mysql-bin.000001, .000003, …) for an
	// entirely unrelated binlog lineage. A name-only resumability
	// check (verifyBinlogFilePresent) false-positives on that and
	// silently starts the syncer at a byte offset in an unrelated
	// file — a silent gap. Stamping the source's server_uuid here and
	// rejecting a resume whose persisted uuid differs from the
	// source's current one turns that silent gap into a loud
	// ir.ErrPositionInvalid → ADR-0022 cold-start re-snapshot.
	//
	// GTID mode does not need this: GTID UUIDs are themselves
	// instance-bound, so verifyGTIDSetReachable already catches a
	// fresh instance (its gtid_purged/gtid_executed carry a different
	// source UUID). Empty on positions persisted before this field
	// existed (zero-users project, but a position could straddle an
	// in-flight upgrade); the verify path treats an empty persisted
	// uuid as "skip the identity check" so no false refusal — the
	// filename check still applies, preserving the pre-existing
	// behaviour for that one transitional case.
	ServerUUID string `json:"server_uuid,omitempty"`
}

// engineNameMySQL is the [ir.Position.Engine] tag the binlog reader
// writes for fresh positions. The VStream reader uses
// [engineNameVStream] ("planetscale") instead. Both decoders accept
// either name via [isMySQLFamilyEngine], because the per-target
// [ChangeApplier].ReadPosition path stamps recovered positions
// with the applier's own engine name (always "mysql" for the
// MySQL applier) regardless of which reader produced the
// original — so a VStream-shape token written by a planetscale-
// flavor stream comes back tagged "mysql" on resume, and a binlog-
// shape token similarly. Cross-package guard is still effective:
// postgres positions get rejected because "postgres" isn't in this
// family.
const engineNameMySQL = "mysql"

// isMySQLFamilyEngine returns true for the engine-name strings the
// MySQL package's two CDC paths (binlog and VStream) accept on
// decode. See [engineNameMySQL] for the rationale.
func isMySQLFamilyEngine(name string) bool {
	return name == engineNameMySQL || name == engineNameVStream
}

// encodeBinlogPos marshals p into an [ir.Position] suitable for
// emission with an [ir.Change]. The Engine field is fixed to "mysql"
// so cross-engine confusion (e.g., feeding a Postgres LSN back to the
// MySQL reader) is caught on the next decode.
func encodeBinlogPos(p binlogPos) (ir.Position, error) {
	if p.Mode != positionModeGTID && p.Mode != positionModeFilePos {
		return ir.Position{}, fmt.Errorf("mysql: encode binlog position: invalid mode %q", p.Mode)
	}
	b, err := json.Marshal(p)
	if err != nil {
		return ir.Position{}, fmt.Errorf("mysql: encode binlog position: %w", err)
	}
	return ir.Position{Engine: engineNameMySQL, Token: string(b)}, nil
}

// decodeBinlogPos parses an [ir.Position] back into a [binlogPos]. The
// zero value of [ir.Position] (empty Engine and Token) is the "from
// now" sentinel and is reported via the second return value; callers
// distinguish that case from a bad token by checking ok before err.
//
// Engine acceptance is broader than just "mysql": both binlog and
// VStream positions originate inside this package, and the
// [ChangeApplier] ReadPosition path stamps every recovered position
// with the applier's engine name (currently "mysql") regardless of
// which reader produced the original. So a planetscale-flavor
// reader resuming on a binlog-mode position would see Engine="mysql"
// — which is correct. We accept "planetscale" as an alias to keep
// the same applier round-tripping VStream positions cleanly. The
// real cross-engine guard is still in place: postgres positions
// (Engine="postgres") are rejected.
func decodeBinlogPos(p ir.Position) (decoded binlogPos, ok bool, err error) {
	if p.Engine == "" && p.Token == "" {
		// "From now" sentinel — caller should query the source for
		// its current position instead of decoding.
		return binlogPos{}, false, nil
	}
	if !isMySQLFamilyEngine(p.Engine) {
		return binlogPos{}, false, fmt.Errorf(
			"mysql: decode binlog position: engine = %q; want %q or %q",
			p.Engine, engineNameMySQL, engineNameVStream,
		)
	}
	if p.Token == "" {
		return binlogPos{}, false, errors.New("mysql: decode binlog position: token is empty")
	}
	if err := json.Unmarshal([]byte(p.Token), &decoded); err != nil {
		return binlogPos{}, false, fmt.Errorf("mysql: decode binlog position: %w", err)
	}
	switch decoded.Mode {
	case positionModeGTID:
		if decoded.GTIDSet == "" {
			return binlogPos{}, false, errors.New("mysql: decode binlog position: gtid mode requires gtid_set")
		}
	case positionModeFilePos:
		if decoded.File == "" {
			return binlogPos{}, false, errors.New("mysql: decode binlog position: file_pos mode requires file")
		}
	default:
		return binlogPos{}, false, fmt.Errorf("mysql: decode binlog position: unknown mode %q", decoded.Mode)
	}
	return decoded, true, nil
}
