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
}

// engineNameMySQL is the [ir.Position.Engine] tag this engine writes
// and accepts. Other engines' positions are rejected on decode.
const engineNameMySQL = "mysql"

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
func decodeBinlogPos(p ir.Position) (decoded binlogPos, ok bool, err error) {
	if p.Engine == "" && p.Token == "" {
		// "From now" sentinel — caller should query the source for
		// its current position instead of decoding.
		return binlogPos{}, false, nil
	}
	if p.Engine != engineNameMySQL {
		return binlogPos{}, false, fmt.Errorf(
			"mysql: decode binlog position: engine = %q; want %q",
			p.Engine, engineNameMySQL)
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
