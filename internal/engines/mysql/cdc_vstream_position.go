// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"vitess.io/vitess/go/vt/proto/binlogdata"

	"sluicesync.dev/sluice/internal/ir"
)

// VStream position encoding lives in its own file because it's
// useful in isolation: the position type is small, fully unit-
// testable, and shipped before the streaming spine in Phase B is
// wired up. Once the actual VStream reader lands, this file's
// shardGtid type maps directly onto vitess.io/vitess/go/vt/proto/
// binlogdata.ShardGtid; keeping the canonical Vitess shape here
// means the conversion later is a one-line struct copy.
//
// Reference: github.com/planetscale/debezium-connector-vitess
// (Vgtid.java), which serialises the same shape as JSON. Matching
// Debezium's shape lets operators move position cursors between
// tools when investigating issues — the JSON surface is portable.

const engineNameVStream = "planetscale"

// shardGtid is the per-shard position primitive in Vitess VStream.
// One Vgtid value carries one shardGtid per shard the operator's
// stream covers; for an unsharded keyspace the slice has exactly
// one entry with Shard="-".
//
// Special Gtid sentinels:
//   - "" (empty) — start at the beginning of the binlog. vtgate
//     will run an internal table COPY before tailing CDC.
//   - "current" — start at the head of the binlog at request time.
//     Skips COPY entirely; the stream emits only post-request
//     events. Useful when the bulk-copy phase ran via a separate
//     mechanism.
//   - any other string — a canonical Vitess GTID set, e.g.
//     "MySQL56/<uuid>:1-N", to resume from.
type shardGtid struct {
	Keyspace string `json:"keyspace"`
	Shard    string `json:"shard"`
	Gtid     string `json:"gtid"`

	// TablePKs is the per-table COPY-resume cursor Vitess carries on
	// each shard's position (binlogdata.ShardGtid.TablePKs). During a
	// cold-start COPY, vtgate emits a VGTID after every LASTPK event
	// whose ShardGtids[i].TablePKs hold the last-copied primary key
	// for each still-copying table; replaying that cursor back into
	// the resume request's VGtid asks vtgate to resume the COPY scan
	// from `WHERE pk > lastpk` rather than restarting from row 0
	// (ADR-0072 Phase A). It is empty once COPY completes for a table
	// (vtgate removes the entry on a Completed LASTPK) and empty for a
	// pure CDC-tailing position.
	//
	// The field is ADDITIVE and JSON-omitempty: a pre-ADR-0072 token
	// has no `table_p_ks` key, so it decodes to a nil slice — "no
	// mid-COPY cursor", i.e. start the COPY from the beginning (the
	// behaviour before this field existed). See encodedTablePK for the
	// per-entry encoding.
	TablePKs []encodedTablePK `json:"table_p_ks,omitempty"`
}

// encodedTablePK is the JSON-serialisable form of one
// binlogdata.TableLastPK (a table name + a query.QueryResult-shaped
// lastpk row). The lastpk payload is a protobuf message whose exact
// bytes vtgate's rowstreamer needs to build the `WHERE pk > lastpk`
// resume clause, so it is round-tripped via deterministic proto
// marshalling base64-encoded into a JSON string rather than re-modelled
// field-by-field — re-modelling would risk dropping a type/charset
// nuance the tablet's PK comparison depends on. The table name is
// carried in the clear so an operator inspecting a position token can
// see which tables still have an in-flight COPY.
type encodedTablePK struct {
	TableName string `json:"table_name"`
	// Lastpk is base64(proto-marshalled binlogdata.TableLastPK). The
	// whole TableLastPK (not just its Lastpk QueryResult) is marshalled
	// so the decode side reconstructs the exact proto vtgate produced.
	Lastpk string `json:"lastpk"`
}

// encodeTablePKs converts the proto per-shard TablePKs into the
// JSON-serialisable form. A nil/empty input yields a nil slice so the
// `table_p_ks` key is omitted entirely (Debezium-adjacent: a position
// with no in-flight COPY looks exactly like a pre-ADR-0072 token).
func encodeTablePKs(pks []*binlogdata.TableLastPK) ([]encodedTablePK, error) {
	if len(pks) == 0 {
		return nil, nil
	}
	out := make([]encodedTablePK, 0, len(pks))
	for _, pk := range pks {
		if pk == nil {
			continue
		}
		b, err := pk.MarshalVT()
		if err != nil {
			return nil, fmt.Errorf("mysql: vstream position: marshal TableLastPK for %q: %w", pk.GetTableName(), err)
		}
		out = append(out, encodedTablePK{
			TableName: pk.GetTableName(),
			Lastpk:    base64.StdEncoding.EncodeToString(b),
		})
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// decodeTablePKs is the inverse of encodeTablePKs: it reconstructs the
// proto TablePKs slice the resume request carries back to vtgate. A
// nil/empty input (the pre-ADR-0072 / no-mid-COPY case) returns nil so
// the resume request omits TablePKs and vtgate starts the COPY from the
// beginning.
func decodeTablePKs(pks []encodedTablePK) ([]*binlogdata.TableLastPK, error) {
	if len(pks) == 0 {
		return nil, nil
	}
	out := make([]*binlogdata.TableLastPK, 0, len(pks))
	for _, pk := range pks {
		raw, err := base64.StdEncoding.DecodeString(pk.Lastpk)
		if err != nil {
			return nil, fmt.Errorf("mysql: vstream position: decode TableLastPK base64 for %q: %w", pk.TableName, err)
		}
		var tpk binlogdata.TableLastPK
		if err := tpk.UnmarshalVT(raw); err != nil {
			return nil, fmt.Errorf("mysql: vstream position: unmarshal TableLastPK for %q: %w", pk.TableName, err)
		}
		out = append(out, &tpk)
	}
	return out, nil
}

// encodeVStreamPos serialises a slice of shardGtid into the
// ir.Position carried through the orchestrator's position layer.
// The serialised form is a JSON array (Debezium-compatible);
// fields appear in canonical-keyspace+shard order so two
// sequential calls with the same logical contents produce
// identical token strings — useful for diffing or log-grepping.
//
// An empty slice is rejected. A position with no shards is not
// resumable and almost certainly an upstream bug; refuse loudly
// rather than return a token that decodes back to "no shards".
func encodeVStreamPos(shards []shardGtid) (ir.Position, error) {
	if len(shards) == 0 {
		return ir.Position{}, errors.New("mysql: vstream position: at least one shardGtid is required")
	}
	for i, s := range shards {
		if s.Keyspace == "" {
			return ir.Position{}, fmt.Errorf("mysql: vstream position: shards[%d]: keyspace is required", i)
		}
		if s.Shard == "" {
			return ir.Position{}, fmt.Errorf("mysql: vstream position: shards[%d] (keyspace=%s): shard is required", i, s.Keyspace)
		}
	}

	out := make([]shardGtid, len(shards))
	copy(out, shards)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Keyspace != out[j].Keyspace {
			return out[i].Keyspace < out[j].Keyspace
		}
		return out[i].Shard < out[j].Shard
	})

	b, err := json.Marshal(out)
	if err != nil {
		return ir.Position{}, fmt.Errorf("mysql: vstream position: marshal: %w", err)
	}
	return ir.Position{Engine: engineNameVStream, Token: string(b)}, nil
}

// decodeVStreamPos is the inverse of encodeVStreamPos. The from-
// now sentinel — an ir.Position with both Engine and Token empty —
// returns ok=false, nil error so callers can branch cleanly between
// "resume" and "fresh start" cases without reinventing the
// distinction at every call site (mirrors decodeBinlogPos's
// shape).
//
// Engine acceptance covers both "mysql" and "planetscale" because
// the [ChangeApplier].ReadPosition path stamps recovered positions
// with the applier's engine name ("mysql") regardless of which
// reader produced the original. A VStream-shape token tagged as
// engine "mysql" therefore needs to round-trip through this
// decoder cleanly. The cross-engine guard still applies — postgres
// positions (Engine="postgres") fail loudly.
func decodeVStreamPos(p ir.Position) (shards []shardGtid, ok bool, err error) {
	if p.Engine == "" && p.Token == "" {
		return nil, false, nil
	}
	if !isMySQLFamilyEngine(p.Engine) {
		return nil, false, fmt.Errorf("mysql: vstream position: wrong engine %q; want %q or %q",
			p.Engine, engineNameMySQL, engineNameVStream)
	}
	if p.Token == "" {
		return nil, false, errors.New("mysql: vstream position: empty token with non-empty engine")
	}
	var decoded []shardGtid
	if err := json.Unmarshal([]byte(p.Token), &decoded); err != nil {
		return nil, false, fmt.Errorf("mysql: vstream position: unmarshal: %w", err)
	}
	if len(decoded) == 0 {
		return nil, false, errors.New("mysql: vstream position: token decoded to empty shard list")
	}
	for i, s := range decoded {
		if s.Keyspace == "" {
			return nil, false, fmt.Errorf("mysql: vstream position: shards[%d]: missing keyspace", i)
		}
		if s.Shard == "" {
			return nil, false, fmt.Errorf("mysql: vstream position: shards[%d] (keyspace=%s): missing shard", i, s.Keyspace)
		}
	}
	return decoded, true, nil
}

// fromNowVStreamPos returns the shardGtid slice that asks vtgate
// to start at the head of the binlog. Operators use this when they
// want CDC-only behaviour with no initial COPY (typical for
// resuming after an out-of-band snapshot). Callers must supply the
// shard layout (keyspace + shard names) — that information isn't
// derivable from the IR position alone.
//
// The function is a small helper so the "current" sentinel string
// has exactly one occurrence in the package, which avoids typos
// migrating between Phase B and Phase C as the call sites grow.
func fromNowVStreamPos(keyspace string, shards []string) []shardGtid {
	out := make([]shardGtid, 0, len(shards))
	for _, s := range shards {
		out = append(out, shardGtid{Keyspace: keyspace, Shard: s, Gtid: "current"})
	}
	return out
}

// fromBeginningVStreamPos asks vtgate to run a full table COPY
// followed by CDC (the snapshot+CDC handoff path is built into
// VStream itself; see the prep doc and the agent survey for the
// COPY_COMPLETED-event shape). Pair with the same shard layout as
// fromNowVStreamPos.
func fromBeginningVStreamPos(keyspace string, shards []string) []shardGtid {
	out := make([]shardGtid, 0, len(shards))
	for _, s := range shards {
		out = append(out, shardGtid{Keyspace: keyspace, Shard: s, Gtid: ""})
	}
	return out
}
