//go:build jsonbench

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package jsonbench

import (
	stdjson "encoding/json"

	expjson "github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"
	expjsonv1 "github.com/go-json-experiment/json/v1"
	goccyjson "github.com/goccy/go-json"
)

// Lib is the per-library adapter the benchmark + fidelity gate consume
// uniformly. Each entry wraps a Marshal / Unmarshal pair operating on
// `any` (the shape the sluice chunk path actually uses:
// `json.Marshal(map[string]any)` on the encode side, and
// `json.Unmarshal(line, &map[string]json.RawMessage)` on the decode
// side — modelled here as Unmarshal into `*any` for an apples-to-apples
// whole-record decode).
//
// The benchmark consumes these uniformly so it can swap libraries
// without per-call type switches; adding a candidate is one entry in
// allLibs.
type Lib struct {
	// Name is the human-readable identifier the markdown emitter uses.
	Name string

	// Surface is a short note rendered in the report: which API of the
	// library this row exercises (v1-compat vs v2 semantic), so a reader
	// understands why two rows for the same module differ.
	Surface string

	// HTMLEscapes records whether this adapter, as configured, escapes
	// `<`, `>`, `&` to `<` etc. on the encode side. sluice's
	// production path uses stdlib `encoding/json` which DOES HTML-escape;
	// the fidelity write-up calls out any candidate that diverges from
	// that observable byte behaviour (it is not a round-trip-correctness
	// problem — both forms decode identically — but it IS an
	// on-disk-bytes / SHA-256 change the operator-facing format would
	// notice, so it is reported explicitly).
	HTMLEscapes bool

	// Marshal serialises v to JSON bytes. Must match stdlib semantics
	// for the sluice envelope (no newline suffix, deterministic enough
	// for the round-trip check).
	Marshal func(v any) ([]byte, error)

	// Unmarshal decodes b into the value pointed to by ptr.
	Unmarshal func(b []byte, ptr any) error
}

// allLibs is the registry of JSON libraries the harness benchmarks and
// fidelity-gates. Ordered by maturity / closeness to what ships today:
// stdlib first (the baseline), then the encoding/json/v2 prototype
// (both its v1-compat surface and its v2 semantic surface), then the
// third-party drop-ins.
//
//   - stdlib_v1            — what `internal/pipeline/backup_chunk.go`
//     uses today. The baseline every other row is judged against.
//   - exp_v1compat         — go-json-experiment via its v1 subpackage
//     (`json/v1`): DefaultOptionsV1, the migration-compatible surface.
//     HTML-escapes like stdlib.
//   - exp_v2               — go-json-experiment top-level API: the
//     encoding/json/v2 semantic. Does NOT HTML-escape by default; here
//     it is explicitly configured with jsontext.EscapeForHTML(true) so
//     the on-disk bytes match stdlib (otherwise the SHA-256 of every
//     chunk would change — a format-observable difference even though
//     round-trip correctness is unaffected).
//   - exp_v2_noescape      — the same v2 API at its native default
//     (no HTML escaping). Included so the report can quantify the cost
//     of the escaping option and show the format-divergence explicitly.
//   - goccy                — github.com/goccy/go-json, drop-in v1-API.
//   - sonic                — github.com/bytedance/sonic, amd64-only
//     (registered in libs_sonic_amd64.go behind a build constraint).
var allLibs = []Lib{
	{
		Name:        "stdlib_v1",
		Surface:     "encoding/json v1 (ships today)",
		HTMLEscapes: true,
		Marshal:     stdjson.Marshal,
		Unmarshal:   stdjson.Unmarshal,
	},
	{
		Name:        "exp_v1compat",
		Surface:     "go-json-experiment json/v1 (DefaultOptionsV1)",
		HTMLEscapes: true,
		Marshal:     expjsonv1.Marshal,
		Unmarshal:   expjsonv1.Unmarshal,
	},
	{
		Name:        "exp_v2",
		Surface:     "go-json-experiment json/v2 (EscapeForHTML=true)",
		HTMLEscapes: true,
		Marshal: func(v any) ([]byte, error) {
			return expjson.Marshal(v, jsontext.EscapeForHTML(true))
		},
		Unmarshal: func(b []byte, ptr any) error {
			return expjson.Unmarshal(b, ptr)
		},
	},
	{
		// json/v2 at its NATIVE default (no jsontext.EscapeForHTML
		// option). Empirically — see TestHTMLEscapeBehaviour — v2 still
		// escapes the HTML-significant `<`, `>`, `&` to \uXXXX; the v2
		// behaviour change only dropped the JS-specific U+2028/U+2029
		// escapes, which sluice's corpora don't exercise. So this row's
		// on-disk chunk bytes are byte-identical to stdlib for the
		// sluice envelope — kept distinct only to make that empirical
		// finding explicit and to measure the (negligible) option cost.
		Name:        "exp_v2_noescape",
		Surface:     "go-json-experiment json/v2 (native default; still escapes <>&)",
		HTMLEscapes: true,
		Marshal: func(v any) ([]byte, error) {
			return expjson.Marshal(v)
		},
		Unmarshal: func(b []byte, ptr any) error {
			return expjson.Unmarshal(b, ptr)
		},
	},
	{
		Name:        "goccy",
		Surface:     "github.com/goccy/go-json (v1-API drop-in)",
		HTMLEscapes: true,
		Marshal:     goccyjson.Marshal,
		Unmarshal:   goccyjson.Unmarshal,
	},
}

// registerLib appends a candidate to allLibs. Used by the
// arch-constrained sonic adapter so the amd64-only dependency stays
// behind its own build file and doesn't break arm64 / other arch
// `go test -tags=jsonbench` builds.
func registerLib(l Lib) {
	allLibs = append(allLibs, l)
}
