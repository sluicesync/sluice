//go:build jsonbench && amd64

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package jsonbench

import (
	"github.com/bytedance/sonic"
)

// sonic is amd64-only (it ships a JIT assembler). It is registered
// behind `jsonbench && amd64` so a `go test -tags=jsonbench` build on
// arm64 / other architectures still compiles — the registry simply
// won't include this row there. This mirrors the build-constraint
// guard the task brief calls for so sonic's arch-specificity doesn't
// complicate the rest of the harness.
//
// Configured with EscapeHTML=true and SortMapKeys=true so the on-disk
// bytes are apples-to-apples comparable with stdlib `encoding/json`
// (which HTML-escapes and emits map keys in sorted order). The sonic
// docs warn both options "hurt performance A LOT" — that cost is part
// of the honest comparison: sluice's format requires HTML escaping
// (the production path uses stdlib defaults), so a sonic row without
// it would be measuring a different, format-incompatible thing.
func init() {
	cfg := sonic.Config{
		EscapeHTML:  true,
		SortMapKeys: true,
		UseInt64:    true, // decode JSON numbers into int64, not float64
	}.Froze()
	registerLib(Lib{
		Name:        "sonic",
		Surface:     "github.com/bytedance/sonic (amd64 JIT, EscapeHTML+SortMapKeys)",
		HTMLEscapes: true,
		Marshal:     cfg.Marshal,
		Unmarshal:   cfg.Unmarshal,
	})
}
