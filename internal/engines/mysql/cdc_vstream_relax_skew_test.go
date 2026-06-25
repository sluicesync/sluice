// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	gomysql "github.com/go-sql-driver/mysql"
)

// TestBuildVStreamRequest_MinimizeSkewRelax pins ADR-0120: the steady-state CDC
// request carries MinimizeSkew=false exactly when the reader's relaxSkew is set,
// and MinimizeSkew=true (today's behaviour) otherwise. The flip is the only
// behavioural change of item 27.
func TestBuildVStreamRequest_MinimizeSkewRelax(t *testing.T) {
	start := []shardGtid{{Keyspace: "main", Shard: "-80", Gtid: "current"}}
	cases := []struct {
		name      string
		relaxSkew bool
		want      bool // expected MinimizeSkew
	}{
		{name: "default keeps MinimizeSkew on", relaxSkew: false, want: true},
		{name: "relax flips MinimizeSkew off", relaxSkew: true, want: false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			r := &vstreamCDCReader{keyspace: "main", shards: []string{"-80"}, relaxSkew: c.relaxSkew}
			req, err := r.buildVStreamRequest(start)
			if err != nil {
				t.Fatalf("buildVStreamRequest: %v", err)
			}
			if got := req.GetFlags().GetMinimizeSkew(); got != c.want {
				t.Errorf("MinimizeSkew = %v; want %v", got, c.want)
			}
			// StopOnReshard is unconditional and must never be disturbed by the
			// relaxation (it closes the only cross-shard same-key window).
			if !req.GetFlags().GetStopOnReshard() {
				t.Error("StopOnReshard must stay true regardless of relaxSkew")
			}
		})
	}
}

// TestVStreamRelaxSkewFromDSN pins the resolution precedence: the CLI override
// (when set) wins; otherwise the source DSN's vstream_relax_skew=true; default
// false. The override is a package global, so reset it around the cases.
func TestVStreamRelaxSkewFromDSN(t *testing.T) {
	defer vstreamRelaxSkewOverride.Store(false) // restore for other tests
	mk := func(param string) *gomysql.Config {
		cfg := gomysql.NewConfig()
		if param != "" {
			cfg.Params = map[string]string{"vstream_relax_skew": param}
		}
		return cfg
	}

	vstreamRelaxSkewOverride.Store(false)
	if vstreamRelaxSkewFromDSN(mk("")) {
		t.Error("no DSN param + no override should be false (MinimizeSkew stays on)")
	}
	if !vstreamRelaxSkewFromDSN(mk("true")) {
		t.Error("vstream_relax_skew=true should resolve true")
	}
	if vstreamRelaxSkewFromDSN(mk("false")) {
		t.Error("vstream_relax_skew=false should resolve false")
	}

	// Override wins over an absent / false DSN param.
	SetVStreamRelaxSkewOverride(true)
	if !vstreamRelaxSkewFromDSN(mk("")) {
		t.Error("override true should win over an absent DSN param")
	}
	if !vstreamRelaxSkewFromDSN(mk("false")) {
		t.Error("override true should win over vstream_relax_skew=false")
	}

	// SetVStreamRelaxSkewOverride(false) is a no-op (write-once-true), so a
	// non-CLI caller can never invert a prior explicit relax — the zero-value
	// safety contract.
	SetVStreamRelaxSkewOverride(false)
	if !vstreamRelaxSkewOverride.Load() {
		t.Error("SetVStreamRelaxSkewOverride(false) must not clear a prior true (write-once-true)")
	}
}
