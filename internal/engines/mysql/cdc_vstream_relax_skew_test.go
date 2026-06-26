// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	gomysql "github.com/go-sql-driver/mysql"
)

// TestBuildVStreamRequest_MinimizeSkewRelax pins ADR-0120: the steady-state CDC
// request carries MinimizeSkew=false exactly when the reader's relaxSkew is set,
// and MinimizeSkew=true (the preserve-skew opt-out) otherwise.
func TestBuildVStreamRequest_MinimizeSkewRelax(t *testing.T) {
	start := []shardGtid{{Keyspace: "main", Shard: "-80", Gtid: "current"}}
	cases := []struct {
		name      string
		relaxSkew bool
		want      bool // expected MinimizeSkew
	}{
		{name: "relaxed (the default) => MinimizeSkew off", relaxSkew: true, want: false},
		{name: "preserve-skew opt-out => MinimizeSkew on", relaxSkew: false, want: true},
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

// TestVStreamPreserveSkewFromDSN pins the resolution precedence for the OPT-OUT
// (ADR-0120, default flipped): the CLI override (when set) wins; otherwise the
// source DSN's vstream_preserve_skew=true; default false = the new relaxed
// default. The override is a package global, so reset it around the cases.
func TestVStreamPreserveSkewFromDSN(t *testing.T) {
	defer vstreamPreserveSkewOverride.Store(false) // restore for other tests
	mk := func(param string) *gomysql.Config {
		cfg := gomysql.NewConfig()
		if param != "" {
			cfg.Params = map[string]string{"vstream_preserve_skew": param}
		}
		return cfg
	}

	vstreamPreserveSkewOverride.Store(false)
	// THE DEFAULT-FLIP PIN: nothing set ⇒ NOT preserving ⇒ relaxed default. So a
	// reader built from a bare config relaxes skew (MinimizeSkew off).
	if vstreamPreserveSkewFromDSN(mk("")) {
		t.Error("no DSN param + no override should be false (the new relaxed default)")
	}
	if relax := !vstreamPreserveSkewFromDSN(mk("")); !relax {
		t.Error("default reader.relaxSkew must be true (MinimizeSkew off by default)")
	}
	if !vstreamPreserveSkewFromDSN(mk("true")) {
		t.Error("vstream_preserve_skew=true should resolve true (preserve = MinimizeSkew on)")
	}
	if vstreamPreserveSkewFromDSN(mk("false")) {
		t.Error("vstream_preserve_skew=false should resolve false (stay relaxed)")
	}

	// Override wins over an absent / false DSN param.
	SetVStreamPreserveSkewOverride(true)
	if !vstreamPreserveSkewFromDSN(mk("")) {
		t.Error("override true should win over an absent DSN param")
	}
	if !vstreamPreserveSkewFromDSN(mk("false")) {
		t.Error("override true should win over vstream_preserve_skew=false")
	}

	// SetVStreamPreserveSkewOverride(false) is a no-op (write-once-true), so a
	// non-CLI caller can never invert a prior explicit preserve — the zero-value
	// safety contract (the safe/common behaviour is the relaxed zero value).
	SetVStreamPreserveSkewOverride(false)
	if !vstreamPreserveSkewOverride.Load() {
		t.Error("SetVStreamPreserveSkewOverride(false) must not clear a prior true (write-once-true)")
	}
}
