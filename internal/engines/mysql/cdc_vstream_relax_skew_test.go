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
// (ADR-0120, default flipped; task 2.5 per-instance form): the CLI override
// (when set) wins; otherwise the source DSN's vstream_preserve_skew=true; default
// false = the new relaxed default. The override is now the per-instance
// engineOptions.preserveSkew passed to the resolver as an explicit argument.
func TestVStreamPreserveSkewFromDSN(t *testing.T) {
	mk := func(param string) *gomysql.Config {
		cfg := gomysql.NewConfig()
		if param != "" {
			cfg.Params = map[string]string{"vstream_preserve_skew": param}
		}
		return cfg
	}

	// THE DEFAULT-FLIP PIN: nothing set ⇒ NOT preserving ⇒ relaxed default. So a
	// reader built from a bare config relaxes skew (MinimizeSkew off).
	if vstreamPreserveSkewFromDSN(mk(""), false) {
		t.Error("no DSN param + no override should be false (the new relaxed default)")
	}
	if relax := !vstreamPreserveSkewFromDSN(mk(""), false); !relax {
		t.Error("default reader.relaxSkew must be true (MinimizeSkew off by default)")
	}
	if !vstreamPreserveSkewFromDSN(mk("true"), false) {
		t.Error("vstream_preserve_skew=true should resolve true (preserve = MinimizeSkew on)")
	}
	if vstreamPreserveSkewFromDSN(mk("false"), false) {
		t.Error("vstream_preserve_skew=false should resolve false (stay relaxed)")
	}

	// Override wins over an absent / false DSN param.
	if !vstreamPreserveSkewFromDSN(mk(""), true) {
		t.Error("override true should win over an absent DSN param")
	}
	if !vstreamPreserveSkewFromDSN(mk("false"), true) {
		t.Error("override true should win over vstream_preserve_skew=false")
	}
}

// TestWithVStreamPreserveSkew_WriteOnceTrue pins the zero-value-safety contract
// (task 2.5): the builder records only a true value, so a non-CLI caller passing
// false can never invert a prior explicit preserve — and a bare engine keeps the
// relaxed (false) default.
func TestWithVStreamPreserveSkew_WriteOnceTrue(t *testing.T) {
	if (Engine{}).opts.preserveSkew {
		t.Error("bare Engine{} must default to the relaxed (false) preserve-skew")
	}
	e := Engine{}.WithVStreamPreserveSkew(true)
	if !e.opts.preserveSkew {
		t.Error("WithVStreamPreserveSkew(true) must record preserve")
	}
	if e2 := e.WithVStreamPreserveSkew(false); !e2.opts.preserveSkew {
		t.Error("WithVStreamPreserveSkew(false) must not clear a prior true (write-once-true)")
	}
}
