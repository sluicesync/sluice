// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"strings"
	"testing"
)

// TestLoadFleetConfig_ParsesCrossShardFields pins that the koanf loader decodes
// the `inject-shard-column` / `allow-cross-shard-merge` YAML keys into the new
// SyncSpec fields (Gap A) — without ErrorUnused they were silently dropped.
func TestLoadFleetConfig_ParsesCrossShardFields(t *testing.T) {
	path := writeFleetYAML(t, `
syncs:
  - stream-id: shard-a
    source-driver: planetscale
    source: mysql://u:p@src:3306/app
    target-driver: postgres
    target: postgres://u:p@dst:5432/app
    allow-cross-shard-merge: true
  - stream-id: shard-b
    source-driver: planetscale
    source: mysql://u:p@src:3306/app
    target-driver: postgres
    target: postgres://u:p@dst:5432/app
    inject-shard-column: source_shard_id=us-east-1
`)
	fleet, err := loadFleetConfig(path)
	if err != nil {
		t.Fatalf("loadFleetConfig: %v", err)
	}
	if !fleet.Syncs[0].AllowCrossShardMerge {
		t.Error("allow-cross-shard-merge: true did not decode onto SyncSpec (silently dropped?)")
	}
	if got := fleet.Syncs[1].InjectShardColumn; got != "source_shard_id=us-east-1" {
		t.Errorf("InjectShardColumn = %q; want %q", got, "source_shard_id=us-east-1")
	}
}

// TestLoadFleetConfig_UnknownKeyRefused pins Gap A's loud-drop fix: an
// unsupported / typo'd YAML key now fails the load (ErrorUnused) instead of
// being silently ignored. The classic trap is the misspelled opt-in — here a
// `allow-cross-shard-merges` typo — which pre-fix parsed clean while the knob
// had no effect.
func TestLoadFleetConfig_UnknownKeyRefused(t *testing.T) {
	cases := map[string]string{
		"typo'd opt-in key": `
syncs:
  - stream-id: a
    source-driver: postgres
    source: postgres://u:p@src:5432/app
    target-driver: mysql
    target: mysql://u:p@dst:3306/app
    allow-cross-shard-merges: true
`,
		"unsupported sync-start flag not in the fleet subset": `
syncs:
  - stream-id: a
    source-driver: postgres
    source: postgres://u:p@src:5432/app
    target-driver: mysql
    target: mysql://u:p@dst:3306/app
    force-cold-start: true
`,
		"unknown restart key": `
syncs:
  - stream-id: a
    source-driver: postgres
    source: postgres://u:p@src:5432/app
    target-driver: mysql
    target: mysql://u:p@dst:3306/app
restart:
  backoff-baze: 2s
`,
	}
	for name, yaml := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := loadFleetConfig(writeFleetYAML(t, yaml))
			if err == nil {
				t.Fatal("loadFleetConfig with an unknown key = nil; want a loud parse error")
			}
		})
	}
}

// TestBuildStreamerFromSpec_CrossShard pins that both cross-shard fields REACH
// the streamer-construction path (Gap A): allow-cross-shard-merge is threaded
// verbatim, and inject-shard-column is parsed into the engaged ShardColumnSpec
// the same way `sync start` does.
func TestBuildStreamerFromSpec_CrossShard(t *testing.T) {
	t.Run("allow-cross-shard-merge reaches the streamer", func(t *testing.T) {
		spec := mysqlSpec("a")
		spec.AllowCrossShardMerge = true
		streamer, err := buildStreamerFromSpec(context.Background(), &spec, testFleetGlobals())
		if err != nil {
			t.Fatalf("buildStreamerFromSpec: %v", err)
		}
		if !streamer.AllowCrossShardMerge {
			t.Error("Streamer.AllowCrossShardMerge = false; want true (flag did not reach the streamer)")
		}
	})

	t.Run("inject-shard-column parses onto the streamer", func(t *testing.T) {
		spec := mysqlSpec("a")
		spec.InjectShardColumn = "source_shard_id=us-east-1"
		streamer, err := buildStreamerFromSpec(context.Background(), &spec, testFleetGlobals())
		if err != nil {
			t.Fatalf("buildStreamerFromSpec: %v", err)
		}
		if !streamer.InjectShardColumn.Engaged() {
			t.Fatal("Streamer.InjectShardColumn is disengaged; want engaged")
		}
		if streamer.InjectShardColumn.Name != "source_shard_id" || streamer.InjectShardColumn.Value != "us-east-1" {
			t.Errorf("InjectShardColumn = %+v; want {source_shard_id us-east-1}", streamer.InjectShardColumn)
		}
	})

	t.Run("malformed inject-shard-column refuses in construction", func(t *testing.T) {
		spec := mysqlSpec("a")
		spec.InjectShardColumn = "no-equals-sign"
		if _, err := buildStreamerFromSpec(context.Background(), &spec, testFleetGlobals()); err == nil {
			t.Fatal("buildStreamerFromSpec with a malformed inject-shard-column = nil; want a refusal")
		}
	})
}

// TestValidateCrossShard pins the config-load contract for the two cross-shard
// opt-ins: the both-set contradiction is refused (naming the stream-id), a
// malformed NAME=VALUE is refused at load, and either one alone is accepted.
func TestValidateCrossShard(t *testing.T) {
	t.Run("both set → mutual-exclusion refusal naming the stream-id", func(t *testing.T) {
		s := mysqlSpec("shard-a")
		s.InjectShardColumn = "source_shard_id=us-east-1"
		s.AllowCrossShardMerge = true
		err := fleetFromSpecs(s).validate()
		if err == nil {
			t.Fatal("validate() with both cross-shard opt-ins = nil; want a mutual-exclusion refusal")
		}
		for _, want := range []string{"mutually exclusive", "shard-a"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q missing substring %q", err.Error(), want)
			}
		}
	})

	t.Run("malformed inject-shard-column → refused at load", func(t *testing.T) {
		s := mysqlSpec("shard-a")
		s.InjectShardColumn = "=missing-name"
		if err := fleetFromSpecs(s).validate(); err == nil {
			t.Fatal("validate() with a malformed inject-shard-column = nil; want a refusal")
		}
	})

	t.Run("inject-shard-column alone → ok", func(t *testing.T) {
		s := mysqlSpec("shard-a")
		s.InjectShardColumn = "source_shard_id=us-east-1"
		if err := fleetFromSpecs(s).validate(); err != nil {
			t.Fatalf("validate() with inject-shard-column alone = %v; want nil", err)
		}
	})

	t.Run("allow-cross-shard-merge alone → ok", func(t *testing.T) {
		s := mysqlSpec("shard-a")
		s.AllowCrossShardMerge = true
		if err := fleetFromSpecs(s).validate(); err != nil {
			t.Fatalf("validate() with allow-cross-shard-merge alone = %v; want nil", err)
		}
	})
}
