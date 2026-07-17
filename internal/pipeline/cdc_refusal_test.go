// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// explainingCDCNoneEngine is a CDCNone engine that supplies its own
// refusal via [ir.CDCUnsupportedExplainer] (the mysql mariadb-flavor
// shape, roadmap item 73).
type explainingCDCNoneEngine struct {
	stubEngine
	explain error
}

func (e explainingCDCNoneEngine) Capabilities() ir.Capabilities {
	return ir.Capabilities{CDC: ir.CDCNone}
}
func (e explainingCDCNoneEngine) ExplainCDCUnsupported() error { return e.explain }

func TestCDCUnsupportedError_PrefersEngineExplanation(t *testing.T) {
	flavorErr := errors.New("mariadb: CDC not supported yet (roadmap item 73 P3)")
	generic := errors.New("generic: declares CDC=None")

	if got := cdcUnsupportedError(explainingCDCNoneEngine{explain: flavorErr}, generic); !errors.Is(got, flavorErr) {
		t.Errorf("cdcUnsupportedError = %v; want the engine's own explanation", got)
	}
	// A nil explanation falls back to the generic message.
	if got := cdcUnsupportedError(explainingCDCNoneEngine{}, generic); !errors.Is(got, generic) {
		t.Errorf("cdcUnsupportedError with nil explanation = %v; want the generic error", got)
	}
	// An engine without the optional interface falls back too.
	if got := cdcUnsupportedError(stubEngine{}, generic); !errors.Is(got, generic) {
		t.Errorf("cdcUnsupportedError without the interface = %v; want the generic error", got)
	}
}

// TestStreamerValidate_UsesEngineCDCExplanation pins that `sync start`'s
// CDC-capability preflight surfaces the engine's flavor-specific
// refusal (the coded mariadb story) instead of the generic
// "declares CDC=None" line.
func TestStreamerValidate_UsesEngineCDCExplanation(t *testing.T) {
	flavorErr := errors.New("mariadb: CDC not supported yet (roadmap item 73 P3)")
	s := &Streamer{
		Source:    explainingCDCNoneEngine{explain: flavorErr},
		Target:    stubEngine{},
		SourceDSN: "src-dsn",
		TargetDSN: "dst-dsn",
	}
	err := s.validate()
	if !errors.Is(err, flavorErr) {
		t.Fatalf("Streamer.validate() = %v; want the engine's CDC explanation", err)
	}
}
