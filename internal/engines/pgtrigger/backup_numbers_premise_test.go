// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"strconv"
	"testing"

	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
)

// TestNumbersArePreserved_PGTriggerCarriesNoFloats pins the premise the
// backup chunk codec's number-preserving read rests on
// ([blobcodec.NumbersArePreserved]): this engine's change payload decode
// produces NO float64 or float32 at any depth — integers become int64 and
// every other number stays json.Number — so every bare JSON number in its
// change chunks is a json.Number and may be read back as one. If this
// reader ever starts producing floats, the codec's opt-in would hand a
// Float column json.Number where it used to hand float64; this fails first.
//
// It also binds the codec's engine-name literal to this engine's name, so
// a rename cannot silently turn the opt-in off.
func TestNumbersArePreserved_PGTriggerCarriesNoFloats(t *testing.T) {
	if blobcodec.PreservedNumberEngine != EngineName {
		t.Fatalf("blobcodec.PreservedNumberEngine = %q; the postgres-trigger engine is %q — the backup read opt-in no longer reaches this engine's chains",
			blobcodec.PreservedNumberEngine, EngineName)
	}
	payloads := []string{
		`{"i": 1, "neg": -2, "big": 9223372036854775807, "past": 9223372036854775808, "nu": 123456789012345678.123456789012}`,
		`{"f8": 1.5, "tiny": -2.5e-300, "huge": 3.4e38, "exp": 1e3, "negz": -0, "zero": 0, "tz": 1.500}`,
		`{"arr": [1, 1.5, [2.25, null, [3, -0.5]], 12345678901234567890], "empty": []}`,
		`{"doc": {"a": 1, "b": 0.1, "c": {"d": [1.5, 2]}}, "s": "1.5", "b": true, "n": null}`,
	}
	for _, p := range payloads {
		row, err := decodeJSONBRow(p)
		if err != nil {
			t.Fatalf("decodeJSONBRow(%s): %v", p, err)
		}
		for col, v := range row {
			if path, ok := floatAt(v, col); ok {
				t.Errorf("payload %s: %s decoded as a float — the backup codec's PreserveNumbers premise no longer holds", p, path)
			}
		}
	}
}

// floatAt reports the path of the first float64/float32 anywhere in v.
func floatAt(v any, path string) (string, bool) {
	switch x := v.(type) {
	case float64, float32:
		return path, true
	case []any:
		for i, e := range x {
			if p, ok := floatAt(e, path+"["+strconv.Itoa(i)+"]"); ok {
				return p, true
			}
		}
	case map[string]any:
		for k, e := range x {
			if p, ok := floatAt(e, path+"."+k); ok {
				return p, true
			}
		}
	}
	return "", false
}
