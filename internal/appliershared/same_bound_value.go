// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"math"
	"reflect"
)

// SameBoundValue reports whether a and b would bind as the identical
// statement argument — the equality an applier needs before it leaves a
// column out of an UPDATE's SET list on the strength of "the change did not
// alter it" (the MySQL GC-41 (e) key trim and the Neki shard-key trim).
//
// reflect.DeepEqual is that equality for every value family the IR carries
// (docs/value-types.md) EXCEPT floats: it compares float64 and float32 with
// ==, under which -0.0 equals +0.0, while a DOUBLE / float8 column stores the
// two distinctly (verified on mysql:8.4 for a DOUBLE PRIMARY KEY). A trim on
// DeepEqual therefore read `SET k = -0` as unchanged and the target kept +0,
// silently. So floats compare by bit pattern, at any depth of an []any, and
// everything else by DeepEqual:
//
//   - string, []byte, []string, int64, uint64, bool: byte-exact under
//     DeepEqual.
//   - time.Time: DeepEqual compares wall clock, monotonic reading and
//     location, so equal values render the same literal; one instant in two
//     locations is (conservatively) unequal.
//   - NaN: unequal under ==, equal here when the bits match — the identical
//     argument binds either way, so calling it equal is sound.
//
// The error direction is the safe one: anything this cannot prove identical
// is "changed", which keeps the column in SET.
func SameBoundValue(a, b any) bool {
	switch x := a.(type) {
	case float64:
		y, ok := b.(float64)
		return ok && math.Float64bits(x) == math.Float64bits(y)
	case float32:
		y, ok := b.(float32)
		return ok && math.Float32bits(x) == math.Float32bits(y)
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) || (x == nil) != (y == nil) {
			return false
		}
		for i := range x {
			if !SameBoundValue(x[i], y[i]) {
				return false
			}
		}
		return true
	}
	return reflect.DeepEqual(a, b)
}
