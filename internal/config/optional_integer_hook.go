// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"

	"github.com/go-viper/mapstructure/v2"
)

// optionalIntegerHook refuses, at load time, every YAML spelling of an
// OPTIONAL integer option that is not an integer.
//
// The class is "a pointer-to-integer Config field" — today the five
// required redaction options (`length`, `m1`, `m2`, `min`, `max`), derived
// by type rather than listed, so a sixth gets the rule for free. They are
// pointers so that "omitted" is representable (A0915-CFG-HIGH-1: the
// zero value read as a present 0 and `strategy: truncate` alone emptied
// every row), and the CLI refuses every non-integer spelling of the same
// options with strconv. But `WeaklyTypedInput`, which the loader needs
// for its env-var overlay, manufactures a PRESENT integer out of things
// that are not one (pre-tag value-fidelity review of v0.153.2):
//
//   - `length: ""` decoded as present 0 — the zero-value substitution
//     the release headlines, through the empty-string spelling;
//     `m1: ""` / `m2: ""` on `mask`/`outer` masks nothing at exit 0;
//   - `min: 9223372036854775808` (2^63) WRAPPED to -2^63, silently;
//   - `length: 5.7` truncated to 5, `length: true` became 1.
//
// What stays exactly as it was, because YAML produces a real integer or
// an integral float for it: `"5"` → 5 (a quoted integer), `0` / `-0` →
// present 0, `1e3` → 1000, `0x10` → 16, and `null` / `~` → nil, which the
// CLI layer then refuses as omitted. The empty string is the one string
// refused outright — it is what an operator gets from `length:` with
// nothing after the colon, and the weak decoder's "" → 0 is the exact
// trap the pointer was introduced to close.
//
// mapstructure prefixes the returned error with the key path
// (`error decoding 'redactions[0].length'`), which is what names the
// key in the load refusal. Pinned by TestLoadYAML_RedactionOptionPresence
// (every option × every refused and every kept spelling) and, against
// the CLI's own refusals, by TestRedactCLIAndYAMLAgreeOnRequiredOptions.
func optionalIntegerHook() mapstructure.DecodeHookFuncType {
	return func(_, to reflect.Type, data any) (any, error) {
		if to.Kind() != reflect.Pointer || !isIntegerKind(to.Elem().Kind()) {
			return data, nil
		}
		if data == nil {
			return nil, nil //nolint:nilnil // a nil input decodes to a nil pointer: the option was omitted
		}
		if v := reflect.ValueOf(data); v.Kind() == reflect.Pointer {
			// A null YAML value reaches the hook as a typed nil pointer
			// (mapstructure substitutes the target's zero value); it
			// decodes to nil, which is "omitted".
			if v.IsNil() {
				return data, nil
			}
			data = v.Elem().Interface()
		}
		n, err := integerFromYAML(data)
		if err != nil {
			return nil, err
		}
		elem := to.Elem()
		if reflect.New(elem).Elem().OverflowInt(n) {
			return nil, fmt.Errorf("%d is out of range for an integer option (%s)", n, elem)
		}
		return reflect.ValueOf(n).Convert(elem).Interface(), nil
	}
}

func isIntegerKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	default:
		return false
	}
}

// integerFromYAML turns the value the YAML parser produced for an integer
// option into an int64, or refuses it. The parser's own typing is the
// contract: an unquoted integer arrives as int (or uint64 past int64's
// range, which is the 2^63 wrap), a float as float64, a quoted value as
// string, a bare `true` as bool.
func integerFromYAML(data any) (int64, error) {
	switch v := data.(type) {
	case int, int8, int16, int32, int64:
		return reflect.ValueOf(v).Int(), nil
	case uint, uint8, uint16, uint32, uint64:
		u := reflect.ValueOf(v).Uint()
		if u > math.MaxInt64 {
			return 0, fmt.Errorf("%d is out of range for an integer option (it exceeds %d)", u, int64(math.MaxInt64))
		}
		return int64(u), nil
	case float32, float64:
		f := reflect.ValueOf(v).Float()
		if math.IsNaN(f) || math.IsInf(f, 0) || f != math.Trunc(f) {
			return 0, fmt.Errorf("%v is not an integer (an integer option takes whole numbers only)", v)
		}
		// Past 2^53 a float64 no longer holds every integer, and the YAML
		// parser produces a float for exactly two things: an exponent
		// spelling (`1e3`, kept) and an integer literal OUTSIDE int64 /
		// uint64 range that it could not parse as one — so a float of
		// this magnitude is either a literal the parser already rounded
		// (`-9223372036854775809` arrives as exactly -2^63, and a range
		// check alone would ACCEPT it as -2^63) or an exponent form no
		// integer option has a use for. Refuse both rather than guess.
		if math.Abs(f) >= 1<<53 {
			return 0, fmt.Errorf("%v is out of range for an integer option (a value past 2^53 written in a form the YAML parser reads as floating-point cannot be represented exactly; write a plain integer within int64 range)", v)
		}
		return int64(f), nil
	case bool:
		return 0, fmt.Errorf("%v is a boolean, not an integer (an integer option takes whole numbers only)", v)
	case string:
		if v == "" {
			return 0, errors.New("an empty value is not an integer (omit the key to leave the option unset, or write a whole number)")
		}
		n, err := strconv.ParseInt(v, 0, 64)
		if err != nil {
			return 0, fmt.Errorf("%q is not an integer (an integer option takes whole numbers only): %w", v, errors.Unwrap(err))
		}
		return n, nil
	default:
		return 0, fmt.Errorf("%v (%T) is not an integer (an integer option takes whole numbers only)", data, data)
	}
}
