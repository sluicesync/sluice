// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package blobcodec

// # A string value that is not valid UTF-8 refuses the backup write
//
// Every chunk record is JSON, and encoding/json — and the fast encoder,
// which reproduces it byte for byte — writes each byte of a Go string that
// does not start a valid UTF-8 sequence as U+FFFD. Nothing reports it: the
// chunk seals, `backup verify` rehashes the sealed bytes and agrees, and a
// restore lands `�` where the source held a value, at exit 0. GC-37 (j)
// measured exactly that: a MySQL latin1 column reached `backup incremental`
// / `backup stream` as raw bytes and a chain restored into Postgres held
// `�` (or, for bytes that happened to form valid UTF-8, a different
// character — which this check cannot see, and which is why the fix for
// that feeder is in the reader).
//
// The value contract (docs/value-types.md) makes a Char/Varchar/Text Go
// string decoded text — the reader interprets the column's charset — and
// carries binary as []byte, which the codec already round-trips byte-exact
// under its "bytes" envelope. So a non-UTF-8 string here is a reader that
// broke the contract, and the codec's job is to stop it at capture, loudly
// and naming the column, rather than launder it into a backup that verifies.
//
// Refusal, not byte-exact carriage, deliberately: (1) no target but SQLite
// can store such a value — Postgres refuses it (22021) and a strict
// utf8mb4 MySQL column refuses it (3988) — so carrying it only moves the
// failure from capture, where the source is still there to repair, to
// restore, where it may not be; (2) a new value envelope would restore
// loudly on an older binary only mid-chain (an unknown tag fails at the
// first record that carries it, after earlier tables have landed), so a
// clean carriage needs a manifest format-version tier as well; and (3) the
// SQLite/D1 trigger readers already refuse non-UTF-8 TEXT for this exact
// reason (sqlite/d1_decode.go), so the whole project draws the line in the
// same place. The resume cursor, which must round-trip whatever key the
// reader produced, is the one surface that carries such a string instead
// (`encodeCursorValue` in internal/ir), and it already does so byte-exact.

import (
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"
	"unicode/utf8"

	"sluicesync.dev/sluice/internal/ir"
)

// nonUTF8ValueMarker is the grep-stable marker a refused backup write
// carries.
const nonUTF8ValueMarker = "BACKUP-VALUE-NOT-UTF8"

// maxUTF8WalkDepth bounds the walk into nested lists and maps. A value
// nested deeper than any real JSON column would is refused rather than
// waved through unchecked.
const maxUTF8WalkDepth = 256

// invalidUTF8At reports where v holds a string (or a map key) that is not
// valid UTF-8: "" for v itself, otherwise a path such as `[2]`, `{"k"}` or
// `key "k…"`. It must reach every string [encodeValue] can hand to the JSON
// encoder. The value contract's own shapes are switched on directly: the
// scalars that hold no text, string, []string, []any and map[string]any.
// Anything else falls through [encodeValue] to json.Marshal's reflection,
// so it is walked the same way ([reflectInvalidUTF8]). Byte slices are not
// text and are carried base64 under the "bytes" envelope, so they are never
// inspected — which includes a JSON document carried as []byte.
func invalidUTF8At(v any, depth int) (string, bool) {
	if depth > maxUTF8WalkDepth {
		return " (nested deeper than " + strconv.Itoa(maxUTF8WalkDepth) + " levels; not checked)", true
	}
	switch x := v.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64,
		float32, float64, []byte, time.Time:
		return "", false
	case string:
		return "", !utf8.ValidString(x)
	case []string:
		for i, s := range x {
			if !utf8.ValidString(s) {
				return "[" + strconv.Itoa(i) + "]", true
			}
		}
		return "", false
	case []any:
		for i, e := range x {
			if p, bad := invalidUTF8At(e, depth+1); bad {
				return "[" + strconv.Itoa(i) + "]" + p, true
			}
		}
		return "", false
	case map[string]any:
		for k, e := range x {
			if !utf8.ValidString(k) {
				return fmt.Sprintf(" key %q", k), true
			}
			if p, bad := invalidUTF8At(e, depth+1); bad {
				return fmt.Sprintf("{%q}", k) + p, true
			}
		}
		return "", false
	}
	return reflectInvalidUTF8(reflect.ValueOf(v), depth)
}

// reflectInvalidUTF8 walks a value outside the value contract's shapes,
// which [encodeValue] hands to json.Marshal unchanged. It mirrors what
// json.Marshal writes as JSON text: a json.Marshaler's output (so a
// json.RawMessage's bytes), an encoding.TextMarshaler's text, strings of any
// named type, and the elements, map keys and values, pointees, interface
// values and serialised struct fields that hold them. Without it a *string,
// a map[string]string, a named string type, a struct with a string field or
// a json.RawMessage all passed and were written as U+FFFD (pre-land review
// of 230e8153, Gap A).
func reflectInvalidUTF8(rv reflect.Value, depth int) (string, bool) {
	if depth > maxUTF8WalkDepth {
		return " (nested deeper than " + strconv.Itoa(maxUTF8WalkDepth) + " levels; not checked)", true
	}
	if !rv.IsValid() {
		return "", false
	}
	if (rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface) && rv.IsNil() {
		return "", false // json.Marshal writes null
	}
	if rv.CanInterface() {
		switch m := rv.Interface().(type) {
		case json.Marshaler:
			b, err := m.MarshalJSON()
			if err != nil {
				return "", false // json.Marshal fails loudly on its own
			}
			return " (as JSON)", !utf8.Valid(b)
		case encoding.TextMarshaler:
			b, err := m.MarshalText()
			if err != nil {
				return "", false
			}
			return " (as text)", !utf8.Valid(b)
		}
	}
	switch rv.Kind() {
	case reflect.String:
		return "", !utf8.ValidString(rv.String())
	case reflect.Pointer, reflect.Interface:
		return reflectInvalidUTF8(rv.Elem(), depth+1)
	case reflect.Slice, reflect.Array:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return "", false // bytes: base64 for a slice, numbers for an array
		}
		for i := 0; i < rv.Len(); i++ {
			if p, bad := reflectInvalidUTF8(rv.Index(i), depth+1); bad {
				return "[" + strconv.Itoa(i) + "]" + p, true
			}
		}
	case reflect.Map:
		iter := rv.MapRange()
		for iter.Next() {
			k := iter.Key()
			if p, bad := reflectInvalidUTF8(k, depth+1); bad {
				return fmt.Sprintf(" key %q%s", fmt.Sprint(k.Interface()), p), true
			}
			if p, bad := reflectInvalidUTF8(iter.Value(), depth+1); bad {
				return fmt.Sprintf("{%q}", fmt.Sprint(k.Interface())) + p, true
			}
		}
	case reflect.Struct:
		t := rv.Type()
		for i := 0; i < rv.NumField(); i++ {
			f := t.Field(i)
			if (!f.IsExported() && !f.Anonymous) || f.Tag.Get("json") == "-" {
				continue // json.Marshal does not write it
			}
			if p, bad := reflectInvalidUTF8(rv.Field(i), depth+1); bad {
				return "." + f.Name + p, true
			}
		}
	}
	return "", false
}

// refuseNonUTF8Value returns the refusal for column's value v, or nil when
// every string in it is valid UTF-8. role names the image a change carries
// the value in ("row", "before-image", "after-image"); empty for a data
// chunk row.
func refuseNonUTF8Value(column, role string, v any) error {
	path, bad := invalidUTF8At(v, 0)
	if !bad {
		return nil
	}
	where := fmt.Sprintf("column %q%s", column, path)
	if role != "" {
		where += " (" + role + ")"
	}
	return &nonUTF8ValueError{msg: fmt.Sprintf("%s: %s holds a string that is not valid UTF-8 (%q); the backup codec is JSON and would "+
		"silently write each invalid byte as U+FFFD, so the backup would verify and restore a different value. The "+
		"source value is intact. Either the source holds bytes that are not valid text in a text column (a SQLite TEXT "+
		"value written from raw bytes, a Postgres SQL_ASCII database, legacy bytes in a MySQL utf8mb4 column) or its "+
		"reader did not convert them from the column's character set. Repair the value at the source or store it as a "+
		"binary type (BLOB / bytea); to back up the rest meanwhile, `backup full` can leave the table out "+
		"(--exclude-table) or null the column (--redact <table>.<column>=null)",
		nonUTF8ValueMarker, where, truncateForMessage(v))}
}

// nonUTF8ValueError is the refusal. Terminal: the same value refuses again on
// every retry, so a `backup stream` retry loop must stop rather than spin.
type nonUTF8ValueError struct{ msg string }

func (e *nonUTF8ValueError) Error() string  { return e.msg }
func (e *nonUTF8ValueError) Terminal() bool { return true }

var _ ir.TerminalError = (*nonUTF8ValueError)(nil)

// changeTableErr names the table a change-chunk encode refusal came from;
// the column is already in err.
func changeTableErr(schema, table string, err error) error {
	name := table
	if schema != "" {
		name = schema + "." + table
	}
	return fmt.Errorf("change chunk: table %q: %w", name, err)
}

// truncateForMessage renders v for a refusal without dumping a large value.
func truncateForMessage(v any) string {
	s := fmt.Sprintf("%v", v)
	const limit = 64
	if len(s) > limit {
		return s[:limit] + "…"
	}
	return s
}
