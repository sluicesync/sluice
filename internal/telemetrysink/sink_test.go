// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package telemetrysink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// errFakeSink is the injected failure for the fan-out isolation test.
var errFakeSink = errors.New("sink is dead")

// The [Record] round-trips through a store, so it is a CODEC and gets the
// full family-matrix treatment (CLAUDE.md "new-surface pre-land checklist"):
// every value family the row can carry × every shape, round-tripped
// value-exact or refused loudly. The two hazards named in that checklist —
// encoding/json silently substituting U+FFFD for invalid UTF-8, and int64
// beyond 2^53 routed through float64 — are pinned directly below.

func f64(v float64) *float64 { return &v }
func i64(v int64) *int64     { return &v }

// decodeLine parses one encoded line back into a Record.
func decodeLine(t *testing.T, line []byte) Record {
	t.Helper()
	var got Record
	if err := json.Unmarshal(line, &got); err != nil {
		t.Fatalf("decode %q: %v", line, err)
	}
	return got
}

// decodeNumbers parses one encoded line with an INDEPENDENT reader posture —
// json.Decoder with UseNumber, which preserves the literal number token
// instead of routing it through float64. This is what catches the >2^53
// precision hazard: the typed round-trip alone would pass even if the writer
// had emitted a lossy value, because both sides would agree.
func decodeNumbers(t *testing.T, line []byte) map[string]any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.UseNumber()
	var got map[string]any
	if err := dec.Decode(&got); err != nil {
		t.Fatalf("decode-with-numbers %q: %v", line, err)
	}
	return got
}

// TestEncodeRecord_StringFamilyRoundTrip covers the string family × shape:
// ASCII, non-ASCII UTF-8, astral-plane, embedded control/quote/backslash,
// HTML-significant bytes, and empty — each must survive byte-exact.
func TestEncodeRecord_StringFamilyRoundTrip(t *testing.T) {
	shapes := map[string]string{
		"ascii":       "shop-prod",
		"empty":       "",
		"non-ascii":   "café-données-日本語",
		"astral":      "db-\U0001F600-emoji",
		"newline":     "line1\nline2",
		"tab-return":  "a\tb\rc",
		"quote":       `say "hi"`,
		"backslash":   `C:\path\to\db`,
		"html-chars":  "a<b>&c",
		"replacement": "already\uFFFDthere", // a LEGITIMATE U+FFFD must pass through
		"nul":         "a\x00b",
	}
	for name, s := range shapes {
		t.Run(name, func(t *testing.T) {
			rec := Record{At: time.Unix(1, 0).UTC(), Watch: s, Database: s, Branch: s}
			line, err := EncodeRecord(rec)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			got := decodeLine(t, line)
			if got.Watch != s || got.Database != s || got.Branch != s {
				t.Fatalf("string round-trip lost fidelity:\n want %q\n  got watch=%q database=%q branch=%q", s, got.Watch, got.Database, got.Branch)
			}
		})
	}
}

// TestEncodeRecord_InvalidUTF8Refused pins the resume-cursor CRITICAL's
// mechanism at this boundary: encoding/json would silently rewrite an
// invalid byte as U+FFFD, so the encoder refuses the row instead — loudly,
// naming the field and the target.
func TestEncodeRecord_InvalidUTF8Refused(t *testing.T) {
	bad := string([]byte{0x61, 0xff, 0xfe, 0x62}) // "a\xff\xfeb"
	for _, tc := range []struct {
		field string
		rec   Record
	}{
		{"watch", Record{Watch: bad, Database: "d", Branch: "main"}},
		{"database", Record{Watch: "w", Database: bad, Branch: "main"}},
		{"branch", Record{Watch: "w", Database: "d", Branch: bad}},
	} {
		t.Run(tc.field, func(t *testing.T) {
			line, err := EncodeRecord(tc.rec)
			if err == nil {
				t.Fatalf("invalid UTF-8 in %s must be refused, got line %q", tc.field, line)
			}
			if !strings.Contains(err.Error(), tc.field) {
				t.Errorf("refusal must name the field %q: %v", tc.field, err)
			}
			if !strings.Contains(err.Error(), "UTF-8") {
				t.Errorf("refusal must explain the cause: %v", err)
			}
		})
	}

	// Proof the refusal is not paranoia: encoding/json really does mangle it.
	mangled, err := json.Marshal(bad)
	if err != nil {
		t.Fatalf("marshal control: %v", err)
	}
	if !strings.Contains(string(mangled), `\ufffd`) {
		t.Fatalf("premise check failed — encoding/json no longer substitutes U+FFFD (%s); revisit the refusal's rationale", mangled)
	}
}

// TestEncodeRecord_IntegerFamilyExact covers the int64 family × shape,
// including the values that lose precision if they ever ride float64.
func TestEncodeRecord_IntegerFamilyExact(t *testing.T) {
	shapes := map[string]int64{
		"zero":          0,
		"small":         42,
		"negative":      -1,
		"pow53":         1 << 53,
		"pow53-plus-1":  (1 << 53) + 1, // 9007199254740993 — not representable in float64
		"max-int64":     math.MaxInt64,
		"min-int64":     math.MinInt64,
		"near-max-odd":  math.MaxInt64 - 1,
		"large-storage": 8_796_093_022_208, // 8 TiB, a plausible real volume
	}
	for name, v := range shapes {
		t.Run(name, func(t *testing.T) {
			rec := Record{
				At: time.Unix(1, 0).UTC(), Watch: "w", Database: "d", Branch: "main", Fresh: true,
				StorageAvailableBytes: i64(v),
				StorageCapacityBytes:  i64(v),
				ActiveConnections:     i64(v),
				MaxConnections:        i64(v),
			}
			line, err := EncodeRecord(rec)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			got := decodeLine(t, line)
			for field, p := range map[string]*int64{
				"storage_available_bytes": got.StorageAvailableBytes,
				"storage_capacity_bytes":  got.StorageCapacityBytes,
				"active_connections":      got.ActiveConnections,
				"max_connections":         got.MaxConnections,
			} {
				if p == nil || *p != v {
					t.Fatalf("%s round-trip: want %d, got %v", field, v, p)
				}
			}
			// Independent reader: the literal token must be the exact
			// decimal, not a float-rounded one.
			nums := decodeNumbers(t, line)
			want := json.Number(strconv.FormatInt(v, 10))
			if got := nums["storage_available_bytes"]; got != want {
				t.Fatalf("raw JSON number token: want %s, got %v (%T) — an int64 was routed through float64", want, got, got)
			}
		})
	}
}

// TestEncodeRecord_FloatFamilyExact covers the float64 family × shape.
func TestEncodeRecord_FloatFamilyExact(t *testing.T) {
	shapes := map[string]float64{
		"zero":           0,
		"one":            1,
		"fraction":       0.4235,
		"high-precision": 0.1234567890123457,
		"denormal":       math.SmallestNonzeroFloat64,
		"max":            math.MaxFloat64,
		"negative":       -0.5,
		"tiny-lag":       1e-9,
		"big-lag":        86400.25,
	}
	for name, v := range shapes {
		t.Run(name, func(t *testing.T) {
			rec := Record{
				At: time.Unix(1, 0).UTC(), Watch: "w", Database: "d", Branch: "main", Fresh: true,
				CPUUtil: f64(v), MemUtil: f64(v), StorageUtil: f64(v), ReplicaLagSeconds: f64(v),
			}
			line, err := EncodeRecord(rec)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			got := decodeLine(t, line)
			for field, p := range map[string]*float64{
				"cpu_util": got.CPUUtil, "mem_util": got.MemUtil,
				"storage_util": got.StorageUtil, "replica_lag_seconds": got.ReplicaLagSeconds,
			} {
				if p == nil || *p != v {
					t.Fatalf("%s round-trip: want %v, got %v", field, v, p)
				}
			}
		})
	}
}

// TestEncodeRecord_NonFiniteRefused pins the NaN/±Inf refusal on EVERY float
// field. JSON has no spelling for these, so a silent substitution is the only
// alternative to refusing — and this row class is reachable in principle
// (the Prometheus exposition format has NaN/+Inf literals; sluice's own
// parser rejects them today, so this is a belt on that brace).
func TestEncodeRecord_NonFiniteRefused(t *testing.T) {
	values := map[string]float64{
		"nan":      math.NaN(),
		"pos-inf":  math.Inf(1),
		"neg-inf":  math.Inf(-1),
		"nan-payl": math.Float64frombits(0x7FF8000000000042),
	}
	fields := map[string]func(*Record, *float64){
		"cpu_util":            func(r *Record, p *float64) { r.CPUUtil = p },
		"mem_util":            func(r *Record, p *float64) { r.MemUtil = p },
		"storage_util":        func(r *Record, p *float64) { r.StorageUtil = p },
		"replica_lag_seconds": func(r *Record, p *float64) { r.ReplicaLagSeconds = p },
	}
	for fname, set := range fields {
		for vname, v := range values {
			t.Run(fname+"/"+vname, func(t *testing.T) {
				rec := Record{At: time.Unix(1, 0).UTC(), Watch: "w", Database: "d", Branch: "main"}
				set(&rec, f64(v))
				line, err := EncodeRecord(rec)
				if err == nil {
					t.Fatalf("non-finite %s in %s must be refused, got %q", vname, fname, line)
				}
				if !strings.Contains(err.Error(), fname) {
					t.Errorf("refusal must name the field %q: %v", fname, err)
				}
			})
		}
	}
}

// TestEncodeRecord_ValidatorCoversEveryField is the Tier-1 RATCHET for this
// codec: it walks [Record] by reflection and asserts that EVERY string field
// is UTF-8-validated and EVERY *float64 field is finiteness-validated. A
// future field (the roadmap-75a egress figures, say) that skips the
// validator fails here instead of silently shipping a mangling path — the
// "pin the class, not the representative" discipline applied to the schema
// itself rather than to a hand-listed set of fields.
func TestEncodeRecord_ValidatorCoversEveryField(t *testing.T) {
	rt := reflect.TypeOf(Record{})
	badUTF8 := string([]byte{0xff, 0xfe})
	nan := math.NaN()

	checkedStrings, checkedFloats := 0, 0
	for i := range rt.NumField() {
		field := rt.Field(i)
		base := Record{At: time.Unix(1, 0).UTC(), Watch: "w", Database: "d", Branch: "main"}
		v := reflect.ValueOf(&base).Elem()
		switch {
		case field.Type.Kind() == reflect.String:
			v.Field(i).SetString(badUTF8)
			checkedStrings++
		case field.Type.Kind() == reflect.Pointer && field.Type.Elem().Kind() == reflect.Float64:
			v.Field(i).Set(reflect.ValueOf(&nan))
			checkedFloats++
		default:
			continue // int64 pointers, bool, time — no invalid value to plant
		}
		if _, err := EncodeRecord(base); err == nil {
			t.Errorf("Record.%s (json %q) accepted a value encoding/json would mangle — add it to EncodeRecord's validator", field.Name, field.Tag.Get("json"))
		}
	}
	// Vacuity guards: if the reflection stopped finding fields, the gate is
	// green for the wrong reason.
	if checkedStrings < 3 {
		t.Fatalf("only %d string fields walked — the Record shape changed; fix this gate before trusting it", checkedStrings)
	}
	if checkedFloats < 4 {
		t.Fatalf("only %d *float64 fields walked — the Record shape changed; fix this gate before trusting it", checkedFloats)
	}
}

// TestEncodeRecord_UnobservedIsNullNotZero pins the honesty contract in its
// PERSISTED form: an unobserved metric is an explicit null, never a 0 a
// portal would plot as "idle" — and the key is always present.
func TestEncodeRecord_UnobservedIsNullNotZero(t *testing.T) {
	line, err := EncodeRecord(Record{At: time.Unix(1, 0).UTC(), Watch: "w", Database: "d", Branch: "main", Fresh: false})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	for _, key := range []string{
		"cpu_util", "mem_util", "storage_util", "storage_available_bytes",
		"storage_capacity_bytes", "replica_lag_seconds", "active_connections",
		"max_connections", "sampled_at",
	} {
		if !strings.Contains(string(line), `"`+key+`":null`) {
			t.Errorf("unobserved %s must serialize as an explicit null; got %s", key, line)
		}
	}
	if strings.Contains(string(line), `"cpu_util":0`) {
		t.Errorf("unobserved metric must never serialize as 0: %s", line)
	}
}

// TestEncodeRecord_TimeFamily covers the temporal family × shape: zero,
// UTC, a non-UTC offset (normalised to UTC but instant-identical), and
// nanosecond precision.
func TestEncodeRecord_TimeFamily(t *testing.T) {
	zone := time.FixedZone("UTC+7", 7*3600)
	shapes := map[string]time.Time{
		"zero":       {},
		"utc":        time.Date(2026, 7, 24, 16, 30, 15, 0, time.UTC),
		"offset":     time.Date(2026, 7, 24, 16, 30, 15, 0, zone),
		"nanos":      time.Date(2026, 7, 24, 16, 30, 15, 123456789, time.UTC),
		"pre-epoch":  time.Date(1969, 7, 20, 20, 17, 40, 0, time.UTC),
		"far-future": time.Date(2999, 12, 31, 23, 59, 59, 0, time.UTC),
	}
	for name, at := range shapes {
		t.Run(name, func(t *testing.T) {
			sampled := at
			rec := Record{At: at, SampledAt: &sampled, Watch: "w", Database: "d", Branch: "main"}
			line, err := EncodeRecord(rec)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			got := decodeLine(t, line)
			if !got.At.Equal(at) {
				t.Errorf("at round-trip: want %v, got %v", at, got.At)
			}
			if got.SampledAt == nil || !got.SampledAt.Equal(at) {
				t.Errorf("sampled_at round-trip: want %v, got %v", at, got.SampledAt)
			}
			if loc := got.At.Location(); loc != time.UTC {
				t.Errorf("at must be normalised to UTC, got location %v", loc)
			}
		})
	}
}

// TestEncodeRecord_JSONLFraming pins the file format's framing invariant:
// exactly one trailing newline, none embedded, and the line alone is valid
// JSON — so a consumer can split on "\n" without a parser.
func TestEncodeRecord_JSONLFraming(t *testing.T) {
	rec := Record{
		At: time.Unix(1, 0).UTC(), Watch: "w\nx", Database: "d", Branch: "main",
		Fresh: true, CPUUtil: f64(0.5),
	}
	line, err := EncodeRecord(rec)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if n := bytes.Count(line, []byte("\n")); n != 1 {
		t.Fatalf("want exactly one newline (the JSONL terminator), got %d in %q", n, line)
	}
	if !bytes.HasSuffix(line, []byte("\n")) {
		t.Fatalf("line must end with the terminator: %q", line)
	}
	if !json.Valid(bytes.TrimRight(line, "\n")) {
		t.Fatalf("line is not valid JSON: %q", line)
	}
}

// TestMultiSink_FailureIsolation pins the load-bearing contract: a failing
// sink never suppresses delivery to a healthy one, and the joined error names
// the failing sink.
func TestMultiSink_FailureIsolation(t *testing.T) {
	good := &recordingSink{name: "good"}
	bad := &recordingSink{name: "bad", fail: true}
	multi := NewMultiSink(bad, nil, good)
	err := multi.Write(t.Context(), []Record{{At: time.Unix(1, 0).UTC(), Database: "d", Branch: "main"}})
	if err == nil {
		t.Fatal("expected the failing sink's error to surface (for the caller to log+swallow)")
	}
	if !strings.Contains(err.Error(), "bad") {
		t.Errorf("joined error must name the failing sink: %v", err)
	}
	if len(good.writes) != 1 {
		t.Errorf("healthy sink must still receive the batch, got %d writes", len(good.writes))
	}
}

// TestNewMultiSink_NilWhenEmpty pins the typed-nil-interface guard: no
// configured sink must yield a TRUE nil so a caller's `!= nil` check is exact.
func TestNewMultiSink_NilWhenEmpty(t *testing.T) {
	if m := NewMultiSink(); m != nil {
		t.Fatalf("no sinks must yield nil, got %#v", m)
	}
	if m := NewMultiSink(nil, nil); m != nil {
		t.Fatalf("all-nil sinks must yield nil, got %#v", m)
	}
}

type recordingSink struct {
	name   string
	fail   bool
	writes [][]Record
}

func (r *recordingSink) Name() string { return r.name }
func (r *recordingSink) Close() error { return nil }

func (r *recordingSink) Write(_ context.Context, recs []Record) error {
	if r.fail {
		return errFakeSink
	}
	r.writes = append(r.writes, recs)
	return nil
}

// TestRecord_CarriesWorstPodStorage pins that the persisted sink record carries
// the worst-POD storage family, and that it is guarded by its OWN flag.
//
// Why this pin exists: the fleet exporter and this record were both written
// before the worst-pod signal landed in the single-database exporter, so
// merging left them silently missing it — a metric present in one mode and
// absent in another, which is the "fixed the representative, missed the
// sibling" shape. The reflection ratchet over this struct could not catch it:
// it proves every RECORD field is validated, not that the record covers every
// snapshot field.
func TestRecord_CarriesWorstPodStorage(t *testing.T) {
	f := func(v float64) *float64 { return &v }
	i := func(v int64) *int64 { return &v }
	rec := Record{
		At: time.Unix(1, 0).UTC(), Watch: "w", Database: "db", Branch: "main", Fresh: true,
		StorageUtil: f(0.5), StorageAvailableBytes: i(1 << 30), StorageCapacityBytes: i(1 << 31),
		StorageUtilWorst: f(0.75), StorageAvailableWorstBytes: i(1 << 29), StorageCapacityWorstBytes: i(1 << 31),
	}
	b, err := EncodeRecord(rec)
	if err != nil {
		t.Fatalf("EncodeRecord: %v", err)
	}
	for _, want := range []string{
		`"storage_util_worst":0.75`,
		`"storage_available_worst_bytes":536870912`,
		`"storage_capacity_worst_bytes":2147483648`,
	} {
		if !strings.Contains(string(b), want) {
			t.Errorf("encoded record omits %s\ngot: %s", want, b)
		}
	}

	// Independently observable: the primary can resolve while the per-pod
	// reduction cannot. A record carrying only the primary must still encode
	// the worst-pod keys as explicit nulls, never as zeros that would read as
	// "the volume is empty".
	onlyPrimary := Record{
		At: time.Unix(1, 0).UTC(), Watch: "w", Database: "db", Branch: "main", Fresh: true,
		StorageUtil: f(0.5),
	}
	b2, err := EncodeRecord(onlyPrimary)
	if err != nil {
		t.Fatalf("EncodeRecord(onlyPrimary): %v", err)
	}
	if !strings.Contains(string(b2), `"storage_util_worst":null`) {
		t.Errorf("unobserved worst-pod storage must encode as explicit null, not be omitted or zeroed\ngot: %s", b2)
	}
}
