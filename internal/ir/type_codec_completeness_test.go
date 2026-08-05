// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The ir.Type ↔ schemaTypeEnvelope codec-completeness gate
// (roadmap item 117; the sibling the columnWire gate's own §4 proposal
// stopped one level short of).
//
// [Column] got a reflective gate — TestColumnCodecCoversEveryField and
// TestColumnCodecRoundTripsEveryWireField — because its MarshalJSON is
// hand-written over an enumerated struct, so a field can be added and simply
// not reach the wire. [MarshalType] / [UnmarshalType] have EXACTLY the same
// shape one level down: a hand-written tagged-union codec over an enumerated
// envelope, with a field-by-field assignment in each direction. And they had
// no reflective gate at all — only TestMarshalType_RoundTrip, a curated list
// of hand-written fixtures, which is the same failure mode the columnWire gate
// was built to replace: it too needs remembering.
//
// What that permits: a field added to a concrete [Type] and wired into
// MarshalType but NOT UnmarshalType (or the reverse) is silently dropped by
// every path that round-trips a schema through JSON — the backup manifest, so
// `restore` rebuilds the column with a narrower type, and the ADR-0049 schema
// history, so a CDC schema-version resolve returns the wrong shape. Nothing
// failed. That is exactly how item 117's own width could have shipped
// half-wired.
//
// The gate is bidirectional, which is the half that keeps getting missed. A
// field NOT in the maps below must survive the round trip; a field IN them
// must NOT — so wiring an exempted field up later fails here and forces the
// map entry to be removed, rather than leaving a stale exemption that reads
// as coverage.

package ir

import (
	"reflect"
	"testing"
)

// envelopeExempt names Type fields that deliberately do NOT ride the type
// envelope, each with the reason. A field here is a DECISION.
//
// Keys are "ConcreteType.Field".
var envelopeExempt = map[string]string{
	"Enum.Collation": "the field's own doc states it: the MySQL-family collation whose `=` a " +
		"filtered --where must reproduce, consumed ONLY by the row-predicate resolver, which " +
		"compiles from a fresh schema read and therefore always has the live collation. DDL " +
		"emission is unaffected, so a decoded schema not carrying it loses nothing a consumer " +
		"could act on",

	// ADJUDICATED 2026-08-04 (roadmap item 121). Promoted from
	// envelopeKnownGap after the three checks the gap entry demanded, each
	// run rather than assumed:
	//
	//  1. ONE consumer. `Determinism` reaches exactly one call site —
	//     rowpredicate/predicate.go's ResolveStringEquality — via the
	//     engines' CollationResolver implementations.
	//  2. NO DDL consumer. Neither the Postgres, MySQL nor SQLite emitter
	//     mentions it, so a decoded schema that reads back Unknown emits
	//     byte-identical DDL. This is the check that would have made it a
	//     MUST-WIRE: had the emitter rendered a non-deterministic collation
	//     from it, dropping it on restore would change `=` semantics on the
	//     restored column.
	//  3. The consumer cannot receive a DECODED schema. The predicate is
	//     compiled in buildWhereCDCFilter from the Streamer's LIVE source
	//     schema read, on a path restricted to MySQL and Postgres sources;
	//     no restore or manifest path compiles a predicate.
	//
	// So this is Enum.Collation's argument, verified for these fields
	// rather than inherited from them. The reflect.DeepEqual concern the
	// gap entry raised (SchemaSignature.Equal, diffRenameColumnIR) resolves
	// the same way Enum.Collation's own doc resolves it: a decoded and a
	// fresh-read column disagreeing can only ADD an advisory delta, never
	// hide a decode-affecting change, and no data is mutated on either path.
	"Char.Determinism":    determinismExemptReason,
	"Varchar.Determinism": determinismExemptReason,
	"Text.Determinism":    determinismExemptReason,
}

const determinismExemptReason = "set by the PG reader from collisdeterministic and read back as " +
	"CollationDeterminismUnknown. Adjudicated as a DECISION (item 121): its only consumer is the " +
	"--where string-equality resolver, no engine's DDL emitter consults it, and the predicate is " +
	"compiled from a LIVE schema read on a path no decoded manifest schema reaches — so a decoded " +
	"schema not carrying it loses nothing a consumer could act on. See envelopeExempt's comment " +
	"for the three checks this rests on"

// envelopeKnownGap names Type fields that do NOT survive the round trip and
// are NOT adjudicated decisions — they are acknowledged holes, left in place
// deliberately because closing one is a fingerprint change and therefore its
// own item.
//
// The distinction from envelopeExempt is the point: an exemption says "this is
// right", a gap says "this is a finding nobody has closed". Collapsing the two
// is how a gate comes to certify its own blind spot.
//
// EMPTY as of item 121, and that is a real state rather than a defaulted one:
// the four entries this gate found when it was built (Enum.TypeName and the
// three *.Determinism fields) have all been closed — the first by WIRING it
// (restore was re-creating enums under a synthesized name), the other three by
// ADJUDICATING them into envelopeExempt above. A future finding belongs here,
// with its reason, until someone does the same for it.
//
// Keys are "ConcreteType.Field".
var envelopeKnownGap = map[string]string{}

// TestTypeEnvelopeRoundTripsEveryExportedField is the gate. It drives
// [AllTypes] — itself held exhaustive by TestAllTypes_CoversEveryIsTypeImplementor
// — so a NEW concrete Type is covered the moment it is registered, and a new
// FIELD on an existing one is covered with no edit at all.
func TestTypeEnvelopeRoundTripsEveryExportedField(t *testing.T) {
	for _, base := range AllTypes() {
		name := reflect.TypeOf(base).Name()
		t.Run(name, func(t *testing.T) {
			rt := reflect.TypeOf(base)
			if rt.Kind() != reflect.Struct {
				t.Fatalf("ir.Type %s is not a struct; teach this gate about it", name)
			}

			// Every exported field NON-ZERO. A field dropped by either half of
			// the codec comes back ZERO, so a zero-valued fixture cannot tell
			// a working round trip from a broken one — the vacuity that made
			// the columnWire gate's first cut useless.
			filled := reflect.New(rt).Elem()
			for i := range rt.NumField() {
				f := rt.Field(i)
				if f.PkgPath != "" {
					continue // unexported
				}
				filled.Field(i).Set(nonZeroValue(t, f.Type, name+"."+f.Name))
			}

			src, ok := filled.Interface().(Type)
			if !ok {
				t.Fatalf("%s does not implement ir.Type by value", name)
			}
			b, err := MarshalType(src)
			if err != nil {
				t.Fatalf("MarshalType(%s): %v\n\nEvery type in ir.AllTypes() must have a case in "+
					"MarshalType — a type that falls to the default arm cannot be persisted in a backup "+
					"manifest or a schema-history entry at all.", name, err)
			}
			back, err := UnmarshalType(b)
			if err != nil {
				t.Fatalf("UnmarshalType(%s): %v\nwire was: %s", name, err, b)
			}
			got := reflect.ValueOf(back)
			if got.Type() != rt {
				t.Fatalf("%s round-tripped to a DIFFERENT concrete type %s — the Kind discriminator "+
					"and the decode arm disagree", name, got.Type())
			}

			for i := range rt.NumField() {
				f := rt.Field(i)
				if f.PkgPath != "" {
					continue
				}
				key := name + "." + f.Name
				want := filled.Field(i).Interface()
				have := got.Field(i).Interface()
				reason, exempt := envelopeExempt[key]
				if !exempt {
					reason, exempt = envelopeKnownGap[key]
				}

				if !exempt {
					if !reflect.DeepEqual(want, have) {
						t.Errorf("%s did not survive the type-envelope round trip: got %#v, want %#v.\n"+
							"wire was: %s\n\n"+
							"A field wired into only one of MarshalType/UnmarshalType — or into neither — "+
							"fails exactly this way, and it is INVISIBLE outside this test: the reader sets "+
							"it and the emitter honours it, and it is dropped in between by every path that "+
							"round-trips a schema through JSON (backup manifests, so `restore` rebuilds the "+
							"column without it; the ADR-0049 schema history, so a CDC schema resolve returns "+
							"the wrong shape).\n\n"+
							"Either wire it into schemaTypeEnvelope + MarshalType + UnmarshalType (all "+
							"three), or add %q to envelopeExempt/envelopeKnownGap with the reason.",
							key, have, want, b, key)
					}
					continue
				}

				// The other direction. An exemption is a claim that the field
				// does NOT round-trip; if it now does, the claim is stale and
				// the entry must go — otherwise the map quietly excuses a
				// field the codec already covers.
				if reason == "" {
					t.Errorf("%s is exempt from the type envelope with an EMPTY reason; an exemption "+
						"without a reason is indistinguishable from an oversight", key)
				}
				if !reflect.DeepEqual(have, reflect.Zero(f.Type).Interface()) {
					t.Errorf("%s is listed as NOT riding the type envelope, but it survived the round "+
						"trip (got %#v). Remove it from envelopeExempt/envelopeKnownGap — a stale entry "+
						"reads as a considered decision and hides the field from this gate forever.",
						key, have)
				}
			}
		})
	}
}

// TestTypeEnvelopeMapsAreLive is the anti-staleness floor on the two maps
// themselves: an entry naming a field that no longer exists is a leftover that
// would silently excuse a FUTURE field of the same name.
func TestTypeEnvelopeMapsAreLive(t *testing.T) {
	live := make(map[string]bool)
	for _, base := range AllTypes() {
		rt := reflect.TypeOf(base)
		for i := range rt.NumField() {
			if rt.Field(i).PkgPath == "" {
				live[rt.Name()+"."+rt.Field(i).Name] = true
			}
		}
	}
	for _, m := range []map[string]string{envelopeExempt, envelopeKnownGap} {
		for key := range m {
			if !live[key] {
				t.Errorf("envelope exemption %q names a field that does not exist on any ir.Type — "+
					"remove it, or it will silently excuse a future field of that name", key)
			}
		}
	}
}

// nonZeroValue builds a distinctive non-zero value of ft. It fails LOUDLY on a
// kind it does not know rather than returning the zero value, because a silent
// zero here would re-open exactly the vacuity this gate exists to close.
func nonZeroValue(t *testing.T, ft reflect.Type, path string) reflect.Value {
	t.Helper()
	switch ft.Kind() {
	case reflect.Bool:
		return reflect.ValueOf(true).Convert(ft)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return reflect.ValueOf(int64(3)).Convert(ft)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return reflect.ValueOf(uint64(3)).Convert(ft)
	case reflect.String:
		return reflect.ValueOf("x").Convert(ft)
	case reflect.Slice:
		out := reflect.MakeSlice(ft, 1, 1)
		out.Index(0).Set(nonZeroValue(t, ft.Elem(), path+"[0]"))
		return out
	case reflect.Struct:
		out := reflect.New(ft).Elem()
		for i := range ft.NumField() {
			if ft.Field(i).PkgPath == "" {
				out.Field(i).Set(nonZeroValue(t, ft.Field(i).Type, path+"."+ft.Field(i).Name))
			}
		}
		return out
	case reflect.Interface:
		// The only interface any Type nests is Type itself (Array.Element,
		// Domain.BaseType). A leaf with no nested type of its own keeps the
		// recursion finite.
		if ft == reflect.TypeOf((*Type)(nil)).Elem() {
			return reflect.ValueOf(Type(Integer{Width: 32}))
		}
	}
	t.Fatalf("%s has type %s, which this gate does not know how to fill with a non-zero value. "+
		"Teach nonZeroValue about it — do NOT leave it zero, or the round-trip assertion for that "+
		"field becomes vacuous (a dropped field also comes back zero).", path, ft)
	return reflect.Value{}
}
