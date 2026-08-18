// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

// The single named DOMAIN unwrap (Bug 233).
//
// # The rule this encodes
//
// A Postgres DOMAIN is a CONSTRAINT wrapper, not a STORAGE one. The
// server stores, transmits and renders a domain-typed value exactly as
// it stores, transmits and renders its base type; the only things the
// wrapper owns are its NAME and its CHECK constraints. So every
// decision about *how a value is stored, encoded, or landed on a
// target* must dispatch on the unwrapped type — and only the two
// decisions that are genuinely about the wrapper (emit `CREATE DOMAIN`
// on a PG target; translate/WARN about the CHECKs on a MySQL target)
// look at [Domain] itself.
//
// # Why it is one function and not three call sites
//
// Before this existed the unwrap had been re-derived, one consumer at a
// time, every time a DOMAIN reached a lane that had not met one yet:
// the PG decoder (Bug 122), the MySQL value writer (Bug 122), the
// MySQL DDL emitter, the chunk-reader's PK-orderability check, the
// cursor-trust check, the parquet column builder, the collation
// resolver, the keyless-index target, the type-override mapper, and the
// array-bytes CDC preflight (v0.116.0). Each was correct; the class was
// not closed, because "did THIS consumer remember?" is a question with
// one answer per consumer. Bug 233 is what the eleventh consumer
// costs: MySQL's LOAD DATA `SET` clause dispatches on the column type
// to decide which values need a `CONVERT(... USING utf8mb4)` re-tag,
// a DOMAIN matched no arm, and every DOMAIN whose base type lands on a
// MySQL JSON column died with the server's own
// `Error 3144 … CHARACTER SET 'binary'` and copied zero rows.
//
// The gate that keeps the class closed is internal/domaingate, which
// walks each target engine's own source for `.Type`-dispatch sites and
// fails on one that neither unwraps nor carries a stated reason.

// UnwrapDomain returns t's STORAGE type: the type a target's storage,
// encoding and value-landing decisions must dispatch on.
//
// For any non-[Domain] type it is the identity. For a [Domain] it is
// the base type, and it unwraps repeatedly so a domain over a domain
// resolves in one call. (Postgres's own `information_schema` reports
// only the IMMEDIATE base of a domain column — one level — NOT the
// ultimate base of a nested domain; verified by the Bug 255 mutation
// run. So the PG schema reader cannot model a `CREATE DOMAIN d2 AS d1`
// nested domain from `information_schema` alone and refuses it loudly by
// name rather than producing a partial wrapper. The repeated unwrap here
// still earns its keep on two paths that CAN carry a genuinely nested
// `Domain.BaseType`: the manifest wire, which round-trips it recursively,
// and the PG CDC reader's `resolveDomainBase`, which walks `pg_type.
// typbasetype` N levels off the wire.) The loop cannot spin: [Domain]
// holds its base as an interface VALUE, so a self-referential domain is
// not constructible.
//
// A [Domain] with a nil BaseType is MALFORMED, and this returns the
// Domain unchanged rather than nil. That is deliberate: every existing
// consumer that can meet one already refuses it loudly and by name
// (`DOMAIN %q has nil BaseType` in the PG decoder, the PG DDL emitter
// and the MySQL DDL emitter), and handing those sites a nil Type would
// swap a named refusal for a nil-dispatch fall-through. Returning the
// wrapper keeps the loud path loud and keeps every `switch` arm below
// unmatched, which is the same "no arm matched" answer they give today.
func UnwrapDomain(t Type) Type {
	for {
		dom, isDomain := t.(Domain)
		if !isDomain || dom.BaseType == nil {
			return t
		}
		t = dom.BaseType
	}
}

// DomainOf reports the [Domain] wrapper a column's declared type
// carries, if any — the complement of [UnwrapDomain], for the two
// consumers that are genuinely about the wrapper rather than the
// storage (a PG target's `CREATE DOMAIN` emit; a MySQL target's
// CHECK-translation and CHECK-drop WARN).
//
// It reads [Column.Type] only. A retarget or `--type-override` that
// REPLACED a domain-typed column's type parks the pre-rewrite type in
// [Column.SourceColumnType]; this deliberately does not consult that
// field, because a column whose Type is no longer a domain no longer
// declares one and re-emitting its CHECKs from provenance would be a
// different decision than the one those two consumers make today.
func DomainOf(c *Column) (Domain, bool) {
	if c == nil {
		return Domain{}, false
	}
	dom, ok := c.Type.(Domain)
	return dom, ok
}
