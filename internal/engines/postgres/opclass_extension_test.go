// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// The message is the REAL one, captured from a live target on 2026-09-10
// migrating a schema whose EXCLUDE constraint uses btree_gist into a database
// that did not have the extension. Pinning the measured wording rather than an
// invented one is the point: this classifier's only job is to agree with what
// PostgreSQL actually emits.
const measuredOpclassMessage = `data type integer has no default operator class for access method "gist"`

func pgErr(code, msg string) error {
	return fmt.Errorf("postgres: create table %q: %w", "bg_src", &pgconn.PgError{Code: code, Message: msg})
}

func TestAnnotateMissingOpclass(t *testing.T) {
	t.Parallel()

	t.Run("the measured btree_gist failure becomes actionable", func(t *testing.T) {
		t.Parallel()
		err := annotateMissingOpclass(pgErr("42704", measuredOpclassMessage), `creating table "bg_src"`)
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeSchemaExtensionNotEnabled {
			t.Fatalf("carried code %v (coded=%v), want %q", ce, ok, sluicecode.CodeSchemaExtensionNotEnabled)
		}
		// The whole reason this exists: PostgreSQL's own sentence names
		// neither the extension nor the remedy.
		for _, want := range []string{"btree_gist", "CREATE EXTENSION btree_gist;"} {
			if !strings.Contains(err.Error()+ce.Hint, want) {
				t.Errorf("the refusal does not name %q, so it is no more actionable than the driver's: %v", want, err)
			}
		}
		// And the original must survive for anyone reading logs.
		if !strings.Contains(err.Error(), "no default operator class") {
			t.Errorf("the underlying error was dropped: %v", err)
		}
	})

	t.Run("gin maps to btree_gin", func(t *testing.T) {
		t.Parallel()
		err := annotateMissingOpclass(
			pgErr("42704", `data type uuid has no default operator class for access method "gin"`), "creating an index",
		)
		if !strings.Contains(err.Error(), "btree_gin") {
			t.Errorf("gin was not mapped: %v", err)
		}
	})

	t.Run("an access method with no well-known extension is not guessed at", func(t *testing.T) {
		t.Parallel()
		// Naming a specific extension here would be an invention. The
		// message must stay useful without becoming wrong.
		err := annotateMissingOpclass(
			pgErr("42704", `data type point has no default operator class for access method "brin"`), "creating an index",
		)
		if strings.Contains(err.Error(), "btree_gist") || strings.Contains(err.Error(), "btree_gin") {
			t.Errorf("an extension was invented for an access method that has no well-known one: %v", err)
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeSchemaExtensionNotEnabled {
			t.Errorf("the generic case lost its code: %v", err)
		}
		if !strings.Contains(ce.Hint, "brin") || !strings.Contains(ce.Hint, "point") {
			t.Errorf("the generic hint does not carry the access method and type, which is all it has to "+
				"offer: %q", ce.Hint)
		}
	})

	t.Run("every other error passes through untouched", func(t *testing.T) {
		t.Parallel()
		cases := []error{
			nil,
			errors.New("connection reset"),
			// Right SQLSTATE, different 42704 cause — a missing type, which
			// has its own refusal elsewhere and must not be relabelled.
			pgErr("42704", `type "geometry" does not exist`),
			// Right message shape, wrong SQLSTATE.
			pgErr("42P01", measuredOpclassMessage),
		}
		for i, in := range cases {
			got := annotateMissingOpclass(in, "creating something")
			if in == nil {
				if got != nil {
					t.Errorf("case %d: nil became %v", i, got)
				}
				continue
			}
			if _, ok := sluicecode.FromError(got); ok {
				t.Errorf("case %d: an unrelated error was relabelled as a missing extension: %v", i, got)
			}
		}
	})
}
