// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"unicode/utf8"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// identityEngine is a stubEngine that DOES describe its identity — the
// shape every registered engine has since audit 2026-09-15 F-1. The
// bare stubEngine, which does not, is the other half of the dispatch and
// is exercised beside it.
type identityEngine struct {
	stubEngine

	name string
	id   ir.SourceIdentity
}

func (e identityEngine) Name() string { return e.name }

func (e identityEngine) SourceIdentity(string) ir.SourceIdentity { return e.id }

// identityOf is the test's shorthand for an identity string built the
// way the production renderer builds one. It calls the REAL framing
// function, so there is no test-local duplicate to drift (the previous
// spelling of this file carried one, because the renderer took a DSN and
// a hostile name could not be fed through a URL parser intact).
func identityOf(engine, database, schema string) string {
	return renderSourceIdentityFields(engine, ir.SourceIdentity{Database: database, Schema: schema})
}

// The DISPATCH: the orchestrator asks the ENGINE, and reports honestly
// when the engine's answer carries no discriminator.
//
// This replaces a matrix over DSN shapes the pipeline used to parse
// itself. That matrix could only ever grade the shapes someone had
// remembered to add — and the finding was precisely the shapes nobody
// had (SQLite files, D1 databases, flat files, mydumper dumps, and the
// `user:pw@/db` MySQL spelling all rendered ONE identity). The per-DSN
// grading now lives against REAL engines, in
// docsync.TestSourceIdentityIsInjectiveThroughRealDSNs.
func TestRenderSourceIdentity_AsksTheEngine(t *testing.T) {
	t.Parallel()

	t.Run("an engine that describes its identity", func(t *testing.T) {
		t.Parallel()
		e := identityEngine{name: "postgres", id: ir.SourceIdentity{Database: "src_db", Schema: "tenant_a"}}
		got, discriminating := renderSourceIdentity(e, "postgres://u:p@host/src_db?schema=tenant_a")
		if want := identityOf("postgres", "src_db", "tenant_a"); got != want {
			t.Errorf("identity = %q; want %q", got, want)
		}
		if !discriminating {
			t.Error("an identity naming a database reported as UNDISCRIMINATED; the door would warn on every run")
		}
	})

	t.Run("an engine that describes a dataset but no namespace", func(t *testing.T) {
		t.Parallel()
		e := identityEngine{name: "sqlite", id: ir.SourceIdentity{Database: "app.db"}}
		got, discriminating := renderSourceIdentity(e, "./app.db")
		if want := identityOf("sqlite", "app.db", ""); got != want {
			t.Errorf("identity = %q; want %q", got, want)
		}
		if !discriminating {
			t.Error("a flat-namespace engine naming a file reported as UNDISCRIMINATED")
		}
	})

	t.Run("an engine that implements nothing", func(t *testing.T) {
		t.Parallel()
		// The fallback is deliberately NOT a DSN guess: the identity
		// carries the engine name and says so.
		got, discriminating := renderSourceIdentity(stubEngine{}, "postgres://u:p@host/src_db")
		if want := identityOf("stub", "", ""); got != want {
			t.Errorf("identity = %q; want %q — the pipeline must not parse a DSN it was never taught", got, want)
		}
		if discriminating {
			t.Error(
				"an engine-name-only identity reported as discriminating; the door would then read the " +
					"absence of a refusal as proof that the source matched", //nolint:gocritic // one message, one line
			)
		}
	})

	t.Run("a describer that cannot parse this DSN", func(t *testing.T) {
		t.Parallel()
		e := identityEngine{name: "sqlite", id: ir.SourceIdentity{}}
		got, discriminating := renderSourceIdentity(e, "")
		if want := identityOf("sqlite", "", ""); got != want {
			t.Errorf("identity = %q; want %q", got, want)
		}
		if discriminating {
			t.Error("a zero-value engine answer reported as discriminating")
		}
	})
}

// The comparison matrix. Each axis of the identity must discriminate on
// its own, and the HOST must not discriminate at all — that last cell is
// the one the mutation run breaks in the second direction. The cells are
// built from identity FIELDS now, because the DSN→fields half belongs to
// the engines and is graded there.
func TestRefuseForeignSourceOnResume_Matrix(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		recorded, live string
		discriminating bool
		wantRefusal    bool
		wantInMessage  []string
	}{
		{
			name:           "identical source resumes",
			recorded:       identityOf("postgres", "src_db", "public"),
			live:           identityOf("postgres", "src_db", "public"),
			discriminating: true,
		},
		{
			name:           "engine matches, DATABASE differs",
			recorded:       identityOf("postgres", "db_one", "public"),
			live:           identityOf("postgres", "db_two", "public"),
			discriminating: true,
			wantRefusal:    true,
			// This is the auto-derived-id collision (Scenario D): one host,
			// two databases, no typed --migration-id anywhere.
			wantInMessage: []string{"db_one", "db_two", "DIFFERENT source"},
		},
		{
			name:           "database matches, ENGINE differs",
			recorded:       identityOf("postgres", "src_db", "public"),
			live:           identityOf("mysql", "src_db", ""),
			discriminating: true,
			wantRefusal:    true,
			wantInMessage:  []string{"postgres", "mysql"},
		},
		{
			name:           "engine and database match, SCHEMA differs",
			recorded:       identityOf("postgres", "src_db", "tenant_a"),
			live:           identityOf("postgres", "src_db", "tenant_b"),
			discriminating: true,
			wantRefusal:    true,
			wantInMessage:  []string{"tenant_a", "tenant_b"},
		},
		{
			// A SQLite source: two files, no host anywhere. Before F-1 the
			// orchestrator rendered ONE identity for every SQLite source and
			// this resume was silently admitted.
			name:           "two SQLite FILES are two sources",
			recorded:       identityOf("sqlite", "one.db", ""),
			live:           identityOf("sqlite", "two.db", ""),
			discriminating: true,
			wantRefusal:    true,
			wantInMessage:  []string{"one.db", "two.db"},
		},
		{
			// THE DNS-MOVE CELL, at the level the door now sees it: the
			// host never enters the identity, so a run against a renamed
			// host, a failover, or a replica renders the SAME string.
			// ADR-0015 requires that resume to proceed.
			name:           "the host is absent from the identity, so a DNS move still resumes",
			recorded:       identityOf("postgres", "src_db", "public"),
			live:           identityOf("postgres", "src_db", "public"),
			discriminating: true,
		},
		{
			// An engine whose DSN names no dataset. It still compares (the
			// engine half is real evidence) but it warns first — pinned by
			// TestRefuseForeignSourceOnResume_WarnsWhenUndiscriminated.
			name:     "undiscriminated identity that MATCHES proceeds",
			recorded: identityOf("stub", "", ""),
			live:     identityOf("stub", "", ""),
		},
		{
			name:          "undiscriminated identity still refuses a different ENGINE",
			recorded:      identityOf("postgres", "", ""),
			live:          identityOf("mysql", "", ""),
			wantRefusal:   true,
			wantInMessage: []string{"postgres", "mysql"},
		},
		{
			// A state row written before the column existed. WARNs and
			// proceeds; refusing would strand every in-flight migration on
			// upgrade.
			name:           "legacy state carries no recorded identity",
			recorded:       "",
			live:           identityOf("postgres", "src_db", "public"),
			discriminating: true,
		},
		{
			// The zero-value caller (sync recording context, tests).
			name:     "caller supplies no live identity",
			recorded: identityOf("postgres", "src_db", "public"),
			live:     "",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			err := refuseForeignSourceOnResume(context.Background(), "mig-1", c.recorded, c.live, c.discriminating)
			if !c.wantRefusal {
				if err != nil {
					t.Fatalf("refused a resume that must proceed: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("resume was ALLOWED against a foreign source; it would exit 0 having copied nothing")
			}
			var coded *sluicecode.CodedError
			if !errors.As(err, &coded) {
				t.Fatalf("refusal carries no SLUICE-E code: %v", err)
			}
			if coded.Code != sluicecode.CodeResumeSourceMismatch {
				t.Errorf("code = %q; want %q", coded.Code, sluicecode.CodeResumeSourceMismatch)
			}
			// The message must NAME both identities — an operator cannot
			// act on "they differ".
			for _, want := range c.wantInMessage {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal does not name %q; got: %v", want, err)
				}
			}
			if coded.Hint == "" {
				t.Error("refusal carries no remedy hint")
			}
		})
	}
}

// The absence of a refusal must never be readable as proof. When the
// live identity carries no dataset, the door says so with a grep-stable
// marker — otherwise an operator sees a clean resume and concludes the
// source was checked, when all that was checked is the engine name.
//
// Not parallel: it installs a default slog handler.
func TestRefuseForeignSourceOnResume_WarnsWhenUndiscriminated(t *testing.T) {
	capture := func(t *testing.T, recorded, live string, discriminating bool) string {
		t.Helper()
		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		t.Cleanup(func() { slog.SetDefault(prev) })
		_ = refuseForeignSourceOnResume(context.Background(), "mig-1", recorded, live, discriminating)
		return buf.String()
	}

	undiscriminated := identityOf("stub", "", "")
	if got := capture(t, undiscriminated, undiscriminated, false); !strings.Contains(got, sourceIdentityUndiscriminatedMarker) {
		t.Errorf("a matching but UNDISCRIMINATED resume logged no %s warning, so an operator reads the clean "+
			"run as proof the source matched. Got:\n%s", sourceIdentityUndiscriminatedMarker, got)
	}

	// A real discriminator must NOT drag the warning along, or the marker
	// stops meaning anything and operators learn to ignore it.
	full := identityOf("postgres", "src_db", "public")
	if got := capture(t, full, full, true); strings.Contains(got, sourceIdentityUndiscriminatedMarker) {
		t.Errorf("an identity naming a database warned %s anyway:\n%s", sourceIdentityUndiscriminatedMarker, got)
	}

	// And the legacy-state marker still fires on its own axis.
	if got := capture(t, "", full, true); !strings.Contains(got, sourceIdentityUnrecordedMarker) {
		t.Errorf("legacy state logged no %s warning:\n%s", sourceIdentityUnrecordedMarker, got)
	}
}

// The encoding is a codec: it is persisted and read back, so it owes the
// codec treatment. Two properties, both adversarial.
//
//   - INJECTIVE: a database or schema name carrying the separator or the
//     key syntax must not alias a different triple. A naive
//     `engine=x;database=y` concatenation fails exactly here.
//   - STORABLE: the rendered value must be valid, NUL-free UTF-8 on
//     every input, because PostgreSQL refuses an invalid byte sequence
//     in a TEXT column (SQLSTATE 22021) and MySQL in strict mode refuses
//     it with Error 1366 — which is how the neighbouring last_error
//     column silently lost its writes.
//
// SCOPE, stated so the name is not read as broader than the truth: this
// grades the FRAMING — fields in, string out. That two different DSNs
// produce two different FIELDS is the engines' half, and it is graded
// against real engines by docsync.TestSourceIdentityIsInjectiveThroughRealDSNs.
// Both halves are needed: an injective framing over a colliding
// extractor is exactly the vacuous door audit 2026-09-15 F-1 found.
func TestRenderSourceIdentity_InjectiveAndStorable(t *testing.T) {
	t.Parallel()

	hostile := []struct {
		name                     string
		engine, database, schema string
	}{
		{"plain", "postgres", "src_db", "public"},
		{"database containing the field separator", "postgres", `a;database=b`, "public"},
		{"database containing an equals sign", "postgres", "a=b", "public"},
		{"database containing a double quote", "postgres", `a"b`, "public"},
		{"database containing a backslash", "postgres", `a\b`, "public"},
		{"database containing a newline", "postgres", "a\nb", "public"},
		{"database containing a NUL", "postgres", "a\x00b", "public"},
		{"database that is invalid UTF-8", "postgres", "a\xffb", "public"},
		{"database with multi-byte runes", "postgres", "数据库", "public"},
		{"engine containing the separator", `a;database=b`, "src_db", "public"},
		{"schema containing the separator", "postgres", "src_db", `a;schema=b`},
		{"schema that is invalid UTF-8", "postgres", "src_db", "s\xffb"},
		{"the field-shifting pair", "postgres", `a";schema="b`, "public"},
		// A file path is a legitimate database value for the file engines,
		// and a Windows one carries backslashes.
		{"a Windows file path", "sqlite", `C:\data\app.db`, ""},
	}

	seen := map[string]string{}
	for _, h := range hostile {
		got := renderSourceIdentityFields(h.engine, ir.SourceIdentity{Database: h.database, Schema: h.schema})
		// Storable on both engines.
		if !utf8.ValidString(got) {
			t.Errorf("%s: rendered identity is not valid UTF-8: %q", h.name, got)
		}
		if strings.ContainsRune(got, 0) {
			t.Errorf("%s: rendered identity carries a NUL byte: %q", h.name, got)
		}
		// Injective: distinct triples must produce distinct strings.
		if prev, dup := seen[got]; dup {
			t.Errorf("%s: renders identically to %s — the encoding is NOT injective, so two different "+
				"sources compare equal and a foreign resume is admitted: %q", h.name, prev, got)
		}
		seen[got] = h.name
	}

	// Anti-vacuity: the hostile set must actually have exercised the
	// aliasing shape, not just a list of tame names.
	if len(seen) != len(hostile) {
		t.Fatalf("collapsed %d hostile names into %d renderings", len(hostile), len(seen))
	}
}

// The persisted column, on BOTH engines, in BOTH halves that matter:
// the CREATE TABLE (a fresh control table has it) and the idempotent
// ALTER (a control table an older binary created gains it). A missing
// ALTER is the expensive half — Read projects the column, so its absence
// fails every migrate against a pre-existing control table.
//
// This gate reaches the two shipping MigrationStateStore engines and no
// others, which is the whole population: they are the only implementors
// of the store (a target engine without one falls back to the
// non-resumable path and never reads this column).
func TestSourceIdentityColumnDeclaredOnBothEngines(t *testing.T) {
	t.Parallel()

	cases := []struct {
		path          string
		createDecl    string
		idempotentAdd string
	}{
		{
			path:          "../engines/postgres/migration_state.go",
			createDecl:    "source_identity TEXT",
			idempotentAdd: "ADD COLUMN IF NOT EXISTS source_identity TEXT NULL",
		},
		{
			path:          "../engines/mysql/migration_state.go",
			createDecl:    "source_identity TEXT",
			idempotentAdd: `ensureHeaderColumn(ctx, "source_identity", "TEXT NULL")`,
		},
	}

	checked := 0
	for _, c := range cases {
		src, err := os.ReadFile(c.path)
		if err != nil {
			t.Fatalf("read %s: %v", c.path, err)
		}
		text := string(src)
		if !strings.Contains(text, c.createDecl) {
			t.Errorf("%s: CREATE TABLE does not declare %q — a fresh control table would have no place to "+
				"record the source, so every --resume would WARN as if the state were legacy", c.path, c.createDecl)
		} else {
			checked++
		}
		if !strings.Contains(text, c.idempotentAdd) {
			t.Errorf("%s: no idempotent column add (%q) — a control table an older binary created never gains "+
				"the column, and the header projection then fails every migrate against that target",
				c.path, c.idempotentAdd)
		} else {
			checked++
		}

		// SET-ONCE: the column is in the INSERT list and must NOT be in
		// the upsert's SET list. If it were, a resume against a FOREIGN
		// source would overwrite the very evidence that refuses it — the
		// door would hold on the first re-run and silently open on the
		// second.
		if !strings.Contains(text, "last_error, source_identity") {
			t.Errorf("%s: UpsertHeader does not carry source_identity in its INSERT column list", c.path)
		} else {
			checked++
		}
		for _, setSpelling := range []string{
			"source_identity = EXCLUDED.source_identity",
			`source_identity = " + upsert.newRowRef("source_identity")`,
		} {
			if strings.Contains(text, setSpelling) {
				t.Errorf("%s: UpsertHeader UPDATES source_identity (%q). It must be set-once: a resume from a "+
					"foreign source would rewrite the recorded identity and the refusal would stop firing",
					c.path, setSpelling)
			}
		}
	}
	// Anti-vacuity: 2 engines x (create + alter + insert-list) = 6.
	if checked != 6 {
		t.Errorf("the walk confirmed %d of 6 declarations; the DDL shape changed and this gate is now "+
			"checking less than its name implies", checked)
	}
}

// The door, through loadOrInitState — the function a real migrate calls.
// Both --resume arms are covered here, because both are ways to exit 0
// having copied nothing.
func TestLoadOrInitState_RefusesForeignSource(t *testing.T) {
	t.Parallel()

	recorded := identityOf("postgres", "db_one", "public")
	live := identityOf("postgres", "db_two", "public")

	for _, phase := range []ir.MigrationPhase{ir.MigrationPhaseComplete, ir.MigrationPhaseBulkCopy} {
		t.Run(string(phase), func(t *testing.T) {
			t.Parallel()
			store := newFakeStateStore()
			store.rows["m1"] = ir.MigrationState{
				MigrationID: "m1", Phase: phase, SourceIdentity: recorded,
			}
			rc := resumeContext{
				store: store, migrationID: "m1", enabled: true,
				sourceIdentity: live, sourceIdentityDiscriminating: true,
			}

			_, exitClean, err := loadOrInitState(context.Background(), rc, true /*resume*/, false)
			if err == nil {
				t.Fatalf("phase %s: resume ALLOWED against a foreign source (exitClean=%v); the run would "+
					"exit 0 having copied nothing", phase, exitClean)
			}
			var coded *sluicecode.CodedError
			if !errors.As(err, &coded) || coded.Code != sluicecode.CodeResumeSourceMismatch {
				t.Fatalf("phase %s: want %s, got %v", phase, sluicecode.CodeResumeSourceMismatch, err)
			}
		})
	}

	t.Run("matching identity resumes", func(t *testing.T) {
		t.Parallel()
		store := newFakeStateStore()
		store.rows["m1"] = ir.MigrationState{
			MigrationID: "m1", Phase: ir.MigrationPhaseBulkCopy, SourceIdentity: recorded,
		}
		rc := resumeContext{
			store: store, migrationID: "m1", enabled: true,
			sourceIdentity: recorded, sourceIdentityDiscriminating: true,
		}
		if _, _, err := loadOrInitState(context.Background(), rc, true, false); err != nil {
			t.Fatalf("refused a resume against the SAME source: %v", err)
		}
	})

	t.Run("legacy state without identity resumes", func(t *testing.T) {
		t.Parallel()
		store := newFakeStateStore()
		store.rows["m1"] = ir.MigrationState{MigrationID: "m1", Phase: ir.MigrationPhaseBulkCopy}
		rc := resumeContext{
			store: store, migrationID: "m1", enabled: true,
			sourceIdentity: live, sourceIdentityDiscriminating: true,
		}
		if _, _, err := loadOrInitState(context.Background(), rc, true, false); err != nil {
			t.Fatalf("refused a resume of state written before the column existed: %v", err)
		}
	})

	t.Run("--reset-target-data is not a resume and is not graded", func(t *testing.T) {
		t.Parallel()
		store := newFakeStateStore()
		store.rows["m1"] = ir.MigrationState{
			MigrationID: "m1", Phase: ir.MigrationPhaseComplete, SourceIdentity: recorded,
		}
		rc := resumeContext{
			store: store, migrationID: "m1", enabled: true,
			sourceIdentity: live, sourceIdentityDiscriminating: true,
		}
		// Nothing is adopted: the row is deleted and the copy re-runs in
		// full, so a differing source is not a mis-attribution.
		if _, _, err := loadOrInitState(context.Background(), rc, false, true /*resetting*/); err != nil {
			t.Fatalf("resetting run refused: %v", err)
		}
	})
}

// A fresh run RECORDS the identity — (a) of the required behaviours, and
// the precondition for every other cell: without this write the door has
// nothing to compare on the next run.
func TestLoadOrInitState_FreshRunRecordsSourceIdentity(t *testing.T) {
	t.Parallel()

	live := identityOf("postgres", "src_db", "public")
	store := newFakeStateStore()
	rc := resumeContext{
		store: store, migrationID: "fresh", enabled: true,
		sourceIdentity: live, sourceIdentityDiscriminating: true,
	}

	if _, _, err := loadOrInitState(context.Background(), rc, false, false); err != nil {
		t.Fatalf("loadOrInitState: %v", err)
	}
	got, ok := store.get("fresh")
	if !ok {
		t.Fatal("no state row persisted")
	}
	if got.SourceIdentity != live {
		t.Errorf("persisted SourceIdentity = %q; want %q", got.SourceIdentity, live)
	}
}
