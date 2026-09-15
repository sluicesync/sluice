// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The DSN forms the identity is derived from. One cell per form
// [redactedHost] accepts, because a form this extractor does not read
// renders every such source as the same identity — which is the silent
// direction.
func TestRenderSourceIdentity_DSNForms(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		engine, dsn  string
		wantDatabase string
		wantSchema   string
	}{
		{
			name: "postgres URI", engine: "postgres",
			dsn:          "postgres://u:p@host:5432/src_db?sslmode=disable",
			wantDatabase: "src_db", wantSchema: "public",
		},
		{
			name: "postgres URI with explicit schema", engine: "postgres",
			dsn:          "postgres://u:p@host:5432/src_db?schema=tenant_a&sslmode=disable",
			wantDatabase: "src_db", wantSchema: "tenant_a",
		},
		{
			name: "postgresql:// alias", engine: "postgres",
			dsn:          "postgresql://u:p@host/src_db",
			wantDatabase: "src_db", wantSchema: "public",
		},
		{
			name: "libpq KV", engine: "postgres",
			dsn:          "host=localhost port=5432 user=u password=p dbname=src_db",
			wantDatabase: "src_db", wantSchema: "public",
		},
		{
			name: "libpq KV with schema", engine: "postgres",
			dsn:          "host=localhost dbname=src_db schema=tenant_b",
			wantDatabase: "src_db", wantSchema: "tenant_b",
		},
		{
			// A password containing '@' must not make this look like a
			// MySQL DSN — the protocol section is the tell, not the '@'.
			name: "libpq KV whose password contains an at-sign", engine: "postgres",
			dsn:          "host=localhost password=a@b dbname=src_db",
			wantDatabase: "src_db", wantSchema: "public",
		},
		{
			name: "mysql tcp", engine: "mysql",
			dsn:          "root:pw@tcp(localhost:3306)/src_db?parseTime=true",
			wantDatabase: "src_db", wantSchema: "",
		},
		{
			name: "mysql unix socket", engine: "mysql",
			dsn:          "root:pw@unix(/var/run/mysqld/mysqld.sock)/src_db",
			wantDatabase: "src_db", wantSchema: "",
		},
		{
			// Underivable is a legitimate value, not a failure: it must
			// render and compare like any other.
			name: "postgres URI naming no database", engine: "postgres",
			dsn:          "postgres://u:p@host:5432/",
			wantDatabase: "", wantSchema: "public",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if got := sourceDSNDatabase(c.dsn); got != c.wantDatabase {
				t.Errorf("sourceDSNDatabase(%q) = %q; want %q", c.dsn, got, c.wantDatabase)
			}
			if got := sourceDSNSchema(c.dsn); got != c.wantSchema {
				t.Errorf("sourceDSNSchema(%q) = %q; want %q", c.dsn, got, c.wantSchema)
			}
			want := `engine="` + c.engine + `";database="` + c.wantDatabase + `";schema="` + c.wantSchema + `"`
			if got := renderSourceIdentity(c.engine, c.dsn); got != want {
				t.Errorf("renderSourceIdentity = %q; want %q", got, want)
			}
		})
	}
}

// The comparison matrix. Each axis of the identity must discriminate on
// its own, and the HOST must not discriminate at all — that last cell is
// the one the mutation run breaks in the second direction.
func TestRefuseForeignSourceOnResume_Matrix(t *testing.T) {
	t.Parallel()

	const pgSrc = "postgres://u:p@host-a:5432/src_db"

	cases := []struct {
		name           string
		recorded, live string
		wantRefusal    bool
		wantInMessage  []string
	}{
		{
			name:     "identical source resumes",
			recorded: renderSourceIdentity("postgres", pgSrc),
			live:     renderSourceIdentity("postgres", pgSrc),
		},
		{
			name:        "engine matches, DATABASE differs",
			recorded:    renderSourceIdentity("postgres", "postgres://u:p@host-a:5432/db_one"),
			live:        renderSourceIdentity("postgres", "postgres://u:p@host-a:5432/db_two"),
			wantRefusal: true,
			// This is the auto-derived-id collision (Scenario D): one host,
			// two databases, no typed --migration-id anywhere.
			wantInMessage: []string{"db_one", "db_two", "DIFFERENT source"},
		},
		{
			name:          "database matches, ENGINE differs",
			recorded:      renderSourceIdentity("postgres", "postgres://u:p@host-a:5432/src_db"),
			live:          renderSourceIdentity("mysql", "root:pw@tcp(host-a:3306)/src_db"),
			wantRefusal:   true,
			wantInMessage: []string{"postgres", "mysql"},
		},
		{
			name:          "engine and database match, SCHEMA differs",
			recorded:      renderSourceIdentity("postgres", "postgres://u:p@host-a:5432/src_db?schema=tenant_a"),
			live:          renderSourceIdentity("postgres", "postgres://u:p@host-a:5432/src_db?schema=tenant_b"),
			wantRefusal:   true,
			wantInMessage: []string{"tenant_a", "tenant_b"},
		},
		{
			// Two spellings of ONE source. Refusing here would be a false
			// refusal on a legitimate resume, which is why the extractor
			// mirrors the engine's own "public" default.
			name:     "explicit schema=public equals an absent schema parameter",
			recorded: renderSourceIdentity("postgres", "postgres://u:p@host-a:5432/src_db?schema=public"),
			live:     renderSourceIdentity("postgres", "postgres://u:p@host-a:5432/src_db"),
		},
		{
			// THE DNS-MOVE CELL. ADR-0015 makes --migration-id the
			// operator's assertion of a stable identity across DNS shifts
			// and host renames, so this must resume. A comparison that
			// included the host would refuse it.
			name:     "same engine and database under a different host alias",
			recorded: renderSourceIdentity("postgres", "postgres://u:p@localhost:5432/src_db"),
			live:     renderSourceIdentity("postgres", "postgres://u:p@127.0.0.1:5432/src_db"),
		},
		{
			name:     "same database reached on a different PORT (failover)",
			recorded: renderSourceIdentity("postgres", "postgres://u:p@host-a:5432/src_db"),
			live:     renderSourceIdentity("postgres", "postgres://u:p@host-a:6432/src_db"),
		},
		{
			// A state row written before the column existed. WARNs and
			// proceeds; refusing would strand every in-flight migration on
			// upgrade.
			name:     "legacy state carries no recorded identity",
			recorded: "",
			live:     renderSourceIdentity("postgres", pgSrc),
		},
		{
			// The zero-value caller (sync recording context, tests).
			name:     "caller supplies no live identity",
			recorded: renderSourceIdentity("postgres", pgSrc),
			live:     "",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			err := refuseForeignSourceOnResume(context.Background(), "mig-1", c.recorded, c.live)
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

// The encoding is a codec: it is persisted and read back, so it owes the
// codec treatment. Two properties, both adversarial.
//
//   - INJECTIVE: a database name carrying the separator or the key
//     syntax must not alias a different triple. A naive
//     `engine=x;database=y` concatenation fails exactly here.
//   - STORABLE: the rendered value must be valid, NUL-free UTF-8 on
//     every input, because PostgreSQL refuses an invalid byte sequence
//     in a TEXT column (SQLSTATE 22021) and MySQL in strict mode refuses
//     it with Error 1366 — which is how the neighbouring last_error
//     column silently lost its writes.
func TestRenderSourceIdentity_InjectiveAndStorable(t *testing.T) {
	t.Parallel()

	hostile := []struct {
		name             string
		engine, database string
	}{
		{"plain", "postgres", "src_db"},
		{"database containing the field separator", "postgres", `a;database=b`},
		{"database containing an equals sign", "postgres", "a=b"},
		{"database containing a double quote", "postgres", `a"b`},
		{"database containing a backslash", "postgres", `a\b`},
		{"database containing a newline", "postgres", "a\nb"},
		{"database containing a NUL", "postgres", "a\x00b"},
		{"database that is invalid UTF-8", "postgres", "a\xffb"},
		{"database with multi-byte runes", "postgres", "数据库"},
		{"engine containing the separator", `a;database=b`, "src_db"},
	}

	seen := map[string]string{}
	for _, h := range hostile {
		got := renderSourceIdentity(h.engine, "postgres://u@host/"+h.database)
		// Storable on both engines.
		if !utf8.ValidString(got) {
			t.Errorf("%s: rendered identity is not valid UTF-8: %q", h.name, got)
		}
		if strings.ContainsRune(got, 0) {
			t.Errorf("%s: rendered identity carries a NUL byte: %q", h.name, got)
		}
		// Injective: render the triple DIRECTLY (not via a DSN, so the
		// URL parser cannot normalise the hostile bytes away) and require
		// distinct triples to produce distinct strings.
		direct := renderSourceIdentityFields(h.engine, h.database, "public")
		if prev, dup := seen[direct]; dup {
			t.Errorf("%s: renders identically to %s — the encoding is NOT injective, so two different "+
				"sources compare equal and a foreign resume is admitted: %q", h.name, prev, direct)
		}
		seen[direct] = h.name
		if !utf8.ValidString(direct) || strings.ContainsRune(direct, 0) {
			t.Errorf("%s: direct render is unstorable: %q", h.name, direct)
		}
	}

	// Anti-vacuity: the hostile set must actually have exercised the
	// aliasing shape, not just a list of tame names.
	if len(seen) != len(hostile) {
		t.Fatalf("collapsed %d hostile names into %d renderings", len(hostile), len(seen))
	}
}

// renderSourceIdentityFields is the test's own spelling of the field
// rendering, used to feed bytes a DSN parser would otherwise normalise.
// It must stay byte-identical to [renderSourceIdentity]'s framing; the
// cell below pins that against drift.
func renderSourceIdentityFields(engine, database, schema string) string {
	return "engine=" + strconv.Quote(engine) +
		";database=" + strconv.Quote(database) +
		";schema=" + strconv.Quote(schema)
}

func TestRenderSourceIdentityFieldsMatchesTheRealRenderer(t *testing.T) {
	t.Parallel()
	const dsn = "postgres://u:p@host:5432/src_db?schema=tenant_a"
	want := renderSourceIdentity("postgres", dsn)
	got := renderSourceIdentityFields("postgres", "src_db", "tenant_a")
	if got != want {
		t.Fatalf("the test helper has drifted from the renderer:\n  helper:   %s\n  renderer: %s", got, want)
	}
}

// The `schema` default is a MIRROR of the Postgres engine's own DSN
// parser, and the pipeline cannot import an engine to check it
// (internal/archgate). Drift would make `?schema=public` and an absent
// parameter render different identities and refuse a legitimate resume,
// silently and only for Postgres sources — so the premise gets a gate
// rather than a comment (the premise-naming rule).
func TestSourceIdentitySchemaDefaultMatchesTheEngine(t *testing.T) {
	t.Parallel()

	const path = "../engines/postgres/connect.go"
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	re := regexp.MustCompile(`schema\s*=\s*"([a-z_]+)"`)
	matches := re.FindAllSubmatch(src, -1)
	// Anti-vacuity: BOTH DSN forms apply the default (parseURIDSN and
	// parseKVDSN). Fewer means the regex or the parser shape changed and
	// this gate has stopped looking at something.
	if len(matches) < 2 {
		t.Fatalf("%s: found %d schema defaults, expected at least 2 (parseURIDSN + parseKVDSN). "+
			"Re-point this gate rather than deleting it: pgDefaultSchema is only correct because it "+
			"matches the engine's own default.", path, len(matches))
	}
	for _, m := range matches {
		if got := string(m[1]); got != pgDefaultSchema {
			t.Errorf("%s defaults the source schema to %q but pgDefaultSchema is %q — two spellings of one "+
				"source would render different identities and a legitimate --resume would be refused",
				path, got, pgDefaultSchema)
		}
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

	recorded := renderSourceIdentity("postgres", "postgres://u@host/db_one")
	live := renderSourceIdentity("postgres", "postgres://u@host/db_two")

	for _, phase := range []ir.MigrationPhase{ir.MigrationPhaseComplete, ir.MigrationPhaseBulkCopy} {
		t.Run(string(phase), func(t *testing.T) {
			t.Parallel()
			store := newFakeStateStore()
			store.rows["m1"] = ir.MigrationState{
				MigrationID: "m1", Phase: phase, SourceIdentity: recorded,
			}
			rc := resumeContext{store: store, migrationID: "m1", enabled: true, sourceIdentity: live}

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
		rc := resumeContext{store: store, migrationID: "m1", enabled: true, sourceIdentity: recorded}
		if _, _, err := loadOrInitState(context.Background(), rc, true, false); err != nil {
			t.Fatalf("refused a resume against the SAME source: %v", err)
		}
	})

	t.Run("legacy state without identity resumes", func(t *testing.T) {
		t.Parallel()
		store := newFakeStateStore()
		store.rows["m1"] = ir.MigrationState{MigrationID: "m1", Phase: ir.MigrationPhaseBulkCopy}
		rc := resumeContext{store: store, migrationID: "m1", enabled: true, sourceIdentity: live}
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
		rc := resumeContext{store: store, migrationID: "m1", enabled: true, sourceIdentity: live}
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

	live := renderSourceIdentity("postgres", "postgres://u@host/src_db")
	store := newFakeStateStore()
	rc := resumeContext{store: store, migrationID: "fresh", enabled: true, sourceIdentity: live}

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
