// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// stubNamespacedEngine is a stubEngine that declares
// SchemaScope=Namespaced (PG-shaped) so validateTargetSchema accepts
// the override. Used by tests that exercise the orchestrator's
// target-schema field round-trip without booting a real PG container.
type stubNamespacedEngine struct{ stubEngine }

func (stubNamespacedEngine) Capabilities() ir.Capabilities {
	return ir.Capabilities{SchemaScope: ir.SchemaScopeNamespaced, CDC: ir.CDCLogicalReplication}
}

func (stubNamespacedEngine) Name() string { return "stub-namespaced" }

// stubFlatEngine declares SchemaScope=Flat so validateTargetSchema
// refuses the override. Used to assert the MySQL-shaped refusal.
type stubFlatEngine struct{ stubEngine }

func (stubFlatEngine) Capabilities() ir.Capabilities {
	return ir.Capabilities{SchemaScope: ir.SchemaScopeFlat, CDC: ir.CDCBinlog}
}

func (stubFlatEngine) Name() string { return "stub-flat" }

func TestValidateTargetSchema(t *testing.T) {
	t.Run("empty target schema is always allowed", func(t *testing.T) {
		if err := validateTargetSchema(stubFlatEngine{}, ""); err != nil {
			t.Errorf("got %v; want nil for empty target schema even on flat engine", err)
		}
	})

	t.Run("namespaced engine accepts override", func(t *testing.T) {
		if err := validateTargetSchema(stubNamespacedEngine{}, "customer_svc"); err != nil {
			t.Errorf("got %v; want nil", err)
		}
	})

	t.Run("flat engine refuses with PG-only message", func(t *testing.T) {
		err := validateTargetSchema(stubFlatEngine{}, "customer_svc")
		if err == nil {
			t.Fatal("got nil; want refusal")
		}
		got := err.Error()
		// The message should explicitly call out the workaround
		// (different MySQL database via --target DSN) and reference
		// the ADR for operators chasing context.
		for _, want := range []string{
			"--target-schema is not supported",
			"stub-flat",
			"different --target DSN",
			"adr-0031",
		} {
			if !strings.Contains(strings.ToLower(got), strings.ToLower(want)) {
				t.Errorf("error %q missing substring %q", got, want)
			}
		}
	})
}

// stubPGEngine is a stubEngine that declares the PG extension catalog
// (and the rest of the vanilla-PG capability surface) so the ADR-0032
// validate gate accepts it on either side.
type stubPGEngine struct{ stubEngine }

func (stubPGEngine) Name() string { return "postgres" }

func (stubPGEngine) Capabilities() ir.Capabilities {
	return ir.Capabilities{
		SchemaScope:            ir.SchemaScopeNamespaced,
		CDC:                    ir.CDCLogicalReplication,
		PostgresBackend:        true,
		PGExtensionCatalog:     true,
		VerbatimExtensionTypes: true,
	}
}

// stubMySQLEngine is a stubEngine without the PG extension catalog so
// the ADR-0032 validate gate refuses it.
type stubMySQLEngine struct{ stubEngine }

func (stubMySQLEngine) Name() string { return "mysql" }

func (stubMySQLEngine) Capabilities() ir.Capabilities {
	return ir.Capabilities{
		SchemaScope: ir.SchemaScopeFlat,
		CDC:         ir.CDCBinlog,
		DDLDialect:  ir.DDLDialectMySQL,
	}
}

func TestValidateEnabledPGExtensions(t *testing.T) {
	t.Run("empty extensions always pass", func(t *testing.T) {
		if err := validateEnabledPGExtensions(stubMySQLEngine{}, stubMySQLEngine{}, nil); err != nil {
			t.Errorf("err = %v; want nil for empty extensions even on MySQL", err)
		}
	})

	t.Run("PG -> PG accepts", func(t *testing.T) {
		err := validateEnabledPGExtensions(stubPGEngine{}, stubPGEngine{}, []string{"vector"})
		if err != nil {
			t.Errorf("err = %v; want nil for PG -> PG", err)
		}
	})

	t.Run("PG -> PG accepts hstore (v0.32.1 COPY binary codec)", func(t *testing.T) {
		// hstore PG → PG was refused at preflight in v0.31.0 because
		// the COPY-protocol binary codec had not landed; the
		// validator turned the missing codec into an actionable
		// preflight refusal rather than a cryptic mid-COPY failure.
		// v0.32.1 ships the codec (internal/engines/postgres/
		// hstore_codec.go) and the refusal branch is gone; the gate
		// must accept hstore PG → PG the same way it accepts vector
		// or citext.
		err := validateEnabledPGExtensions(stubPGEngine{}, stubPGEngine{}, []string{"hstore"})
		if err != nil {
			t.Errorf("err = %v; want nil for PG -> PG hstore", err)
		}
	})

	t.Run("PG -> PG accepts citext + hstore together", func(t *testing.T) {
		// Mixed list with hstore + citext: both Tier 1 entries pass
		// since the v0.32.1 hstore codec landed. Pin both arrangements
		// (citext alone and the mixed list) so a future regression
		// surfaces clearly.
		err := validateEnabledPGExtensions(stubPGEngine{}, stubPGEngine{}, []string{"citext"})
		if err != nil {
			t.Errorf("err = %v; want nil for PG -> PG citext alone", err)
		}
		err = validateEnabledPGExtensions(stubPGEngine{}, stubPGEngine{}, []string{"citext", "hstore"})
		if err != nil {
			t.Errorf("err = %v; want nil for PG -> PG mixed citext+hstore", err)
		}
	})

	t.Run("MySQL source refused", func(t *testing.T) {
		err := validateEnabledPGExtensions(stubMySQLEngine{}, stubPGEngine{}, []string{"vector"})
		if err == nil {
			t.Fatal("err = nil; want refusal")
		}
		if !strings.Contains(err.Error(), "PG sources") {
			t.Errorf("err = %v; want \"PG sources\"", err)
		}
	})

	t.Run("MySQL target refused for non-translatable extension", func(t *testing.T) {
		err := validateEnabledPGExtensions(stubPGEngine{}, stubMySQLEngine{}, []string{"vector"})
		if err == nil {
			t.Fatal("err = nil; want refusal")
		}
		if !strings.Contains(err.Error(), "cross-engine") {
			t.Errorf("err = %v; want \"cross-engine\"", err)
		}
		if !strings.Contains(err.Error(), "--type-override") {
			t.Errorf("err = %v; want hint mentioning --type-override", err)
		}
	})

	t.Run("MySQL target refused when source has no translator capability", func(t *testing.T) {
		// stubPGEngine doesn't implement ir.CrossEngineExtensionTranslator,
		// so even hstore / citext fall back to the strict refusal — the
		// gate is per-engine-declared, not hard-coded.
		err := validateEnabledPGExtensions(stubPGEngine{}, stubMySQLEngine{}, []string{"hstore"})
		if err == nil {
			t.Fatal("err = nil; want refusal when source doesn't declare translator capability")
		}
	})

	t.Run("MySQL target accepts when source declares cross-engine translator", func(t *testing.T) {
		// stubPGEngineWithTranslator declares hstore and citext as
		// cross-engine-translatable, mirroring the real postgres.Engine
		// implementation.
		err := validateEnabledPGExtensions(stubPGEngineWithTranslator{}, stubMySQLEngine{}, []string{"hstore"})
		if err != nil {
			t.Errorf("err = %v; want nil for hstore PG → MySQL via declared translator", err)
		}
	})

	t.Run("MySQL target accepts citext via declared translator", func(t *testing.T) {
		err := validateEnabledPGExtensions(stubPGEngineWithTranslator{}, stubMySQLEngine{}, []string{"citext"})
		if err != nil {
			t.Errorf("err = %v; want nil for citext PG → MySQL via declared translator", err)
		}
	})

	t.Run("MySQL target refuses mixed list with non-translatable extension", func(t *testing.T) {
		// hstore has a translator but vector does not — the gate must
		// surface the refusal on vector rather than silently passing.
		err := validateEnabledPGExtensions(stubPGEngineWithTranslator{}, stubMySQLEngine{}, []string{"hstore", "vector"})
		if err == nil {
			t.Fatal("err = nil; want refusal naming the non-translatable extension")
		}
		if !strings.Contains(err.Error(), "vector") {
			t.Errorf("err = %v; want refusal naming vector", err)
		}
	})
}

// stubPGEngineWithTranslator implements ir.CrossEngineExtensionTranslator
// declaring hstore and citext (the v1 cross-engine-translatable set).
// Mirrors the real postgres.Engine surface without booting a real DB.
type stubPGEngineWithTranslator struct{ stubPGEngine }

func (stubPGEngineWithTranslator) HasCrossEngineDefaultTranslator(name string) bool {
	return name == "hstore" || name == "citext"
}

// TestApplyEnabledPGExtensions_NonExtensionAware confirms the
// helper no-ops cleanly against a target that doesn't implement
// the optional ir.ExtensionAware surface (e.g. MySQL).
func TestApplyEnabledPGExtensions_NonExtensionAware(t *testing.T) {
	// stubEngine doesn't implement ExtensionAware. Calling
	// applyEnabledPGExtensions against an arbitrary value (e.g. a
	// raw stub) must not panic.
	type plain struct{}
	if err := applyEnabledPGExtensions(context.Background(), plain{}, []string{"vector"}); err != nil {
		t.Errorf("err = %v; want nil for non-aware target", err)
	}
}

// TestApplyEnabledPGExtensions_Empty no-ops cleanly with an empty
// extensions list — preserves today's behaviour where the
// orchestrator field defaults to empty.
func TestApplyEnabledPGExtensions_Empty(t *testing.T) {
	if err := applyEnabledPGExtensions(context.Background(), nil, nil); err != nil {
		t.Errorf("err = %v; want nil for empty extensions", err)
	}
}

// fakeExtensionAware records the names passed to EnableExtensions
// and optionally returns a configured error.
type fakeExtensionAware struct {
	got []string
	err error
}

func (f *fakeExtensionAware) EnableExtensions(_ context.Context, names []string) error {
	f.got = append(f.got, names...)
	return f.err
}

// TestApplyEnabledPGExtensions_RoutesToAware confirms the helper
// dispatches through the ir.ExtensionAware surface when the target
// implements it.
func TestApplyEnabledPGExtensions_RoutesToAware(t *testing.T) {
	target := &fakeExtensionAware{}
	if err := applyEnabledPGExtensions(context.Background(), target, []string{"vector"}); err != nil {
		t.Errorf("err = %v; want nil", err)
	}
	if len(target.got) != 1 || target.got[0] != "vector" {
		t.Errorf("EnableExtensions got %v; want [vector]", target.got)
	}
}

// TestApplyEnabledPGExtensions_PropagatesError confirms refusal
// errors from the engine surface back through the helper.
func TestApplyEnabledPGExtensions_PropagatesError(t *testing.T) {
	target := &fakeExtensionAware{err: errors.New("missing on target")}
	err := applyEnabledPGExtensions(context.Background(), target, []string{"vector"})
	if err == nil || !strings.Contains(err.Error(), "missing on target") {
		t.Errorf("err = %v; want propagated \"missing on target\"", err)
	}
}

func TestFingerprintSourceDSN(t *testing.T) {
	cases := []struct {
		name    string
		dsn     string
		nonEmpt bool
	}{
		{
			name:    "postgres URI",
			dsn:     "postgres://alice:secret@db.example.com:5432/customers?sslmode=disable",
			nonEmpt: true,
		},
		{
			name:    "postgres KV",
			dsn:     "host=db.example.com port=5432 dbname=customers user=alice password=secret",
			nonEmpt: true,
		},
		{
			name:    "mysql URI",
			dsn:     "mysql://alice:secret@db.example.com:3306/customers",
			nonEmpt: true,
		},
		{
			name:    "mysql DSN",
			dsn:     "alice:secret@tcp(db.example.com:3306)/customers?parseTime=true",
			nonEmpt: true,
		},
		{
			name:    "empty DSN",
			dsn:     "",
			nonEmpt: false,
		},
		{
			name:    "garbage DSN",
			dsn:     "this is not a DSN",
			nonEmpt: false,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := fingerprintSourceDSN(c.dsn)
			if c.nonEmpt && got == "" {
				t.Errorf("fingerprintSourceDSN(%q) = %q; want non-empty", c.dsn, got)
			}
			if !c.nonEmpt && got != "" {
				t.Errorf("fingerprintSourceDSN(%q) = %q; want empty", c.dsn, got)
			}
			if c.nonEmpt && len(got) != 12 {
				t.Errorf("fingerprintSourceDSN(%q) length = %d; want 12", c.dsn, len(got))
			}
		})
	}
}

func TestFingerprintSourceDSN_PasswordRotationStable(t *testing.T) {
	// A genuine credential rotation must NOT change the fingerprint —
	// the threat-model item that drove user/password exclusion.
	a := "postgres://alice:old_secret@db.example.com:5432/customers"
	b := "postgres://alice:new_rotated_secret@db.example.com:5432/customers"
	c := "postgres://bob:bobs_password@db.example.com:5432/customers"
	if fingerprintSourceDSN(a) != fingerprintSourceDSN(b) {
		t.Errorf("fingerprint changed across password rotation: a=%q b=%q",
			fingerprintSourceDSN(a), fingerprintSourceDSN(b))
	}
	if fingerprintSourceDSN(a) != fingerprintSourceDSN(c) {
		t.Errorf("fingerprint changed across user rotation: a=%q c=%q",
			fingerprintSourceDSN(a), fingerprintSourceDSN(c))
	}
}

func TestFingerprintSourceDSN_DatabaseChangeChangesFingerprint(t *testing.T) {
	// Different database on the same host should give different
	// fingerprints — the load-bearing distinguisher for the typical
	// "two source services on one PG cluster" multi-source shape.
	a := "postgres://alice:s@db.example.com:5432/customers"
	b := "postgres://alice:s@db.example.com:5432/billing"
	if fingerprintSourceDSN(a) == fingerprintSourceDSN(b) {
		t.Errorf("fingerprint stable across database change; a=b=%q",
			fingerprintSourceDSN(a))
	}
}

func TestFingerprintSourceDSN_HostChangeChangesFingerprint(t *testing.T) {
	a := "postgres://alice:s@db1.example.com:5432/customers"
	b := "postgres://alice:s@db2.example.com:5432/customers"
	if fingerprintSourceDSN(a) == fingerprintSourceDSN(b) {
		t.Errorf("fingerprint stable across host change; a=b=%q",
			fingerprintSourceDSN(a))
	}
}

func TestFingerprintSourceDSN_DefaultPortNormalisation(t *testing.T) {
	// A DSN that elides the default port should fingerprint the same
	// as one that explicitly names it. Avoids spurious mismatches
	// across DSN-shape variations.
	withPort := "postgres://alice:s@db.example.com:5432/customers"
	withoutPort := "postgres://alice:s@db.example.com/customers"
	if fingerprintSourceDSN(withPort) != fingerprintSourceDSN(withoutPort) {
		t.Errorf("default port normalisation broken; %q != %q",
			fingerprintSourceDSN(withPort), fingerprintSourceDSN(withoutPort))
	}
}

func TestCheckStreamIDCollision(t *testing.T) {
	t.Run("matching fingerprint is allowed", func(t *testing.T) {
		streams := []ir.StreamStatus{
			{StreamID: "customer-svc", SourceDSNFingerprint: "abcd1234ef56"},
		}
		err := checkStreamIDCollision("customer-svc", "abcd1234ef56", streams)
		if err != nil {
			t.Errorf("got %v; want nil for matching fingerprint", err)
		}
	})

	t.Run("different fingerprint refuses loudly", func(t *testing.T) {
		streams := []ir.StreamStatus{
			{StreamID: "customer-svc", SourceDSNFingerprint: "abcd1234ef56"},
		}
		err := checkStreamIDCollision("customer-svc", "9876fedc5432", streams)
		if err == nil {
			t.Fatal("got nil; want refusal")
		}
		if !errors.Is(err, errStreamIDCollision) {
			t.Errorf("error %v; want errStreamIDCollision", err)
		}
		got := err.Error()
		for _, want := range []string{"customer-svc", "abcd1234ef56", "9876fedc5432", "--reset-target-data"} {
			if !strings.Contains(got, want) {
				t.Errorf("error %q missing substring %q", got, want)
			}
		}
	})

	t.Run("legacy row with empty fingerprint is allowed", func(t *testing.T) {
		// Pre-v0.25.0 rows have NULL → empty after COALESCE. The
		// check treats this as "unknown — allow" so an upgrade
		// doesn't false-positive on existing streams.
		streams := []ir.StreamStatus{
			{StreamID: "customer-svc", SourceDSNFingerprint: ""},
		}
		err := checkStreamIDCollision("customer-svc", "9876fedc5432", streams)
		if err != nil {
			t.Errorf("got %v; want nil for legacy row", err)
		}
	})

	t.Run("empty current fingerprint skips check", func(t *testing.T) {
		// Engine doesn't compute a fingerprint (unknown DSN shape) →
		// orchestrator skips the collision check rather than refusing.
		// Loud-failure tenet applies once we have ground truth; the
		// empty case is the no-info case.
		streams := []ir.StreamStatus{
			{StreamID: "customer-svc", SourceDSNFingerprint: "abcd1234ef56"},
		}
		err := checkStreamIDCollision("customer-svc", "", streams)
		if err != nil {
			t.Errorf("got %v; want nil for empty current fingerprint", err)
		}
	})

	t.Run("different stream-id is unrelated", func(t *testing.T) {
		streams := []ir.StreamStatus{
			{StreamID: "billing-svc", SourceDSNFingerprint: "abcd1234ef56"},
		}
		err := checkStreamIDCollision("customer-svc", "9876fedc5432", streams)
		if err != nil {
			t.Errorf("got %v; want nil for unrelated stream-id", err)
		}
	})

	t.Run("empty streams list", func(t *testing.T) {
		err := checkStreamIDCollision("customer-svc", "abcd1234ef56", nil)
		if err != nil {
			t.Errorf("got %v; want nil for empty streams", err)
		}
	})
}

func TestApplyTargetSchema(t *testing.T) {
	t.Run("empty name is no-op", func(t *testing.T) {
		s := &recordingSchemaSetter{}
		applyTargetSchema(s, "")
		if s.lastSchema != "" || s.calls != 0 {
			t.Errorf("recordingSchemaSetter = %+v; want no calls", s)
		}
	})

	t.Run("non-empty name calls SetSchema", func(t *testing.T) {
		s := &recordingSchemaSetter{}
		applyTargetSchema(s, "customer_svc")
		if s.lastSchema != "customer_svc" || s.calls != 1 {
			t.Errorf("recordingSchemaSetter = %+v; want lastSchema=customer_svc calls=1", s)
		}
	})

	t.Run("non-setter target is silently passed through", func(_ *testing.T) {
		// A bare struct without SetSchema. The helper must not panic
		// (engines that don't implement the optional surface degrade
		// gracefully — same shape as MaxBufferBytesSetter).
		applyTargetSchema(struct{}{}, "customer_svc")
	})
}

// recordingSchemaSetter is a test double that records SetSchema
// invocations. Used to assert the orchestrator's threading without
// instantiating a real engine.
type recordingSchemaSetter struct {
	lastSchema string
	calls      int
}

func (r *recordingSchemaSetter) SetSchema(name string) {
	r.lastSchema = name
	r.calls++
}

// TestMigrator_TargetSchemaValidation asserts the validate-time
// refusal for flat-namespace engines. The message should name the
// engine and the DSN-choice workaround.
func TestMigrator_TargetSchemaValidation(t *testing.T) {
	t.Run("flat engine refuses --target-schema at validate time", func(t *testing.T) {
		m := &Migrator{
			Source:       stubNamespacedEngine{},
			Target:       stubFlatEngine{},
			SourceDSN:    "src",
			TargetDSN:    "tgt",
			TargetSchema: "customer_svc",
		}
		err := m.Run(context.Background())
		if err == nil {
			t.Fatal("got nil; want refusal")
		}
		if !strings.Contains(err.Error(), "--target-schema is not supported") {
			t.Errorf("error %q missing PG-only message", err.Error())
		}
	})

	t.Run("namespaced engine accepts --target-schema at validate time", func(t *testing.T) {
		// Source / target both stubNamespacedEngine; source declares
		// CDC=LogicalReplication. The Migrator's validate passes; the
		// downstream Open path will panic via stubEngine because we
		// don't actually run a migration here. The test asserts that
		// validate doesn't refuse the field — Run wraps validate +
		// real work, so a panic from the schema-reader open is
		// expected if validate passes.
		m := &Migrator{
			Source:       stubNamespacedEngine{},
			Target:       stubNamespacedEngine{},
			SourceDSN:    "src",
			TargetDSN:    "tgt",
			TargetSchema: "customer_svc",
		}
		// Recover from the inevitable stubEngine panic; what we care
		// about is that we got past validate.
		defer func() {
			r := recover()
			if r == nil {
				t.Errorf("expected stubEngine panic past validate; got nil")
			}
		}()
		_ = m.Run(context.Background())
	})
}

// TestStreamer_TargetSchemaValidation mirrors the Migrator test for
// the streamer's validate path.
func TestStreamer_TargetSchemaValidation(t *testing.T) {
	t.Run("flat engine refuses --target-schema at validate time", func(t *testing.T) {
		s := &Streamer{
			Source:       stubNamespacedEngine{},
			Target:       stubFlatEngine{},
			SourceDSN:    "src",
			TargetDSN:    "tgt",
			TargetSchema: "customer_svc",
		}
		err := s.Run(context.Background())
		if err == nil {
			t.Fatal("got nil; want refusal")
		}
		if !strings.Contains(err.Error(), "--target-schema is not supported") {
			t.Errorf("error %q missing PG-only message", err.Error())
		}
	})
}
