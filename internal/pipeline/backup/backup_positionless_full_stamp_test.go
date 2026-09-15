// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// positionlessSourceEngine is a backupRecorderEngine with a declared CDC
// method and a PositionCapturer that answers whatever the cell says — the
// shapes that reach `backup full`'s post-sweep capture: a Neki router
// (ErrPositionUnavailable), a MySQL server with the binlog off (the EMPTY
// position, no error), and a healthy source (a real position).
type positionlessSourceEngine struct {
	*backupRecorderEngine

	cdc     ir.CDCMethod
	capture func(ctx context.Context, slot string) (ir.Position, error)
}

func (e positionlessSourceEngine) Capabilities() ir.Capabilities { return ir.Capabilities{CDC: e.cdc} }

func (e positionlessSourceEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return &capturingSchemaReader{
		recordingSchemaReader: recordingSchemaReader{schema: e.schema},
		capture:               e.capture,
	}, nil
}

// TestBackup_PositionlessFullStampSurvivesFinalize is the writer-level pin
// for FormatVersionPositionlessFull, graded on what the store holds after
// Run — not on an in-memory manifest — across every cell of
// {CDC method} × {what the capturer returns} × {committer mode}.
//
// The committer axis is the load-bearing one: LocalStore is an Appender,
// so a real `backup full --output-dir` runs in sidecar mode, whose
// finalize RESTORES the version captured before the sweep. A stamp
// applied after the sweep and not carried through raiseFinalVersion
// would be silently undone there — and only there, since the legacy
// (memStore) mode restores nothing. One mode green says nothing about
// the other.
func TestBackup_PositionlessFullStampSurvivesFinalize(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
	rows := map[string][]ir.Row{"users": {{"id": int64(1)}}}
	recorded := ir.Position{Engine: "mysql", Token: "mysql-bin.000003:1234"}

	captures := []struct {
		name    string
		capture func(context.Context, string) (ir.Position, error)
		empty   bool
	}{
		{"empty-position-no-error (MySQL binlog off)", func(context.Context, string) (ir.Position, error) {
			return ir.Position{}, nil
		}, true},
		{"unavailable (Neki router)", func(context.Context, string) (ir.Position, error) {
			return ir.Position{}, fmt.Errorf("no WAL position on a router: %w", irbackup.ErrPositionUnavailable)
		}, true},
		{"recorded", func(context.Context, string) (ir.Position, error) { return recorded, nil }, false},
	}
	stores := []struct {
		name     string
		appender bool
		open     func(t *testing.T) irbackup.Store
	}{
		{"sidecar (LocalStore, Appender)", true, func(t *testing.T) irbackup.Store {
			s, err := blobcodec.NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatalf("NewLocalStore: %v", err)
			}
			return s
		}},
		{"legacy (memStore)", false, func(*testing.T) irbackup.Store { return newMemStore() }},
	}
	cdcs := []ir.CDCMethod{ir.CDCBinlog, ir.CDCLogicalReplication, ir.CDCVStream, ir.CDCTriggers, ir.CDCNone}

	stamped := 0
	for _, st := range stores {
		if _, isAppender := st.open(t).(irbackup.Appender); isAppender != st.appender {
			t.Fatalf("store %q: Appender=%v — the committer-mode axis is mislabelled, so the cells below "+
				"would grade the wrong mode", st.name, isAppender)
		}
		for _, cdc := range cdcs {
			for _, c := range captures {
				t.Run(fmt.Sprintf("%s/cdc=%s/%s", st.name, cdc, c.name), func(t *testing.T) {
					store := st.open(t)
					src := positionlessSourceEngine{
						backupRecorderEngine: newBackupRecorderEngine("mysql", schema, rows),
						cdc:                  cdc,
						capture:              c.capture,
					}
					b := &Backup{Source: src, SourceDSN: "src", Store: store, SluiceVersion: "test"}
					if err := b.Run(context.Background()); err != nil {
						t.Fatalf("Backup.Run: %v", err)
					}

					// The stamp rule, restated from the store's point of view: a
					// CDC-less source never captures, so its position is empty
					// and it is exempt; a trigger source is exempt; every other
					// empty position is the hazard.
					want := irbackup.FormatVersionFor(schema)
					positionless := c.empty || cdc == ir.CDCNone
					if positionless && cdc != ir.CDCTriggers && cdc != ir.CDCNone {
						want = irbackup.FormatVersionPositionlessFull
						stamped++
					}

					raw := rawManifestFormatVersion(t, store)
					if raw != want {
						t.Errorf("manifest.json records format_version %d; want %d", raw, want)
					}
					m, err := lineage.ReadManifest(context.Background(), store)
					if err != nil {
						t.Fatalf("ReadManifest: %v", err)
					}
					if m.FormatVersion != want {
						t.Errorf("ReadManifest: FormatVersion = %d; want %d", m.FormatVersion, want)
					}
					if gotEmpty := m.EndPosition == (ir.Position{}); gotEmpty != positionless {
						t.Errorf("EndPosition = %+v; empty=%v, want empty=%v", m.EndPosition, gotEmpty, positionless)
					}
					// The id was computed AT the recorded version: a recompute
					// (what verifyBackupIDs does on every read) must agree.
					if recomputed := irbackup.ComputeBackupID(m); recomputed != m.BackupID {
						t.Errorf("BackupID recorded %s, recomputes to %s at format_version %d — the stamp landed after the id",
							m.BackupID, recomputed, m.FormatVersion)
					}
					if m.PartialState != irbackup.BackupStateComplete || m.ProgressSidecar != nil {
						t.Errorf("finalized manifest is not self-contained: state=%q sidecar=%+v", m.PartialState, m.ProgressSidecar)
					}
				})
			}
		}
	}
	// Two committer modes × three resuming methods × two empty shapes.
	if want := 2 * 3 * 2; stamped != want {
		t.Fatalf("%d cells expected the stamp, want %d — the matrix is not exercising the hazard arm", stamped, want)
	}
}

// rawManifestFormatVersion reads format_version straight out of the
// stored JSON, independent of lineage.ReadManifest's normalization — the
// number an OLDER binary's ceiling check will see.
func rawManifestFormatVersion(t *testing.T, store irbackup.Store) int {
	t.Helper()
	rc, err := store.Get(context.Background(), lineage.ManifestFileName)
	if err != nil {
		t.Fatalf("Get manifest: %v", err)
	}
	defer func() { _ = rc.Close() }()
	body, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var probe struct {
		FormatVersion int `json:"format_version"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	return probe.FormatVersion
}

// TestBackup_PositionlessFullRefusalBoundaryIsTheAADEncodingBoundary pins
// where stampPositionlessFull refuses rather than raises, and derives that
// boundary from the AAD renderers instead of restating a constant.
//
// The stamp is the only version raise applied AFTER chunks are sealed. For
// every version a full can finalize at below the stamp, and for plaintext
// and encrypted alike, the expected verdict is computed from what the
// version decides about already-sealed bytes — a row chunk's GCM AAD and
// the chain-CEK wrap binding. If those render identically at the recorded
// version and at the stamped one, the raise must go through; if they
// differ, an encrypted full's chunks would not open at the stamped version
// and the run must refuse. A future tier that changes an encoding moves the
// boundary here without anyone editing this test; a refusal keyed on the
// wrong constant fails it on the versions in between.
func TestBackup_PositionlessFullRefusalBoundaryIsTheAADEncodingBoundary(t *testing.T) {
	cek := bytes.Repeat([]byte{7}, 32)
	manifestAt := func(fv int, encrypted bool) *irbackup.Manifest {
		m := &irbackup.Manifest{
			FormatVersion: fv,
			CreatedAt:     time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC),
			SourceEngine:  "mysql",
			Kind:          irbackup.BackupKindFull,
		}
		if encrypted {
			m.ChainEncryption = &irbackup.ChainEncryption{Mode: crypto.EncryptModePerChain}
		}
		return m
	}
	sealedUnder := func(m *irbackup.Manifest) string {
		return string(irbackup.ChunkAADForWrite(m, "chunks/users/users-0.jsonl.gz", "app", "users", cek)) +
			"\x00" + irbackup.CEKBinding(m)
	}
	atStamp := sealedUnder(manifestAt(irbackup.FormatVersionPositionlessFull, true))
	src := positionlessSourceEngine{cdc: ir.CDCBinlog}

	refused, raised := 0, 0
	for fv := irbackup.FormatVersionLegacy; fv < irbackup.FormatVersionPositionlessFull; fv++ {
		for _, encrypted := range []bool{false, true} {
			name := fmt.Sprintf("format_version=%d/encrypted=%v", fv, encrypted)
			m := manifestAt(fv, encrypted)
			wantRefuse := encrypted && sealedUnder(m) != atStamp
			committer, err := newManifestCommitter(newMemStore(), m)
			if err != nil {
				t.Fatalf("%s: newManifestCommitter: %v", name, err)
			}
			err = (&Backup{Source: src}).stampPositionlessFull(m, committer)
			switch {
			case wantRefuse && err == nil:
				t.Errorf("%s: raised to %d although the chunks were sealed under an encoding that version does not open — "+
					"the full is unrestorable", name, m.FormatVersion)
			case wantRefuse:
				if !strings.Contains(err.Error(), "POSITIONLESS-FULL-ROOT") || !strings.Contains(err.Error(), "--force-overwrite") {
					t.Errorf("%s: refused without naming the marker and the remedy: %v", name, err)
				}
				if m.FormatVersion != fv {
					t.Errorf("%s: refused but moved FormatVersion to %d", name, m.FormatVersion)
				}
				refused++
			case err != nil:
				t.Errorf("%s: refused a raise that changes no sealed encoding: %v", name, err)
			default:
				if m.FormatVersion != irbackup.FormatVersionPositionlessFull || committer.finalVersion != irbackup.FormatVersionPositionlessFull {
					t.Errorf("%s: FormatVersion=%d finalVersion=%d, want both %d", name, m.FormatVersion,
						committer.finalVersion, irbackup.FormatVersionPositionlessFull)
				}
				raised++
			}
		}
	}
	// Both arms, or a refusal stuck on one answer grades green. Today's
	// shape is refused below v9 encrypted and raised everywhere else.
	if refused == 0 || raised == 0 {
		t.Fatalf("boundary matrix exercised refused=%d raised=%d; both must be non-zero", refused, raised)
	}
}

// TestBackup_PositionlessFullStampRestoresOnEveryCryptoTier is the
// round-trip half of the boundary above, on the real seal/open path: a
// positionless full is stamped 11 after its chunks were sealed, so for
// every crypto tier a fresh `backup full` can take — plaintext, per-chain
// and per-chunk encryption, Ed25519 signing with and without encryption —
// and on both committer modes, a real Restore must open every chunk and
// verify every signature at the version the manifest RECORDS. The expected
// rows are the source fixture's, independent of anything the backup wrote.
func TestBackup_PositionlessFullStampRestoresOnEveryCryptoTier(t *testing.T) {
	params := injectiveAADParams(t)
	pub, priv, err := crypto.GenerateEd25519Keypair()
	if err != nil {
		t.Fatalf("GenerateEd25519Keypair: %v", err)
	}
	tiers := []struct {
		name      string
		encrypt   string // "" plaintext, else the encryption mode
		signed    bool
		wantChunk bool // chunks carry encryption metadata
	}{
		{"plaintext", "", false, false},
		{"encrypted per-chain", crypto.EncryptModePerChain, false, true},
		{"encrypted per-chunk", crypto.EncryptModePerChunk, false, true},
		{"ed25519-signed plaintext", "", true, false},
		{"ed25519-signed per-chain", crypto.EncryptModePerChain, true, true},
	}
	stores := []struct {
		name string
		open func(t *testing.T) irbackup.Store
	}{
		{"sidecar (LocalStore)", func(t *testing.T) irbackup.Store {
			s, err := blobcodec.NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatalf("NewLocalStore: %v", err)
			}
			return s
		}},
		{"legacy (memStore)", func(*testing.T) irbackup.Store { return newMemStore() }},
	}
	schema := injectiveAADFixtureSchema()
	want := injectiveAADFixtureRows()

	for _, st := range stores {
		for _, tier := range tiers {
			t.Run(st.name+"/"+tier.name, func(t *testing.T) {
				ctx := context.Background()
				store := st.open(t)
				b := &Backup{
					Source: positionlessSourceEngine{
						backupRecorderEngine: newBackupRecorderEngine("mysql", schema, want),
						cdc:                  ir.CDCBinlog,
						capture: func(context.Context, string) (ir.Position, error) {
							return ir.Position{}, nil
						},
					},
					SourceDSN: "src",
					Store:     store,
					ChunkRows: 2,
				}
				rs := &Restore{TargetDSN: "tgt", Store: store}
				if tier.encrypt != "" {
					b.Encryption = &lineage.BackupEncryption{Envelope: newInjectiveAADEnvelope(t, params), Mode: tier.encrypt}
					rs.Envelope = newInjectiveAADEnvelope(t, params)
				}
				if tier.signed {
					b.Ed25519Signer = lineage.NewEd25519Signer(priv)
					rs.VerifyKey, rs.RequireSignature = pub, true
				}
				if err := b.Run(ctx); err != nil {
					t.Fatalf("Backup.Run: %v", err)
				}

				if raw := rawManifestFormatVersion(t, store); raw != irbackup.FormatVersionPositionlessFull {
					t.Fatalf("manifest.json records format_version %d; want %d — the cell is not exercising the stamp",
						raw, irbackup.FormatVersionPositionlessFull)
				}
				m, err := lineage.ReadManifest(ctx, store)
				if err != nil {
					t.Fatalf("ReadManifest: %v", err)
				}
				chunks := 0
				for _, tm := range m.Tables {
					for _, c := range tm.Chunks {
						chunks++
						if (c.Encryption != nil) != tier.wantChunk {
							t.Fatalf("chunk %s: encrypted=%v, want %v — the tier did not engage", c.File, c.Encryption != nil, tier.wantChunk)
						}
					}
				}
				if chunks < 2 {
					t.Fatalf("backup wrote %d chunk(s); want at least 2 so the per-chunk binding is exercised more than once", chunks)
				}

				tgt := newRestoreRecorderEngine("postgres")
				rs.Target = tgt
				if err := rs.Run(ctx); err != nil {
					t.Fatalf("Restore.Run at format_version %d: %v — a full whose version was raised after sealing must "+
						"still open and verify", m.FormatVersion, err)
				}
				_, got := tgt.snapshot()
				rowSetEqual(t, "orders", got["orders"], want["orders"])
			})
		}
	}
}
