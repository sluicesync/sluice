package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

func TestConfirmDestructiveAccepts(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		wantBool bool
	}{
		{"empty refuses", "\n", false},
		{"n refuses", "n\n", false},
		{"no refuses", "no\n", false},
		{"y accepts", "y\n", true},
		{"Y accepts", "Y\n", true},
		{"yes accepts", "yes\n", true},
		{"YES accepts", "YES\n", true},
		{"  y   with whitespace accepts", "  y  \n", true},
		{"random word refuses", "maybe\n", false},
		{"empty stream refuses", "", false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			out := &bytes.Buffer{}
			in := strings.NewReader(c.input)
			got, err := confirmDestructive(in, out, "Are you sure? ")
			if err != nil {
				t.Fatalf("confirmDestructive: %v", err)
			}
			if got != c.wantBool {
				t.Errorf("got %v; want %v", got, c.wantBool)
			}
			if !strings.Contains(out.String(), "Are you sure?") {
				t.Errorf("prompt not written to out: %q", out.String())
			}
		})
	}
}

func TestConfirmTypedDestructiveAccepts(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected string
		want     bool
	}{
		{"empty refuses", "\n", "reset", false},
		{"y refuses", "y\n", "reset", false},
		{"yes refuses", "yes\n", "reset", false},
		{"reset accepts", "reset\n", "reset", true},
		{"trim whitespace", "  reset  \n", "reset", true},
		{"case-sensitive: RESET refuses", "RESET\n", "reset", false},
		{"close-but-typo refuses", "rest\n", "reset", false},
		{"empty stream refuses", "", "reset", false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			out := &bytes.Buffer{}
			in := strings.NewReader(c.input)
			got, err := confirmTypedDestructive(context.Background(), in, out, "Type 'reset' to confirm: ", c.expected)
			if err != nil {
				t.Fatalf("confirmTypedDestructive: %v", err)
			}
			if got != c.want {
				t.Errorf("got %v; want %v", got, c.want)
			}
			if !strings.Contains(out.String(), "Type 'reset' to confirm") {
				t.Errorf("prompt not written to out: %q", out.String())
			}
		})
	}
}

// TestSlotDropRefusesWithoutYes pins the non-interactive contract: an
// agent running `slot drop` without --yes must get a loud, coded
// refusal (exit 3) — never a prompt that reads EOF on a non-TTY and
// silently no-ops. The refusal fires before any source connection, so
// the test needs no database.
func TestSlotDropRefusesWithoutYes(t *testing.T) {
	cmd := &SlotDropCmd{
		SourceDriver: "postgres",
		Source:       "postgres://localhost/db",
		Name:         "myslot",
	}
	err := cmd.Run(nil)
	if err == nil {
		t.Fatal("slot drop without --yes must refuse, got nil error")
	}

	ce, ok := sluicecode.FromError(err)
	if !ok {
		t.Fatalf("want a CodedError, got %T: %v", err, err)
	}
	if ce.Code != sluicecode.CodeConfirmationRequired {
		t.Errorf("code: got %q, want %q", ce.Code, sluicecode.CodeConfirmationRequired)
	}
	if ce.ExitCode() != sluicecode.ExitRefusal {
		t.Errorf("exit code: got %d, want %d (ExitRefusal)", ce.ExitCode(), sluicecode.ExitRefusal)
	}
	if !strings.Contains(err.Error(), "myslot") {
		t.Errorf("refusal must name the slot; got %q", err.Error())
	}
}

// TestSlotDropProceedsWithYes pins that --yes clears the confirmation
// gate: Run gets past the refusal and reaches the source-connection
// path (which then fails on an unknown driver here — proving the gate
// was skipped, not that a drop succeeded).
func TestSlotDropProceedsWithYes(t *testing.T) {
	cmd := &SlotDropCmd{
		SourceDriver: "not-a-real-engine",
		Source:       "dsn",
		Name:         "myslot",
		Yes:          true,
	}
	err := cmd.Run(nil)
	if err == nil {
		t.Fatal("expected an engine-resolution error past the confirmation gate")
	}
	if ce, ok := sluicecode.FromError(err); ok && ce.Code == sluicecode.CodeConfirmationRequired {
		t.Fatalf("--yes must skip the confirmation refusal; got %v", err)
	}
}

// errFakeSlotNotFound stands in for the postgres manager's errSlotNotFound
// sentinel (unexported there, and cmd/ deliberately doesn't import an engine
// package). It mirrors slot_manager.go: the operator-facing prose these tests
// pin, wrapping the engine-neutral [ir.ErrSlotNotFound] marker that
// isSlotNotFoundErr actually classifies on.
//
// The claim this comment used to make — that copying the sentinel TEXT made the
// fake "trip isSlotNotFoundErr exactly the way the real path does" — is the
// trap: a fake and a classifier agreeing on a string prove only that the test
// author wrote the same string twice. What binds this fake to reality is
// TestSlotManager_DropMissing in the postgres package, which asserts a real
// server's Drop wraps the same marker (audit backlog C-1).
var errFakeSlotNotFound = fmt.Errorf("postgres: %w", ir.ErrSlotNotFound)

// fakeSlotManager is an in-memory [ir.SlotManager] for the `slot drop`
// did-you-mean tests. Drop mirrors the Postgres manager's contract: an
// unknown name fails not-found, a known name removes the row. It
// records every name it was ASKED to drop so a test can prove the
// literal name was used — the no-silent-auto-prefix half of the
// contract.
type fakeSlotManager struct {
	slots    []ir.SlotInfo
	attempts []string
	dropped  []string
	listErr  error
	listCals int
}

func (f *fakeSlotManager) List(context.Context) ([]ir.SlotInfo, error) {
	f.listCals++
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.slots, nil
}

func (f *fakeSlotManager) Drop(_ context.Context, name string, _ bool) error {
	f.attempts = append(f.attempts, name)
	for i, s := range f.slots {
		if s.Name == name {
			f.slots = append(f.slots[:i], f.slots[i+1:]...)
			f.dropped = append(f.dropped, name)
			return nil
		}
	}
	return fmt.Errorf("%w: %q", errFakeSlotNotFound, name)
}

func (f *fakeSlotManager) Close() error { return nil }

// slotRows builds a pg_replication_slots-shaped listing from names.
func slotRows(names ...string) []ir.SlotInfo {
	out := make([]ir.SlotInfo, 0, len(names))
	for _, n := range names {
		out = append(out, ir.SlotInfo{Name: n, Plugin: "pgoutput"})
	}
	return out
}

// TestSlotDropUnprefixedNamesTheSluiceSibling is the gap this surface
// closed: `sync start --slot-name shard_a` creates `sluice_shard_a`, so
// `slot drop shard_a` is the natural next command and it cannot work.
// The literal-name semantics stay (nothing is dropped), but the failure
// now names the sibling, the convention, and the exact command.
func TestSlotDropUnprefixedNamesTheSluiceSibling(t *testing.T) {
	mgr := &fakeSlotManager{slots: slotRows("sluice_shard_a")}
	cmd := &SlotDropCmd{SourceDriver: "postgres", Source: "dsn", Name: "shard_a", Yes: true}

	out := &bytes.Buffer{}
	err := cmd.drop(context.Background(), mgr, out)
	if err == nil {
		t.Fatal("dropping an unprefixed name must still fail: the literal slot does not exist")
	}

	// Non-vacuous: the drop was attempted against the LITERAL name only,
	// and nothing was removed. A fixture whose Drop succeeded would make
	// the assertions below meaningless.
	if len(mgr.dropped) != 0 {
		t.Fatalf("nothing must be dropped; dropped %v", mgr.dropped)
	}
	if len(mgr.attempts) != 1 || mgr.attempts[0] != "shard_a" {
		t.Fatalf("drop must be attempted with the literal name; attempts %v", mgr.attempts)
	}
	if out.Len() != 0 {
		t.Errorf("no stdout on the failing path; got %q", out.String())
	}

	// The guidance wraps, never replaces, the engine's not-found error:
	// same chain, same (uncoded) classification, same exit 1. Adding a
	// remedy to the prose must not reclassify the failure — an operator's
	// `|| true` and an agent's exit-code branch both keep working.
	if !errors.Is(err, errFakeSlotNotFound) {
		t.Errorf("guidance must preserve the not-found chain; got %v", err)
	}
	if ce, ok := sluicecode.FromError(err); ok {
		t.Errorf("not-found stays uncoded; got code %q", ce.Code)
	}
	if got := exitCodeLikeKong(err); got != sluicecode.ExitFailure {
		t.Errorf("exit code = %d; want %d (unchanged generic failure)", got, sluicecode.ExitFailure)
	}
	msg := err.Error()
	for _, want := range []string{
		`"sluice_shard_a"`,    // the slot they probably mean
		"SUFFIX",              // the convention, named
		`"sluice_"`,           // the prefix itself
		"--slot-name shard_a", // what they ran to create it
		"sluice slot drop sluice_shard_a --source-driver postgres", // the exact command
		"--yes",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("did-you-mean missing %q; got: %s", want, msg)
		}
	}
	if strings.Contains(msg, "--force") {
		t.Errorf("suggested command must not add --force when the operator didn't pass it; got: %s", msg)
	}
}

// TestSlotDropSuggestionCarriesForce pins that the copy-pasteable
// command echoes the operator's own --force back: they already decided
// they needed it, and losing it turns the retry into a second failure.
func TestSlotDropSuggestionCarriesForce(t *testing.T) {
	mgr := &fakeSlotManager{slots: slotRows("sluice_shard_a")}
	cmd := &SlotDropCmd{SourceDriver: "postgres", Source: "dsn", Name: "shard_a", Yes: true, Force: true}
	err := cmd.drop(context.Background(), mgr, &bytes.Buffer{})
	if err == nil {
		t.Fatal("want the not-found failure")
	}
	if !strings.Contains(err.Error(), "--yes --force") {
		t.Errorf("suggested command must carry --force; got: %s", err.Error())
	}
}

// TestSlotDropPrefixedStillDrops is the must-not-break direction: a
// fully-qualified name drops exactly as before, with the same stdout
// line and no guidance anywhere.
func TestSlotDropPrefixedStillDrops(t *testing.T) {
	mgr := &fakeSlotManager{slots: slotRows("sluice_shard_a", "sluice_slot")}
	cmd := &SlotDropCmd{SourceDriver: "postgres", Source: "dsn", Name: "sluice_shard_a", Yes: true}

	out := &bytes.Buffer{}
	if err := cmd.drop(context.Background(), mgr, out); err != nil {
		t.Fatalf("drop of an existing slot: %v", err)
	}
	if len(mgr.dropped) != 1 || mgr.dropped[0] != "sluice_shard_a" {
		t.Fatalf("dropped %v; want [sluice_shard_a]", mgr.dropped)
	}
	if got := out.String(); got != "dropped slot \"sluice_shard_a\"\n" {
		t.Errorf("stdout = %q; want the unchanged dropped line", got)
	}
	if mgr.listCals != 0 {
		t.Errorf("the success path must not list slots; List called %d times", mgr.listCals)
	}
}

// TestSlotDropGenuinelyAbsentKeepsTodaysError pins that a name with no
// sibling and no sluice-owned neighbours produces exactly today's bare
// engine error — the guidance is additive, not a new failure mode.
func TestSlotDropGenuinelyAbsentKeepsTodaysError(t *testing.T) {
	mgr := &fakeSlotManager{slots: slotRows("debezium_slot")}
	cmd := &SlotDropCmd{SourceDriver: "postgres", Source: "dsn", Name: "typo_slot", Yes: true}

	err := cmd.drop(context.Background(), mgr, &bytes.Buffer{})
	if err == nil {
		t.Fatal("an absent slot must still error")
	}
	if got, want := err.Error(), `postgres: slot not found: "typo_slot"`; got != want {
		t.Errorf("error = %q; want today's bare message %q", got, want)
	}
}

// TestSlotDropAbsentNamesNearbySluiceSlots covers the weak signal: no
// sibling for this name, but the source does carry sluice-owned slots
// worth naming (wrong source, or a typo). Capped so the diagnostic
// never turns into a slot dump.
func TestSlotDropAbsentNamesNearbySluiceSlots(t *testing.T) {
	mgr := &fakeSlotManager{slots: slotRows(
		"sluice_a", "sluice_b", "sluice_c", "sluice_d", "sluice_e", "sluice_f", "sluice_g",
		"pghoard_local",
	)}
	cmd := &SlotDropCmd{SourceDriver: "postgres", Source: "dsn", Name: "sluice_typo", Yes: true}

	err := cmd.drop(context.Background(), mgr, &bytes.Buffer{})
	if err == nil {
		t.Fatal("an absent slot must still error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "sluice_a, sluice_b, sluice_c, sluice_d, sluice_e (and 2 more)") {
		t.Errorf("want the capped sluice-owned listing; got: %s", msg)
	}
	if strings.Contains(msg, "pghoard_local") {
		t.Errorf("only sluice-owned slots belong in the listing; got: %s", msg)
	}
	if !strings.Contains(msg, "sluice slot list") {
		t.Errorf("want the pointer at the full listing; got: %s", msg)
	}
}

// TestSlotDropListErrorDoesNotMaskNotFound pins the degrade rule: the
// listing is a diagnostic ON an already-failing path, so a List failure
// must leave the original not-found error intact rather than replace it
// with a listing complaint.
func TestSlotDropListErrorDoesNotMaskNotFound(t *testing.T) {
	mgr := &fakeSlotManager{
		slots:   slotRows("sluice_shard_a"),
		listErr: errors.New("permission denied for view pg_replication_slots"),
	}
	cmd := &SlotDropCmd{SourceDriver: "postgres", Source: "dsn", Name: "shard_a", Yes: true}

	err := cmd.drop(context.Background(), mgr, &bytes.Buffer{})
	if err == nil {
		t.Fatal("want the not-found failure")
	}
	if got, want := err.Error(), `postgres: slot not found: "shard_a"`; got != want {
		t.Errorf("error = %q; want the unmasked not-found %q", got, want)
	}
	if strings.Contains(err.Error(), "permission denied") {
		t.Errorf("the List failure must not surface here; got: %s", err.Error())
	}
}

// TestSlotDropIfExistsWarnsAboutTheSibling pins the --if-exists
// decision: --if-exists means "a missing slot is not an ERROR", not
// "say nothing". Exiting 0 on "nothing to do" while the sibling the
// operator actually meant sits there un-dropped is a silent no-op — the
// exact shape that let a cleanup loop run until max_replication_slots
// was exhausted. So the exit code and the stdout line stay EXACTLY as
// before (scripts parse them) and the information rides a WARN record.
func TestSlotDropIfExistsWarnsAboutTheSibling(t *testing.T) {
	var (
		mu      sync.Mutex
		records []slog.Record
	)
	prev := slog.Default()
	slog.SetDefault(slog.New(captureHandler{mu: &mu, records: &records}))
	t.Cleanup(func() { slog.SetDefault(prev) })

	mgr := &fakeSlotManager{slots: slotRows("sluice_shard_a")}
	cmd := &SlotDropCmd{SourceDriver: "postgres", Source: "dsn", Name: "shard_a", Yes: true, IfExists: true}

	out := &bytes.Buffer{}
	if err := cmd.drop(context.Background(), mgr, out); err != nil {
		t.Fatalf("--if-exists must still exit 0 on a missing slot; got %v", err)
	}
	if len(mgr.dropped) != 0 {
		t.Fatalf("nothing must be dropped; dropped %v", mgr.dropped)
	}
	if got := out.String(); got != "slot \"shard_a\" does not exist; nothing to do\n" {
		t.Errorf("stdout = %q; want the unchanged nothing-to-do line and nothing else", got)
	}

	if len(records) != 1 {
		t.Fatalf("want exactly one WARN record; got %d", len(records))
	}
	if records[0].Level != slog.LevelWarn {
		t.Errorf("level = %v; want WARN", records[0].Level)
	}
	attrs := map[string]string{}
	records[0].Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.String()
		return true
	})
	if attrs["slot"] != "shard_a" {
		t.Errorf("slot attr = %q; want the name the operator passed", attrs["slot"])
	}
	if !strings.Contains(attrs["hint"], `"sluice_shard_a"`) {
		t.Errorf("hint must name the sibling; got %q", attrs["hint"])
	}
}

// TestSlotDropIfExistsStaysQuietWhenNothingRhymes pins the other half:
// with no sibling, --if-exists is the plain idempotent no-op it always
// was — no WARN noise on a source that simply has nothing to clean up.
func TestSlotDropIfExistsStaysQuietWhenNothingRhymes(t *testing.T) {
	var (
		mu      sync.Mutex
		records []slog.Record
	)
	prev := slog.Default()
	slog.SetDefault(slog.New(captureHandler{mu: &mu, records: &records}))
	t.Cleanup(func() { slog.SetDefault(prev) })

	mgr := &fakeSlotManager{slots: slotRows("sluice_other")}
	cmd := &SlotDropCmd{SourceDriver: "postgres", Source: "dsn", Name: "gone", Yes: true, IfExists: true}

	out := &bytes.Buffer{}
	if err := cmd.drop(context.Background(), mgr, out); err != nil {
		t.Fatalf("--if-exists: %v", err)
	}
	if got := out.String(); got != "slot \"gone\" does not exist; nothing to do\n" {
		t.Errorf("stdout = %q; want the unchanged nothing-to-do line", got)
	}
	if len(records) != 0 {
		t.Errorf("no sibling means no WARN; got %d records", len(records))
	}
}

// TestSlotDropOtherErrorsPassThrough pins that a non-not-found failure
// (the platform-internal-slot refusal, a permission error) is returned
// untouched and never triggers the listing.
func TestSlotDropOtherErrorsPassThrough(t *testing.T) {
	mgr := &failingDropSlotManager{err: errors.New("postgres: drop slot \"x\": permission denied")}
	cmd := &SlotDropCmd{SourceDriver: "postgres", Source: "dsn", Name: "x", Yes: true, IfExists: true}

	err := cmd.drop(context.Background(), mgr, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("want the untouched engine error; got %v", err)
	}
	if mgr.listCals != 0 {
		t.Errorf("a non-not-found error must not list slots; List called %d times", mgr.listCals)
	}
}

// failingDropSlotManager fails every Drop with a fixed error.
type failingDropSlotManager struct {
	err      error
	listCals int
}

func (f *failingDropSlotManager) List(context.Context) ([]ir.SlotInfo, error) {
	f.listCals++
	return nil, nil
}

func (f *failingDropSlotManager) Drop(context.Context, string, bool) error { return f.err }
func (f *failingDropSlotManager) Close() error                             { return nil }

func TestIsSlotNotFoundErr(t *testing.T) {
	if isSlotNotFoundErr(nil) {
		t.Error("nil error should not be slot-not-found")
	}
	if !isSlotNotFoundErr(fmt.Errorf("postgres: drop slot %q: %w", "x", errFakeSlotNotFound)) {
		t.Error("wrapped slot-not-found should match")
	}
	// The direction that matters: PostgreSQL says "does not exist" for a whole
	// family of conditions, and the substring form this replaced classified
	// every one of them as a missing slot.
	if isSlotNotFoundErr(errors.New(`drop slot "s": database "app" does not exist (SQLSTATE 3D000)`)) {
		t.Error("a vanished DATABASE must not classify as a missing slot")
	}
	if isSlotNotFoundErr(errors.New("permission denied")) {
		t.Error("unrelated error should not match")
	}
}

func TestWALStatusOrDashRenders(t *testing.T) {
	if got := walStatusOrDash(""); got != "-" {
		t.Errorf("empty got %q; want -", got)
	}
	if got := walStatusOrDash("reserved"); got != "reserved" {
		t.Errorf("reserved got %q; want reserved", got)
	}
}

func TestLSNOrDashRenders(t *testing.T) {
	if got := lsnOrDash(""); got != "-" {
		t.Errorf("empty got %q; want -", got)
	}
	if got := lsnOrDash("0/16B7350"); got != "0/16B7350" {
		t.Errorf("lsn got %q; want passthrough", got)
	}
}
