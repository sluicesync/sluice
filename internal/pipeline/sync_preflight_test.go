// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Bug 36 (v0.17.3) unit coverage: the --patroni-mode flag, its
// ValidatePatroniMode normaliser, and the matchManagedPGHostname DSN
// pattern matcher.

import (
	"strings"
	"testing"
)

func TestValidatePatroniMode(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"", PatroniModeAuto, false},
		{"auto", PatroniModeAuto, false},
		{"AUTO", PatroniModeAuto, false},
		{"  on ", PatroniModeOn, false},
		{"On", PatroniModeOn, false},
		{"off", PatroniModeOff, false},
		{"yes", "", true},
		{"true", "", true},
		{"force", "", true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.in, func(t *testing.T) {
			got, err := ValidatePatroniMode(c.in)
			if c.wantErr {
				if err == nil {
					t.Fatalf("ValidatePatroniMode(%q): want err; got %q", c.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ValidatePatroniMode(%q): unexpected err: %v", c.in, err)
			}
			if got != c.want {
				t.Errorf("ValidatePatroniMode(%q) = %q; want %q", c.in, got, c.want)
			}
		})
	}
}

func TestMatchManagedPGHostname(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
		want string
	}{
		{
			name: "psdb.cloud URI",
			dsn:  "postgres://user:pass@aws-us-east-1.psdb.cloud:5432/db?sslmode=require",
			want: "*.psdb.cloud",
		},
		{
			name: "psdb.cloud libpq",
			dsn:  "host=aws-us-east-1.psdb.cloud port=5432 user=u dbname=d sslmode=require",
			want: "*.psdb.cloud",
		},
		{
			name: "Aurora cluster endpoint",
			dsn:  "postgres://u:p@my-cluster.cluster-cabc1234.us-east-1.rds.amazonaws.com:5432/db",
			want: "*.cluster*.rds.amazonaws.com (Aurora)",
		},
		{
			name: "vanilla RDS instance (NOT flagged — too broad)",
			dsn:  "postgres://u:p@my-instance.cabc1234.us-east-1.rds.amazonaws.com:5432/db",
			want: "",
		},
		{
			name: "Azure",
			dsn:  "postgres://u:p@myserver.postgres.database.azure.com:5432/db",
			want: "*.postgres.database.azure.com",
		},
		{
			name: "Cloud SQL private IP",
			dsn:  "postgres://u:p@my-instance.cloudsql.google.internal:5432/db",
			want: "*.cloudsql.google.internal",
		},
		{
			name: "Archil aws",
			dsn:  "postgres://u:p@cluster1.aws.prod.archil.com:5432/db",
			want: "*.aws.prod.archil.com",
		},
		{
			name: "Archil gcp",
			dsn:  "postgres://u:p@cluster1.gcp.prod.archil.com:5432/db",
			want: "*.gcp.prod.archil.com",
		},
		{
			name: "self-hosted single host",
			dsn:  "postgres://u:p@db.example.com:5432/db",
			want: "",
		},
		{
			name: "localhost",
			dsn:  "postgres://u:p@localhost:5432/db",
			want: "",
		},
		{
			name: "empty DSN",
			dsn:  "",
			want: "",
		},
		{
			name: "case-insensitive psdb",
			dsn:  "postgres://u:p@AWS-US-EAST-1.PSDB.CLOUD:5432/db",
			want: "*.psdb.cloud",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := matchManagedPGHostname(c.dsn)
			if got != c.want {
				t.Errorf("matchManagedPGHostname(%q) = %q; want %q", c.dsn, got, c.want)
			}
		})
	}
}

// TestApplyPatroniMode_Off pins that --patroni-mode=off strips
// every Patroni warning the engine emitted, leaves other warnings
// (e.g. wal_keep_size) intact, and never modifies the Refusal field.
func TestApplyPatroniMode_Off(t *testing.T) {
	s := &Streamer{SourceDSN: "postgres://u:p@aws-us-east-1.psdb.cloud:5432/db"}
	in := PreflightReport{
		Warnings: []string{
			"this PG cluster is HA-managed (Patroni-set GUC detected). [...]",
			"wal_keep_size = 16 MB looks small. [...]",
		},
		Refusal: "",
	}
	out := s.applyPatroniMode(in, PatroniModeOff)
	if len(out.Warnings) != 1 {
		t.Fatalf("len(Warnings) = %d; want 1 (only wal_keep_size kept)", len(out.Warnings))
	}
	if !strings.Contains(out.Warnings[0], "wal_keep_size") {
		t.Errorf("kept warning = %q; want wal_keep_size", out.Warnings[0])
	}
}

// TestApplyPatroniMode_On pins that --patroni-mode=on always emits
// the warning, even when the engine returned no Patroni signals AND
// the DSN hostname doesn't match a managed-PG pattern.
func TestApplyPatroniMode_On(t *testing.T) {
	s := &Streamer{SourceDSN: "postgres://u:p@db.example.com:5432/db"}
	in := PreflightReport{}
	out := s.applyPatroniMode(in, PatroniModeOn)
	if len(out.Warnings) != 1 {
		t.Fatalf("len(Warnings) = %d; want 1 (operator-forced warning)", len(out.Warnings))
	}
	if !strings.Contains(out.Warnings[0], "operator forced") {
		t.Errorf("warning = %q; want 'operator forced' substring", out.Warnings[0])
	}
}

// TestApplyPatroniMode_OnDeduplicates pins that --patroni-mode=on
// strips the engine-emitted Patroni warning before appending its own
// (avoid double-warn on the same condition).
func TestApplyPatroniMode_OnDeduplicates(t *testing.T) {
	s := &Streamer{SourceDSN: "postgres://u:p@aws-us-east-1.psdb.cloud:5432/db"}
	in := PreflightReport{
		Warnings: []string{
			"this PG cluster is HA-managed (Patroni-set GUC detected). [...]",
			"wal_keep_size = 16 MB looks small. [...]",
		},
	}
	out := s.applyPatroniMode(in, PatroniModeOn)
	if len(out.Warnings) != 2 {
		t.Fatalf("len(Warnings) = %d; want 2 (operator-forced + wal_keep_size)", len(out.Warnings))
	}
	patroniCount := 0
	for _, w := range out.Warnings {
		if strings.HasPrefix(w, patroniWarningPrefix) {
			patroniCount++
		}
	}
	if patroniCount != 1 {
		t.Errorf("Patroni-prefix warnings = %d; want exactly 1", patroniCount)
	}
}

// TestApplyPatroniMode_Auto_HostnameSignalAdded pins that under
// --patroni-mode=auto, when the engine returned no Patroni warning
// but the DSN hostname matches a managed-PG pattern, the streamer
// adds a Patroni warning citing the hostname pattern.
func TestApplyPatroniMode_Auto_HostnameSignalAdded(t *testing.T) {
	s := &Streamer{SourceDSN: "postgres://u:p@aws-us-east-1.psdb.cloud:5432/db"}
	in := PreflightReport{
		Warnings: []string{"wal_keep_size = 16 MB looks small. [...]"},
	}
	out := s.applyPatroniMode(in, PatroniModeAuto)
	patroniCount := 0
	for _, w := range out.Warnings {
		if strings.HasPrefix(w, patroniWarningPrefix) {
			patroniCount++
			if !strings.Contains(w, "*.psdb.cloud") {
				t.Errorf("Patroni warning = %q; want '*.psdb.cloud' substring", w)
			}
		}
	}
	if patroniCount != 1 {
		t.Errorf("Patroni-prefix warnings = %d; want exactly 1 (hostname-derived)", patroniCount)
	}
}

// TestApplyPatroniMode_Auto_NoDoubleWarn pins that under
// --patroni-mode=auto, when the engine ALREADY emitted a Patroni
// warning (signal 1-6 fired), the streamer does NOT also append the
// hostname-pattern warning — same condition, one warning.
func TestApplyPatroniMode_Auto_NoDoubleWarn(t *testing.T) {
	s := &Streamer{SourceDSN: "postgres://u:p@aws-us-east-1.psdb.cloud:5432/db"}
	in := PreflightReport{
		Warnings: []string{
			"this PG cluster is HA-managed (Patroni-set GUC detected). [...]",
		},
	}
	out := s.applyPatroniMode(in, PatroniModeAuto)
	patroniCount := 0
	for _, w := range out.Warnings {
		if strings.HasPrefix(w, patroniWarningPrefix) {
			patroniCount++
		}
	}
	if patroniCount != 1 {
		t.Errorf("Patroni-prefix warnings = %d; want exactly 1 (no double-warn)", patroniCount)
	}
}

// TestApplyPatroniMode_Auto_NoSignalNoWarn pins that under
// --patroni-mode=auto, with no engine Patroni warning AND no
// hostname-pattern match, no Patroni warning is added.
func TestApplyPatroniMode_Auto_NoSignalNoWarn(t *testing.T) {
	s := &Streamer{SourceDSN: "postgres://u:p@db.example.com:5432/db"}
	in := PreflightReport{
		Warnings: []string{"wal_keep_size = 16 MB looks small. [...]"},
	}
	out := s.applyPatroniMode(in, PatroniModeAuto)
	for _, w := range out.Warnings {
		if strings.HasPrefix(w, patroniWarningPrefix) {
			t.Errorf("unexpected Patroni warning under auto + no signal: %q", w)
		}
	}
}

// TestApplyPatroniMode_RefusalPreserved pins that no mode shape
// modifies the Refusal field — slot-missing / wal_status='lost'
// always trips regardless of operator's --patroni-mode choice.
func TestApplyPatroniMode_RefusalPreserved(t *testing.T) {
	s := &Streamer{SourceDSN: "postgres://u:p@aws-us-east-1.psdb.cloud:5432/db"}
	for _, mode := range []string{PatroniModeAuto, PatroniModeOn, PatroniModeOff} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			in := PreflightReport{
				Refusal: `replication slot "sluice_slot" wal_status="lost"; recreate slot.`,
				Warnings: []string{
					"this PG cluster is HA-managed (...).",
				},
			}
			out := s.applyPatroniMode(in, mode)
			if out.Refusal != in.Refusal {
				t.Errorf("mode=%s: Refusal modified; got %q; want %q",
					mode, out.Refusal, in.Refusal)
			}
		})
	}
}
