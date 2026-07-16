// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Command duckdbverify is the external-reader compatibility gate for
// the Parquet export (audit MED-T3). Every in-repo pin of the export
// reads the files back with the SAME parquet-go the writer uses, so a
// symmetric parquet-go regression — files sluice both writes and reads
// "correctly" that external readers cannot decode — would pass every
// pin and ship silently-unreadable files (the Bug-74 shape relocated
// to the file-format boundary). This tool closes that gap by producing
// a deterministic export covering the full value family × shape matrix
// through the REAL TableCodec + writer configuration, together with
// hand-derived expected values, for a real DuckDB to read back and
// compare exactly (.github/workflows/duckdb-verify.yml).
//
// Two modes:
//
//	duckdbverify gen -out DIR
//	    writes the .parquet files, checks.json (the expected values,
//	    per check), and script.sql (the DuckDB queries, one per check,
//	    in checks.json order).
//	duckdbverify check -dir DIR -actual FILE
//	    compares DuckDB's JSON output (one JSON array per SELECT, in
//	    script order — `duckdb -json < script.sql`) against checks.json
//	    exactly; any mismatch is a loud per-check FAIL + exit 1.
//
// The comparison is exact-by-canonical-JSON: both sides are decoded
// with json.Number (no float64 mangling of uint64 max or wide
// decimals) and re-marshalled with sorted keys before comparing.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/parquet-go/parquet-go"

	"sluicesync.dev/sluice/internal/pipeline/parquetexport"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "duckdbverify:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New(`usage: duckdbverify gen -out DIR | check -dir DIR -actual FILE`)
	}
	switch args[0] {
	case "gen":
		fs := flag.NewFlagSet("gen", flag.ContinueOnError)
		out := fs.String("out", "", "output directory (created if missing)")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *out == "" {
			return errors.New("gen: -out is required")
		}
		return generate(*out)
	case "check":
		fs := flag.NewFlagSet("check", flag.ContinueOnError)
		dir := fs.String("dir", "", "directory holding checks.json")
		actual := fs.String("actual", "", "file holding DuckDB's -json output for script.sql")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *dir == "" || *actual == "" {
			return errors.New("check: -dir and -actual are required")
		}
		return compare(*dir, *actual)
	}
	return fmt.Errorf("unknown mode %q (want gen or check)", args[0])
}

// checksFileName and scriptFileName are the two gen artifacts beside
// the .parquet files.
const (
	checksFileName = "checks.json"
	scriptFileName = "script.sql"
)

// generate writes the matrix export + expected values + query script.
func generate(outDir string) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	for _, spec := range tableSpecs() {
		if err := writeTable(outDir, spec); err != nil {
			return fmt.Errorf("table %s: %w", spec.name, err)
		}
	}
	checks := allChecks()
	b, err := json.MarshalIndent(checks, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outDir, checksFileName), b, 0o644); err != nil {
		return err
	}
	var script strings.Builder
	// UTC pins the TIMESTAMPTZ / to_json(TIMESTAMPTZ[]) renderings.
	script.WriteString("SET TimeZone='UTC';\n")
	for _, c := range checks {
		script.WriteString(c.Query)
		script.WriteString("\n")
	}
	if err := os.WriteFile(filepath.Join(outDir, scriptFileName), []byte(script.String()), 0o644); err != nil {
		return err
	}
	fmt.Printf("duckdbverify: wrote %d tables + %d checks to %s\n", len(tableSpecs()), len(checks), outDir)
	return nil
}

// writeTable encodes one spec through the real TableCodec with the
// exporter's writer configuration (zstd, footer kv metadata, one Flush
// — hence one row group — per chunk; see export_parquet.go).
func writeTable(outDir string, spec tableSpec) error {
	tc, err := parquetexport.NewTableCodec(spec.table)
	if err != nil {
		return err
	}
	f, err := os.Create(filepath.Join(outDir, spec.name+".parquet"))
	if err != nil {
		return err
	}
	opts := []parquet.WriterOption{
		tc.Schema,
		parquet.Compression(&parquet.Zstd),
		parquet.KeyValueMetadata("sluice:format", "1"),
		parquet.KeyValueMetadata("sluice:table", spec.name),
	}
	for k, v := range tc.Metadata {
		opts = append(opts, parquet.KeyValueMetadata(k, v))
	}
	w := parquet.NewGenericWriter[map[string]any](f, opts...)
	for _, chunk := range spec.chunks {
		batch := make([]map[string]any, 0, len(chunk))
		for i, row := range chunk {
			enc, err := tc.EncodeRow(row)
			if err != nil {
				_ = f.Close()
				return fmt.Errorf("encode row %d: %w", i, err)
			}
			batch = append(batch, enc)
		}
		if _, err := w.Write(batch); err != nil {
			_ = f.Close()
			return fmt.Errorf("write chunk: %w", err)
		}
		if err := w.Flush(); err != nil {
			_ = f.Close()
			return fmt.Errorf("flush row group: %w", err)
		}
	}
	if err := w.Close(); err != nil {
		_ = f.Close()
		return fmt.Errorf("close writer: %w", err)
	}
	return f.Close()
}

// compare reads checks.json and DuckDB's concatenated -json output
// (one JSON array per SELECT, in script order) and compares each
// check's rows exactly.
func compare(dir, actualPath string) error {
	cb, err := os.ReadFile(filepath.Join(dir, checksFileName))
	if err != nil {
		return err
	}
	var checks []check
	// UseNumber: a plain Unmarshal would route uint64-range wants
	// through float64 (18446744073709551615 → …552000) — exactly the
	// mangling class this gate exists to catch on the DuckDB side.
	cdec := json.NewDecoder(strings.NewReader(string(cb)))
	cdec.UseNumber()
	if err := cdec.Decode(&checks); err != nil {
		return fmt.Errorf("parse %s: %w", checksFileName, err)
	}
	af, err := os.Open(actualPath)
	if err != nil {
		return err
	}
	defer func() { _ = af.Close() }()

	dec := json.NewDecoder(af)
	dec.UseNumber()
	failed := 0
	for _, c := range checks {
		var actual any
		if err := dec.Decode(&actual); err != nil {
			return fmt.Errorf("check %q: DuckDB output ended early (or is not JSON): %w — run script.sql with `duckdb -bail -json` and capture stdout", c.Name, err)
		}
		wantCanon, err := canonicalJSON(c.Want)
		if err != nil {
			return fmt.Errorf("check %q: canonicalize want: %w", c.Name, err)
		}
		gotCanon, err := canonicalJSON(actual)
		if err != nil {
			return fmt.Errorf("check %q: canonicalize got: %w", c.Name, err)
		}
		if wantCanon != gotCanon {
			failed++
			fmt.Printf("FAIL %s\n  want %s\n  got  %s\n", c.Name, wantCanon, gotCanon)
			continue
		}
		fmt.Printf("PASS %s\n", c.Name)
	}
	// Trailing output means script.sql and checks.json disagree — a
	// generator bug, refused as loudly as a value mismatch.
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("DuckDB produced more result sets than checks.json has checks (script/checks drift): %v", extra)
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d checks FAILED — the Parquet export is not readable/exact under real DuckDB", failed, len(checks))
	}
	fmt.Printf("duckdbverify: all %d checks passed\n", len(checks))
	return nil
}

// canonicalJSON renders v (a json.Number-decoded or raw-Go value tree)
// as canonical JSON: sorted object keys, numbers verbatim.
func canonicalJSON(v any) (string, error) {
	// Round-trip through json.Number decoding so a raw-Go want (built
	// by the matrix) and DuckDB's decoded output canonicalize the same
	// way regardless of the Go types used to express them.
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.UseNumber()
	var norm any
	if err := dec.Decode(&norm); err != nil {
		return "", err
	}
	out, err := json.Marshal(norm)
	if err != nil {
		return "", err
	}
	return string(out), nil
}
