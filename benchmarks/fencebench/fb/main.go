// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// fb — fence benchmark helper: setup/seed, backlog generation, catch-up wait,
// verify. A separate module (benchmarks/fencebench/fb/go.mod) so it builds
// on its own and stays out of sluice's own build, vet and lint.
package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func open(kind, dsn string) *sql.DB {
	drv := "pgx"
	if kind == "mysql" {
		drv = "mysql"
	}
	db, err := sql.Open(drv, dsn)
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(32)
	return db
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERR:", err)
		os.Exit(1)
	}
}

// tables for workload: A=u (unique email), B=p (no unique), C=u+p.
func tablesFor(w string) []string {
	switch w {
	case "A":
		return []string{"u"}
	case "B":
		return []string{"p"}
	case "D":
		return []string{"k"}
	default:
		return []string{"u", "p"}
	}
}

const seedRows = 10000

func setup(kind string, db *sql.DB, w string) {
	for _, t := range tablesFor(w) {
		uniq := ""
		if t == "u" {
			uniq = " UNIQUE"
		}
		ddl := fmt.Sprintf("CREATE TABLE %s (id BIGINT PRIMARY KEY, email VARCHAR(100) NOT NULL%s, name VARCHAR(100) NOT NULL, n BIGINT NOT NULL)", t, uniq)
		_, err := db.Exec(ddl)
		must(err)
		// seed
		for base := 1; base <= seedRows; base += 1000 {
			var sb strings.Builder
			sb.WriteString("INSERT INTO " + t + " (id,email,name,n) VALUES ")
			for i := base; i < base+1000; i++ {
				if i > base {
					sb.WriteString(",")
				}
				fmt.Fprintf(&sb, "(%d,'seed%d@x','name%d',0)", i, i, i)
			}
			_, err := db.Exec(sb.String())
			must(err)
		}
	}
}

func ph(kind string, i int) string {
	if kind == "mysql" {
		return "?"
	}
	return fmt.Sprintf("$%d", i)
}

// gen issues n source transactions of 1-3 rows each across `workers` connections.
func gen(kind string, db *sql.DB, w string, n, workers int) {
	var wg sync.WaitGroup
	per := n / workers
	start := time.Now()
	for k := 0; k < workers; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(k) + 42))
			nextID := int64(1_000_000 * (k + 1)) // disjoint insert ranges per worker
			var owned []int64
			for id := int64(k + 1); id <= seedRows; id += int64(workers) {
				owned = append(owned, id)
			}
			for i := 0; i < per; i++ {
				tbl := "u"
				switch w {
				case "B":
					tbl = "p"
				case "C":
					if r.Intn(100) >= 20 {
						tbl = "p"
					}
				case "D":
					tbl = "k"
				}
				rows := 1 + r.Intn(3)
				tx, err := db.Begin()
				must(err)
				for j := 0; j < rows; j++ {
					ord := int64(k)*1_000_000_000 + int64(i)*10 + int64(j) + 1
					if w == "D" && r.Intn(2) == 1 {
						// PK-changing update: a lane barrier
						ix := r.Intn(len(owned))
						nextID++
						_, err = tx.Exec(fmt.Sprintf("UPDATE %s SET id=%s, n=%s WHERE id=%s", tbl, ph(kind, 1), ph(kind, 2), ph(kind, 3)), nextID, ord, owned[ix])
						owned[ix] = nextID
					} else if r.Intn(2) == 0 {
						nextID++
						_, err = tx.Exec(fmt.Sprintf("INSERT INTO %s (id,email,name,n) VALUES (%s,%s,%s,%s)", tbl, ph(kind, 1), ph(kind, 2), ph(kind, 3), ph(kind, 4)),
							nextID, fmt.Sprintf("w%d-%d@x", k, nextID), fmt.Sprintf("ins%d", ord), ord)
					} else {
						// update a seeded row owned by this worker (id ≡ k mod workers) to avoid lock waits
						id := int64(r.Intn(seedRows/workers)*workers + k + 1)
						if id > seedRows {
							id = int64(k + 1)
						}
						_, err = tx.Exec(fmt.Sprintf("UPDATE %s SET name=%s, n=%s, email=%s WHERE id=%s", tbl, ph(kind, 1), ph(kind, 2), ph(kind, 3), ph(kind, 4)),
							fmt.Sprintf("upd%d", ord), ord, fmt.Sprintf("e%d-%d@x", id, ord), id)
					}
					must(err)
				}
				must(tx.Commit())
			}
		}(k)
	}
	wg.Wait()
	fmt.Printf("gen: %d txns in %.1fs\n", per*workers, time.Since(start).Seconds())
}

func cksum(db *sql.DB, tables []string, qual string) string {
	var parts []string
	for _, t := range tables {
		var c, s1, s2, s3 sql.NullInt64
		err := db.QueryRow(fmt.Sprintf("SELECT count(*), sum(n), sum(id), sum(length(email)+length(name)) FROM %s%s", qual, t)).Scan(&c, &s1, &s2, &s3)
		if err != nil {
			return "err:" + err.Error()
		}
		parts = append(parts, fmt.Sprintf("%s:%d/%d/%d/%d", t, c.Int64, s1.Int64, s2.Int64, s3.Int64))
	}
	return strings.Join(parts, ",")
}

func fullHash(db *sql.DB, tables []string, qual string) string {
	h := sha256.New()
	rowsN := 0
	for _, t := range tables {
		rows, err := db.Query(fmt.Sprintf("SELECT id,email,name,n FROM %s%s ORDER BY id", qual, t))
		must(err)
		for rows.Next() {
			var id, n int64
			var e, nm string
			must(rows.Scan(&id, &e, &nm, &n))
			fmt.Fprintf(h, "%s|%d|%s|%s|%d\n", t, id, e, nm, n)
			rowsN++
		}
		must(rows.Err())
		rows.Close()
	}
	return fmt.Sprintf("%d:%s", rowsN, hex.EncodeToString(h.Sum(nil))[:16])
}

func main() {
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	sk := fs.String("sk", "mysql", "source kind")
	sd := fs.String("sd", "", "source dsn")
	tk := fs.String("tk", "postgres", "target kind")
	td := fs.String("td", "", "target dsn")
	tq := fs.String("tq", "", "target table qualifier (e.g. bench.)")
	w := fs.String("w", "A", "workload")
	n := fs.Int("n", 1000, "txns")
	workers := fs.Int("workers", 8, "gen workers")
	timeout := fs.Duration("timeout", 30*time.Minute, "wait timeout")
	must(fs.Parse(os.Args[2:]))
	switch cmd {
	case "setup":
		setup(*sk, open(*sk, *sd), *w)
	case "gen":
		gen(*sk, open(*sk, *sd), *w, *n, *workers)
	case "ping":
		db := open(*tk, *td)
		must(db.Ping())
		t0 := time.Now()
		for i := 0; i < 20; i++ {
			var x int
			must(db.QueryRow("SELECT 1").Scan(&x))
		}
		fmt.Printf("target rtt: %.2fms\n", float64(time.Since(t0).Microseconds())/20/1000)
	case "wait":
		src := open(*sk, *sd)
		tgt := open(*tk, *td)
		tables := tablesFor(*w)
		want := cksum(src, tables, "")
		t0 := time.Now()
		base := cksum(tgt, tables, *tq)
		first := -1.0
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		lastPrint := time.Now()
		for {
			got := cksum(tgt, tables, *tq)
			el := time.Since(t0).Seconds()
			if first < 0 && got != base && !strings.HasPrefix(got, "err:") {
				first = el
			}
			if got == want {
				fmt.Printf("RESULT first=%.2f done=%.2f\n", first, el)
				return
			}
			if time.Since(lastPrint) > 30*time.Second {
				fmt.Printf("  t=%.0fs tgt=%s want=%s\n", el, got, want)
				lastPrint = time.Now()
			}
			if ctx.Err() != nil {
				fmt.Printf("RESULT TIMEOUT first=%.2f at=%.2f tgt=%s want=%s\n", first, el, got, want)
				os.Exit(2)
			}
			time.Sleep(200 * time.Millisecond)
		}
	case "verify":
		tables := tablesFor(*w)
		a := fullHash(open(*sk, *sd), tables, "")
		b := fullHash(open(*tk, *td), tables, *tq)
		if a == b {
			fmt.Println("VERIFY OK", a)
		} else {
			fmt.Println("VERIFY MISMATCH src", a, "tgt", b)
			os.Exit(3)
		}
	}
}
