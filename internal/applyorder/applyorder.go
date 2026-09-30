// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package applyorder records the statements each target transaction sent, in
// order, and checks them against the appliers' DATA BEFORE CONTROL rule. It
// is test support: an engine's write-core roster wires a [Recorder] into a
// recording connection (a database/sql driver wrapper on MySQL, a pgx tracer
// on Postgres), drives each write core, and hands the transactions to
// [Violations].
//
// # The rule (GC-41 (c))
//
// Every row statement of a target transaction is sent before its first
// control-table statement. On a vtgate target in transaction_mode=MULTI with
// the control tables in a --control-keyspace sidecar, the commit has no 2PC:
// vtgate commits the shards in the order the transaction first touched them
// and stops at the first failure. Data first, a tear leaves the position
// behind the data and the change replays; control first, a tear leaves the
// position PAST rows that never committed — silent loss. v0.156.5 and
// v0.156.6 shipped the second order on the MySQL batch paths.
//
// Classification is by text, so its reach is stated: a statement naming a
// sluice control table (every one is spelled sluice_…) is CONTROL whatever
// its verb; an INSERT / UPDATE / DELETE / REPLACE / TRUNCATE naming none is
// DATA; everything else (BEGIN, SET, a catalog SELECT) is neither. A test
// table whose name contains "sluice_" would read as control — the rosters'
// tables avoid it.
package applyorder

import (
	"fmt"
	"strings"
	"sync"
)

// Kind is a statement's role in the rule.
type Kind int

// The statement kinds.
const (
	Other Kind = iota
	Data
	Control
)

// Classify names sql's kind (see the package doc for the reach).
func Classify(sql string) Kind {
	s := strings.ToLower(strings.TrimSpace(sql))
	if strings.Contains(s, "sluice_") {
		return Control
	}
	for _, verb := range []string{"insert", "update", "delete", "replace", "truncate"} {
		if strings.HasPrefix(s, verb) {
			return Data
		}
	}
	return Other
}

// Tx is one target transaction: the statements its session sent between
// BEGIN and COMMIT or ROLLBACK, in order.
type Tx struct {
	Stmts     []string
	Committed bool
}

// Has reports whether the transaction sent a statement of kind k.
func (tx Tx) Has(k Kind) bool {
	for _, s := range tx.Stmts {
		if Classify(s) == k {
			return true
		}
	}
	return false
}

// Mixed reports whether the transaction carried both data and control — the
// only shape the rule can be broken in.
func (tx Tx) Mixed() bool { return tx.Has(Data) && tx.Has(Control) }

// Violation describes a data statement sent after a control statement in
// tx, or returns "" when the order holds.
func (tx Tx) Violation() string {
	firstControl := -1
	for i, s := range tx.Stmts {
		switch Classify(s) {
		case Control:
			if firstControl < 0 {
				firstControl = i
			}
		case Data:
			if firstControl >= 0 {
				return fmt.Sprintf("data statement #%d %q follows control statement #%d %q",
					i, abbreviate(tx.Stmts[i]), firstControl, abbreviate(tx.Stmts[firstControl]))
			}
		case Other:
		}
	}
	return ""
}

// Recorder collects transactions per session. A session is whatever the
// connection layer can key on (a connection pointer); statements outside a
// transaction are ignored. Safe for concurrent use.
type Recorder struct {
	mu   sync.Mutex
	open map[any]*Tx
	done []Tx
}

// Begin opens a transaction on session.
func (r *Recorder) Begin(session any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.open == nil {
		r.open = map[any]*Tx{}
	}
	r.open[session] = &Tx{}
}

// Statement records sql on session's open transaction, if any.
func (r *Recorder) Statement(session any, sql string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if tx, ok := r.open[session]; ok {
		tx.Stmts = append(tx.Stmts, sql)
	}
}

// End closes session's transaction.
func (r *Recorder) End(session any, committed bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if tx, ok := r.open[session]; ok {
		tx.Committed = committed
		r.done = append(r.done, *tx)
		delete(r.open, session)
	}
}

// Take returns the transactions closed since the last Take.
func (r *Recorder) Take() []Tx {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.done
	r.done = nil
	return out
}

func abbreviate(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 80 {
		return s[:80] + "…"
	}
	return s
}
