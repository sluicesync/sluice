// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"sluicesync.dev/sluice/internal/diagnose"
	"sluicesync.dev/sluice/internal/engines/postgres"
)

// pgConfig is the engine-local mirror of the vanilla postgres
// engine's pgConfig. We don't import the sibling package's
// unexported type, but the parse shape is intentionally identical —
// operators thread DSNs through one or the other engine and expect
// the same `schema` query parameter to land in the same place.
type pgConfig struct {
	dsn    string // DSN with `schema` stripped, ready for the pgx driver
	schema string // PG schema (namespace), defaulting to "public"
}

// parseDSNCompat extracts the schema from a PG DSN and returns the
// driver-ready remainder. Both URI and KV forms are accepted; the
// shape mirrors postgres.parseDSN.
func parseDSNCompat(dsn string) (*pgConfig, error) {
	if dsn == "" {
		return nil, errors.New("pgtrigger: DSN is empty")
	}
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		return parseURIDSN(dsn)
	}
	return parseKVDSN(dsn)
}

func parseURIDSN(dsn string) (*pgConfig, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgtrigger: invalid DSN URI: %w", diagnose.SafeParseError(err))
	}
	if strings.TrimPrefix(u.Path, "/") == "" {
		return nil, errors.New("pgtrigger: DSN must include a database name")
	}
	q := u.Query()
	schema := q.Get("schema")
	if schema == "" {
		schema = "public"
	}
	q.Del("schema")
	u.RawQuery = q.Encode()
	return &pgConfig{dsn: u.String(), schema: schema}, nil
}

// parseKVDSN reads sluice's `schema` setting out of a libpq key/value
// connection string and hands the driver the rest of it VERBATIM.
//
// This was its own whitespace-splitting copy of the postgres engine's
// parser, and it carried that parser's defect with it (audit
// A0915-VF2-PGDSN-1): a token living INSIDE a quoted value was
// recognised as a `schema=` setting and deleted from the string handed
// to pgx, so `password='s schema=x' dbname=real` reached the driver as
// `password='s dbname=real` — a truncated credential. Seven call sites
// in this package reach it through [parseDSNCompat].
//
// It now delegates, for the reason [Engine.SourceIdentity] already
// delegates: two copies that disagree about a schema make a
// `migrate --resume` across the two drivers against ONE database refuse,
// and no test exercising either engine alone would notice.
func parseKVDSN(dsn string) (*pgConfig, error) {
	schema, rest := postgres.SplitSchemaFromKVDSN(dsn)
	if schema == "" {
		schema = "public"
	}
	return &pgConfig{dsn: rest, schema: schema}, nil
}
