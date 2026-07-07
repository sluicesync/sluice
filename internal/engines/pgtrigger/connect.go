// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"sluicesync.dev/sluice/internal/diagnose"
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

func parseKVDSN(dsn string) (*pgConfig, error) {
	schema := ""
	keepers := []string{}
	for _, tok := range strings.Fields(dsn) {
		k, v, ok := strings.Cut(tok, "=")
		if !ok {
			keepers = append(keepers, tok)
			continue
		}
		if strings.EqualFold(k, "schema") {
			schema = v
			continue
		}
		keepers = append(keepers, tok)
	}
	if schema == "" {
		schema = "public"
	}
	return &pgConfig{dsn: strings.Join(keepers, " "), schema: schema}, nil
}
