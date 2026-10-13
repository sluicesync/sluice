// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"net/url"
	"strings"
)

// EncodeURIQuery renders the query string of a `postgres://` connection
// URI that sluice has rewritten (stripped `schema`, stamped
// `application_name`, added `replication=database`, …).
//
// It exists because [url.Values.Encode] is an HTML-form encoder, not a
// connection-URI encoder: it writes a space as `+`. libpq — and pgx from
// v5.11, which adopted libpq's URI grammar — reads `+` in a query value as
// a LITERAL plus. So `options=-c%20search_path%3Dapp`, re-encoded by
// Encode, reaches pgx 5.11 as `options="-c+search_path=app"`. That fails
// loudly when the `+` lands in a parameter name (`unrecognized
// configuration parameter "+search_path"`) and silently when it lands in
// a value: `-csearch_path=app -cstatement_timeout=5` connects with
// search_path `app+-cstatement_timeout=5` and no statement timeout. The
// Neki shard recipe (`options=-c __neki.shard=<uid>`) is the same shape.
//
// Encoding a space as `%20` is read as a space by every grammar in play
// (pgx before and after 5.11, libpq). The rewrite below is sound because
// Encode never emits a `+` for anything except a space: a real `+` in a
// value is escaped to `%2B` first, so after Encode every remaining `+`
// stands for a space. TestEncodeURIQuery_PlusIsOnlyEverASpace pins that
// premise against Encode itself.
func EncodeURIQuery(q url.Values) string {
	return strings.ReplaceAll(q.Encode(), "+", "%20")
}
