// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package safeerr holds error-shaping helpers that must be usable from
// dependency-light packages.
//
// It exists because of a layering violation (audit 2026-07-26 ARCH-2):
// internal/telemetrysink's package doc declared "imports NO engine package and
// NOT internal/ir", and that claim was false — it imported internal/diagnose
// for one six-line helper, and diagnose's transitive closure includes
// internal/ir. The doc was the only thing holding the boundary, and it had
// already been breached.
//
// The helper itself is a genuine leaf (net/url + errors), so the honest fix was
// to give it a leaf home rather than weaken the claim or duplicate the code.
// Anything added here must stay leaf: standard library only.
package safeerr

import (
	"errors"
	"net/url"
)

// SafeParseError unwraps a *url.Error to its cause.
//
// url.Error's Error() embeds the full URL, so logging one verbatim leaks
// whatever the URL carries — for a signed scrape or sink endpoint that is a
// credential in the query string. Returning the inner error keeps the useful
// part (the transport failure) and drops the locator.
func SafeParseError(err error) error {
	var uerr *url.Error
	if errors.As(err, &uerr) {
		return uerr.Err
	}
	return err
}
