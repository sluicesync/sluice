// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import "sluicesync.dev/sluice/internal/ir"

// cdcUnsupportedError resolves the refusal for a CDC-requiring mode
// whose source engine declares [ir.CDCNone]. If the engine supplies a
// flavor-specific story via [ir.CDCUnsupportedExplainer] (e.g. the
// mysql engine's mariadb flavor: a coded refusal naming the MariaDB
// domain-GTID gap and the bulk-migrate/backup alternatives), that
// error wins; otherwise the caller's generic message applies. Called
// ONLY after the caller has established Capabilities().CDC == CDCNone
// — the explainer never overrides a real CDC declaration.
func cdcUnsupportedError(e ir.Engine, generic error) error {
	if x, ok := e.(ir.CDCUnsupportedExplainer); ok {
		if err := x.ExplainCDCUnsupported(); err != nil {
			return err
		}
	}
	return generic
}
