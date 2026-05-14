// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import "crypto/sha256"

// sha256SumImpl is the concrete crypto/sha256 wrapper; isolated in
// its own file to keep redact_flag.go's import surface focused on
// flag-parsing concerns.
func sha256SumImpl(b []byte) [32]byte {
	return sha256.Sum256(b)
}
