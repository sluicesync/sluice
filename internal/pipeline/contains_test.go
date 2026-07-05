// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// contains is the std library's strings.Contains hand-rolled to keep
// the test files' import lists minimal. A local copy lives in migcore's
// chunk_test.go too — a private test helper does not cross a package
// boundary, so both packages carry their own (the blobcodec-carve
// convention).
func contains(haystack, needle string) bool {
	if needle == "" {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
