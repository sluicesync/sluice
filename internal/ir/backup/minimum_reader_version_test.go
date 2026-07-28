package backup

import (
	"fmt"
	"regexp"
	"testing"
)

// TestMinimumReaderVersion_CoversEveryDeclaredVersion is the lock-step gate
// on [minimumReaderVersion]. The table exists so an operator told to
// "upgrade sluice" is told WHICH version to upgrade to; a table that is
// missing the newest tier answers that question with silence at exactly the
// moment it is asked, so the next [BackupFormatVersion] bump must fail here
// until its entry is added.
//
// This is the cheap half of the roadmap-item-90 discipline: the expensive
// half is knowing a chain's floor moved, and neither is any use if the
// version-to-release map has a hole in it.
func TestMinimumReaderVersion_CoversEveryDeclaredVersion(t *testing.T) {
	// Release tags, not bare semver: the string goes straight into operator
	// messages next to a `git`/download reference, so the leading v matters.
	tagRE := regexp.MustCompile(`^v\d+\.\d+\.\d+$`)

	for fv := FormatVersionLegacy; fv <= BackupFormatVersion; fv++ {
		got := MinimumReaderVersion(fv)
		if got == "" {
			t.Errorf("MinimumReaderVersion(%d) = \"\"; every format version through BackupFormatVersion (%d) needs an entry — "+
				"a bump without one leaves the cross-version refusal telling operators to upgrade without saying to what",
				fv, BackupFormatVersion)
			continue
		}
		if !tagRE.MatchString(got) {
			t.Errorf("MinimumReaderVersion(%d) = %q; want a release tag like \"v0.104.0\"", fv, got)
		}
	}

	// A version from the future has no honest answer, and guessing one
	// ("upgrade to v0.104.0") would send an operator to a binary that still
	// cannot read it. Silence is the contract.
	if got := MinimumReaderVersion(BackupFormatVersion + 1); got != "" {
		t.Errorf("MinimumReaderVersion(%d) = %q; want \"\" — an unknown future version must not be answered with a guess",
			BackupFormatVersion+1, got)
	}
}

// TestMinimumReaderVersion_IsMonotonic pins the property that makes the
// table usable as a floor: a higher format version can never be readable by
// an OLDER release than a lower one. If that inverted, the chain-floor
// warning would understate which binaries it just locked out.
func TestMinimumReaderVersion_IsMonotonic(t *testing.T) {
	for fv := FormatVersionLegacy + 1; fv <= BackupFormatVersion; fv++ {
		prev, cur := MinimumReaderVersion(fv-1), MinimumReaderVersion(fv)
		if prev == "" || cur == "" {
			continue // covered by the coverage test above
		}
		if compareReleaseTags(t, cur, prev) < 0 {
			t.Errorf("MinimumReaderVersion(%d) = %q is OLDER than MinimumReaderVersion(%d) = %q; "+
				"the table must be non-decreasing or it cannot be read as a floor", fv, cur, fv-1, prev)
		}
	}
}

// compareReleaseTags orders two vMAJOR.MINOR.PATCH tags numerically per
// component, so v0.99.228 sorts before v0.104.0 (a string compare gets that
// backwards, which is the whole reason this exists).
func compareReleaseTags(t *testing.T, a, b string) int {
	t.Helper()
	pa, pb := parseReleaseTag(t, a), parseReleaseTag(t, b)
	for i := range pa {
		switch {
		case pa[i] < pb[i]:
			return -1
		case pa[i] > pb[i]:
			return 1
		}
	}
	return 0
}

func parseReleaseTag(t *testing.T, tag string) [3]int {
	t.Helper()
	var out [3]int
	n, err := fmt.Sscanf(tag, "v%d.%d.%d", &out[0], &out[1], &out[2])
	if err != nil || n != 3 {
		t.Fatalf("parse release tag %q: parsed %d components: %v", tag, n, err)
	}
	return out
}
