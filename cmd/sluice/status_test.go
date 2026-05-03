package main

import (
	"strings"
	"testing"
	"time"
)

// TestHumanAgo covers the small "5m ago" / "2h ago" formatter the
// status command uses to make stuck-stream detection easier at a
// glance. The formatter degrades to seconds for sub-minute ages
// and to days past 24 hours.
func TestHumanAgo(t *testing.T) {
	cases := []struct {
		name string
		d    time.Duration
		want string
	}{
		{"30 seconds", 30 * time.Second, "30s ago"},
		{"under a second", 200 * time.Millisecond, "0s ago"},
		{"5 minutes", 5 * time.Minute, "5m ago"},
		{"59 minutes", 59 * time.Minute, "59m ago"},
		{"3 hours", 3 * time.Hour, "3h ago"},
		{"23 hours", 23 * time.Hour, "23h ago"},
		{"2 days", 48 * time.Hour, "2d ago"},
		{"future negative", -time.Minute, "in the future"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := humanAgo(c.d); got != c.want {
				t.Errorf("humanAgo(%v) = %q; want %q", c.d, got, c.want)
			}
		})
	}
}

// TestTruncatePositionToken covers the token-truncation helper.
// The status table prints a fixed-width column; long JSON tokens
// must elide cleanly so other columns stay aligned.
func TestTruncatePositionToken(t *testing.T) {
	cases := []struct {
		name string
		in   string
		max  int
		want string
	}{
		{"short token unchanged", "abc", 60, "abc"},
		{"exactly at limit", strings.Repeat("a", 60), 60, strings.Repeat("a", 60)},
		{"longer truncates with ellipsis", strings.Repeat("a", 100), 10, strings.Repeat("a", 9) + "…"},
		{"empty stays empty", "", 60, ""},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := truncatePositionToken(c.in, c.max); got != c.want {
				t.Errorf("got %q; want %q", got, c.want)
			}
		})
	}
}
