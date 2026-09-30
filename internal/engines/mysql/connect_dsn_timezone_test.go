// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestFinishParseDSN_TimeZoneSpellings pins GC-39 item 1 through the real
// DSN parser (the driver URL-decodes parameter values, so the encoded and
// raw spellings must land on the same verdict): a DSN `time_zone=` that
// names UTC — in any spelling — is accepted and kept, and one that names
// any other zone is refused with DSN-TIME-ZONE-NOT-UTC rather than
// silently shifting every TIMESTAMP by its offset. Both entry points
// (parseDSN and the database-optional parseServerDSN) share the check.
func TestFinishParseDSN_TimeZoneSpellings(t *testing.T) {
	const base = "u:p@tcp(127.0.0.1:3306)/db?"
	accepted := []string{
		"time_zone=%27%2B00%3A00%27", // '+00:00', URL-encoded
		"time_zone='+00:00'",
		"time_zone=%27%2B0%3A00%27", // '+0:00'
		"time_zone=%27-00%3A00%27",  // '-00:00'
		"time_zone='UTC'",
		"time_zone=UTC",
		"time_zone=%27utc%27",
		"time_zone=%22UTC%22", // "UTC"
		"time_zone=%27Etc%2FUniversal%27",
		"time_zone=%27Etc%2FUTC%27",
		"time_zone='GMT'",
		"TIME_ZONE='+00:00'",
		"LOCAL time_zone='+00:00'",
		"@@local.time_zone=%27UTC%27",
	}
	for _, q := range accepted {
		for name, parse := range map[string]func(string) error{
			"parseDSN":       func(d string) error { _, err := parseDSN(d); return err },
			"parseServerDSN": func(d string) error { _, err := parseServerDSN(d); return err },
		} {
			if err := parse(base + q); err != nil {
				t.Errorf("%s(%q) = %v; want accepted — the value names UTC", name, q, err)
			}
		}
	}
	cfg, err := parseDSN(base + "time_zone=%27UTC%27")
	if err != nil || cfg.Params["time_zone"] != "'UTC'" {
		t.Errorf("an accepted UTC override must be kept as given: params[time_zone] = %q (err %v)", cfg.Params["time_zone"], err)
	}
	cfg, err = parseDSN(base + "parseTime=true")
	if err != nil || cfg.Params["time_zone"] != "'+00:00'" {
		t.Errorf("absent time_zone must still be injected as '+00:00': got %q (err %v)", cfg.Params["time_zone"], err)
	}

	refused := []string{
		"time_zone=%27%2B09%3A00%27", // '+09:00' — the measured ±9h shift
		"time_zone='+09:00'",
		"time_zone=%27-07%3A00%27",
		"time_zone='+00:30'",
		"time_zone='+01:00'",
		"time_zone=%27Asia%2FTokyo%27",
		"time_zone=%27America%2FLos_Angeles%27",
		"time_zone=%27Europe%2FLondon%27", // UTC in winter only
		"time_zone='SYSTEM'",              // the server host's zone, unknowable here
		"time_zone=''",
		"TIME_ZONE='+09:00'", // MySQL variable names are case-insensitive
		"Time_Zone='+09:00'", // …so every casing reaches the same session variable
		"@@time_zone='+09:00'",
		"@@session.time_zone='+09:00'",
		// The pre-tag review's F1 spellings: scope prefixes and words the
		// driver sends raw in SET <key>=<val>.
		"@@local.time_zone='+09:00'",
		"@@LOCAL.time_zone='+09:00'",
		"LOCAL time_zone='+09:00'",
		"SESSION time_zone='+09:00'",
		// GLOBAL would change the server for every client: refused whatever the value.
		"@@global.time_zone='+00:00'",
		"GLOBAL time_zone='UTC'",
	}
	for _, q := range refused {
		for name, parse := range map[string]func(string) error{
			"parseDSN":       func(d string) error { _, err := parseDSN(d); return err },
			"parseServerDSN": func(d string) error { _, err := parseServerDSN(d); return err },
		} {
			err := parse(base + q)
			if !errors.Is(err, ir.ErrDSNTimeZoneNotUTC) || !strings.Contains(err.Error(), "DSN-TIME-ZONE-NOT-UTC") {
				t.Errorf("%s(%q) = %v; want the DSN-TIME-ZONE-NOT-UTC refusal", name, q, err)
			}
		}
	}
}
