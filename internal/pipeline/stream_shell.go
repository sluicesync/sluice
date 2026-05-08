// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// OS-shell helpers for the rollover-hook in [BackupStream]. Pulled
// into a tiny file of its own so the platform branch (`runtime.GOOS ==
// "windows"`) doesn't bloat stream.go's main flow.

import (
	"os"
	"runtime"
)

// defaultShell returns the (shell, flag) pair to invoke a single-
// string command on the current OS. Windows: cmd /C. Unix-y: sh -c.
//
// Operators who want a different shell can wrap their command via the
// default shell, e.g. `--rollover-hook="bash -c 'pushgateway-push.sh'"`
// invokes bash through the OS default shell.
func defaultShell() (shell, flag string) {
	if runtime.GOOS == "windows" {
		return "cmd", "/C"
	}
	return "sh", "-c"
}

// processEnv returns a copy of the process's current environment.
// Hooks inherit the env so PATH + operator-exported vars are visible.
func processEnv() []string {
	src := os.Environ()
	out := make([]string, len(src))
	copy(out, src)
	return out
}

// defaultPidHost returns (os.Getpid(), os.Hostname()) — the default
// (pid, host) pair recorded on `stream_state.json`. Pulled out of the
// stream so tests can override via [BackupStream.pidHostFn].
func defaultPidHost() (pid int, host string) {
	h, err := os.Hostname()
	if err != nil {
		h = "unknown"
	}
	return os.Getpid(), h
}
