// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package netkeepalive

import (
	"testing"
	"time"
)

func TestDialerCarriesKeepAlivePolicy(t *testing.T) {
	d := Dialer()
	if d == nil {
		t.Fatal("Dialer() returned nil")
	}
	if !d.KeepAliveConfig.Enable {
		t.Error("KeepAliveConfig.Enable = false, want true")
	}
	if d.KeepAliveConfig.Idle != Idle {
		t.Errorf("KeepAliveConfig.Idle = %v, want %v", d.KeepAliveConfig.Idle, Idle)
	}
	if d.KeepAliveConfig.Interval != Interval {
		t.Errorf("KeepAliveConfig.Interval = %v, want %v", d.KeepAliveConfig.Interval, Interval)
	}
	if d.KeepAliveConfig.Count != Count {
		t.Errorf("KeepAliveConfig.Count = %d, want %d", d.KeepAliveConfig.Count, Count)
	}
	// The legacy single-period field is set too, so paths that only
	// read it still get a sane (non-zero, not-disabled) keep-alive.
	if d.KeepAlive != Idle {
		t.Errorf("KeepAlive = %v, want %v", d.KeepAlive, Idle)
	}
}

func TestKeepAliveConstantsBoundDetection(t *testing.T) {
	// Idle must stay under the lowest cloud idle-timeout (~60s) so the
	// probes keep the NAT mapping warm before it is evicted.
	if Idle > 45*time.Second {
		t.Errorf("Idle = %v, want well under the ~60s cloud idle-timeout floor", Idle)
	}
	// Idle + Interval*Count bounds how long a dead peer goes undetected;
	// it should be seconds, not the kernel default of many minutes.
	bound := Idle + Interval*time.Duration(Count)
	if bound > 90*time.Second {
		t.Errorf("dead-peer detection bound = %v, want under ~90s", bound)
	}
}
