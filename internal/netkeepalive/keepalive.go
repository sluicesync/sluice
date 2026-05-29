// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package netkeepalive centralises the TCP keep-alive policy sluice
// applies to its long-lived database connections.
//
// Why this exists: sluice's CDC streams (Postgres pgoutput, MySQL
// binlog) and the postgres-trigger poller hold a TCP connection open
// across quiet periods when no rows are changing. Cloud NAT gateways
// and L4 load balancers (Heroku, Render, GCP Cloud SQL, AWS NLB, ...)
// silently evict idle connections after an idle timeout — sometimes as
// low as 60s. Once the mapping is gone the next read/write doesn't fail
// fast: it stalls until the kernel's default (multi-minute) TCP
// retransmit budget expires, which presents to the operator as a
// mysterious replication hang.
//
// The engines already send *application-level* keepalives on the CDC
// streams (pgoutput's periodic SendStandbyStatusUpdate, the binlog
// syncer's HeartbeatPeriod). Those keep the server from timing the
// replica out, but they do not by themselves defeat a NAT that drops
// the mapping mid-idle, and a write into a half-open connection
// succeeds locally while the bytes never arrive. TCP keep-alive probes
// are the transport-layer complement: they keep the NAT mapping warm
// and, via the Idle/Interval/Count bound, surface a dead peer as a
// clean connection error in seconds rather than minutes.
//
// The three knobs mirror what PlanetScale's heroku-migrator found
// necessary against managed Postgres (their PR #7: enable + idle +
// interval/count). The values are deliberately fixed here rather than
// exposed as config: they are a hardening default that should be
// correct for every cloud we target, and there is no current operator
// requirement to tune them. If one emerges, lift these into a koanf
// `connection:` block.
package netkeepalive

import (
	"net"
	"time"
)

// The keep-alive policy for sluice's long-lived connections. Idle is
// well under the lowest cloud idle-timeout we know of (~60s), and
// Interval*Count bounds dead-peer detection to ~Idle+Interval*Count
// (~60s) rather than the kernel default of many minutes.
const (
	// Idle is how long a connection sits idle before the first probe.
	Idle = 30 * time.Second
	// Interval is the gap between probes once they start.
	Interval = 10 * time.Second
	// Count is how many unacknowledged probes mark the peer dead.
	Count = 3
)

// Dialer returns a *net.Dialer carrying sluice's standard TCP
// keep-alive policy. Its DialContext method satisfies the dial-hook
// signatures of pgx/pgconn ([pgconn.DialFunc]), the MySQL driver
// ([mysql.DialContextFunc], after a thin adapter), and the go-mysql
// binlog syncer ([replication.BinlogSyncerConfig.Dialer]), so every
// long-lived connection path can share one policy.
//
// A fresh dialer is returned per call; callers typically build one at
// connection-config time and reuse its DialContext across reconnects.
func Dialer() *net.Dialer {
	return &net.Dialer{
		// KeepAlive is honoured by platforms/paths that read the
		// legacy single-period field; KeepAliveConfig (Go 1.23+) is
		// the authoritative source and takes precedence where both
		// are understood.
		KeepAlive: Idle,
		KeepAliveConfig: net.KeepAliveConfig{
			Enable:   true,
			Idle:     Idle,
			Interval: Interval,
			Count:    Count,
		},
	}
}
