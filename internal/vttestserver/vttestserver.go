// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package vttestserver holds the container-side port layout every
// integration harness gives the vitess/vttestserver image, so the harnesses
// cannot drift apart and none of them can wander back into the kernel's
// ephemeral port range.
//
// vttestserver lays its listeners out from one base, the image's PORT
// environment variable: vtcombo's web port at the base, its gRPC port at
// base+1, the embedded mysqld at base+2 and vtgate's MySQL protocol port at
// base+3. Those are binds INSIDE the container — and inside the container
// the kernel also hands out source ports for outbound connections (vtcombo
// dialling its own mysqld, the tablets' health checks) from
// net.ipv4.ip_local_port_range, 32768–60999 by default on Linux. The image's
// own default base, 33574, sits inside that range, so an outbound connection
// made while vtcombo is still starting can take a listener's port first and
// vtcombo exits with `bind: address already in use`. That flaked CI once
// (run 38017771916, VSTREAM-TEST-EPHEMERAL-PORT). A layout wholly below the
// range cannot collide.
//
// Two checks hold this: the package's tests keep the layout below
// [EphemeralFloor] and walk the tree for a vttestserver harness that spells
// a port in the range instead of using these constants; and
// [CheckOutsideEphemeral] lets a harness verify the PREMISE — that the booted
// container's ephemeral range really does start above the layout — against
// the container's own /proc value, rather than trusting the Linux default.
package vttestserver

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	// BasePort is the PORT the harnesses pass to vttestserver.
	BasePort = 23574

	// GRPCPort is vtcombo's gRPC (VStream) listener: base+1.
	GRPCPort = BasePort + 1

	// MySQLPort is vtgate's MySQL-protocol listener: base+3.
	MySQLPort = BasePort + 3

	// PortSpan is how many consecutive ports from BasePort the layout is
	// treated as owning. vttestserver uses four today; the margin keeps a
	// future image that adds a listener from silently straddling a range
	// boundary.
	PortSpan = 10

	// EphemeralFloor is the bottom of Linux's default ephemeral range
	// (net.ipv4.ip_local_port_range = 32768 60999).
	EphemeralFloor = 32768

	// EphemeralCeiling is the top of that default range.
	EphemeralCeiling = 60999
)

// ContainerPort renders a container port in testcontainers' "N/tcp" form.
func ContainerPort(port int) string {
	return strconv.Itoa(port) + "/tcp"
}

// CheckOutsideEphemeral parses the content of a container's
// /proc/sys/net/ipv4/ip_local_port_range ("low<TAB>high") and refuses when
// the layout [BasePort, BasePort+PortSpan) overlaps it. A harness calls it
// after boot so that a runner whose containers use a different range fails
// with the real reason instead of an intermittent bind error.
func CheckOutsideEphemeral(procRange string) error {
	fields := strings.Fields(procRange)
	if len(fields) != 2 {
		return fmt.Errorf("vttestserver: cannot parse ip_local_port_range %q (want two numbers)", procRange)
	}
	low, errLow := strconv.Atoi(fields[0])
	high, errHigh := strconv.Atoi(fields[1])
	if errLow != nil || errHigh != nil || low > high {
		return fmt.Errorf("vttestserver: cannot parse ip_local_port_range %q (want two ascending numbers)", procRange)
	}
	last := BasePort + PortSpan - 1
	if BasePort <= high && last >= low {
		return fmt.Errorf("vttestserver: the container's ephemeral port range %d-%d overlaps the vttestserver "+
			"port layout %d-%d, so an outbound connection can take a listener's port before vtcombo binds it; "+
			"move BasePort in internal/vttestserver", low, high, BasePort, last)
	}
	return nil
}
