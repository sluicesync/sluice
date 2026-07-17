// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"fmt"
	"strings"
)

// MariaDB native uuid / inet6 / inet4 CDC binlog decode (ADR-0171).
//
// MariaDB grew native UUID (10.7+), INET6 (10.5+), and INET4 (10.10+)
// types. The bulk-copy read path reads them as TEXT via the driver (a
// plain SELECT returns MariaDB's own canonical rendering), so decodeString
// is correct there and on every MySQL VARCHAR-backed uuid/inet. The binlog
// is different: it carries the RAW STORAGE bytes, which decodeString would
// stringify into a wrong-but-valid-looking value that a MySQL-family target
// (CHAR(36)/VARCHAR(45)) SILENTLY accepts — the exact Bug-74 class. v0.99.271
// therefore REFUSED CDC over these columns; this decode lifts that refusal.
//
// The byte->text mapping below was derived EMPIRICALLY from mariadb:11.4 and
// mariadb:10.11 (identical on both) — the raw bytes go-mysql's RowsEvent
// delivers were captured for known values and the canonical text was
// ground-truthed against the server's own SELECT/HEX rendering. Two findings
// are load-bearing and NOT what the ADR's swap-flag prior assumed:
//
//  1. NO byte reordering. MariaDB stores UUID in CANONICAL big-endian order
//     — NOT the MySQL UUID_TO_BIN(x,1) time-field swap. A known uuid
//     01234567-89ab-cdef-8123-456789abcdef arrives as bytes
//     01 23 45 67 89 ab cd ef 81 23 45 67 89 ab cd ef (identical to the text
//     with the dashes removed). A big-endian format is therefore correct;
//     the reorder the ADR feared does not happen on the binlog wire.
//
//  2. TRAILING-ZERO STRIPPING. MariaDB frames these fixed-width types in the
//     binlog as length-prefixed CHAR/BINARY, and go-mysql's decodeString
//     honours that prefix — which MariaDB writes as the significant length
//     with trailing 0x00 bytes REMOVED. So an all-zero uuid / 0.0.0.0 / ::
//     arrives as a ZERO-LENGTH value, and a trailing-zero value like
//     2001:db8:: arrives at 4 bytes. The decode MUST right-pad the received
//     bytes back to the fixed width (16 for uuid/inet6, 4 for inet4) before
//     formatting; the significant length is NOT the type width.
//
// Consequence of (2): the received byte length CANNOT distinguish inet4 from
// inet6 (a stripped inet6 can be <=4 bytes). The pad width therefore comes
// from the column's DECLARED native kind ([mariadbNativeKind], captured from
// information_schema.data_type in loadTableSchema), never from len(raw).

// mariadbNativeKind names a MariaDB native fixed-width value type whose CDC
// binlog bytes need the [decodeMariaDBNative] path (raw storage bytes ->
// canonical text) instead of decodeString (which is right for the text the
// bulk-copy driver path returns). The zero value is [mariadbNativeNone] so a
// column that is NOT one of these types (every MySQL column, every MariaDB
// non-native column) falls through to the ordinary decodeValue dispatch.
type mariadbNativeKind uint8

const (
	mariadbNativeNone mariadbNativeKind = iota
	mariadbNativeUUID
	mariadbNativeInet4
	mariadbNativeInet6
)

// mariadbNativeKindOf maps an information_schema.columns data_type string
// (already lowercased by loadTableSchema's query) to its native kind. Only
// MariaDB reports these data_types, so on every other flavor this returns
// [mariadbNativeNone] for every column.
func mariadbNativeKindOf(dataType string) mariadbNativeKind {
	switch dataType {
	case "uuid":
		return mariadbNativeUUID
	case "inet4":
		return mariadbNativeInet4
	case "inet6":
		return mariadbNativeInet6
	}
	return mariadbNativeNone
}

// decodeMariaDBNative decodes the RAW binlog storage bytes of a MariaDB
// native uuid / inet6 / inet4 column into the SAME canonical text the
// bulk-copy driver path produces for the same value, so a cold-start
// snapshot and the CDC tail converge byte-for-byte on the target. NULL
// (nil) passes through. See the file comment for the empirical byte order
// and the trailing-zero-stripping contract.
func decodeMariaDBNative(raw any, kind mariadbNativeKind) (any, error) {
	if raw == nil {
		return nil, nil
	}
	var b []byte
	switch v := raw.(type) {
	case string:
		// go-mysql delivers MYSQL_TYPE_STRING (how MariaDB frames these
		// fixed-width types) as a Go string of raw bytes.
		b = []byte(v)
	case []byte:
		b = v
	default:
		return nil, fmt.Errorf("mariadb: cannot decode %T as native %s bytes", raw, kind)
	}
	switch kind {
	case mariadbNativeUUID:
		return formatMariaDBUUID(b)
	case mariadbNativeInet6:
		return formatMariaDBInet(b, 16)
	case mariadbNativeInet4:
		return formatMariaDBInet(b, 4)
	}
	return nil, fmt.Errorf("mariadb: no native decoder for kind %d", kind)
}

func (k mariadbNativeKind) String() string {
	switch k {
	case mariadbNativeUUID:
		return "uuid"
	case mariadbNativeInet4:
		return "inet4"
	case mariadbNativeInet6:
		return "inet6"
	}
	return "none"
}

// padNativeToWidth right-pads b with 0x00 to exactly width bytes,
// reconstructing the trailing zero bytes MariaDB strips from the binlog
// (see the file comment). A value LONGER than width is a corruption signal
// — more significant bytes than the fixed storage width can hold — and is
// refused loudly rather than truncated (loud-failure tenet).
func padNativeToWidth(b []byte, width int, label string) ([]byte, error) {
	if len(b) > width {
		return nil, fmt.Errorf(
			"mariadb: native %s binlog value has %d bytes, exceeding the %d-byte storage width — "+
				"refusing rather than truncate a corrupt value",
			label, len(b), width,
		)
	}
	out := make([]byte, width)
	copy(out, b) // right-pad with 0x00
	return out, nil
}

// formatMariaDBUUID renders 16 canonical big-endian storage bytes as the
// lowercase 8-4-4-4-12 uuid text MariaDB's own SELECT returns.
func formatMariaDBUUID(b []byte) (string, error) {
	p, err := padNativeToWidth(b, 16, "uuid")
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", p[0:4], p[4:6], p[6:8], p[8:10], p[10:16]), nil
}

// formatMariaDBInet renders network-order address bytes (4 for inet4, 16
// for inet6) as the canonical text MariaDB's own SELECT returns. inet4 is a
// dotted quad; inet6 uses MariaDB's inet_ntop6 rendering ([mariadbInet6Text]).
func formatMariaDBInet(b []byte, width int) (string, error) {
	p, err := padNativeToWidth(b, width, "inet")
	if err != nil {
		return "", err
	}
	switch width {
	case 4:
		return fmt.Sprintf("%d.%d.%d.%d", p[0], p[1], p[2], p[3]), nil
	case 16:
		return mariadbInet6Text(p), nil
	}
	return "", fmt.Errorf("mariadb: unsupported inet width %d", width)
}

// mariadbInet6Text renders 16 network-order bytes as MariaDB's canonical
// IPv6 text — a faithful reimplementation of the classic BSD inet_ntop6
// algorithm MariaDB uses, verified byte-exact against mariadb:11.4/10.11
// across the family × shape matrix (see value_decode_mariadb_test.go).
//
// It is NOT interchangeable with Go's net/netip String(): MariaDB (like
// BSD) renders IPv4-COMPATIBLE addresses (::a.b.c.d, best-zero-run length 6)
// in dotted form, which netip renders as pure hextets (::102:304). Using
// netip would silently diverge cold-start bulk-copy text from the CDC tail
// on that shape. The two renderers agree on IPv4-MAPPED (::ffff:a.b.c.d) and
// on every non-embedded address, so this only differs where it must.
//
// The rule (empirically confirmed): render the trailing two words as a
// dotted quad only when the leading zero run starts at word 0 AND is exactly
// 6 words long (IPv4-compatible) OR is 5 words long with word 5 == 0xffff
// (IPv4-mapped). A 7-word leading zero run (e.g. ::2, ::100, ::ffff) is NOT
// dotted — that is the one place MariaDB drops BIND9's extra clause.
func mariadbInet6Text(ip []byte) string {
	var w [8]uint16
	for i := 0; i < 8; i++ {
		w[i] = uint16(ip[2*i])<<8 | uint16(ip[2*i+1])
	}
	// Longest run of zero words (RFC 5952: length >= 2), leftmost on tie.
	bestBase, bestLen := -1, 0
	curBase, curLen := -1, 0
	for i := 0; i < 8; i++ {
		if w[i] == 0 {
			if curBase == -1 {
				curBase, curLen = i, 1
			} else {
				curLen++
			}
			if curLen > bestLen {
				bestBase, bestLen = curBase, curLen
			}
		} else {
			curBase, curLen = -1, 0
		}
	}
	if bestLen < 2 {
		bestBase, bestLen = -1, 0
	}

	var sb strings.Builder
	for i := 0; i < 8; i++ {
		if bestBase != -1 && i >= bestBase && i < bestBase+bestLen {
			if i == bestBase {
				sb.WriteByte(':')
			}
			continue
		}
		if i != 0 {
			sb.WriteByte(':')
		}
		if i == 6 && bestBase == 0 && (bestLen == 6 || (bestLen == 5 && w[5] == 0xffff)) {
			fmt.Fprintf(&sb, "%d.%d.%d.%d", ip[12], ip[13], ip[14], ip[15])
			break
		}
		fmt.Fprintf(&sb, "%x", w[i])
	}
	if bestBase != -1 && bestBase+bestLen == 8 {
		sb.WriteByte(':')
	}
	return sb.String()
}
