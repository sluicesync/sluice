// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"reflect"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func TestFilterByUpperBound_TupleClip(t *testing.T) {
	t.Run("single_column", func(t *testing.T) {
		src := make(chan ir.Row, 8)
		// Ascending PK order, straddling the upper bound 20 (inclusive).
		for _, id := range []int64{10, 15, 20, 21, 25} {
			src <- ir.Row{"id": id}
		}
		close(src)
		out := filterByUpperBound(context.Background(), src, []string{"id"}, []any{int64(20)})
		var got []int64
		for row := range out {
			got = append(got, row["id"].(int64))
		}
		want := []int64{10, 15, 20} // 20 kept (inclusive); 21,25 dropped
		if !reflect.DeepEqual(got, want) {
			t.Errorf("clipped PKs: got %v; want %v", got, want)
		}
	})

	t.Run("composite", func(t *testing.T) {
		src := make(chan ir.Row, 8)
		rows := []ir.Row{
			{"tenant_id": int64(1), "user_id": int64(5)},
			{"tenant_id": int64(2), "user_id": int64(10)}, // == upper → keep
			{"tenant_id": int64(2), "user_id": int64(11)}, // past upper → drop
			{"tenant_id": int64(3), "user_id": int64(1)},  // past → drop
		}
		for _, r := range rows {
			src <- r
		}
		close(src)
		out := filterByUpperBound(context.Background(), src, []string{"tenant_id", "user_id"}, []any{int64(2), int64(10)})
		var got [][]int64
		for row := range out {
			got = append(got, []int64{row["tenant_id"].(int64), row["user_id"].(int64)})
		}
		want := [][]int64{{1, 5}, {2, 10}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("clipped composite PKs: got %v; want %v", got, want)
		}
	})

	t.Run("nil_upper_passthrough", func(t *testing.T) {
		src := make(chan ir.Row, 2)
		src <- ir.Row{"id": int64(1)}
		src <- ir.Row{"id": int64(9_999_999)}
		close(src)
		out := filterByUpperBound(context.Background(), src, []string{"id"}, nil)
		n := 0
		for range out {
			n++
		}
		if n != 2 {
			t.Errorf("nil upper bound should pass all rows; got %d", n)
		}
	})
}
