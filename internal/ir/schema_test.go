package ir

import "testing"

// TestColumnIsGenerated exercises the IsGenerated helper across the
// three column shapes the IR distinguishes: plain (no expression),
// stored generated, and virtual generated.
func TestColumnIsGenerated(t *testing.T) {
	cases := []struct {
		name string
		col  Column
		want bool
	}{
		{
			name: "plain column",
			col:  Column{Name: "id", Type: Integer{Width: 64}},
			want: false,
		},
		{
			name: "stored generated",
			col: Column{
				Name:            "total",
				Type:            Integer{Width: 64},
				GeneratedExpr:   "qty * price",
				GeneratedStored: true,
			},
			want: true,
		},
		{
			name: "virtual generated",
			col: Column{
				Name:            "label",
				Type:            Varchar{Length: 64},
				GeneratedExpr:   "CONCAT(first_name, ' ', last_name)",
				GeneratedStored: false,
			},
			want: true,
		},
		{
			name: "empty expression with stored=true is not generated",
			col: Column{
				Name:            "id",
				Type:            Integer{Width: 64},
				GeneratedStored: true, // ignored: predicate is on Expr
			},
			want: false,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := c.col.IsGenerated(); got != c.want {
				t.Errorf("IsGenerated() = %v; want %v", got, c.want)
			}
		})
	}
}
