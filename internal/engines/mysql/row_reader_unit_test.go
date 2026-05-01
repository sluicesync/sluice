package mysql

import (
	"testing"

	"github.com/orware/sluice/internal/ir"
)

func TestQuoteIdent(t *testing.T) {
	cases := []struct{ in, want string }{
		{"users", "`users`"},
		{"with space", "`with space`"},
		{"weird`name", "`weird``name`"},
		{"", "``"},
	}
	for _, c := range cases {
		if got := quoteIdent(c.in); got != c.want {
			t.Errorf("quoteIdent(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}

func TestBuildSelect(t *testing.T) {
	table := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
			{Name: "weird`name", Type: ir.Boolean{}},
		},
	}
	got := buildSelect(table)
	want := "SELECT `id`, `email`, `weird``name` FROM `users`"
	if got != want {
		t.Errorf("buildSelect:\n got  %q\n want %q", got, want)
	}
}
