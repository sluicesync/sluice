package mysql

import (
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

func TestEmitColumnType(t *testing.T) {
	cases := []struct {
		name string
		in   ir.Type
		want string
	}{
		// ---- Numeric / Boolean ----
		{"boolean", ir.Boolean{}, "TINYINT(1)"},
		{"tinyint", ir.Integer{Width: 8}, "TINYINT"},
		{"smallint", ir.Integer{Width: 16}, "SMALLINT"},
		{"mediumint", ir.Integer{Width: 24}, "MEDIUMINT"},
		{"int", ir.Integer{Width: 32}, "INT"},
		{"bigint", ir.Integer{Width: 64}, "BIGINT"},
		{"int unsigned", ir.Integer{Width: 32, Unsigned: true}, "INT UNSIGNED"},
		{"bigint unsigned auto_increment", ir.Integer{Width: 64, Unsigned: true, AutoIncrement: true}, "BIGINT UNSIGNED AUTO_INCREMENT"},
		{"decimal", ir.Decimal{Precision: 10, Scale: 2}, "DECIMAL(10,2)"},
		{"float single", ir.Float{Precision: ir.FloatSingle}, "FLOAT"},
		{"float double", ir.Float{Precision: ir.FloatDouble}, "DOUBLE"},

		// ---- Character ----
		{"char no charset", ir.Char{Length: 10}, "CHAR(10)"},
		{"varchar with charset", ir.Varchar{Length: 255, Charset: "utf8mb4", Collation: "utf8mb4_unicode_ci"},
			"VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"},
		{"text tiny", ir.Text{Size: ir.TextTiny}, "TINYTEXT"},
		{"text regular", ir.Text{Size: ir.TextRegular}, "TEXT"},
		{"text medium", ir.Text{Size: ir.TextMedium}, "MEDIUMTEXT"},
		{"text long", ir.Text{Size: ir.TextLong}, "LONGTEXT"},
		{"text with charset", ir.Text{Size: ir.TextRegular, Charset: "utf8mb4"}, "TEXT CHARACTER SET utf8mb4"},

		// ---- Binary ----
		{"binary", ir.Binary{Length: 16}, "BINARY(16)"},
		{"varbinary", ir.Varbinary{Length: 64}, "VARBINARY(64)"},
		{"blob tiny", ir.Blob{Size: ir.BlobTiny}, "TINYBLOB"},
		{"blob regular", ir.Blob{Size: ir.BlobRegular}, "BLOB"},
		{"blob medium", ir.Blob{Size: ir.BlobMedium}, "MEDIUMBLOB"},
		{"blob long", ir.Blob{Size: ir.BlobLong}, "LONGBLOB"},

		// ---- Temporal ----
		{"date", ir.Date{}, "DATE"},
		{"time precision 0", ir.Time{Precision: 0}, "TIME"},
		{"time precision 6", ir.Time{Precision: 6}, "TIME(6)"},
		{"datetime precision 0", ir.DateTime{Precision: 0}, "DATETIME"},
		{"datetime precision 3", ir.DateTime{Precision: 3}, "DATETIME(3)"},
		{"timestamp precision 0", ir.Timestamp{Precision: 0, WithTimeZone: true}, "TIMESTAMP"},
		{"timestamp precision 6", ir.Timestamp{Precision: 6, WithTimeZone: true}, "TIMESTAMP(6)"},

		// ---- Structured ----
		{"json", ir.JSON{Binary: true}, "JSON"},

		// ---- Categorical ----
		{"enum", ir.Enum{Values: []string{"red", "green", "blue"}}, "ENUM('red','green','blue')"},
		{"enum with apostrophe", ir.Enum{Values: []string{"it's"}}, "ENUM('it''s')"},
		{"set", ir.Set{Values: []string{"a", "b"}}, "SET('a','b')"},

		// ---- Identity / spatial ----
		{"uuid", ir.UUID{}, "CHAR(36)"},
		{"geometry default", ir.Geometry{Subtype: ir.GeometryUnspecified}, "GEOMETRY"},
		{"point", ir.Geometry{Subtype: ir.GeometryPoint}, "POINT"},
		{"polygon", ir.Geometry{Subtype: ir.GeometryPolygon}, "POLYGON"},
		{"geometrycollection", ir.Geometry{Subtype: ir.GeometryCollection}, "GEOMETRYCOLLECTION"},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := emitColumnType(c.in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("emitColumnType(%T) = %q; want %q", c.in, got, c.want)
			}
		})
	}
}

func TestEmitColumnTypeUnsupported(t *testing.T) {
	cases := []ir.Type{
		ir.Inet{},
		ir.Cidr{},
		ir.Macaddr{},
		ir.Array{Element: ir.Integer{Width: 32}},
	}
	for _, c := range cases {
		c := c
		t.Run(typeName(c), func(t *testing.T) {
			if _, err := emitColumnType(c); err == nil {
				t.Errorf("expected error for %T; got nil", c)
			}
		})
	}
}

func TestEmitDefault(t *testing.T) {
	cases := []struct {
		name      string
		in        ir.DefaultValue
		want      string
		wantEmit  bool
	}{
		{"none", ir.DefaultNone{}, "", false},
		{"nil", nil, "", false},
		{"literal zero", ir.DefaultLiteral{Value: "0"}, "'0'", true},
		{"literal text", ir.DefaultLiteral{Value: "hello"}, "'hello'", true},
		{"literal with quote", ir.DefaultLiteral{Value: "it's"}, "'it''s'", true},
		{"expression", ir.DefaultExpression{Expr: "CURRENT_TIMESTAMP"}, "CURRENT_TIMESTAMP", true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, ok := emitDefault(c.in)
			if ok != c.wantEmit {
				t.Errorf("emit flag = %v; want %v", ok, c.wantEmit)
			}
			if got != c.want {
				t.Errorf("emitDefault = %q; want %q", got, c.want)
			}
		})
	}
}

func TestEmitColumnDef(t *testing.T) {
	cases := []struct {
		name string
		in   *ir.Column
		want string
	}{
		{
			name: "id bigint unsigned auto_increment not null",
			in: &ir.Column{
				Name: "id",
				Type: ir.Integer{Width: 64, Unsigned: true, AutoIncrement: true},
			},
			want: "`id` BIGINT UNSIGNED AUTO_INCREMENT NOT NULL",
		},
		{
			name: "email varchar not null",
			in: &ir.Column{
				Name: "email",
				Type: ir.Varchar{Length: 255, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"},
			},
			want: "`email` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL",
		},
		{
			name: "active boolean default 1",
			in: &ir.Column{
				Name:    "active",
				Type:    ir.Boolean{},
				Default: ir.DefaultLiteral{Value: "1"},
			},
			want: "`active` TINYINT(1) NOT NULL DEFAULT '1'",
		},
		{
			name: "created_at default current_timestamp",
			in: &ir.Column{
				Name:    "created_at",
				Type:    ir.Timestamp{Precision: 6, WithTimeZone: true},
				Default: ir.DefaultExpression{Expr: "CURRENT_TIMESTAMP(6)"},
			},
			want: "`created_at` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)",
		},
		{
			name: "nullable with comment",
			in: &ir.Column{
				Name:     "notes",
				Type:     ir.Text{Size: ir.TextRegular},
				Nullable: true,
				Comment:  "User notes",
			},
			want: "`notes` TEXT COMMENT 'User notes'",
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := emitColumnDef(c.in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("\n got  %q\n want %q", got, c.want)
			}
		})
	}
}

func TestEmitTableDef(t *testing.T) {
	table := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, Unsigned: true, AutoIncrement: true}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
		},
		PrimaryKey: &ir.Index{
			Name:    "PRIMARY",
			Unique:  true,
			Columns: []ir.IndexColumn{{Column: "id"}},
		},
	}
	got, err := emitTableDef(table)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Spot checks rather than exact-string match: the multi-line
	// CREATE TABLE is verbose enough that a structural check reads
	// more clearly than a giant string literal.
	wants := []string{
		"CREATE TABLE `users` (",
		"`id` BIGINT UNSIGNED AUTO_INCREMENT NOT NULL,",
		"`email` VARCHAR(255) NOT NULL,",
		"PRIMARY KEY (`id`)",
		"ENGINE=InnoDB",
	}
	for _, w := range wants {
		if !strings.Contains(got, w) {
			t.Errorf("output missing %q; got:\n%s", w, got)
		}
	}
}

func TestEmitCreateIndex(t *testing.T) {
	cases := []struct {
		name string
		idx  *ir.Index
		want string
	}{
		{
			name: "secondary unique",
			idx: &ir.Index{
				Name:    "users_email_unique",
				Unique:  true,
				Kind:    ir.IndexKindBTree,
				Columns: []ir.IndexColumn{{Column: "email"}},
			},
			want: "ALTER TABLE `users` ADD UNIQUE INDEX `users_email_unique` (`email`) USING BTREE;",
		},
		{
			name: "non-unique multi-column",
			idx: &ir.Index{
				Name: "users_lookup",
				Kind: ir.IndexKindBTree,
				Columns: []ir.IndexColumn{
					{Column: "tenant_id"},
					{Column: "created_at", Desc: true},
				},
			},
			want: "ALTER TABLE `users` ADD INDEX `users_lookup` (`tenant_id`, `created_at` DESC) USING BTREE;",
		},
		{
			name: "fulltext",
			idx: &ir.Index{
				Name:    "posts_body_ft",
				Kind:    ir.IndexKindFullText,
				Columns: []ir.IndexColumn{{Column: "body"}},
			},
			want: "ALTER TABLE `users` ADD FULLTEXT INDEX `posts_body_ft` (`body`);",
		},
		{
			name: "prefix length",
			idx: &ir.Index{
				Name:    "users_name_prefix",
				Kind:    ir.IndexKindBTree,
				Columns: []ir.IndexColumn{{Column: "name", Length: 16}},
			},
			want: "ALTER TABLE `users` ADD INDEX `users_name_prefix` (`name`(16)) USING BTREE;",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := emitCreateIndex("users", c.idx)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("\n got  %q\n want %q", got, c.want)
			}
		})
	}
}

func TestEmitCreateIndexRejectsPrimary(t *testing.T) {
	idx := &ir.Index{
		Name:    "PRIMARY",
		Columns: []ir.IndexColumn{{Column: "id"}},
	}
	if _, err := emitCreateIndex("t", idx); err == nil {
		t.Error("expected error for PRIMARY index; got nil")
	}
}

func TestEmitAddForeignKey(t *testing.T) {
	fk := &ir.ForeignKey{
		Name:              "posts_user_id_fk",
		Columns:           []string{"user_id"},
		ReferencedTable:   "users",
		ReferencedColumns: []string{"id"},
		OnDelete:          ir.FKActionCascade,
		OnUpdate:          ir.FKActionRestrict,
	}
	got, err := emitAddForeignKey("posts", fk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "ALTER TABLE `posts` ADD CONSTRAINT `posts_user_id_fk` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE ON UPDATE RESTRICT;"
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

func TestEmitAddForeignKey_NoActions(t *testing.T) {
	// FKs with NoAction (the MySQL default) shouldn't emit ON DELETE
	// / ON UPDATE clauses — they'd be redundant noise.
	fk := &ir.ForeignKey{
		Name:              "fk",
		Columns:           []string{"a"},
		ReferencedTable:   "parent",
		ReferencedColumns: []string{"id"},
		OnDelete:          ir.FKActionNoAction,
		OnUpdate:          ir.FKActionNoAction,
	}
	got, err := emitAddForeignKey("child", fk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(got, "ON DELETE") || strings.Contains(got, "ON UPDATE") {
		t.Errorf("output should not contain ON DELETE/ON UPDATE for NoAction; got %q", got)
	}
}

func TestQuoteSQLString(t *testing.T) {
	cases := []struct{ in, want string }{
		{"hello", "'hello'"},
		{"it's", "'it''s'"},
		{"", "''"},
		{"two''quotes", "'two''''quotes'"},
	}
	for _, c := range cases {
		if got := quoteSQLString(c.in); got != c.want {
			t.Errorf("quoteSQLString(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}
