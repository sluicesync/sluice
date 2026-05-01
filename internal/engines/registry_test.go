package engines

import (
	"context"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// fakeEngine is a minimal ir.Engine for registry tests. It implements
// only what the tests exercise; opening readers/writers panics.
type fakeEngine struct {
	name string
	caps ir.Capabilities
}

func (f *fakeEngine) Name() string                                                     { return f.name }
func (f *fakeEngine) Capabilities() ir.Capabilities                                    { return f.caps }
func (*fakeEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error)   { panic("not implemented") }
func (*fakeEngine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error)   { panic("not implemented") }
func (*fakeEngine) OpenRowReader(context.Context, string) (ir.RowReader, error)         { panic("not implemented") }
func (*fakeEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error)         { panic("not implemented") }
func (*fakeEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error)         { panic("not implemented") }
func (*fakeEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) { panic("not implemented") }

func TestRegisterAndGet(t *testing.T) {
	t.Cleanup(reset)
	reset()

	mysql := &fakeEngine{name: "mysql"}
	pg := &fakeEngine{name: "postgres"}

	Register(mysql)
	Register(pg)

	if got, ok := Get("mysql"); !ok || got != mysql {
		t.Errorf("Get(mysql) = %v, %v; want %v, true", got, ok, mysql)
	}
	if got, ok := Get("postgres"); !ok || got != pg {
		t.Errorf("Get(postgres) = %v, %v; want %v, true", got, ok, pg)
	}
	if _, ok := Get("sqlite"); ok {
		t.Error("Get(sqlite) returned ok = true for unregistered engine")
	}

	want := []string{"mysql", "postgres"}
	got := Names()
	if len(got) != len(want) {
		t.Fatalf("Names() = %v; want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Names()[%d] = %q; want %q", i, got[i], want[i])
		}
	}
}

func TestRegisterDuplicatePanics(t *testing.T) {
	t.Cleanup(reset)
	reset()

	Register(&fakeEngine{name: "mysql"})

	defer func() {
		if r := recover(); r == nil {
			t.Error("Register did not panic on duplicate registration")
		}
	}()
	Register(&fakeEngine{name: "mysql"})
}

func TestRegisterEmptyNamePanics(t *testing.T) {
	t.Cleanup(reset)
	reset()

	defer func() {
		if r := recover(); r == nil {
			t.Error("Register did not panic on empty name")
		}
	}()
	Register(&fakeEngine{name: ""})
}
