package ir

// Position is an opaque, durable bookmark within a source's change
// stream. Engines populate Token in their own format (binlog file plus
// offset, Postgres LSN, etc.); the IR treats it as opaque. Engine names
// the engine that produced the position so a position store can guard
// against cross-engine confusion.
type Position struct {
	Engine string
	Token  string
}

// Row is a single tuple of values keyed by column name. Values use
// Go-native types corresponding to the column's IR type; the engine
// reader is responsible for converting from driver-native types into
// IR-native ones.
type Row map[string]any

// Change is the sealed interface for events in a continuous-sync change
// stream. Implementations are [Insert], [Update], [Delete], and
// [Truncate]. New variants must be added in this package.
type Change interface {
	// isChange seals the interface.
	isChange()
	// Pos returns the durable position from which this change can be
	// resumed. The value is opaque to consumers of the IR.
	Pos() Position
	// QualifiedName returns "schema.table" for the affected table, or
	// just "table" when Schema is empty.
	QualifiedName() string
}

// qualified is a small helper that mirrors Table's identification logic.
func qualified(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}

// Insert is a row-insertion change event.
type Insert struct {
	Position Position
	Schema   string
	Table    string
	Row      Row
}

func (Insert) isChange()               {}
func (e Insert) Pos() Position         { return e.Position }
func (e Insert) QualifiedName() string { return qualified(e.Schema, e.Table) }

// Update is a row-modification change event. Before captures the prior
// state (when available from the source); After captures the new state.
// Engines that cannot supply a Before image should leave it nil; the
// applier's behaviour in that case is engine-pair specific.
type Update struct {
	Position Position
	Schema   string
	Table    string
	Before   Row // optional; nil when the source does not supply it
	After    Row
}

func (Update) isChange()               {}
func (e Update) Pos() Position         { return e.Position }
func (e Update) QualifiedName() string { return qualified(e.Schema, e.Table) }

// Delete is a row-removal change event. Before holds the row that was
// removed, when the source supplies it.
type Delete struct {
	Position Position
	Schema   string
	Table    string
	Before   Row // optional; nil when the source does not supply it
}

func (Delete) isChange()               {}
func (e Delete) Pos() Position         { return e.Position }
func (e Delete) QualifiedName() string { return qualified(e.Schema, e.Table) }

// Truncate is a whole-table truncation event. Some sources emit this as
// a DDL-flavoured event; the IR treats it as a data-stream change so
// appliers can react without parsing DDL.
type Truncate struct {
	Position Position
	Schema   string
	Table    string
}

func (Truncate) isChange()               {}
func (e Truncate) Pos() Position         { return e.Position }
func (e Truncate) QualifiedName() string { return qualified(e.Schema, e.Table) }
