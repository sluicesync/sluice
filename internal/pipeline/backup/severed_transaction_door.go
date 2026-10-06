// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// SeveredTransactionDoor is the read-side door for F-E1-SEVERED-TAIL-REPLAY.
//
// A chain must never carry one source transaction across two consecutive
// incrementals: chain restore and the broker apply every incremental's
// changes in order, so a transaction present in both is applied twice. That
// is idempotent only for primary-key inserts and same-key updates — a keyless
// row is restored twice, and a key-reusing transaction (UPDATE 1→2,
// DELETE 2, UPDATE 3→1) replayed onto its own end state loses a row. Both
// were measured, at exit 0, with no crash anywhere.
//
// The writers no longer produce the shape (`backup stream` and
// `backup incremental` never commit a window inside a source transaction).
// This door is for chains ALREADY written by older binaries, in three shapes:
//
//   - (A) SEVERED TAIL. A non-final incremental whose change stream ends
//     inside an open transaction that has recorded at least one row — its
//     last transaction marker is a TxBegin with a row after it, or (when an
//     ADD COLUMN fill follows the window) a TxBegin arrives while the
//     window's transaction is still open ([scanWholeIncremental]). A BARE
//     TxBegin with no row after it carries none of the transaction and
//     severs nothing; it passes.
//     The next incremental re-delivers the transaction whole on a source
//     whose in-transaction rows carry a position a resume re-reads the
//     transaction from (Postgres: its commit LSN; MySQL/MariaDB GTID and
//     VStream: the set or VGTID from before it), or resumes part-way through
//     it on MySQL file/pos (every row of a multi-row ROWS event shares the
//     event's END position, so the rest of the event is gone).
//   - (B) RE-DELIVERED BOUNDARY TRANSACTION. Postgres chains written before
//     v0.138.0 recorded EndPosition at the commit record's START, and a
//     resume from there re-delivers that committed transaction: the next
//     incremental opens with rows at or before the previous incremental's
//     last rows. Every resumed window had it.
//   - (C) END POSITION PAST THE RECORDED CHANGES. Any incremental, final
//     included, whose EndPosition is not the last position its chunks
//     record — the old cancel drain's dropped chunk, whose complete
//     transactions are in no link and LOST ([endPastRecordedError]; coded
//     SLUICE-E-BACKUP-INCOMPLETE, the code restore's tail backstop uses).
//
// Callers: chain restore and `backup verify` judge every finding (Check);
// the broker refuses only findings that land on a link it has not applied
// yet and WARNs the rest once (CheckFrom).
//
// THE INDEPENDENT EXPECTED VALUE (the 2026-08-01 rule): the verdicts come
// from the decoded change chunks — the transaction markers and the row
// positions the SOURCE stamped — never from the manifests' own position
// fields, which are what the old writers got wrong. (B) compares two
// different incrementals' recorded rows against each other.
//
// What the door does NOT see, stated so it is not read as broader:
//   - a chain whose FINAL incremental ends open is not refused (nothing
//     follows it to re-deliver anything; the live tail of a running stream
//     has this shape only on an old binary). Chain restore WARNs instead.
//     "Final" and "follows" are by INCREMENTAL: a segment full in between
//     does not break a pair ([nextIncremental]).
//   - (B) is judged only where the source engine orders positions
//     ([ir.PositionMonotonicChecker] — Postgres today). By the code, no other
//     source re-delivers a COMPLETED transaction: the MySQL/MariaDB and
//     VStream TxCommit carries the post-commit position (item 132), file/pos
//     resumes after the XID, and the trigger-CDC sources resume at
//     `id > position`.
//   - a smart-compacted chain keeps (A) visible because smart compaction
//     REFUSES to rewrite an incremental this door's rule judges severed
//     (`keepsTheSeveredVerdict`, over the door's own [openTxTracker]); the
//     chain is left as it was and still refused
//     (TestSmartCompaction_RefusesASeveredFramedIncremental). b953b09d broke
//     this by closing such a transaction with an empty boundary pair. Collapse
//     also hides (B), silently, two ways: it LOWERS the first incremental's
//     last-row position (a collapsed event carries its chain's first
//     position), after which the re-delivered transaction is applied twice;
//     and it RAISES the second incremental's first-row position when the
//     re-delivered leading row collapses away (an INSERT followed by a later
//     DELETE of the same key), after which that DELETE is LOST and the row the
//     source deleted stays — where the uncompacted chain re-applied the INSERT
//     idempotently and was correct. So smart compaction refuses a chain
//     carrying (B) on any pair that involves a link it would rewrite,
//     including a pair that crosses a merge-group boundary, and compares this
//     door's findings link by link before and after the rewrite; both need the
//     source engine's position order (CompactOpts.PositionOrder), and without
//     one (B) stays unjudged there exactly as here, said at INFO
//     (TestSmartCompaction_RefusesToHideShapeB, TestSmartCompaction_BeltSeesShapeBLost,
//     TestSmartCompaction_CrossGroupPairShapeB).
//
// PREMISES, named per the premise rule (each is what would have to be false
// for a verdict to be wrong):
//   - (B)'s no-false-refusal argument: on Postgres every row carries its
//     transaction's commit LSN (BeginMessage.FinalLSN — code-read in
//     postgres/cdc_reader.go), and logical decoding emits transactions in
//     commit order, so a LATER transaction's rows sit strictly after an
//     earlier one's. The keepalive boundary that can share a position with
//     the next commit is row-less, which is why (B) compares ROWS, not
//     EndPositions (pinned by the keepalive case in the door's tests). The
//     commit-order emission is a PostgreSQL property, UNVERIFIED PREMISE
//     here; if it failed, the door would refuse loudly, never pass silently.
//   - (A) on the trigger-CDC sources: they emit no markers, so (A) can never
//     fire there — sound only because their reader resumes STRICTLY after
//     the recorded change-log id (`WHERE id > $1`, pgtrigger/cdc_reader.go and
//     the sqlite-trigger poll), so a cut between two rows of one source
//     transaction neither re-delivers nor skips. Code-read, not re-pinned
//     here.
type SeveredTransactionDoor struct {
	// Store is the chain's root store; each link resolves its segment's
	// store from it.
	Store irbackup.Store

	// CEK resolves a change chunk's content key (nil for a plaintext chunk).
	// The caller's own resolver, so the door opens chunks exactly as the
	// replay that follows it will.
	CEK func(owner *irbackup.Manifest, chunk *irbackup.ChunkInfo) ([]byte, error)

	// Comparator orders source positions for shape (B). Nil disables (B).
	// The pipeline may not know engines by name (archgate), so the callers
	// pass [lineage.SameEngineComparator] over their TARGET: (B) is judged
	// on a same-engine Postgres restore or broker, and NOT on a cross-engine
	// one (a PG chain into MySQL) nor by `backup verify`, which has no
	// engine — a residual, stated here and in the audit backlog.
	Comparator ir.PositionMonotonicChecker

	// SkipUndecodable makes a link whose chunks the door cannot decode a
	// SKIP instead of a stop: the link is recorded in [Undecodable] and every
	// other link is still judged. `backup verify` sets it, because its own
	// chunk checks own a corrupt or unreadable chunk and its job is to report
	// every finding — stopping at the first undecodable link left every later
	// one unjudged, a shape-(C) loss included, at exit 0. Zero value (false)
	// is the restore/broker behaviour: an undecodable link that is not yet
	// applied refuses before anything is applied.
	SkipUndecodable bool

	// Undecodable collects the decode failures a SkipUndecodable run
	// skipped, in chain order.
	Undecodable []error

	// edges caches each incremental's decoded verdicts by manifest path +
	// backup id, so the broker pays for a link once across all its ticks.
	edges map[string]incrementalEdges

	// skipped dedupes [SeveredTransactionDoor.Undecodable] by link.
	skipped map[string]bool

	// warned remembers which already-applied findings CheckFrom has WARNed,
	// so a broker logs each once per run rather than once per tick.
	warned map[string]bool
}

// NewSeveredTransactionDoor builds the door; cmp may be nil (shape (B) off).
func NewSeveredTransactionDoor(store irbackup.Store, cmp ir.PositionMonotonicChecker, cek func(owner *irbackup.Manifest, chunk *irbackup.ChunkInfo) ([]byte, error)) *SeveredTransactionDoor {
	return &SeveredTransactionDoor{Store: store, CEK: cek, Comparator: cmp}
}

// incrementalEdges is what the door needs from one incremental: how its
// change stream ENDS (shape A), where its first and last rows sit (shape B),
// and the last position its chunks actually record (shape C).
type incrementalEdges struct {
	endsOpen  bool        // a transaction with rows is open at the window's end (see scanWholeIncremental for a fill)
	openAt    ir.Position // that TxBegin's position
	firstData ir.Position // first row change's position (zero = none)
	lastData  ir.Position // last row change's position (zero = none)
	hasData   bool

	// lastPos is the position of the last position-bearing change the
	// chunks record, of any kind — the value a writer that records only
	// what it stores stamps as EndPosition, and the value chain restore's
	// own tail backstop compares EndPosition against (shape C).
	lastPos ir.Position
	hasPos  bool

	// markerless: the scanned chunks carry no transaction marker — a
	// trigger-CDC stream. Used only to name a likely cause in shape (C)'s
	// message; it gates no verdict.
	markerless bool

	// carriesFill marks an incremental with an ADD COLUMN fill appended
	// after its window ([irbackup.AddColumnFill], v0.156.1+). Its trailing
	// fill rows carry the window's EndPosition, so lastData is not the
	// window's last SOURCE row, and shape (B) is not judged against it —
	// which loses nothing: (B) is a pre-v0.138.0 writer's shape and no such
	// writer recorded a fill. Shape (C) still is: the fill's positions ARE
	// the EndPosition, by construction (captureAddColumnFill).
	carriesFill bool
}

// Check refuses the chain when any incremental link carries part of a source
// transaction that the next incremental carries again (shapes A and B), or
// records an EndPosition its chunks never reach (shape C). links is the flat
// chain in lineage order, fulls included. Every finding refuses: this is the
// form for a caller about to apply the whole chain (chain restore) or vouch for
// it (`backup verify`).
//
// "The next incremental" is the next INCREMENTAL link, across any segment
// fulls between them ([nextIncremental]). Both consumers apply it next: the
// broker skips a segment full outright, and chain restore applies the full's
// snapshot first and then that incremental — which, after a rotation, resumes
// from the previous segment's EndPosition (`rotationBoundaryResumeStart`), so
// it re-delivers exactly what a same-segment successor would. One pairing,
// for both, and for the landing rule below.
func (d *SeveredTransactionDoor) Check(ctx context.Context, links []lineage.SegmentRecord) error {
	return d.CheckFrom(ctx, links, 0)
}

// CheckFrom is Check for a caller that has ALREADY applied links[:from] — the
// broker, whose position names the last fully applied link. A finding whose
// harm lands on an already-applied link cannot be prevented any more, and
// refusing it would halt the broker forever on history it applied under an
// older binary (a Postgres chain spanning a pre-v0.138.0 resume carries a
// shape-B pair for good). Those are WARNed once each, naming the links so
// the operator can check that range against the source; only findings that
// would land on a link not yet applied refuse. Which link a finding "lands
// on": shapes A and B the SECOND incremental of the pair (applying it is the
// duplicate), shape C the link itself (it is the one missing changes).
func (d *SeveredTransactionDoor) CheckFrom(ctx context.Context, links []lineage.SegmentRecord, from int) error {
	return d.walk(ctx, links, func(f doorFinding) error {
		if f.shape == shapeUndecodable {
			return d.unjudgeable(ctx, f.landing < from, &links[f.link], f.err)
		}
		return d.judge(ctx, f.landing < from, f.err)
	})
}

// doorShape names what a [doorFinding] is.
type doorShape string

const (
	shapeSevered     doorShape = "A"
	shapeRedelivered doorShape = "B"
	shapeEndPast     doorShape = "C"
	shapeUndecodable doorShape = "undecodable"
)

// doorFinding is one thing the door found, before any caller's policy is
// applied: link is the incremental it is ABOUT (the first of an A/B pair),
// pair the second incremental of an A/B pair (-1 otherwise), landing the link
// whose application the harm lands on.
type doorFinding struct {
	shape   doorShape
	link    int
	pair    int
	landing int
	err     error
}

// findings returns every finding the door makes on links, undecodable links
// included, in chain order, with no applied-prefix policy. It is the form
// `backup compact` uses to compare a chain's verdict before and after a
// rewrite, LINK BY LINK.
func (d *SeveredTransactionDoor) findings(ctx context.Context, links []lineage.SegmentRecord) []doorFinding {
	var out []doorFinding
	seen := map[string]bool{}
	_ = d.walk(ctx, links, func(f doorFinding) error {
		key := fmt.Sprintf("%s/%d/%d", f.shape, f.link, f.pair)
		if !seen[key] {
			seen[key] = true
			out = append(out, f)
		}
		return nil
	})
	return out
}

// walk is the door: it visits every incremental, pairs it with the next
// INCREMENTAL ([nextIncremental]), and hands each finding to emit, stopping at
// the first error emit returns. The policy — refuse, WARN, skip, collect — is
// the caller's.
func (d *SeveredTransactionDoor) walk(ctx context.Context, links []lineage.SegmentRecord, emit func(doorFinding) error) error {
	for i := range links {
		if !isIncrementalLink(&links[i]) {
			continue
		}
		j := nextIncremental(links, i) // the pair's second link; -1 when i is the last incremental
		cur, err := d.edgesOf(ctx, &links[i])
		if err != nil {
			// Link i unreadable. Its OWN finding (C) lands on i; its tail is
			// also the evidence for the A/B findings that land on j. So the
			// WARN-and-skip a broker gets for an applied link is allowed only
			// when every link a finding from here could land on is applied —
			// otherwise an unreadable applied link would hide a severed or
			// re-delivered transaction in the next, UNAPPLIED incremental,
			// including one across a segment full.
			landing := i
			if j >= 0 {
				landing = j
			}
			if eerr := emit(doorFinding{shape: shapeUndecodable, link: i, pair: -1, landing: landing, err: err}); eerr != nil {
				return eerr
			}
			continue
		}
		if c := endPastRecordedError(&links[i], cur); c != nil {
			if eerr := emit(doorFinding{shape: shapeEndPast, link: i, pair: -1, landing: i, err: c}); eerr != nil {
				return eerr
			}
		}
		if j < 0 {
			continue // the chain's last incremental: nothing follows to re-deliver it
		}
		if cur.endsOpen {
			if eerr := emit(doorFinding{shape: shapeSevered, link: i, pair: j, landing: j, err: severedTailError(&links[i], cur)}); eerr != nil {
				return eerr
			}
			continue
		}
		if d.Comparator == nil || !cur.hasData || cur.carriesFill {
			continue
		}
		next, err := d.edgesOf(ctx, &links[j])
		if err != nil {
			if eerr := emit(doorFinding{shape: shapeUndecodable, link: j, pair: -1, landing: j, err: err}); eerr != nil {
				return eerr
			}
			continue
		}
		if !next.hasData {
			continue
		}
		atOrBefore, cerr := d.Comparator.PrecedesOrEqual(next.firstData, cur.lastData)
		if cerr != nil {
			// Positions this engine cannot order (representation drift
			// across a version boundary): no verdict, which is no refusal.
			slog.DebugContext(
				ctx, "severed-transaction door: cannot order a link boundary; shape (B) not judged there",
				slog.String("backup_id", lineage.ManifestBackupID(links[j].Manifest)),
				slog.String("err", cerr.Error()),
			)
			continue
		}
		if atOrBefore {
			if eerr := emit(doorFinding{shape: shapeRedelivered, link: i, pair: j, landing: j, err: redeliveredBoundaryError(&links[i], &links[j], cur, next)}); eerr != nil {
				return eerr
			}
		}
	}
	return nil
}

// nextIncremental is the index of the first INCREMENTAL link after i, or -1.
func nextIncremental(links []lineage.SegmentRecord, i int) int {
	for j := i + 1; j < len(links); j++ {
		if isIncrementalLink(&links[j]) {
			return j
		}
	}
	return -1
}

// judge turns a finding into a refusal, or — when the link it lands on is
// already applied — into a once-per-finding WARN. A nil finding is a pass.
func (d *SeveredTransactionDoor) judge(ctx context.Context, applied bool, finding error) error {
	if finding == nil {
		return nil
	}
	if !applied {
		return finding
	}
	if d.warned == nil {
		d.warned = map[string]bool{}
	}
	if msg := finding.Error(); !d.warned[msg] {
		d.warned[msg] = true
		slog.WarnContext(
			ctx, "CHAIN-APPLIED-SEVERED-TRANSACTION: a link this broker ALREADY applied carries a severed-transaction finding "+
				"(this release refuses it on a link not yet applied). It cannot be undone by refusing now, so the broker continues; "+
				"verify the tables those links touched against the source, or rebuild the target from a new full backup",
			slog.String("finding", msg),
		)
	}
	return nil
}

// unjudgeable handles a link whose chunks the door could not decode (a
// corrupt, missing or unreadable chunk, a wrong key — after the verified
// fetch's own retries, so a transient store error that clears within them
// never reaches here, and one that does not is indistinguishable from a
// persistent one). For a link NOT yet applied it refuses with the fetch's own
// coded error (-CHUNK-CORRUPT, -CHUNK-AUTH-FAILED, …): the apply would fail
// on the same chunk, and failing now means nothing of the chain was applied.
// For a link the broker ALREADY applied it WARNs and leaves the link
// unjudged: that chunk is never read again, and refusing would halt the
// broker forever over history it already holds. Nothing is cached, so the
// next tick tries again. Under [SeveredTransactionDoor.SkipUndecodable]
// (`backup verify`) every undecodable link is recorded once and skipped.
func (d *SeveredTransactionDoor) unjudgeable(ctx context.Context, applied bool, link *lineage.SegmentRecord, err error) error {
	if ctx.Err() != nil {
		return err
	}
	if d.SkipUndecodable {
		key := link.Path + "\x00" + lineage.ManifestBackupID(link.Manifest)
		if !d.skipped[key] {
			if d.skipped == nil {
				d.skipped = map[string]bool{}
			}
			d.skipped[key] = true
			d.Undecodable = append(d.Undecodable, err)
		}
		return nil
	}
	if !applied {
		return err
	}
	slog.WarnContext(
		ctx, "severed-transaction door: could not decode the chunks of an incremental this broker already applied, so that link was NOT judged",
		slog.String("backup_id", lineage.ManifestBackupID(link.Manifest)),
		slog.String("err", err.Error()),
	)
	return nil
}

// WarnOpenFinalTail logs CHAIN-TAIL-OPEN-TRANSACTION when the chain's LAST
// incremental ends inside an open source transaction: not refused (nothing
// follows it to re-deliver anything), but the restore applies only that
// transaction's head, a state the source never had. Uses the cache Check
// filled; a link Check did not reach is decoded here.
func (d *SeveredTransactionDoor) WarnOpenFinalTail(ctx context.Context, links []lineage.SegmentRecord) {
	last := -1
	for i := range links {
		if isIncrementalLink(&links[i]) {
			last = i
		}
	}
	if last < 0 {
		return
	}
	link := &links[last]
	e, err := d.edgesOf(ctx, link)
	if err != nil || !e.endsOpen {
		return
	}
	slog.WarnContext(
		ctx, "CHAIN-TAIL-OPEN-TRANSACTION: the chain's last incremental ends inside an open source transaction (written by a sluice before the severed-tail fix); "+
			"this restore applies only that transaction's head, a state the source never had (when a segment full follows that incremental, its snapshot is upserted over it, which overwrites keyed head rows the snapshot also holds, while head rows the snapshot lacks stay). A CDC resume from this chain's end position re-delivers the whole transaction",
		slog.String("backup_id", lineage.ManifestBackupID(link.Manifest)),
		slog.Any("open_transaction_at", e.openAt),
	)
}

func isIncrementalLink(l *lineage.SegmentRecord) bool {
	return lineage.CanonicalKind(l.Manifest.Kind) == irbackup.BackupKindIncremental
}

// edgesOf decodes (and caches) what the door needs from one incremental.
// Decoding reads as few chunks as it can: the head walks forward from the
// first chunk, the tail back from the last.
func (d *SeveredTransactionDoor) edgesOf(ctx context.Context, link *lineage.SegmentRecord) (incrementalEdges, error) {
	key := link.Path + "\x00" + lineage.ManifestBackupID(link.Manifest)
	if d.edges == nil {
		d.edges = map[string]incrementalEdges{}
	}
	if e, ok := d.edges[key]; ok {
		return e, nil
	}
	e, err := d.scanEdges(ctx, link)
	if err != nil {
		return incrementalEdges{}, err
	}
	d.edges[key] = e
	return e, nil
}

// chunkSummary is one decoded chunk's contribution.
type chunkSummary struct {
	hasMarker  bool
	lastMarker ir.Change // TxBegin or TxCommit
	// rowsAfterMarker: a row change follows the chunk's last marker.
	rowsAfterMarker bool
	firstData       ir.Position
	lastData        ir.Position
	hasData         bool
	lastPos         ir.Position // last position-bearing change of any kind
	hasPos          bool
}

func (d *SeveredTransactionDoor) scanEdges(ctx context.Context, link *lineage.SegmentRecord) (incrementalEdges, error) {
	var e incrementalEdges
	chunks := link.Manifest.ChangeChunks
	if len(chunks) == 0 {
		return e, nil
	}
	if manifestCarriesFill(link.Manifest) {
		return d.scanWholeIncremental(ctx, link)
	}
	summaries := make(map[int]chunkSummary, 2)
	summary := func(i int) (chunkSummary, error) {
		if s, ok := summaries[i]; ok {
			return s, nil
		}
		s, err := d.summarizeChunk(ctx, link, i)
		if err != nil {
			return chunkSummary{}, err
		}
		summaries[i] = s
		return s, nil
	}

	// Tail: the last position, the last row change and the last transaction
	// marker, walking back from the last chunk until all three are known.
	var markerKnown, lastDataKnown, lastPosKnown, rowsAfterOpen bool
	first, err := summary(0)
	if err != nil {
		return e, err
	}
	for i := len(chunks) - 1; i >= 0 && (!markerKnown || !lastDataKnown || !lastPosKnown); i-- {
		s, err := summary(i)
		if err != nil {
			return e, err
		}
		if !lastPosKnown && s.hasPos {
			e.lastPos, e.hasPos, lastPosKnown = s.lastPos, true, true
		}
		if !lastDataKnown && s.hasData {
			e.lastData, lastDataKnown = s.lastData, true
		}
		if !markerKnown {
			switch {
			case s.hasMarker:
				markerKnown = true
				if b, ok := s.lastMarker.(ir.TxBegin); ok {
					// A TxBegin with NO row after it (in this chunk or a
					// later one) severs nothing: the next link re-delivers
					// the transaction whole and this one carries none of it.
					if rowsAfterOpen || s.rowsAfterMarker {
						e.endsOpen, e.openAt = true, b.Position
					}
				}
			case s.rowsAfterMarker:
				rowsAfterOpen = true
			}
		}
		// A stream with no transaction markers at all (the trigger-CDC
		// sources) is a marker-less stream: if neither the last chunk nor the
		// FIRST carries one, stop looking for a marker — every change is its
		// own boundary there, and walking the whole incremental would buy
		// nothing. (A framed incremental whose first chunk is all rows would be
		// misread the same way; that needs a transaction longer than a chunk
		// at the very start of an incremental, and costs a missed verdict,
		// never a false refusal.)
		if !markerKnown && !first.hasMarker {
			markerKnown, e.markerless = true, true
		}
	}

	// Head: the first row change, scanning forward (shape B only).
	if !lastDataKnown {
		return e, nil // no rows at all: nothing to compare
	}
	for i := range chunks {
		s, err := summary(i)
		if err != nil {
			return e, err
		}
		if s.hasData {
			e.firstData, e.hasData = s.firstData, true
			break
		}
	}
	return e, nil
}

// summarizeChunk reports one change chunk's last transaction marker (and
// whether a row follows it), its first and last row positions, and its last
// position-bearing change of any kind.
func (d *SeveredTransactionDoor) summarizeChunk(ctx context.Context, link *lineage.SegmentRecord, idx int) (chunkSummary, error) {
	var s chunkSummary
	err := d.decodeChunk(ctx, link, idx, func(c ir.Change) {
		p := c.Pos()
		positioned := p.Engine != "" || p.Token != ""
		if positioned {
			s.lastPos, s.hasPos = p, true
		}
		switch c.(type) {
		case ir.TxBegin, ir.TxCommit:
			s.hasMarker, s.lastMarker, s.rowsAfterMarker = true, c, false
		case ir.Insert, ir.Update, ir.Delete, ir.Truncate:
			s.rowsAfterMarker = true
			if !positioned {
				return
			}
			if !s.hasData {
				s.firstData = p
			}
			s.lastData, s.hasData = p, true
		}
	})
	return s, err
}

// decodeChunk streams one change chunk through visit, decoded exactly as the
// replay will (the same store, codec, content key, AAD binding and number
// handling). Errors are coded the way chain restore's replay codes them,
// because this is the first place the chunk is opened.
func (d *SeveredTransactionDoor) decodeChunk(ctx context.Context, link *lineage.SegmentRecord, idx int, visit func(ir.Change)) error {
	if err := d.decodeChunkRaw(ctx, link, idx, visit); err != nil {
		return &chunkDecodeError{err: err}
	}
	return nil
}

// chunkDecodeError marks a failure to READ a link's chunks — the door could
// not look — as distinct from any verdict it gives. Typed rather than told
// apart by its code, because the door's verdicts and the fetch's failures
// share codes (shape C is -BACKUP-INCOMPLETE; a truncated chunk read is
// -BACKUP-CHUNK-CORRUPT); an allow-list of verdict codes is what let `backup
// verify` swallow shape C. It wraps, so the fetch's own code still reaches the
// operator through [sluicecode.FromError].
type chunkDecodeError struct{ err error }

func (e *chunkDecodeError) Error() string { return e.err.Error() }
func (e *chunkDecodeError) Unwrap() error { return e.err }

// isChunkDecodeError reports whether err is the door failing to read, not
// the door refusing.
func isChunkDecodeError(err error) bool {
	var d *chunkDecodeError
	return errors.As(err, &d)
}

func (d *SeveredTransactionDoor) decodeChunkRaw(ctx context.Context, link *lineage.SegmentRecord, idx int, visit func(ir.Change)) error {
	chunk := link.Manifest.ChangeChunks[idx]
	src, err := blobcodec.FetchChunkVerified(ctx, link.Segment.Store(d.Store), chunk.File, chunk.SHA256)
	if err != nil {
		return lineage.CodeChunkHashError(fmt.Errorf("severed-transaction door: incremental %s chunk %d (%s): open chunk: %w",
			lineage.ManifestBackupID(link.Manifest), idx, chunk.File, err))
	}
	var cek []byte
	if d.CEK != nil {
		if cek, err = d.CEK(link.Manifest, chunk); err != nil {
			_ = src.Close()
			return fmt.Errorf("severed-transaction door: resolve change chunk cek: %w", err)
		}
	}
	cr, err := blobcodec.NewChangeChunkReader(src, chunk.SHA256, cek, link.Segment.CodecOrDefault(), irbackup.ChangeChunkAADFor(link.Manifest, chunk, idx))
	if err != nil {
		return lineage.CodeChunkAuthError(fmt.Errorf("severed-transaction door: open chunk reader: %w", err))
	}
	if blobcodec.NumbersArePreserved(link.Manifest.SourceEngine) {
		cr.PreserveNumbers()
	}
	for {
		c, rerr := cr.ReadChange()
		if errors.Is(rerr, io.EOF) {
			break
		}
		if rerr != nil {
			_ = cr.Close()
			return fmt.Errorf("severed-transaction door: incremental %s chunk %d: read change: %w",
				lineage.ManifestBackupID(link.Manifest), idx, rerr)
		}
		visit(c)
	}
	return lineage.CodeChunkHashError(cr.Close())
}

// endPastRecordedError is shape (C): the manifest's EndPosition is not the
// position of the last change its chunks record. The predicate is
// [EndPositionUnreached] — the same function chain restore's tail backstop
// and the broker's backstop call — so the door refuses nothing either of them
// would accept; it refuses BEFORE anything is applied, and names the causes.
//
// The rule, per writer and engine: a window's EndPosition is the position of
// the last change it RECORDED, whatever kind — since v0.117.0 an
// [ir.SchemaSnapshot] (which rides the manifest, not the chunks) cannot move
// it (`recordedInChangeChunkStream`). A window closed by any of the stream's
// exits ends on a TxCommit (Postgres: its TransactionEndLSN since v0.138.0,
// or a keepalive boundary's walsender position — both carried by the
// recorded TxCommit; MySQL GTID: the post-commit set; file/pos: the XID's
// end LogPos), a trigger-CDC window on its last row's change-log id, and an
// ADD COLUMN fill appends transactions stamped AT EndPosition. Smart
// compaction keeps it on the inputs it rewrites: a collapsed event carries its
// chain's FIRST position, so the rewriter holds a COMMITTED framed window's
// closing TxCommit back and, on a MARKER-LESS window only, appends an empty
// TxBegin/TxCommit pair at the window's last input position
// (applySmartCompactionToIncrementalSized). A framed window that ends inside
// an open transaction is not rewritten at all — compaction refuses it, because
// a pair there would close the transaction and hide shape (A)
// (TestSmartCompaction_RefusesASeveredFramedIncremental,
// TestSmart_ClosingPairOnlyForAMarkerlessInput). Every producer's half is
// graded by TestSeveredTransactionDoor_ShapeC_AgreementTable's producer-written
// rows and the integration chains in stream_severed_tail_integration_test.go.
//
// The causes, all of which leave the same evidence:
//   - an OLD binary's (v0.19.0–v0.156.11) `backup stream` cancel drain
//     (SIGTERM/SIGINT) that skipped its final flush inside a transaction, or
//     whose final flush FAILED on any engine (trigger-CDC included — it has no
//     transactions, but the failed flush dropped its chunk the same way), and
//     still committed the chunks stored before it with EndPosition already
//     advanced to the last change READ. Every change in the dropped chunk is
//     in no incremental and the next one resumes after them: lost;
//   - a pre-v0.117.0 window whose last event was a schema snapshot, which
//     recorded that snapshot's position as EndPosition — not loss, but this
//     release's restore refuses it too (its tail check stopped trusting a
//     schema anchor at EndPosition, audit 2026-07-12 item 60);
//   - a smart compaction, before this release, of a MARKER-LESS (trigger-CDC)
//     chain, whose collapsed tail carried an earlier position — not loss;
//     restore refused these already, so the door names the cause rather than
//     exempting it (the evidence cannot tell a collapsed tail from a dropped
//     one); and
//   - a truncated change-chunk list.
//
// Its reach, measured: on Postgres and MySQL/MariaDB GTID every row of one
// transaction carries the same position, so a drop confined to ONE
// transaction leaves EndPosition equal to the last stored row's position
// and (C) cannot see it — but then the stored tail ends inside that
// transaction and (A) refuses it (the producer-reverted integration run
// showed exactly that split: (A) on PG and GTID, (C) on file/pos, where
// every event has its own position). (C) is what catches the drop of a
// whole committed transaction.
func endPastRecordedError(link *lineage.SegmentRecord, e incrementalEdges) error {
	if !EndPositionUnreached(link.Manifest, e.lastPos) {
		return nil
	}
	end := link.Manifest.EndPosition
	last := "no position-bearing change at all"
	if e.hasPos {
		last = fmt.Sprintf("%+v", e.lastPos)
	}
	likely := ""
	if e.markerless && link.Segment != nil && link.Segment.CapReason == CompactedCapReason {
		likely = " This link sits in a COMPACTED segment and its chunks carry no transaction markers, so the likeliest cause is a smart compaction (before this release) of a trigger-CDC chain — its collapsed tail kept an earlier position; nothing was lost, but the chain cannot be restored as written."
	}
	// Coded SLUICE-E-BACKUP-INCOMPLETE, not -CHAIN-SEVERED-TRANSACTION: it is the
	// same evidence restore's tail backstop already refuses under that code,
	// now refused before anything is applied.
	return sluicecode.Wrap(sluicecode.CodeBackupIncomplete,
		"take a new full backup (`sluice backup full`) and restore or replay from that chain; if this chain was not written by an old `backup stream` cancel, a pre-v0.117.0 schema-only window or a pre-fix smart compaction, restore from an untampered copy",
		fmt.Errorf("incremental %s (%s) records EndPosition %+v but its change chunks end at %s, so replaying it cannot reach the position the next incremental resumes from (F-E1-SEVERED-TAIL-REPLAY shape C). Causes that leave this evidence: a `backup stream` cancel (SIGTERM/SIGINT) on sluice v0.19.0–v0.156.11 whose final flush was skipped inside a transaction or FAILED, on any engine — the changes between the last stored change and EndPosition are then in no incremental, LOST; a window written before v0.117.0 whose last event was a schema snapshot (no loss); a smart compaction, before this release, of a trigger-CDC chain (no loss); or a truncated change-chunk list.%s",
			lineage.ManifestBackupID(link.Manifest), link.Path, end, last, likely))
}

func severedTailError(link *lineage.SegmentRecord, e incrementalEdges) error {
	// A chain that an OLDER sluice smart-compacted can present this shape
	// without a severed head: a window that ended on a BARE TxBegin (which this
	// door passes) came out as that TxBegin followed by collapsed rows from
	// earlier, committed transactions. Nothing in the chunks tells those rows
	// from a genuine severed head — a collapsed row can carry the open
	// transaction's own changes folded into an earlier position — so the door
	// cannot exempt it without opening a silent pass for the real thing. It
	// names the possibility instead; the remedy is the same.
	compacted := ""
	if link.Segment != nil && link.Segment.CapReason == CompactedCapReason {
		compacted = ". This incremental sits in a COMPACTED segment: if the chain was smart-compacted by an older sluice and this window ended on a bare transaction begin, the rows after the begin may be collapsed rows from earlier transactions rather than a severed head — the two cannot be told apart from the chain, so the remedy is the same"
	}
	return sluicecode.Wrap(sluicecode.CodeBackupChainSeveredTransaction,
		"take a new full backup (`sluice backup full`) and restore or replay from that chain; this one carries a source transaction in two incrementals and cannot be replayed exactly",
		fmt.Errorf("incremental %s (%s) ends inside an open source transaction (TxBegin at %+v, no TxCommit) and is not the chain's last link — the next incremental re-delivers that transaction (or, on MySQL file/pos, resumes part-way through it), so replaying both would apply its head twice: a keyless row restored twice, a key-reusing transaction losing rows. Written by a `backup stream` stop or cancel that landed mid-transaction on a sluice before the severed-tail fix (F-E1-SEVERED-TAIL-REPLAY)%s",
			lineage.ManifestBackupID(link.Manifest), link.Path, e.openAt, compacted))
}

func redeliveredBoundaryError(prev, next *lineage.SegmentRecord, p, n incrementalEdges) error {
	return sluicecode.Wrap(sluicecode.CodeBackupChainSeveredTransaction,
		"take a new full backup (`sluice backup full`) and restore or replay from that chain; this one carries a source transaction in two incrementals and cannot be replayed exactly",
		fmt.Errorf("incremental %s opens with rows at %+v, at or before the last rows of the incremental before it (%s, last rows at %+v) — the resume re-delivered the transaction that incremental ended on, so replaying both would apply it twice. This is the shape every resumed window had on Postgres chains written before v0.138.0 (F-E1-SEVERED-TAIL-REPLAY shape B)",
			lineage.ManifestBackupID(next.Manifest), n.firstData, lineage.ManifestBackupID(prev.Manifest), p.lastData))
}

// chainEncrypted reports whether the chain's identity manifest records
// chain encryption.
func chainEncrypted(identity *irbackup.Manifest) bool {
	return identity != nil && identity.ChainEncryption != nil
}

// verifySeveredTransactions is `backup verify`'s run of the door. Restore
// refuses a chain that carries one source transaction across two
// incrementals, so verify must not report it healthy (the Bug 217/218
// doctrine: verify predicts restore). Chain-path lineages only (walk), as
// restore. It opens change chunks through the verify prober's keys; on an
// encrypted chain with no key supplied it cannot decode them and says so at
// WARN instead of passing silently.
func verifySeveredTransactions(ctx context.Context, store irbackup.Store, chain []lineage.SegmentRecord, walk, encrypted, keyed bool, prober *chunkAuthProber) error {
	if !walk || len(chain) == 0 {
		return nil
	}
	if encrypted && !keyed {
		slog.WarnContext(
			ctx, "verify: the severed-transaction check (SLUICE-E-BACKUP-CHAIN-SEVERED-TRANSACTION) was SKIPPED — it decodes change chunks, and this encrypted chain was verified without key material; restore runs it",
		)
		return nil
	}
	door := NewSeveredTransactionDoor(store, nil, func(owner *irbackup.Manifest, c *irbackup.ChunkInfo) ([]byte, error) {
		cek, _, err := prober.changeChunk(owner, c, 0) // the door binds its own AAD; only the key is wanted
		return cek, err
	})
	// A link the door cannot DECODE is not its verdict to give: the chunk scan
	// below reports a corrupt or spliced chunk with its own code, and an
	// intact-but-unreadable one is `--depth read`'s
	// SLUICE-E-BACKUP-CHUNK-UNREADABLE by contract — the hash depth must keep
	// blessing it (TestVerifyDepthRead_HashDepthBlessesWhatTheReadDepthRefuses).
	// So that link is skipped, said out loud, and every OTHER link is still
	// judged — stopping at it left every later link unjudged, a shape-(C) loss
	// included (TestVerifySeveredTransactions_JudgesPastAnUndecodableLink).
	door.SkipUndecodable = true
	err := door.Check(ctx, chain)
	for _, skipped := range door.Undecodable {
		slog.WarnContext(
			ctx, "verify: the severed-transaction check (SLUICE-E-BACKUP-CHAIN-SEVERED-TRANSACTION) could not decode a change chunk and did NOT judge that incremental (nor a severed or re-delivered transaction it would pair with the next one); the chunk checks report that chunk",
			slog.String("err", skipped.Error()),
		)
	}
	if err != nil {
		// Every VERDICT the door gives reaches verify, whatever its code — shape
		// C is coded -BACKUP-INCOMPLETE, and an allow-list of the severed code
		// once let verify report a shape-C chain healthy.
		return fmt.Errorf("verify: %w", err)
	}
	return nil
}

// manifestCarriesFill reports whether m recorded ADD COLUMN fill rows, which
// the capture lanes append as complete transactions AFTER the window — so the
// incremental's last transaction marker is the fill's, not the window's.
func manifestCarriesFill(m *irbackup.Manifest) bool {
	for _, d := range m.SchemaDelta {
		if d != nil && d.AddColumnFill != nil && d.AddColumnFill.Rows > 0 {
			return true
		}
	}
	return false
}

// scanWholeIncremental judges an incremental whose window is followed by an
// ADD COLUMN fill. The tail walk cannot be used there: a window severed on a
// v0.156.1–v0.156.11 binary and then followed by its fill ENDS on the fill's
// TxCommit, so "the last marker is a TxBegin" is false while the window's
// own transaction is still open. Every chunk is decoded in order instead, and
// a TxBegin arriving while a transaction is open marks the severed one — the
// fill opened its own transaction without the window's ever committing. Rare
// (it needs a schema change in the window), so the full decode is affordable.
func (d *SeveredTransactionDoor) scanWholeIncremental(ctx context.Context, link *lineage.SegmentRecord) (incrementalEdges, error) {
	e := incrementalEdges{carriesFill: true}
	var tail openTxTracker
	for i := range link.Manifest.ChangeChunks {
		err := d.decodeChunk(ctx, link, i, func(c ir.Change) {
			tail.observe(c)
			p := c.Pos()
			if p.Engine != "" || p.Token != "" {
				e.lastPos, e.hasPos = p, true
			}
			switch c.(type) {
			case ir.Insert, ir.Update, ir.Delete, ir.Truncate:
				if p.Engine == "" && p.Token == "" {
					return
				}
				if !e.hasData {
					e.firstData = p
				}
				e.lastData, e.hasData = p, true
			}
		})
		if err != nil {
			return e, err
		}
	}
	e.endsOpen, e.openAt = tail.endsOpen()
	return e, nil
}

// openTxTracker is the severed-transaction rule over a stream of changes, in
// order: a stream is severed when it ends inside a transaction that has
// recorded a row, or when a TxBegin arrives while such a transaction is still
// open (an ADD COLUMN fill opening its own transaction after a window that
// never committed). A transaction with no row in it severs nothing — the
// bare-TxBegin exemption.
//
// One rule, two callers: the door's whole-incremental scan judges a STORED
// incremental with it, and smart compaction judges its own INPUT and OUTPUT
// with it, refusing a rewrite that would turn a severed incremental into one
// the door passes (TestSmartCompaction_RefusesASeveredFramedIncremental).
type openTxTracker struct {
	open      bool
	openRow   bool // the open transaction has recorded a row
	beganAt   ir.Position
	severed   bool
	severedAt ir.Position
	sawMarker bool // any TxBegin or TxCommit at all: a framed stream
}

func (t *openTxTracker) observe(c ir.Change) {
	switch v := c.(type) {
	case ir.TxBegin:
		t.sawMarker = true
		if t.open && t.openRow && !t.severed {
			t.severed, t.severedAt = true, t.beganAt
		}
		t.open, t.openRow, t.beganAt = true, false, v.Position
	case ir.TxCommit:
		t.sawMarker = true
		t.open = false
	case ir.Insert, ir.Update, ir.Delete, ir.Truncate:
		t.openRow = true
	}
}

// endsOpen reports whether the stream observed so far is severed, and the
// position of the transaction it is severed in.
func (t *openTxTracker) endsOpen() (bool, ir.Position) {
	switch {
	case t.severed:
		return true, t.severedAt
	case t.open && t.openRow:
		return true, t.beganAt
	}
	return false, ir.Position{}
}
