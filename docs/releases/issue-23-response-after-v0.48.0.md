## v0.48.0 ships the Phase A diagnostic surface for this — please re-test with `--pprof-listen`

Thanks for the third reproduction + the narrowed hypothesis — "one batch then silence after every in-process retry" is materially more actionable than the original report.

**v0.48.0 (releasing imminently — race-fix CI in progress, expect publish within ~30 min of this comment) lands the diagnostic surface for exactly this case.** Two pieces:

1. **`stream: heartbeat` INFO log line** every `--heartbeat-interval` (default 60s) per stream. The stall now produces a visible signature in the default log: heartbeats stop, no `applier: batch` lines, no error. Distinguishable in <1 minute instead of waiting for `sluice sync health` to flag it.

2. **`--pprof-listen ADDR` global flag** binds `net/http/pprof`'s debug endpoints for the lifetime of any subcommand. When the next stall fires, fetch `/debug/pprof/goroutine?debug=2` — that dump localises the wedge point in seconds.

### Suggested re-test

```bash
sluice sync start \
    --pprof-listen 127.0.0.1:6060 \
    --heartbeat-interval 30s \
    ... your usual flags ...
```

When the next stall hits (the rig reliably reproduces within hours):

```bash
# Capture goroutine dump immediately
curl -s 'http://127.0.0.1:6060/debug/pprof/goroutine?debug=2' > sluice-stall-stacks.txt

# Also worth grabbing the regular heap + mutex profiles
curl -s 'http://127.0.0.1:6060/debug/pprof/heap?debug=2' > sluice-stall-heap.txt
curl -s 'http://127.0.0.1:6060/debug/pprof/mutex?debug=2' > sluice-stall-mutex.txt
```

Attach `sluice-stall-stacks.txt` to this issue and I can turn the Phase A diagnosis into a Phase B targeted fix.

### Why I'm not patching speculatively

Your hypothesis ("retry path's stream re-initialization either doesn't get a working source-side `VStream` after re-init, or the apply loop is wired to a dead channel") matches what I'd predict from the `v0.42.0` retry + `v0.46.0` pump-handle re-capture code paths. The `runWithRetry`-driven `runOnce` re-entry opens a fresh applier + fresh CDC reader + fresh changes channel on each attempt — if the new gRPC `VStream`'s server side silently half-closes after re-establishing without surfacing an error, you'd see exactly "one batch from the previous reader's drain buffer, then silence."

That's the *plausible* mechanism. I've avoided patching it without confirmation because:

- The CLAUDE.md three-phase debug protocol explicitly forbids speculative patching past v0.42.0's `runWithRetry` introduction — the costs of getting it wrong are high (sluice's retry path is now load-bearing for every operator running v0.42.0+).
- Past projects' Phase A → Phase B work (e.g. Bug 37 heartbeat-clobber, the ADR-0036 add-table race) only became clean fixes once a goroutine dump pinned the exact wedge point. Patching without that has produced multi-round retries in the past.

One goroutine dump should be enough to commit to a targeted fix.

### v0.46.0 + v0.48.0 fixed adjacent classes (so we can rule them out)

For symptom-overlap clarity:
- **#19** (silent exit-0 on source-side transient) — fixed v0.46.0; the retry path now activates on TCP-reset.
- **#21** (MySQL applier `invalid connection` not retried) — fixed v0.48.0.
- **#22** (`backup stream run` doesn't retry source transients) — fixed v0.48.0.
- **#23 (this issue)** — the retry path now ACTIVATES correctly (we can confirm from your reports), but the post-retry stream wedges. This is the next layer down.

Three confirmed reproductions on two engines + two sluice versions narrows it well; the goroutine dump should narrow it further.
