// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package expandcontract

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/planetscale/api"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// legRunner drives ONE dev-branch → DDL → deploy-request → deploy →
// finalize cycle — the ADR-0162 deploy leg, extracted (ADR-0165) so
// expand-contract's two legs and deploy-ddl's single leg compose the
// same machine instead of forking it. Everything command-specific is
// a field: the narration/error prefixes, the optional post-DDL branch
// staging hook, and the operator-guidance strings spliced into the
// shared failure shapes.
type legRunner struct {
	api      *api.Client
	org      string
	database string
	// branch is the PRODUCTION branch deploy requests merge into.
	branch string

	pollInterval  time.Duration
	deployTimeout time.Duration
	out           io.Writer

	// execDDL applies one verbatim DDL statement to the dev branch
	// over a direct MySQL connection (tests inject a fake).
	execDDL func(ctx context.Context, pw *api.BranchPassword, database, ddl string) error

	// name opens narration lines ("expand", "contract", "deploy-ddl");
	// errPrefix opens error messages ("expand-contract expand",
	// "deploy-ddl"); passwordName labels the ephemeral branch password
	// in the PlanetScale UI.
	name         string
	errPrefix    string
	passwordName string

	// stage, when non-nil, runs on the dev branch after the DDL (the
	// expand leg's control-table staging); stageNote is the narration
	// printed after it succeeds (pre-formatted, newline-terminated).
	stage     func(ctx context.Context, pw *api.BranchPassword) error
	stageNote string

	// Command-specific operator guidance spliced into the shared
	// failure shapes (each completes the sentence it is spliced into):
	//
	//	leftoverAdvice        — "…if the DDL already deployed, %s; otherwise…"
	//	alreadyDeployedAdvice — "…the DDL looks already deployed; %s"
	//	reviewTimeoutAdvice   — "…requires deploy-request review, %s"
	//	deployTimeoutAdvice   — "…keeps running in PlanetScale; %s"
	leftoverAdvice        string
	alreadyDeployedAdvice string
	reviewTimeoutAdvice   string
	deployTimeoutAdvice   string

	// expectedDiffTables is the leg's intended blast radius: the table
	// names its DDL (+ staging hook) may alter/create. When non-empty,
	// the runner fetches the deploy request's computed diff BEFORE
	// calling Deploy and refuses on any object outside this set (audit
	// MED-D0-7): a stranger object means the branch base was stale (the
	// live-caught phantom revert the freshness gate exists for) or the
	// branch was touched out-of-band. Empty skips the assertion — the
	// one legitimate holder is deploy-ddl, whose DDL is an arbitrary
	// operator statement sluice deliberately does not parse (no regex
	// over DDL), so it has no intended set to assert against.
	expectedDiffTables []string

	// now is the wall clock, injectable so the post-wait freshness
	// recheck's elapsed threshold is testable without real waits. nil
	// (every production construction) means time.Now.
	now func() time.Time
}

// legFreshnessRecheckAfter is the review/deploy-wait duration beyond
// which the runner re-verifies production's schema hasn't moved before
// calling Deploy (audit MED-D0-7, the TOCTOU half): the provisioning
// freshness gate is point-in-time, and a deploy request can sit in an
// org's review queue for up to --deploy-timeout (default 1h) while
// production keeps shipping schema changes. Whether PlanetScale
// re-diffs a DR at deploy time is derived-not-verified, so the runner
// assumes it does not. Short waits skip the recheck — the window is
// negligible and every skipped GET keeps the fast path fast.
const legFreshnessRecheckAfter = 2 * time.Minute

// branch-readiness polling gets its own (generous, fixed) deadline:
// branch creation is near-instant next to a deploy, and reusing the
// operator's whole --deploy-timeout here would just delay the real
// error.
const branchReadyTimeout = 5 * time.Minute

// legBranchName derives the DETERMINISTIC dev-branch name for a leg
// from a scope string (the table for expand-contract, empty for
// deploy-ddl) + the leg's DDL, so a re-run after a crash finds (and
// refuses on) its own leftover branch by name instead of silently
// minting sluice-branch litter — the v1 resumability design
// (ADR-0162).
func legBranchName(kind, scope, ddl string) string {
	h := sha256.New()
	h.Write([]byte(scope))
	h.Write([]byte{0})
	h.Write([]byte(ddl))
	return "sluice-" + kind + "-" + hex.EncodeToString(h.Sum(nil))[:10]
}

// preflightSafeMigrations verifies the service token / org / database /
// branch in one GET, then the safe-migrations prerequisite (ADR-0148
// finding #1): deploy requests cannot be created into a branch without
// it. Enabling it is a behavior change on the operator's production
// branch (direct DDL becomes blocked), so sluice REFUSES and names the
// toggle rather than flipping it (contain-complexity tenet); the
// enable/disable propagation lag (finding #7) makes a
// toggle-around-the-run design unsafe anyway. command prefixes the
// messages ("expand-contract", "deploy-ddl").
func preflightSafeMigrations(ctx context.Context, client *api.Client, org, database, branch, command string) error {
	br, err := client.GetBranch(ctx, org, database, branch)
	if err != nil {
		if api.IsNotFound(err) {
			return fmt.Errorf("%s preflight: branch %q of %s/%s not found — check --org/--database/--branch: %w",
				command, branch, org, database, err)
		}
		return fmt.Errorf("%s preflight: read branch %q: %w", command, branch, err)
	}
	if !br.SafeMigrations {
		return sluicecode.Wrap(
			sluicecode.CodePSSafeMigrationsDisabled,
			"enable the branch's \"Safe migrations\" setting in the PlanetScale UI, or run `pscale branch safe-migrations enable "+database+" "+branch+" --org "+org+"` — note this blocks direct DDL on the branch from then on",
			fmt.Errorf("%s: branch %q of %s/%s does not have safe migrations enabled — PlanetScale refuses deploy requests into it, and sluice never enables the toggle for you (it changes how every future schema change on the branch must ship)",
				command, branch, org, database),
		)
	}
	return nil
}

// run drives the full leg: refuse-on-leftover, freshness-gated branch
// provisioning, DDL (+ optional staging), deploy request, deploy,
// skip-revert finalize.
func (r *legRunner) run(ctx context.Context, branchName, ddl string, cleanup *branchCleanup) (*api.DeployRequest, error) {
	out := r.out

	// Refuse-on-leftover: a branch with our deterministic name means a
	// previous run died mid-leg. Guessing whether its DDL/DR state is
	// reusable would be the silent path; name it and let the operator
	// decide.
	if _, err := r.api.GetBranch(ctx, r.org, r.database, branchName); err == nil {
		return nil, fmt.Errorf(
			"%s: dev branch %q already exists — a previous run left it behind. Inspect its deploy request in PlanetScale; if the DDL already deployed, %s; otherwise delete the branch (`pscale branch delete %s %s --org %s`) and re-run",
			r.errPrefix, branchName, r.leftoverAdvice, r.database, branchName, r.org,
		)
	} else if !api.IsNotFound(err) {
		return nil, fmt.Errorf("%s: probe dev branch %q: %w", r.errPrefix, branchName, err)
	}

	prodBaseline, err := r.provisionFreshBranch(ctx, branchName, cleanup)
	if err != nil {
		return nil, err
	}
	baselineAt := r.nowFunc()()

	pw, err := r.api.CreateBranchPassword(ctx, r.org, r.database, branchName, r.passwordName)
	if err != nil {
		return nil, fmt.Errorf("%s: create branch password for %q: %w", r.errPrefix, branchName, err)
	}
	if err := r.execDDL(ctx, pw, r.database, ddl); err != nil {
		return nil, fmt.Errorf("%s: apply DDL on dev branch %q: %w", r.errPrefix, branchName, err)
	}
	fmt.Fprintf(out, "%s: applied DDL on %q: %s\n", r.name, branchName, ddl)

	if r.stage != nil {
		if err := r.stage(ctx, pw); err != nil {
			return nil, fmt.Errorf("%s: %w", r.errPrefix, err)
		}
		fmt.Fprint(out, r.stageNote)
	}

	dr, err := r.api.CreateDeployRequest(ctx, r.org, r.database, branchName, r.branch)
	if err != nil {
		return nil, fmt.Errorf("%s: create deploy request %q → %q: %w", r.errPrefix, branchName, r.branch, err)
	}
	fmt.Fprintf(out, "%s: opened deploy request #%d (%s)\n", r.name, dr.Number, dr.HTMLURL)

	if err := r.waitDeployable(ctx, branchName, dr.Number); err != nil {
		// The review-timeout remedy is "approve the deploy request and
		// re-run" — but deleting the dev branch closes its still-open DR,
		// making the remedy self-defeating (audit L-D0-10). On exactly
		// this path the branch is exempted from cleanup (auto
		// --keep-branches semantics); the timeout message names the kept
		// branch and how to delete it once the DR closes. Every other
		// failure path still cleans up.
		var pending *reviewPendingError
		if errors.As(err, &pending) {
			cleanup.remove(branchName)
		}
		return nil, err
	}
	// Pre-Deploy blast-radius + freshness gates (audit MED-D0-7). Both
	// run between "PlanetScale computed the diff / review finished" and
	// the deploy call — the last moment sluice can still refuse without
	// anything having shipped; cleanup tears the dev branch down as on
	// every other failure path.
	if err := r.assertDiffWithinExpected(ctx, dr.Number); err != nil {
		return nil, err
	}
	if err := r.recheckProductionFreshness(ctx, dr.Number, prodBaseline, baselineAt); err != nil {
		return nil, err
	}
	if err := r.deployWithValidatingRetry(ctx, dr); err != nil {
		return nil, err
	}
	final, err := r.waitDeployed(ctx, dr.Number)
	if err != nil {
		return nil, err
	}

	// Finalize the revert window: PlanetScale holds a
	// complete_pending_revert deployment "in progress" and blocks
	// lifecycle ops (branch/database deletes) until it closes
	// (ADR-0148 finding #4). The schema change itself IS applied at
	// this point, so a skip-revert failure is a loud WARN, not a run
	// failure — the operator can finalize from the DR page.
	if final.DeploymentState == "complete_pending_revert" {
		if _, err := r.api.SkipRevert(ctx, r.org, r.database, dr.Number); err != nil {
			slog.WarnContext(ctx, r.errPrefix+": skip-revert failed; finalize the deployment manually from the deploy-request page",
				"deploy_request", dr.Number, "url", final.HTMLURL, "err", err.Error())
		}
	}
	fmt.Fprintf(out, "%s: deploy request #%d deployed\n", r.name, dr.Number)
	return final, nil
}

// provisionFreshBranch creates the dev branch and guarantees its
// schema base matches production before any DDL is applied. It
// returns production's rendered schema at gate-pass time — the
// baseline the post-wait freshness recheck compares against.
//
// A new branch's schema base can LAG production (live-caught
// 2026-07-15, intermittent: a branch created 14 minutes after a
// deploy still lacked the deployed column, and a deploy request from
// it diffed as DROPPING that column from production; a branch created
// one minute after another deploy was current). Deploying from a
// stale base silently reverts every production schema change newer
// than the backup — for expand-contract's contract leg, that would
// drop the freshly backfilled expand column. The guarantee: create →
// compare branch schema to production via the API → if stale, delete
// the branch, take an on-demand backup of production, recreate,
// recheck → still stale is a coded runtime refusal.
func (r *legRunner) provisionFreshBranch(ctx context.Context, branchName string, cleanup *branchCleanup) (prodBaseline string, err error) {
	out := r.out
	if err := r.createBranchAndWait(ctx, branchName, cleanup); err != nil {
		return "", err
	}
	stale, prodBaseline, err := r.branchBaseStale(ctx, branchName)
	if err != nil {
		return "", fmt.Errorf("%s: compare dev-branch schema to %q: %w", r.errPrefix, r.branch, err)
	}
	if !stale {
		return prodBaseline, nil
	}

	fmt.Fprintf(out, "%s: dev branch %q came up with a schema older than %q's current one (a new PlanetScale branch's base can lag production); taking a fresh backup to rebase\n",
		r.name, branchName, r.branch)
	if err := r.api.DeleteBranch(ctx, r.org, r.database, branchName); err != nil {
		return "", fmt.Errorf("%s: delete stale dev branch %q: %w", r.errPrefix, branchName, err)
	}
	cleanup.remove(branchName)
	if err := r.backupProduction(ctx); err != nil {
		return "", err
	}
	if err := r.createBranchAndWait(ctx, branchName, cleanup); err != nil {
		return "", err
	}
	if stale, prodBaseline, err = r.branchBaseStale(ctx, branchName); err != nil {
		return "", fmt.Errorf("%s: recheck dev-branch schema against %q: %w", r.errPrefix, r.branch, err)
	}
	if stale {
		return "", sluicecode.Wrap(sluicecode.CodePSBranchStaleBase,
			"take a fresh backup of the production branch (pscale backup create), then re-run",
			fmt.Errorf(
				"%s: dev branch %q still differs from %q after a fresh backup — deploying from it would silently revert newer production schema; inspect `pscale branch schema %s %s --org %s` vs %q and retry once the schemas converge",
				r.errPrefix, branchName, r.branch, r.database, branchName, r.org, r.branch,
			))
	}
	fmt.Fprintf(out, "%s: rebased dev branch %q now matches %q\n", r.name, branchName, r.branch)
	return prodBaseline, nil
}

// createBranchAndWait creates the dev branch, registers it for
// cleanup, and waits for PlanetScale to report it ready.
func (r *legRunner) createBranchAndWait(ctx context.Context, branchName string, cleanup *branchCleanup) error {
	if _, err := r.api.CreateBranch(ctx, r.org, r.database, branchName, r.branch); err != nil {
		return fmt.Errorf("%s: create dev branch %q off %q: %w", r.errPrefix, branchName, r.branch, err)
	}
	cleanup.add(branchName)
	fmt.Fprintf(r.out, "%s: created dev branch %q off %q\n", r.name, branchName, r.branch)
	if err := r.waitBranchReady(ctx, branchName); err != nil {
		return fmt.Errorf("%s: %w", r.errPrefix, err)
	}
	return nil
}

// branchBaseStale reports whether the dev branch's schema differs from
// the production branch's — the from-a-stale-backup signal — and
// returns production's rendered schema so the caller can baseline the
// post-wait freshness recheck without a second GET.
func (r *legRunner) branchBaseStale(ctx context.Context, branchName string) (stale bool, prodRender string, err error) {
	dev, err := r.api.GetBranchSchema(ctx, r.org, r.database, branchName)
	if err != nil {
		return false, "", err
	}
	prod, err := r.api.GetBranchSchema(ctx, r.org, r.database, r.branch)
	if err != nil {
		return false, "", err
	}
	prodRender = renderSchema(prod)
	return renderSchema(dev) != prodRender, prodRender, nil
}

// nowFunc resolves the injectable clock.
func (r *legRunner) nowFunc() func() time.Time {
	if r.now != nil {
		return r.now
	}
	return time.Now
}

// assertDiffWithinExpected fetches the deploy request's computed diff
// and refuses when it touches any object OUTSIDE the leg's intended
// table set (audit MED-D0-7). The check is subset-only — production
// already carrying part of the intent (e.g. the expand leg's staged
// control tables) legitimately shrinks the diff — but never EMPTY: a
// deployable DR with zero decoded diff entries is a response-shape
// drift signal and refuses fail-closed (see the tripwire below). An
// EMPTY intended set means the caller has nothing to assert
// (deploy-ddl's arbitrary operator DDL) so the fetch is skipped
// entirely.
func (r *legRunner) assertDiffWithinExpected(ctx context.Context, number int) error {
	if len(r.expectedDiffTables) == 0 {
		return nil
	}
	diffs, err := r.api.GetDeployRequestDiff(ctx, r.org, r.database, number)
	if err != nil {
		return fmt.Errorf("%s: fetch deploy request #%d diff: %w", r.errPrefix, number, err)
	}
	// Fail-closed tripwire (audit 2026-07-16): a DEPLOYABLE deploy
	// request structurally cannot have an empty diff — waitDeployable
	// already refused `no_changes` — so zero decoded entries on a leg
	// that intends changes means sluice's model of the diff response no
	// longer matches the API (the DeployRequestDiff shape is DERIVED,
	// not live-captured; see api.Client.GetDeployRequestDiff). Passing
	// here would silently turn the whole blast-radius gate into a no-op
	// on every leg while every fake-backed test stays green.
	if len(diffs) == 0 {
		dr, drErr := r.api.GetDeployRequest(ctx, r.org, r.database, number)
		if drErr != nil {
			// The refusal must not be masked by a failed URL lookup.
			dr = &api.DeployRequest{Number: number, HTMLURL: "(unavailable)"}
		}
		return r.drFailure(dr, fmt.Sprintf(
			"deploy request #%d is deployable but its computed diff decoded EMPTY while this %s leg intends to change %s — sluice's model of the diff response shape (derived, not live-captured) likely no longer matches the API; refusing before the deploy rather than deploying with the blast-radius gate blind",
			number, r.name, strings.Join(r.expectedDiffTables, ", "),
		))
	}
	expected := make(map[string]struct{}, len(r.expectedDiffTables))
	for _, t := range r.expectedDiffTables {
		expected[t] = struct{}{}
	}
	var strangers []string
	for _, d := range diffs {
		if _, ok := expected[d.Name]; !ok {
			strangers = append(strangers, d.Name)
		}
	}
	if len(strangers) == 0 {
		return nil
	}
	dr, drErr := r.api.GetDeployRequest(ctx, r.org, r.database, number)
	if drErr != nil {
		// The refusal must not be masked by a failed URL lookup.
		dr = &api.DeployRequest{Number: number, HTMLURL: "(unavailable)"}
	}
	return r.drFailure(dr, fmt.Sprintf(
		"deploy request #%d would change object(s) this %s leg never intended (%s; intended only: %s) — the dev branch's base was stale or the branch was modified outside sluice, and deploying would ship those changes to %q; refusing before the deploy",
		number, r.name, strings.Join(strangers, ", "), strings.Join(r.expectedDiffTables, ", "), r.branch,
	))
}

// recheckProductionFreshness re-fetches production's schema after a
// long deployable/review wait and refuses when it moved since the
// provisioning baseline (audit MED-D0-7, the TOCTOU half): the DR's
// diff was computed against the OLD production schema, and whether
// PlanetScale re-diffs at deploy time is derived-not-verified — the
// empirically-observed failure shape (ADR-0162 finding 3) is a deploy
// that silently reverts the newer production change. Waits shorter
// than legFreshnessRecheckAfter skip the extra GET.
func (r *legRunner) recheckProductionFreshness(ctx context.Context, number int, prodBaseline string, baselineAt time.Time) error {
	if r.nowFunc()().Sub(baselineAt) <= legFreshnessRecheckAfter {
		return nil
	}
	prod, err := r.api.GetBranchSchema(ctx, r.org, r.database, r.branch)
	if err != nil {
		return fmt.Errorf("%s: recheck %q schema before deploying #%d: %w", r.errPrefix, r.branch, number, err)
	}
	if renderSchema(prod) == prodBaseline {
		return nil
	}
	return sluicecode.Wrap(sluicecode.CodePSBranchStaleBase,
		"re-run the command — it re-provisions the dev branch from current production and recomputes the deploy request",
		fmt.Errorf(
			"%s: %q's schema changed while deploy request #%d waited to deploy — the request's diff was computed against the old schema, and deploying it could silently revert the newer change; refusing before the deploy",
			r.errPrefix, r.branch, number,
		))
}

// renderSchema canonicalizes a branch schema for comparison: raw DDL
// concatenated in table-name order (both sides come from the same
// PlanetScale renderer, so identical schemas render identically).
func renderSchema(tables []api.SchemaTable) string {
	sorted := append([]api.SchemaTable(nil), tables...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	var b strings.Builder
	for _, t := range sorted {
		b.WriteString(t.Name)
		b.WriteString("\x00")
		b.WriteString(t.Raw)
		b.WriteString("\x00")
	}
	return b.String()
}

// backupProduction takes an on-demand backup of the production branch
// and polls it to success, bounded by the deploy timeout (backup
// duration scales with database size; the narration names what's
// happening so a long wait is explained).
func (r *legRunner) backupProduction(ctx context.Context) error {
	bk, err := r.api.CreateBackup(ctx, r.org, r.database, r.branch)
	if err != nil {
		return fmt.Errorf("%s: create rebase backup of %q: %w", r.errPrefix, r.branch, err)
	}
	fmt.Fprintf(r.out, "%s: backup of %q started (rebase base for the dev branch; duration scales with database size)\n", r.name, r.branch)
	deadline := time.Now().Add(r.deployTimeout)
	for {
		cur, err := r.api.GetBackup(ctx, r.org, r.database, r.branch, bk.ID)
		if err != nil {
			return fmt.Errorf("%s: poll rebase backup: %w", r.errPrefix, err)
		}
		switch cur.State {
		case "success":
			return nil
		case "failed", "canceled", "cancelled":
			return sluicecode.Wrap(sluicecode.CodePSBranchStaleBase,
				"take a fresh backup of the production branch (pscale backup create), then re-run",
				fmt.Errorf(
					"%s: rebase backup of %q ended %q — a fresh backup is required for the dev branch to see current production schema",
					r.errPrefix, r.branch, cur.State,
				))
		}
		if time.Now().After(deadline) {
			return sluicecode.Wrap(sluicecode.CodePSBranchStaleBase,
				"take a fresh backup of the production branch (pscale backup create), then re-run",
				fmt.Errorf(
					"%s: rebase backup of %q still %q after %s — re-run once it completes (the backup keeps running in PlanetScale)",
					r.errPrefix, r.branch, cur.State, r.deployTimeout,
				))
		}
		if err := r.sleepPoll(ctx); err != nil {
			return err
		}
	}
}

// waitBranchReady polls the dev branch until PlanetScale reports it
// ready (branch provisioning is async).
func (r *legRunner) waitBranchReady(ctx context.Context, branchName string) error {
	deadline := time.Now().Add(branchReadyTimeout)
	for {
		br, err := r.api.GetBranch(ctx, r.org, r.database, branchName)
		if err != nil {
			return fmt.Errorf("poll dev branch %q readiness: %w", branchName, err)
		}
		if br.Ready {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("dev branch %q did not become ready within %s", branchName, branchReadyTimeout)
		}
		if err := r.sleepPoll(ctx); err != nil {
			return err
		}
	}
}

// Deploy-request lifecycle classification (ADR-0148 finding #3 ground
// truth: open/pending → ready (deployable=true) → queued →
// complete_pending_revert). The poller is deliberately TOLERANT of
// state names it doesn't know: terminal-success and terminal-failure
// are matched by name, everything else keeps waiting until the
// deadline — a new intermediate PlanetScale state must not fail a
// healthy deploy, and the timeout bounds the unknown-terminal risk.
var (
	drSuccessStates = map[string]bool{
		"complete":                true,
		"complete_pending_revert": true,
	}
	drFailureStates = map[string]bool{
		"error":                 true,
		"complete_error":        true,
		"cancelled":             true,
		"complete_cancel":       true,
		"complete_revert":       true,
		"complete_revert_error": true,
	}
)

// reviewPendingError marks waitDeployable's deadline failure: the
// deploy request is still OPEN (likely awaiting review approval), so
// the caller must exempt the dev branch from cleanup — deleting the
// branch closes the DR the operator was just told to approve (audit
// L-D0-10). Wraps the coded drFailure so the exit surface is unchanged.
type reviewPendingError struct{ err error }

func (e *reviewPendingError) Error() string { return e.err.Error() }
func (e *reviewPendingError) Unwrap() error { return e.err }

// waitDeployable polls until the deploy request is deployable (the
// diff computed and PlanetScale accepts a deploy call). branchName is
// the leg's dev branch, named in the still-in-review timeout message
// (the branch is kept on that path — see [reviewPendingError]).
func (r *legRunner) waitDeployable(ctx context.Context, branchName string, number int) error {
	deadline := time.Now().Add(r.deployTimeout)
	for {
		dr, err := r.api.GetDeployRequest(ctx, r.org, r.database, number)
		if err != nil {
			return fmt.Errorf("%s: poll deploy request #%d: %w", r.errPrefix, number, err)
		}
		switch {
		case dr.CanDeploy():
			return nil
		case dr.DeploymentState == "no_changes":
			// The branch's diff against production is empty — the DDL
			// is almost certainly already deployed (a crashed earlier
			// run, or the operator shipping something the schema
			// already has). Deploying nothing would silently
			// "succeed", so refuse with the command's guidance instead.
			return r.drFailure(dr, fmt.Sprintf(
				"deploy request #%d has no schema changes — the DDL looks already deployed; %s",
				number, r.alreadyDeployedAdvice,
			))
		case drFailureStates[dr.DeploymentState] || dr.State == "closed":
			return r.drFailure(dr, fmt.Sprintf(
				"deploy request #%d cannot be deployed (state %q, deployment_state %q)",
				number, dr.State, dr.DeploymentState,
			))
		}
		if time.Now().After(deadline) {
			return &reviewPendingError{err: r.drFailure(dr, fmt.Sprintf(
				"deploy request #%d did not become deployable within %s (deployment_state %q) — if your organization requires deploy-request review, %s; the dev branch %q was KEPT (deleting it would close the still-open deploy request) — once the request closes, delete it with `pscale branch delete %s %s --org %s`",
				number, r.deployTimeout, dr.DeploymentState, r.reviewTimeoutAdvice,
				branchName, r.database, branchName, r.org,
			))}
		}
		if err := r.sleepPoll(ctx); err != nil {
			return err
		}
	}
}

// waitDeployed polls a deploying request to a terminal state.
func (r *legRunner) waitDeployed(ctx context.Context, number int) (*api.DeployRequest, error) {
	deadline := time.Now().Add(r.deployTimeout)
	for {
		dr, err := r.api.GetDeployRequest(ctx, r.org, r.database, number)
		if err != nil {
			return nil, fmt.Errorf("%s: poll deploy request #%d: %w", r.errPrefix, number, err)
		}
		switch {
		case drSuccessStates[dr.DeploymentState]:
			return dr, nil
		case drFailureStates[dr.DeploymentState]:
			return nil, r.drFailure(dr, fmt.Sprintf(
				"deploy request #%d failed (deployment_state %q)", number, dr.DeploymentState,
			))
		}
		if time.Now().After(deadline) {
			return nil, r.drFailure(dr, fmt.Sprintf(
				"deploy request #%d still deploying after %s (deployment_state %q) — the deploy keeps running in PlanetScale; %s",
				number, r.deployTimeout, dr.DeploymentState, r.deployTimeoutAdvice,
			))
		}
		if err := r.sleepPoll(ctx); err != nil {
			return nil, err
		}
	}
}

// The deploy call itself races PlanetScale's own safety validation:
// even after the DR reports deployable, POST /deploy can come back
// HTTP 422 "We're currently validating that these changes are safe to
// deploy. Please try again in a few moments." (live-caught 2026-07-15,
// first psverify CI dispatch — timing-dependent). That is a settling
// state, not a failure; the retry budget below comfortably covers the
// advertised "few moments" while any OTHER deploy error still fails
// straight through.
const (
	deployValidatingRetryAttempts = 6
	deployValidatingRetryInterval = 15 * time.Second
)

// deployWithValidatingRetry issues the deploy call, retrying only the
// still-validating 422 (bounded; backoff rides the client's injectable
// Sleep so tests spend no wall-clock). Exhausted retries surface the
// coded DR failure with the last validating error attached; any other
// deploy error keeps the pre-existing immediate failure shape.
func (r *legRunner) deployWithValidatingRetry(ctx context.Context, dr *api.DeployRequest) error {
	for attempt := 1; ; attempt++ {
		_, err := r.api.Deploy(ctx, r.org, r.database, dr.Number)
		if err == nil {
			return nil
		}
		if !isDeployValidating422(err) {
			return fmt.Errorf("%s: deploy request #%d: %w", r.errPrefix, dr.Number, err)
		}
		if attempt >= deployValidatingRetryAttempts {
			return r.drFailure(dr, fmt.Sprintf(
				"deploy request #%d was still validating after %d deploy attempts %s apart — PlanetScale had not finished checking the changes are safe to deploy; re-run once it settles (%v)",
				dr.Number, attempt, deployValidatingRetryInterval, err,
			))
		}
		if sleepErr := r.api.SleepFor(ctx, deployValidatingRetryInterval); sleepErr != nil {
			return fmt.Errorf("%s: deploy request #%d: %w", r.errPrefix, dr.Number, err)
		}
	}
}

// isDeployValidating422 reports whether err is the deploy endpoint's
// still-validating 422. The live capture's envelope code is the
// GENERIC "invalid_params" — shared with real parameter errors — so
// there is no structural discriminator beyond the status, and the
// message fragment is the narrowest stable shape (a conservative
// substring: PlanetScale rewording the sentence degrades to the old
// immediate-failure behavior, never to a wrong retry). Deliberately
// DISTINCT from [isDeleteRace422]: that one is status-only on the
// branch-DELETE endpoint, where a spurious retry merely delays a
// best-effort WARN — on the deploy endpoint a status-only match would
// retry genuine parameter errors, so the message gate is load-bearing
// here (pinned).
func isDeployValidating422(err error) bool {
	var se *api.StatusError
	return errors.As(err, &se) && se.Status == http.StatusUnprocessableEntity &&
		strings.Contains(se.Message, "currently validating")
}

// drFailure wraps a deploy-request failure/timeout in the coded
// runtime error, always carrying the DR state and URL.
func (r *legRunner) drFailure(dr *api.DeployRequest, msg string) error {
	return sluicecode.Wrap(
		sluicecode.CodePSDeployRequestFailed,
		"inspect the deploy request in PlanetScale: "+dr.HTMLURL,
		fmt.Errorf("%s: %s: %s", r.errPrefix, msg, dr.HTMLURL),
	)
}

func (r *legRunner) sleepPoll(ctx context.Context) error {
	t := time.NewTimer(r.pollInterval)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// ---- cleanup ----

// branchCleanup deletes the dev branches a run created — always,
// including on failure (best-effort with a WARN), unless the operator
// asked to keep them for debugging. It runs on a cancel-immune context
// so a Ctrl-C mid-deploy still tears the branches down.
type branchCleanup struct {
	api      *api.Client
	org      string
	database string
	keep     bool
	out      io.Writer
	// command prefixes the could-not-delete WARN ("expand-contract",
	// "deploy-ddl").
	command  string
	branches []string
}

func (c *branchCleanup) add(name string) { c.branches = append(c.branches, name) }

// remove forgets a branch the leg runner already deleted itself (the
// stale-base rebase path), so cleanup doesn't re-delete it.
func (c *branchCleanup) remove(name string) {
	kept := c.branches[:0]
	for _, b := range c.branches {
		if b != name {
			kept = append(kept, b)
		}
	}
	c.branches = kept
}

// The post-deploy delete races the deploy/skip-revert settling window:
// for ~1 min after a deploy finalizes, the control plane refuses the
// branch delete with HTTP 422 "cannot be deleted while a deployment is
// in progress" (live-caught 2026-07-15 — EVERY deploy-ddl invocation
// stranded its branch with a WARN; a manual delete minutes later
// succeeded). The retry budget below comfortably covers that window;
// any other delete error still WARNs immediately.
const (
	deleteRetryAttempts = 6
	deleteRetryInterval = 20 * time.Second
)

func (c *branchCleanup) run(ctx context.Context) {
	if len(c.branches) == 0 {
		return
	}
	if c.keep {
		fmt.Fprintf(c.out, "cleanup: keeping dev branches (--keep-branches): %s\n", strings.Join(c.branches, ", "))
		return
	}
	deleteCtx := context.WithoutCancel(ctx)
	for _, name := range c.branches {
		if err := c.deleteBranch(deleteCtx, name); err != nil && !api.IsNotFound(err) {
			slog.WarnContext(deleteCtx, c.command+": could not delete dev branch; delete it manually",
				"branch", name, "err", err.Error())
			continue
		}
		fmt.Fprintf(c.out, "cleanup: deleted dev branch %q\n", name)
	}
}

// deleteBranch deletes one dev branch, retrying only the
// delete-races-deploy 422 (bounded; backoff rides the client's
// injectable Sleep so tests spend no wall-clock). Retries exhausted
// returns the last 422 — the caller's WARN names the branch.
func (c *branchCleanup) deleteBranch(ctx context.Context, name string) error {
	for attempt := 1; ; attempt++ {
		err := c.api.DeleteBranch(ctx, c.org, c.database, name)
		if err == nil || attempt >= deleteRetryAttempts || !isDeleteRace422(err) {
			return err
		}
		if sleepErr := c.api.SleepFor(ctx, deleteRetryInterval); sleepErr != nil {
			return err
		}
	}
}

// isDeleteRace422 reports whether err is the control-plane 422 the
// branch delete gets while a just-finished deployment's settling
// window still holds the branch. Matched by status alone: message
// wording is PlanetScale's to change, and a persistent non-race 422
// merely delays the same WARN by the retry budget (~100 s) in a
// best-effort cleanup path.
func isDeleteRace422(err error) bool {
	var se *api.StatusError
	return errors.As(err, &se) && se.Status == http.StatusUnprocessableEntity
}
