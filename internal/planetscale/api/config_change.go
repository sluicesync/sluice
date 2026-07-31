// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Keyspace config-change verbs (ADR-0182)
//
// The PlanetScale control plane exposes per-keyspace vttablet/mysqld
// configuration (queryserver-config-query-timeout and friends) NOT through
// the keyspace PATCH — that endpoint does nothing for these options — but
// through a stage → submit → roll-out workflow:
//
//  1. GET  .../branches/{branch}/keyspaces                    (resolve the keyspace)
//  2. GET  .../branches/{branch}/keyspaces/{ks}               (read current vttablet_options + config_change_in_progress)
//  3. POST .../branches/{branch}/keyspaces/{ks}/config-changes   {"change_type":"vttablet","options":{...}}  → draft {id, previous_options}
//  4. POST .../branches/{branch}/config-changes/submit          {"ids":["<id>"]}                              → state:"applying"
//  5. GET  .../branches/{branch}/keyspaces/{ks}/rollout-status  (poll to state:"complete")
//
// The wire quirks below were confirmed by live probing (2026-07-31): the
// config-change body field is `options` (NOT `vttablet_options`) and each
// value is a JSON STRING ("3600", not the number 3600) — the number form
// and the vttablet_options field name both fail 422. These verbs are
// deliberately shape-faithful to that; the raise/revert WORKFLOW (polling,
// keyspace resolution, previous-value bookkeeping) lives in the querytimeout
// package, mirroring how the deploy-request poller sits above the client.

package api

import (
	"context"
	"net/url"
)

// Keyspace is the subset of a PlanetScale keyspace object the ADR-0182
// query-timeout raise reads. VTTabletOptions is the keyspace's current
// vttablet configuration — an EMPTY map (or absent) means every option is
// at its Vitess default (queryserver-config-query-timeout defaults to 900).
// ConfigChangeInProgress is true while a config change is rolling out
// (a rolling tablet restart); staging a second one on top would stack
// changes, so the workflow waits it out first.
type Keyspace struct {
	Name                   string            `json:"name"`
	VTTabletOptions        map[string]string `json:"vttablet_options"`
	ConfigChangeInProgress bool              `json:"config_change_in_progress"`
}

// ConfigChange is a staged keyspace config-change. A freshly POSTed change
// comes back State:"draft" carrying the id to submit and PreviousOptions —
// the vttablet options as they stood BEFORE this change, the ground truth
// the raise records so revert can restore exactly what it found. After the
// submit it walks draft → applying → (rollout) → applied.
type ConfigChange struct {
	ID              string            `json:"id"`
	State           string            `json:"state"`
	ChangeType      string            `json:"change_type"`
	PreviousOptions map[string]string `json:"previous_options"`
	NewOptions      map[string]string `json:"new_options"`
}

// RolloutStatus is a keyspace config-change rollout's progress. State walks
// pending_rollout → in_progress → complete (the rolling tablet restart,
// ~2–6m); the caller pairs a "complete" here with Keyspace.ConfigChangeInProgress
// flipping false to know the change has fully landed.
type RolloutStatus struct {
	State string `json:"state"`
}

// keyspacesPath builds the escaped org/database/branch keyspace-collection
// path. Keyspace config-changes are keyspace-level; the SUBMIT is
// branch-level (see [Client.SubmitConfigChanges]).
func keyspacesPath(org, db, branch string) string {
	return branchesPath(org, db) + "/" + url.PathEscape(branch) + "/keyspaces"
}

// ListKeyspaces returns the branch's keyspaces — the resolution step, so a
// sharded/multi-keyspace database is caught and refused rather than guessed
// at (keyspace name equals the database name only for a single-keyspace
// PlanetScale MySQL, which must be verified, not assumed).
func (c *Client) ListKeyspaces(ctx context.Context, org, db, branch string) ([]Keyspace, error) {
	var out struct {
		Data []Keyspace `json:"data"`
	}
	if err := c.Get(ctx, keyspacesPath(org, db, branch), &out); err != nil {
		return nil, err
	}
	return out.Data, nil
}

// GetKeyspace reads one keyspace — the current vttablet_options and the
// config_change_in_progress gate.
func (c *Client) GetKeyspace(ctx context.Context, org, db, branch, keyspace string) (*Keyspace, error) {
	var out Keyspace
	if err := c.Get(ctx, keyspacesPath(org, db, branch)+"/"+url.PathEscape(keyspace), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// CreateKeyspaceConfigChange stages a draft config-change setting the given
// options for changeType ("vttablet" or "mysqld" at the keyspace level).
// The returned draft carries the id to submit and previous_options. NOTE the
// on-wire body field is `options` and its values are JSON strings — the live-
// verified shape; see the file header.
func (c *Client) CreateKeyspaceConfigChange(ctx context.Context, org, db, branch, keyspace, changeType string, options map[string]string) (*ConfigChange, error) {
	body := struct {
		ChangeType string            `json:"change_type"`
		Options    map[string]string `json:"options"`
	}{ChangeType: changeType, Options: options}
	var out ConfigChange
	if err := c.post(ctx, keyspacesPath(org, db, branch)+"/"+url.PathEscape(keyspace)+"/config-changes", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// SubmitConfigChanges submits staged draft config-changes for rollout. The
// endpoint is BRANCH-level (not keyspace-level), taking the draft ids.
func (c *Client) SubmitConfigChanges(ctx context.Context, org, db, branch string, ids []string) error {
	body := struct {
		IDs []string `json:"ids"`
	}{IDs: ids}
	return c.post(ctx, branchesPath(org, db)+"/"+url.PathEscape(branch)+"/config-changes/submit", body, nil)
}

// GetKeyspaceRolloutStatus polls a keyspace's config-change rollout — the
// rolling tablet restart the raise/revert waits on before proceeding.
func (c *Client) GetKeyspaceRolloutStatus(ctx context.Context, org, db, branch, keyspace string) (*RolloutStatus, error) {
	var out RolloutStatus
	if err := c.Get(ctx, keyspacesPath(org, db, branch)+"/"+url.PathEscape(keyspace)+"/rollout-status", &out); err != nil {
		return nil, err
	}
	return &out, nil
}
