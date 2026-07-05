// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"io"
	"sort"
	"strings"
	"testing"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// memStore is a minimal in-memory BackupStore for catalog/lineage/
// chain tests. The real LocalStore + BlobStore have integration
// coverage; the tested behaviour is store-agnostic. (Mirror of the
// lineage-package test copy — a test-only helper, duplicated across
// the two packages so neither imports the other's test tree.)
type memStore struct {
	data map[string][]byte
}

func newMemStore() *memStore {
	return &memStore{data: make(map[string][]byte)}
}

func (s *memStore) Put(_ context.Context, path string, r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.data[path] = b
	return nil
}

func (s *memStore) Get(_ context.Context, path string) (io.ReadCloser, error) {
	b, ok := s.data[path]
	if !ok {
		return nil, &storeNotFoundErr{path: path}
	}
	return io.NopCloser(strings.NewReader(string(b))), nil
}

func (s *memStore) List(_ context.Context, prefix string) ([]string, error) {
	out := make([]string, 0)
	for k := range s.data {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (s *memStore) Delete(_ context.Context, path string) error {
	delete(s.data, path)
	return nil
}

func (s *memStore) Exists(_ context.Context, path string) (bool, error) {
	_, ok := s.data[path]
	return ok, nil
}

type storeNotFoundErr struct{ path string }

func (e *storeNotFoundErr) Error() string { return "memstore: not found: " + e.path }

func mustWriteManifest(t *testing.T, store irbackup.Store, path string, m *irbackup.Manifest) {
	t.Helper()
	if err := lineage.WriteManifestAt(context.Background(), store, path, m); err != nil {
		t.Fatalf("WriteManifestAt(%q): %v", path, err)
	}
}
