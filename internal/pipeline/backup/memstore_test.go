// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"io"
	"sort"
	"strings"
	"sync"
)

// memStore is a minimal in-memory BackupStore for catalog/lineage/
// chain tests. The real LocalStore + BlobStore have integration
// coverage; the tested behaviour is store-agnostic. (Mirror of the
// lineage-package test copy — a test-only helper, duplicated across
// the two packages so neither imports the other's test tree.)
//
// Guarded by a mutex: the restore/backup pools reach it from worker
// goroutines, and an unsynchronised map is a `-race` failure waiting on
// the first test that drives a real Run against it.
type memStore struct {
	mu   sync.RWMutex
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
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[path] = b
	return nil
}

func (s *memStore) Get(_ context.Context, path string) (io.ReadCloser, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.data[path]
	if !ok {
		return nil, &storeNotFoundErr{path: path}
	}
	return io.NopCloser(strings.NewReader(string(b))), nil
}

func (s *memStore) List(_ context.Context, prefix string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, path)
	return nil
}

func (s *memStore) Exists(_ context.Context, path string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.data[path]
	return ok, nil
}

type storeNotFoundErr struct{ path string }

func (e *storeNotFoundErr) Error() string { return "memstore: not found: " + e.path }
