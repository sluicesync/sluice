// Package engines provides the registry through which database engine
// implementations make themselves available to the rest of sluice.
//
// Each engine package is expected to call [Register] from its init
// function. The orchestrator then looks engines up by name based on
// the user's configuration:
//
//	package mysql
//
//	import "github.com/orware/sluice/internal/engines"
//
//	func init() {
//	    engines.Register(&Engine{})
//	}
//
// Adding a new engine is therefore a self-contained operation: drop a
// package under internal/engines/<name>/, register it in init(), and
// add a blank import in cmd/sluice/main.go to ensure the init runs.
package engines

import (
	"fmt"
	"sort"
	"sync"

	"github.com/orware/sluice/internal/ir"
)

var (
	mu       sync.RWMutex
	registry = map[string]ir.Engine{}
)

// Register adds e to the registry. It panics if an engine with the
// same Name has already been registered, since duplicate registration
// almost always indicates a programming error and is not safely
// recoverable.
func Register(e ir.Engine) {
	mu.Lock()
	defer mu.Unlock()

	name := e.Name()
	if name == "" {
		panic("engines.Register: engine returned an empty Name")
	}
	if _, exists := registry[name]; exists {
		panic(fmt.Sprintf("engines.Register: engine %q already registered", name))
	}
	registry[name] = e
}

// Get returns the engine registered under name, if any.
func Get(name string) (ir.Engine, bool) {
	mu.RLock()
	defer mu.RUnlock()
	e, ok := registry[name]
	return e, ok
}

// Names returns the sorted list of registered engine names. The slice
// is freshly allocated; callers may modify it without affecting the
// registry.
func Names() []string {
	mu.RLock()
	defer mu.RUnlock()

	out := make([]string, 0, len(registry))
	for name := range registry {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// reset clears the registry. It is unexported and intended only for
// tests within the engines package itself.
func reset() {
	mu.Lock()
	defer mu.Unlock()
	registry = map[string]ir.Engine{}
}
