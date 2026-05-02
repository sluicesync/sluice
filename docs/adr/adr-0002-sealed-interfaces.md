# ADR-0002: Sealed interfaces for IR types

## Status

Accepted.

## Context

The `ir.Type` interface is implemented by a closed set of types defined in `internal/ir`: `Boolean`, `Integer`, `Decimal`, `Float`, `Char`, `Varchar`, `Text`, `Date`, `Timestamp`, `Time`, `JSON`, `Enum`, `UUID`, `Inet`, `Cidr`, `Macaddr`, `Array`, plus a small extension set. Translation code in engine packages and in `internal/pipeline` switches on these types via `switch v := t.(type)` to emit DDL, decode values, and route to per-element handlers.

If the interface were openly satisfiable, an outside package — or a future internal one — could define a new `Type` implementation. Existing type switches in DDL emitters and value decoders would silently fall through to a default branch, and the failure would surface as a runtime "no decoder for IR type X" error rather than at compile time. Worse, every engine writer would have to defensively assume unknown types might appear.

## Decision

`ir.Type` is sealed by an unexported method (`isType()`) declared on the interface. Only types defined inside `internal/ir` can satisfy the interface, because only code in that package can declare a method with the unexported name. The same pattern is applied to `ir.Change` (CDC events), `ir.DefaultValue` (column defaults), and any other variant-style interface where exhaustiveness matters.

The convention is documented in the package doc comment of `internal/ir/types.go` so the reason isn't lost.

## Consequences

Type switches in engine code can be treated as exhaustive: missing a case is a real bug, not a placeholder for forward-compatibility. New core or extension types are intentionally a coordinated change — adding one requires touching every engine's reader and writer, which surfaces incomplete coverage at review time rather than at runtime.

The cost is that out-of-tree engines (third-party plugins satisfying `ir.Engine`) cannot define their own `Type` shapes. They must compose existing IR types or contribute new ones upstream. For a tool whose value depends on dialect-neutral translation, this is the correct tradeoff: the IR is the contract, and the contract has one editor.
