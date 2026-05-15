# Loose Coupling in Rust

This repository uses **compile-time composition** as the default architectural style.

The goal is to maximize:

* static resolution
* performance
* local reasoning
* independent development of modules
* build stability while interfaces evolve

The default tools are:

* **workspace crates** for module boundaries
* **traits** for contracts
* **generics** for composition
* **explicit wiring** in bootstrap code
* **small typed context bags** for shared state when an A3 application layer is present

We do **not** use runtime dependency injection patterns as a primary design approach.

## How to Read This in Aurelia

This document is **normative** for the coupling style used in Aurelia. It applies across the
current multi-crate workspace and to future modules or crates added to the repository.

Apply this document like this:

* keep dependencies aligned with the workspace crate boundaries documented in `docs/aurelia.md`
* treat crate roots, `src/lib/src/lib.rs`, and any future `bin` crates as composition edges
* prefer narrow traits only where substitution and independent evolution are real needs
* when adding a new supporting crate under `src/crates/<crate-name>`, add matching docs under
  `docs/<crate-name>/`

The rules below define how new work and refactors should preserve loose coupling in the current
workspace structure.

## Repository Mapping

To keep the guidance grounded in this repository:

* `Cargo.toml` at the workspace root defines the current member crates.
* `src/lib/` is the main, publishable crate (`aurelia`).
* Supporting crates live under `src/crates/<crate-name>/` and are workspace members.
* Crate-specific docs live under `docs/<crate-name>/`.
* Composition edges live at crate roots (and at `bin` crates once they exist).

## 0. Domain Modules vs. Library Helpers

If a `common` or `shared` module exists, it is reserved for **domain-neutral, dependency-free
helpers** (for example validation functions). It must not accumulate domain-specific rules.

Rules:

* Domain-specific behavior belongs in a **domain module** (for example `peering`, `routing`,
  `transport`), not in `common` or `shared`.
* There is no catch-all `types` module. Types live with their owning domain unless they are truly
  domain-neutral, in which case they belong in `common` or `shared`.
* If a cross-domain concept emerges, it should become its own module or crate rather than being
  placed in `common` or `shared`.

`aurelia-platform` is the explicit exception for cross-crate process services. It is reserved for
platform invariants that must be shared by multiple internal crates, such as the singleton Aurelia
runtime. It must not own domain contracts, domain data, logging policy, peering configuration,
transport logic, test helpers, or general utility functions.

---

## 1. Crate boundaries

Code must be split by responsibility into separate workspace crates where that improves ownership
and parallel development. Functional elements should be kept in separate crates once their
boundaries are clear.

Use this shape by default:

* `*_api` or `*_contract`: shared traits and shared types
* `*_impl_*`: concrete implementations
* `app` or domain/application crates: orchestration and use cases
* `bin` crate: process startup and wiring

Crate naming convention:

* Workspace crates use the `aurelia-<area>` prefix (for example `aurelia-core`, `aurelia-config`,
  `aurelia-peering-contract`).
* Keep names flat and descriptive; avoid extra hierarchy unless a boundary truly needs it.

Rules:

* Shared contracts go in a dedicated boundary crate.
* Concrete implementations do not define the contracts they satisfy unless they are purely
  internal.
* Consumers depend on contract crates, not on implementation crates.
* Wiring happens at the edge of the program, typically in `main` or a bootstrap module.

## 2. Traits define boundaries

Every cross-module dependency must be expressed in terms of a trait when the dependency is intended
to be substitutable or independently developed.

Rules:

* Traits must be **small** and **capability-oriented**.
* Traits must be owned by the consuming side or by a dedicated contract crate.
* Traits must describe required behavior only.
* Traits must not accumulate unrelated responsibilities.

Prefer:

```rust
pub trait UserReader {
    fn get_user(&self, id: UserId) -> Result<User>;
}

pub trait UserWriter {
    fn put_user(&self, user: User) -> Result<()>;
}
```

Do not prefer:

```rust
pub trait UserStore {
    fn get_user(&self, id: UserId) -> Result<User>;
    fn put_user(&self, user: User) -> Result<()>;
    fn delete_user(&self, id: UserId) -> Result<()>;
    fn list_users(&self) -> Result<Vec<User>>;
}
```

unless all consumers genuinely require the full surface.

---

## 3. Generics are the default composition mechanism

Cross-module composition must use generics and trait bounds by default.

Prefer:

```rust
pub struct UserService<S: UserReader> {
    store: S,
}
```

Also acceptable:

```rust
pub struct UserService<S>
where
    S: UserReader,
{
    store: S,
}
```

Rules:

* Use generic parameters for dependencies whenever the implementation choice is known at compile
  time.
* Favor static dispatch unless runtime dispatch is required by an actual use case.
* Keep trait bounds narrow and local.

---

## 4. Runtime polymorphism is opt-in, not default

`dyn Trait` is allowed only when runtime selection is required.

Valid uses include:

* plugin loading
* heterogeneous collections
* runtime-selected adapters from configuration
* APIs that must erase type differences across branches

Rules:

* Do not use `Box<dyn Trait>` or `Arc<dyn Trait>` by habit.
* Use trait objects only when generics or enums are materially worse for the concrete use case.
* Any use of runtime dispatch should be explainable in one sentence.

---

## 5. Enums are preferred for closed implementation sets

When the set of implementations is known and intentionally closed, use an enum instead of trait
objects.

Example:

```rust
pub enum UserStore {
    Memory(InMemoryUserStore),
    Postgres(PostgresUserStore),
}
```

Rules:

* Use enums when variants are known inside this repository and are not intended as an open
  extension point.
* Prefer enums over trait objects for closed sets.
* Keep the enum in the owning layer (A1/A2/A3) that owns the selection decision.

---

## 6. Wiring happens in one place

Dependency assembly must be explicit.

Rules:

* Construct implementations in bootstrap code.
* Pass dependencies through constructors.
* Do not hide construction behind global registries or service locators.
* Do not use ambient mutable application state as a dependency mechanism.

Prefer:

```rust
fn main() {
    let store = PostgresUserStore::new(...);
    let service = UserService::new(store);
    run(service);
}
```

Do not prefer a shared `AppContext` unless the scope is deliberately small and stable.

---

## 7. No default trait methods

This repository does **not** use default trait methods on boundary traits.

Rules:

* Every trait method must be required.
* Trait evolution must not rely on default implementations.
* We do this to avoid compatibility ambiguity and hidden semantic drift.

If new behavior is needed, add a new trait.

---

## 8. Interface evolution is additive

Interfaces must evolve additively.

Rules:

* Do not add required methods to an existing widely-used trait.
* Do not broaden an existing trait just because one consumer needs more.
* Add a new trait for each new capability.
* Existing consumers must continue compiling unchanged unless there is a deliberate breaking
  change.

Prefer:

```rust
pub trait UserReader {
    fn get_user(&self, id: UserId) -> Result<User>;
}

pub trait UserLister {
    fn list_users(&self) -> Result<Vec<User>>;
}
```

Not:

```rust
pub trait UserReader {
    fn get_user(&self, id: UserId) -> Result<User>;
    fn list_users(&self) -> Result<Vec<User>>;
}
```

if `list_users` is new and not universally needed.

---

## 9. Version capabilities by trait, not by mutation

When a contract grows, introduce a new trait rather than mutating the old one.

Prefer:

```rust
pub trait UserReader {
    fn get_user(&self, id: UserId) -> Result<User>;
}

pub trait UserSearch {
    fn find_users(&self, query: &UserQuery) -> Result<Vec<User>>;
}
```

or, if a hierarchical relationship is useful:

```rust
pub trait UserReader {
    fn get_user(&self, id: UserId) -> Result<User>;
}

pub trait UserReaderSearch: UserReader {
    fn find_users(&self, query: &UserQuery) -> Result<Vec<User>>;
}
```

Rules:

* New traits should be named by capability, not by numeric suffix, unless a true versioned
  migration is in progress.
* Prefer `UserSearch` over `UserReaderV2`.
* Numeric suffixes are acceptable only for short-lived migration periods.

---

## 10. Consumers depend on the minimum contract

A module must depend only on the capabilities it actually uses.

Rules:

* Constructor signatures must use the smallest trait surface possible.
* Do not accept a broad trait when a narrower trait will do.
* Do not couple read-only code to write capabilities.

Prefer:

```rust
pub struct GetUser<S: UserReader> {
    store: S,
}
```

Not:

```rust
pub struct GetUser<S: UserStore> {
    store: S,
}
```

when only reads are needed.

---

## 11. Shared types belong at the boundary

Types that cross crate boundaries must live in the contract crate unless there is a strong reason
otherwise.

Rules:

* Request/response structs shared across modules go in the boundary crate.
* Core identifiers and domain-neutral DTOs go in the boundary crate.
* Implementation-specific internal types stay private to the implementation crate.

This keeps contracts explicit and reduces incidental dependency spread.

Public reporting structures are not an exception to this rule. Reports must not leak internal
implementation enums or type aliases. Numeric report fields use the primitive value type, and
internal enum values are converted to stable string labels through private `*_label` functions
located next to the enum definition. Only deliberately supported domain contracts, such as
`AureliaError` and `ErrorId`, should cross the public boundary as typed values.

---

## 12. Avoid cyclic dependencies

Workspace crates must form a clear dependency direction.

Rules:

* Contract crates must not depend on implementation crates.
* Higher-level orchestration crates may depend on multiple contract crates.
* Implementation crates may depend on contract crates and internal utility crates.
* Cycles are not allowed.

Target direction:

`contract -> app/use-case -> bootstrap`

and

`implementation -> contract`

with selection happening only at the bootstrap edge.

---

## 13. Features are not the interface-evolution mechanism

Cargo features are for conditional compilation, optional dependencies, and build-time product
shaping.

Rules:

* Do not use features to paper over contract drift between modules.
* Do not use features as a substitute for additive trait design.
* Use features only when the capability itself is optional at build time.

Examples of acceptable feature use:

* enabling a database backend
* enabling metrics
* enabling tracing
* enabling an HTTP transport

---

## 14. Testing must reinforce decoupling

Every boundary trait should be easy to fake in tests.

Rules:

* A3 application-layer tests should usually use in-memory or fake implementations.
* Tests should not require the real infrastructure unless the test is explicitly integration-level.
* If a component is hard to test without booting half the system, the dependency surface is too
  broad.

---

## 15. Small shared context bags

This repository allows shared context bags for cross-cutting state such as configuration, pools,
clients, and process-level services.

Shared context is allowed because it makes it easy to add new shared capabilities independently. It
is not allowed to become a catch-all dependency surface.

### Policy

* Shared state must be split into **multiple small typed bags**, not one oversized `AppState`.
* Each bag must represent one coherent concern.
* Consumers must request only the bags they actually use.
* Adding a new shared capability must normally mean adding a **new bag type**, not expanding an
  existing bag arbitrarily.
* Large root state objects may exist only as bootstrap-owned assembly structures and must not be
  passed through the application verbatim.

Preferred examples:

* `AppConfig`
* `DbPool`
* `SearchClient`
* `MetricsHandle`
* `AuthSettings`

Not preferred:

* `AppState` containing config, pools, clients, feature flags, caches, and request metadata for
  the whole process

---

## 16. Avoid universal context objects

Do not make a single, universal context object the default shape.

That pattern is allowed only when all of the following are true:

* the state is still small
* the fields are tightly related
* nearly every consumer genuinely needs most of it
* the type is acting as one cohesive unit, not as a service locator

Otherwise, split it into multiple bags.

---

## 17. Keep shared state extensible

To allow independent development:

* new cross-cutting capability means a **new bag type**
* existing consumers remain unchanged unless they need that capability
* unrelated consumers must not be forced to accept a widened shared-state struct

The basic rule is:

**add a bag, do not widen a universal bag**

---

## 18. Use scope-level state when appropriate

Not all state is global.

Rules:

* Put truly global state at the application level.
* Put bounded-context state at the scope level when only part of the system needs it.
* Prefer narrower registration when that reduces incidental coupling.

Examples:

* admin-only config under an admin scope
* feature-specific clients for a sub-module
* tenant-specific services under a tenant scope

---

## 19. Cheap clone rule

Shared context should usually contain handles, pools, `Arc`s, or other cheap-clone resources.

Rules:

* Shared bags should contain cheap handles, not large mutable graphs.
* Do not expose broad internal object graphs through shared state.
* Shared state should hold stable infrastructure, not act as the in-memory model of the whole
  system.

---

## 20. Operation context is separate from shared state

Per-operation context must be modeled separately from shared, long-lived state.

Use per-operation context for:

* authenticated user
* correlation or request ID
* resolved tenant
* authorization outcome
* tracing decorations derived during middleware

Rules:

* Middleware or entry points write operation context.
* Handlers or use cases read operation context.
* Operation context types must stay small and purpose-specific.
* Operation context must not be stuffed into global shared state.

---

## 21. Module registration must stay modular

Large systems must keep route or module registration separate.

Rules:

* Each module should expose its own registration or assembly function.
* Module registration should depend only on the bag types that module uses.
* Do not centralize all routing and dependency concerns into one giant startup file beyond final
  assembly.

---

## 22. Recommended default pattern

Use this as the default for new work:

1. Define shared types and a narrow trait in a contract crate.
2. Implement the trait in one or more implementation crates.
3. Make the consumer generic over that trait.
4. Wire the concrete implementation in bootstrap code.
5. Register shared state as multiple small context bags.
6. Let handlers or use cases extract only the bags they need.
7. When new capability is needed, add a new trait or a new bag type.
8. Keep old traits unchanged unless performing a deliberate breaking migration.

For current Aurelia work inside `src/lib/src/`, interpret step 1 as:

* first establish a clean module-local contract and ownership boundary
* then extract it into a dedicated contract crate when the decomposition is justified

---

## 23. Repository policy summary

In this repository:

* we prefer **workspace crate boundaries**
* we define **small capability traits**
* we use **generics by default**
* we use **explicit constructor wiring**
* we avoid **runtime DI patterns**
* we avoid **default trait methods**
* we evolve interfaces by **adding new traits**
* we keep consumers dependent on the **smallest possible contract**
* we use **trait objects only by exception**
* we use **enums for closed implementation sets**
* we use **many small typed context bags**
* we avoid **one universal context object**
* we separate **shared state** from **operation context**
* we register state at the **narrowest practical scope**

The operating principle is:

**small traits, generic composition, explicit wiring, many small typed bags**

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
