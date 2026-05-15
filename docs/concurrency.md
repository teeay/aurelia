# Concurrency Patterns

This is a reference document. It defines the patterns that Aurelia and its supporting crates
must use when building concurrent state machines on top of `tokio`. It is **not** a Caravaggio
document: it has no action plan and no lifecycle status. It captures rules of construction.

Implementation work that adopts these patterns lives in the relevant Caravaggio (e.g.
`docs/peering/transport-model.md` for the callis layer); those Caravaggios reference this
document rather than restating its rules.

## Core tasks

A **core task** is a long-running `tokio` task that owns mutable state, drives state
transitions, and exchanges progress with other tasks via `tokio::sync` primitives. Examples
include the per-callis receive loop, primary callis transmitter tasks, the peer state task, and
the observability task. Core tasks are distinct from one-shot helper tasks (e.g. dial
scaffolds, drop cleanups) and from passive state containers.

The race-free patterns in this document are **mandatory for all core tasks** and are the
**preferred pattern for all long-running tasks**. Code review should reject new core tasks
that do not follow them and should treat any divergence in long-running tasks as requiring
a documented justification.

## Why these patterns exist

The workspace runs many concurrent tasks that exchange progress through `tokio::sync`
primitives. Used naively, several of these primitives have ordering requirements that, if
violated, produce **missed-notify** races: a state change happens, a wakeup fires, and a
consumer that should have observed both ends up waiting until an unrelated timeout fires.
The symptoms are flaky tests, latency tails at exactly `send_timeout`, and code that "mostly
works".

The patterns below are how `caducus` and well-structured parts of `aurelia-peering` already
build their loops. They are mandatory for new core tasks and expected of code under
refactor.

## Pattern 1: The double-check `Notify` pattern

`tokio::sync::Notify::notify_waiters()` only wakes waiters **already registered** at the
moment of the call. It does not store a permit for future waiters. A consumer that reads
state, decides it must wait, then calls `notified()` will miss any notification that fired
between the read and the registration.

The fix is to construct the waiter **before** the final state check.

### Reference shape (from `caducus/src/receiver.rs`)

```rust
loop {
    // 1. Try to consume immediately.
    if let Some(item) = try_receive(&self.ring)? {
        return Ok(item);
    }

    // 2. Construct the waiter BEFORE re-checking state.
    //    `notified()` captures the current notify epoch; any
    //    `notify_waiters()` after this call will wake us.
    let waiter = self.notify.notified();
    tokio::pin!(waiter);

    // 3. Re-check after the waiter is registered.
    //    Catches any state change that happened between step 1 and step 2.
    if let Some(item) = try_receive(&self.ring)? {
        return Ok(item);
    }

    // 4. Wait. Any notify after step 2 wakes us.
    tokio::select! {
        _ = &mut waiter => {}
        _ = tokio::time::sleep(remaining) => {
            return Err(timeout());
        }
    }
}
```

The shape is **arm waiter -> recheck -> await**. Both checks of state are required; both
are cheap. Skipping the second is the bug.

### Use in a multi-arm `tokio::select!`

When a loop has more than one progress source, the same discipline applies: every notify
source must be armed before the state read that decides whether to wait. Concretely:

```rust
loop {
    // Arm every Notify-based wakeup BEFORE doing the state check.
    let callis_waiter = blob.callis_notify().notified();
    let accept_waiter = accept_notify.notified();
    tokio::pin!(callis_waiter, accept_waiter);

    // Drain everything that is already actionable.
    drain_all(&mut state).await;

    // Now wait. Anything that fires after the drain wakes us.
    tokio::select! {
        _ = &mut callis_waiter => {}
        _ = &mut accept_waiter => {}
        result = read_frame(...) => { handle(result).await; }
        _ = sleep_until(deadline) => {}
    }
}
```

### Anti-pattern (forbidden)

```rust
loop {
    // Conditional arm guards read state; the notify future is constructed
    // INSIDE the select, AFTER the state was read. A notify_waiters fired
    // between reading the guard and the select expansion is lost.
    let has_pending = !state.pending.is_empty();
    tokio::select! {
        _ = source.notified(), if has_pending => { drain }
        _ = sleep_until(deadline) => { drain }
        ...
    }
}
```

If you need conditional arms, the consumer must still arm the waiter before reading the
state that decides "is there work to do". The current `if has_x` arm guards in
`aurelia-peering` are an anti-pattern in their current form.

## Pattern 2: Single-mutex shared state, notify after unlock

The state read and written by a producer/consumer pair belongs behind one lock when
practical. Multi-lock state structures invite ordering bugs and require a mental
"lock-ordering protocol" to audit.

### Reference shape (`caducus/src/concurrency.rs`)

```rust
pub fn send_spsc(&self, item: T) -> Result<(), Error<T>> {
    let mut ring = self.lock();      // single lock
    ring.try_push_spsc(item)?;
    drop(ring);                      // drop BEFORE notifying
    self.notify_consumer.notify_waiters();
    Ok(())
}
```

The `drop(ring)` before `notify_waiters` is intentional. The woken consumer immediately
tries to lock; if the producer still held it, the consumer would wake and immediately
block. Notifying after the unlock lets the consumer proceed without a re-park.

### Rules

- Do not hold a lock across `notify_waiters` / `notify_one`.
- Prefer one `Mutex<State>` over multiple `Mutex<Subset>` for state that is mutated
  together.
- Methods that mutate-and-notify should look like the reference shape above: lock,
  mutate, drop guard, notify.

### When multiple locks are unavoidable

If different facets of state genuinely have independent contention profiles, document a
**lock-ordering invariant** at the type definition (e.g. "always acquire `outbound` before
`retained`"). Without a documented order, multi-lock structures are not auditable.

## Pattern 3: Drain returns a snapshot, callers dispatch

State-change consumers often need to react to multiple kinds of update at once: items that
became ready, items that expired, the next deadline, whether shutdown was observed. The
function that reads the state should return a snapshot of all of these in one call, and
the caller dispatches on the result.

### Reference shape (`caducus/src/concurrency.rs`)

```rust
pub(crate) struct DrainResult<T> {
    pub expired: Vec<PopResult<T>>,
    pub live: Option<PopResult<T>>,
    pub next_deadline: Option<Instant>,
    pub is_shutdown: bool,
}

pub fn drain(&self, now: Instant, mode: DrainMode) -> DrainResult<T> { ... }
```

One lock acquisition produces a complete view. Compare to a design where each piece of
information requires its own async function: every additional call is another race window
and another point where the state can change underneath the consumer.

### Rules

- Prefer one `drain(...) -> Snapshot` over `try_pop() / next_deadline() / is_shutdown()`
  called separately.
- The snapshot type belongs in the same module as the lock; it is the public shape of "what
  happened on this call".

## Pattern 4: Pick the right primitive

Three commonly-confused primitives:

| Primitive | Latches | Multi-consumer | Use when |
|---|---|---|---|
| `tokio::sync::Notify` (notify_one) | One permit stored | First waiter wins | Fire-and-forget wakeups; producer doesn't care who wakes. |
| `tokio::sync::Notify` (notify_waiters) | No permits | All current waiters wake | Broadcasting "state changed"; **must** be paired with the double-check pattern in pattern 1. |
| `tokio::sync::watch<T>` | Latest value held | All consumers see the last value | "What is the current value of X?" — consumers see the freshest state regardless of registration timing. |
| `tokio::sync::mpsc` | Bounded queue | Single consumer | Command channels, work queues, unidirectional pipelines. |

### Guidance

- **Single producer, single consumer, latching state**: use `watch`. The latching behaviour
  eliminates the missed-notify race entirely.
- **Multiple producers, single consumer command stream**: use `mpsc`.
- **Producer-consumer wakeup with pattern 1 in place**: `Notify` is fine.
- **"Wait for any of these state changes"**: prefer multiple `watch` channels over multiple
  `Notify`s where state-snapshot semantics fit.

A `Notify` that exists purely to broadcast "something in this struct changed, go look" is
usually better expressed as a `watch<u64>` whose value is a generation counter. Bumping the
counter is the same notification cost; consumers see the latest generation regardless of
registration timing.

## Pattern 5: Loop epilogue discipline

Loops whose state mutations require post-conditions (e.g. "after every state change,
republish a snapshot to the watch channel and update the impaired-since deadline") must
invoke the post-conditions in **exactly one** place per iteration. The pattern is:

```rust
loop {
    // Wait for an event.
    let event = recv_event().await;

    // Mutate state.
    apply(event, &mut state).await;

    // Epilogue: invariants that must hold after every iteration.
    update_impaired_since(&mut state).await;
    publish_snapshot(&snapshot_tx, &state);
}
```

A state-mutation arm that does its own epilogue inline, in addition to or instead of the
loop-bottom epilogue, is a bug surface. If a new event handler is added that forgets the
epilogue, the invariant silently breaks.

When arms need to skip an iteration without going through the epilogue, use `continue` and
make sure the epilogue runs exactly when intended.

## Pattern 6: Worker decomposition

A loop that handles four or more independent kinds of progress (frame I/O, accept-channel
drain, deadline-driven housekeeping, shutdown signalling) is harder to keep correct than
the same logic split into:

- A reader/writer pair that owns the I/O.
- A state actor that owns the mutable state and accepts commands over a single mpsc.
- Optional housekeeping tasks for periodic work.

Refactoring a monolithic core task into a worker-decomposition is not always required, but the
threshold to keep monolithic is "you can read the loop top to bottom and reason about
every notify/lock/select interaction." When the body crosses ~150 lines or contains nested
matches deeper than two levels, decomposition is the better answer.

## Anti-pattern checklist

For new code review, the following are red flags:

1. `tokio::select!` arm using `notified()` without the matching armed-before-recheck
   pattern in pattern 1.
2. `notify_waiters()` called while holding a `Mutex` guard that the woken consumer must
   also acquire.
3. A state struct with three or more `Mutex<...>` fields that are mutated together.
4. Async functions that return one piece of state (e.g. `is_shutdown()`) and are called in
   sequence in a hot loop. Replace with a single snapshot-returning call.
5. Per-arm side effects in a `tokio::select!` body that include "publish snapshot" or
   "update derived state". Move to a loop epilogue.
6. Conditional arm guards (`if condition`) on `tokio::select!` arms. They almost always
   indicate state being read outside the arm-before-recheck discipline.

## See also

- `caducus/src/receiver.rs` — reference implementation of pattern 1.
- `caducus/src/concurrency.rs` — reference implementation of patterns 2 and 3.
- `tokio::sync::Notify` documentation, particularly the section on `notify_waiters` vs
  `notify_one` semantics.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
