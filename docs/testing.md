# Testing Harness and Strategy

Status: Developed

## Objectives

- Provide a reliable test harness to validate the Aurelia workspace at unit, integration, and
  end-to-end levels.
- Ensure integration tests use mocking for all external dependencies while preserving realistic
  behavior.
- Enable Docker Compose-based end-to-end validation across suites.
- Support network failure simulation using `tc/netem` in Docker.
- Reuse a locally generated CA and derived certs across test suites, with optional per-suite certs
  when required.
- **Test-driver control of peer test apps must not travel on the system-under-test transport.**
  Each suite specifies its own out-of-band (OOB) control plane. The peering suite's OOB plane is
  defined in `docs/peering/e2e-tests.md`.
- **Tests must synchronise on observable events, not on timer-based delays**, unless no event is
  available. Where a delay is unavoidable, document why an event-based alternative is not
  possible.

## Technical Details

### Timeout Calibration Enforcement

Async test deadlines are part of the test contract. A timeout wrapper is acceptable only when its
duration matches the work under test. Large generic wrappers are treated as missing calibration
unless the test path genuinely spans loopback transport, a process boundary, Docker, OOB control,
or an intentionally configured multi-second timeout.

Use named constants or approved local helper functions for test-body deadlines. Raw numeric
deadlines are allowed for small operation-level waits only when they are adjacent to the assertion
they bound.

| Test category | Expected wrapper | Typical operation waits | Applies to |
| --- | ---: | ---: | --- |
| Pure in-memory unit tests | `100ms` | `10ms` to `50ms` | reducers, config parsing, ID/error helpers, ring-buffer state without spawned transport |
| In-memory async coordination | `250ms` to `500ms` | `25ms` to `100ms` | `Notify`, channels, local actors, mock sinks, bounded queues |
| Configured timeout behavior | configured timeout plus small margin, normally `100ms` to `1s` | equal to the configured timeout path | tests that intentionally assert timeout or expiry semantics |
| Local loopback transport integration | `2s` to `5s` | `200ms` to `1s` | socket/TCP loopback, handshake, callis setup, reconnect, blob flow without Docker |
| Multi-peer or stress-style local integration | `5s` to `10s` | scenario-specific | mesh, burst, replay, or backpressure tests with many spawned tasks |
| Test-app unit tests | `250ms` to `1s` | `25ms` to `250ms` | OOB parser/control unit tests under `testing/<suite>/apps/` |
| E2E driver operation | `2s` to `15s` | scenario-specific | OOB command round trips, Docker app readiness, network partition expectations |
| E2E suite hang guard | default `600s` | not used for scenario assertions | Docker image build, container startup, complete suite execution |

The timeout scan fails any async test body that uses a raw default-large wrapper such as
`timeout(Duration::from_millis(10_000), ...)` or `timeout(Duration::from_secs(10), ...)`. In-memory
test paths also fail when they use a named test-body timeout constant above the in-memory ceiling.
Transport and E2E tests that need long wrappers use named constants, for example
`LOCAL_TRANSPORT_TEST_TIMEOUT`, `MULTI_PEER_TEST_TIMEOUT`, or `E2E_SCENARIO_TIMEOUT`, so the reason
is visible at the call site.

### Fixed Sleep Enforcement

E2E apps and drivers must not use fixed sleeps to guess that system-under-test work completed.
The scanner covers Rust source under `testing/**/apps/**/*.rs` and flags:

- `tokio::time::sleep(...)`
- `tokio::time::sleep_until(...)`
- `actix::clock::sleep(...)`
- `std::thread::sleep(...)`

Allowed sleeps require an adjacent comment using the marker
`aurelia-test-allow-sleep: <reason>`. Valid reasons are:

- `behavior-duration`: the sleep is the behavior under test, such as a handler block duration or
  configured application downtime.
- `negative-assertion`: the test must prove that an event does not occur within a bounded window,
  and no positive observable event can replace the wait.
- `poll-interval`: the sleep is the delay between bounded probes for an observable state.
- `explicit-backoff`: the sleep is part of a documented retry or backoff contract.

The comment must explain why an observable event cannot replace the sleep or name the observable
condition being polled. Sleeps without a marker are harness violations.

### Timeout Policy

- Every test must have a clear expectation for how long it should run.
- Any operation that waits on external signals (channels, sockets, notifies, I/O) must be wrapped
  in a timeout that exceeds the expected duration, so tests cannot hang indefinitely.
- Every async test body must have an explicit deadline, either by wrapping the body in an approved
  timeout helper or by using an equivalent local timeout pattern. Operation-specific timeouts remain
  required inside the body when the test waits on external signals.
- Entire test suites must also be bounded by a timeout that exceeds the expected total runtime.
  Use the test harness scripts so the suite is always time-bounded.
- Test-level and operation-level timeouts must be short enough to expose concurrency defects. A
  local loopback operation that should complete promptly must not inherit laggy-network production
  defaults such as a multi-second or 30-second `send_timeout`.
- Suite-level timeouts are only outer hang guards. They must not be the first timeout that detects
  a failed local concurrency expectation.

### Timeout Calibration Requirements

Tests must use timeout values that match the path under test:

- Unit tests over in-memory state, notifies, limiters, reducers, or state machines must use
  millisecond-scale operation deadlines unless the test explicitly validates a longer configured
  timeout.
- Integration tests that run on local loopback must set scenario-specific `DomusConfig` values for
  `send_timeout`, accept timeout, callback timeout, keepalive interval, and reconnect windows when
  those values are part of the exercised path. They must not rely on transport defaults intended
  for laggy networks.
- E2E tests must keep the suite timeout high enough for Docker startup and image build variance,
  but each scenario must use its own shorter operation deadlines for expected progress and expected
  failure.
- Tests that expect timeout errors must configure the component timeout to the shortest reasonable
  value for the scenario and assert the specific timeout or close error before the suite harness
  timeout is approached.
- Tests that expect concurrency progress must fail if completion clusters around an unrelated
  maintenance interval, keepalive tick, reconnect ceiling, or suite timeout.

### Test Harness Overview

The test harness supports A1/A2/A3 layers:

- Unit tests: fast, isolated, deterministic.
- Integration tests: mock all external dependencies while exercising real internal components.
- End-to-end tests: run small test applications that embed Aurelia in Docker Compose.

### Directory Layout

- `src/lib/src/tests/`: unit tests kept out of module files.
- `src/lib/tests/`: integration tests for the main crate.
- `src/crates/<crate-name>/src/tests/`: unit tests for supporting crates.
- `src/crates/<crate-name>/src/<module>/tests/`: module-local unit tests for large internal
  modules where colocating the test tree keeps the module boundary clearer.
- `src/crates/<crate-name>/tests/`: integration tests for supporting crates.
- `testing/<crate-name>/apps/<app-name>/`: end-to-end test applications per crate.
- `scripts/`: all test-related scripts.
- `containers/shared/`: shared builder and runtime base images for test applications.
- `containers/<suite>/`: suite Dockerfiles, Compose files, and suite-specific container assets.
- `tmp/`: ephemeral test artifacts (git-ignored).

Reusable test helpers live with the crate that owns the behavior under test. The peering crate's
shared unit-test sinks and resolvers live under `src/crates/peering/src/tests/`.

There is no standalone `src/crates/testing` workspace member. Test utility ownership is crate-local
unless a future reviewed requirement introduces a dedicated helper crate.

### Shared Mock Utilities

Peering unit tests share mock sinks and resolvers under `src/crates/peering/src/tests/`:

- `MockSink`: captures delivered messages for assertions and can simulate rejection, queue-full, or
  busy outcomes.
- `StaticRouteResolver`: returns a fixed peer address or `UnknownTaberna` (blob resolution uses the
  same address).

### Unit Tests

- Cover core logic with deterministic inputs and outputs.
- No network or filesystem dependencies.
- Use the Rust built-in test framework with tests isolated in dedicated files.
- Test-only helpers must be gated with `#[cfg(test)]` or placed under test-only modules to avoid
  dead-code in library builds.
- Async unit tests over in-memory logic use millisecond-scale deadlines around the whole test body.
- Async tests that intentionally exercise longer configured transport or application deadlines must
  name the longer deadline with a local constant and keep the enclosing test timeout close to the
  configured path.

### Test Placement And Test Hooks

Implementation modules contain production code. Unit test bodies live in dedicated test files under
the layout above, including module-local `tests/` trees for large internal modules.

Crate roots, module roots, and leaf implementation files may mount dedicated test files with a
minimal `#[cfg(test)] mod tests;` or `#[cfg(test)] #[path = "..."] mod tests;` declaration. They
do not embed inline `mod tests { ... }` bodies.

Test-only methods on production types are grouped in a separate `#[cfg(test)] impl Type` extension
after the live impl for that type. These extensions may expose focused inspection or construction
helpers for unit tests, while live impl blocks remain production-only.

Test-only fields inside production structs are avoided. When tests need to observe internal state,
they use a test impl extension, a test-local wrapper, or a test-only snapshot type kept outside the
live struct layout.

### Test Input Validation

Tests must validate all parameters that are part of the exercised path. Sinks, mocks, and harness
components should reject unexpected values instead of ignoring them (for example, validate
`msg_type`, taberna IDs, flags, and payload shape when they are part of the tested behavior).
Unexpected test input is a harness violation and should fail distinctly from intentional protocol
or application errors. When a test exercises a rejection, queue-full, busy, timeout, or unknown
route path, the helper must configure that failure mode explicitly and still validate the inputs
that lead to it. Positive and negative paths should be paired where practical so the expected input
is accepted and the adjacent unexpected input is rejected by the helper.

### Integration Tests

- Exercise real component boundaries such as routing, peer management, ACK tracking, replay logic,
  and callis isolation with mocked external dependencies.
- Mocks are explicit and reusable across crates.
- Integration tests run without Docker.
- Shared peering mocks live in `src/crates/peering/src/tests/`.

### End-to-End Tests (Docker Compose)

- Build small test applications that link against the `aurelia` crate.
- Test applications live under `testing/<crate-name>/apps/<app-name>/`.
- Compose topology and domus count are defined per test suite.
- Runner scripts under `scripts/testing/` generate certs and run suites end-to-end.
- The harness treats any non-driver container exiting during a run as a failure and surfaces its
  logs.
- End-to-end apps are full applications and must own their Tokio runtime for app-level work (use
  `#[tokio::main]` or an explicit `tokio::runtime::Builder`).
- End-to-end apps must use the `aurelia` wrapper (`aurelia::Aurelia` and
  `Aurelia::domus_builder`) and must not call `aurelia_peering::DomusBuilder` directly.
- The Aurelia runtime is internal and segregated. E2E apps must not access it directly, pass its
  handle around, or rely on it as the application runtime.
- Scenario drivers must wait for peer state through explicit OOB observations before triggering a
  dependent action. Examples include handler-entered-block, Actix-handler-entered-block, peer
  readiness, recipient stopped, and transport-visible completion signals.
- Fixed sleeps are permitted only for behavior duration under test or for a documented negative
  assertion where there is no observable event. Fixed sleeps must not be used to guess that a
  message, blob stream, shutdown, partition, reload, or handler block reached the desired state.

### OOB Synchronization Requirements

Each E2E suite's OOB plane must expose commands that let the driver observe application-side test
state without using A1. The peering suite must provide:

- `ready`: proves A1 setup and taberna registration completed before OOB accepted the command.
- app behavior controls that return after the requested behavior is installed.
- app handler block observation that returns only after a normal taberna handler entered the
  blocking path.
- Actix handler block observation that returns only after the Actix recipient handler entered the
  blocking path.
- recipient stopped observation for Actix shutdown scenarios.

OOB observation commands must be bounded by per-command deadlines. Timeout, malformed command, and
wrong-state responses are part of the test contract and must be covered by app-level unit tests.

### Network Failure Simulation (`tc/netem`)

- Use `tc/netem` within containers to simulate loss, delay, jitter, and partitioning.
- Containers requiring network manipulation run with `NET_ADMIN` capability.
- Failure profiles and enable/disable scripts live under `scripts/testing/`:
  - `netem-apply.sh loss-1|loss-5|delay-100ms|jitter-50ms|partition [iface]`
  - `netem-clear.sh [iface]`

### Certificate Management

- `scripts/testing/generate-certs.sh` creates a local CA and per-domus certificates under
  `tmp/certs/`.
- Generated certs are reusable across suites by default.
- Per-suite cert generation remains available when a suite needs isolated identities.

### Test Artifacts and Output

- End-to-end tests use standard output for results.
- Intermediate artifacts live under `tmp/` and are not retained by default.

### Docker Test Images

All Docker-backed E2E suites use shared base images from `containers/shared/`.

- `containers/shared/` owns the reusable test builder base and runtime base Dockerfiles.
- Suite Dockerfiles under `containers/<suite>/` use those shared bases through build arguments or
  deterministic image tags supplied by the suite runner.
- Builder bases standardise the Rust toolchain image and build prerequisites used for test apps.
- Runtime bases standardise runtime packages, `tc`/netem support, capability setup, and the non-root
  test user.
- Suite Dockerfiles copy only the source tree needed to build the test app and only the compiled
  binary into the runtime image.
- `.dockerignore` must exclude generated publish trees, local logs, editor state, temporary
  artifacts, and other files that are not test image inputs.
- Generated certs under `tmp/certs/` are mounted at runtime and are never copied into image layers.

### Security and Reliability Considerations

- Keep test images free of secrets or unrelated local build artifacts.
- Keep test networking isolated from host-sensitive services.
- Favor deterministic configs and bounded resource usage.

### Required Artifacts

- Dockerfiles and Compose files under `containers/`.
- Scripts under `scripts/` to:
  - build test apps
  - generate certificates
  - run Docker Compose suites
  - apply and remove `tc/netem` profiles
- Documentation for running tests locally under `docs/`.

### Docker and Scripts Layout

- `containers/shared/`: shared builder and runtime base images for test apps.
- `containers/<suite>/`: suite Dockerfiles and Compose files.
- `scripts/testing/`: runner, cert, and netem scripts.

### Local Workflow

1. For suite E2E, run `scripts/testing/run-<suite>-e2e.sh`; it derives `COMPOSE_PROJECT_NAME` as
   `aurelia-<repo-root-basename>` and generates certs for the derived IPs.
2. If you need to run Compose directly, build and run a suite with
   `scripts/testing/run-compose.sh containers/<suite>/docker-compose.yml --build` after generating
   certs with `scripts/testing/generate-certs.sh`.
3. Suite-specific Compose overrides must be documented in the suite’s E2E doc under
   `docs/<suite>/e2e-tests.md`.
4. Apply netem profiles inside containers using `scripts/testing/netem-apply.sh <profile>` when
   manual fault injection is needed.
5. Cleanup: `scripts/testing/run-compose.sh` tears down the stack; remove `tmp/certs` to reset
   identities.

### Suite Timeout Harness

- Workspace tests: `scripts/testing/run-workspace-tests.sh` (bounded by
  `AURELIA_TEST_SUITE_TIMEOUT_SECS`, default 600).
- Test-app unit tests: each `testing/<suite>/apps/<name>` crate is a nested workspace and is
  not reached by the workspace runner. The peering suite uses
  `scripts/testing/run-peering-app-tests.sh` (also bounded by `AURELIA_TEST_SUITE_TIMEOUT_SECS`).
- End-to-end tests: `scripts/testing/run-<suite>-e2e.sh` (bounded by
  `AURELIA_E2E_TIMEOUT_SECS`, default 600).

### Async Test Deadline Coverage

`scripts/testing/check-async-test-timeouts.py` scans `src/` and `testing/` for `#[tokio::test]`
and `#[actix::test]` async test bodies. Each async test body must contain an explicit
`tokio::time::timeout(...)`, `tokio::time::timeout_at(...)`, or approved local helper wrapper.

The scan also guards the helper-crate ownership policy by failing if a standalone
`src/crates/testing` directory exists or stale `src/crates/testing` / `aurelia-testing` references
return outside this document and the scan implementation.

The workspace test runner and peering app test runner execute the scan before running tests:

- `scripts/testing/run-workspace-tests.sh`
- `scripts/testing/run-peering-app-tests.sh`

### Parallel E2E Instances

- Compose project name must be derived from the repo root directory name and prefixed with
  `aurelia-`.
- The E2E subnet must be derived from a stable hash of the project name.
- Subnet format: `172.20.<hash-octet>.0/24` where `<hash-octet>` is the low 8 bits of a stable
  hash.
- Static IPs remain fixed offsets inside the derived `/24` (for example `.11`, `.12`, `.13`,
  `.20`).
- E2E Docker image tags must be derived from the repo suffix but sanitized to meet Docker image
  reference rules.
- Collision detection must detect conflicts with existing Docker network subnets and host routes.
- If a collision is detected, retry with a deterministic salted hash (`<project-name>#<n>`) until a
  free subnet is found or a bounded retry limit is reached.
- If `docker compose` fails due to an overlap detected after the pre-check, retry with the next
  salted hash.

### Comprehensive Testing Scope

- Unit, integration, and end-to-end coverage with explicit pass/fail criteria.
- Integration tests mock external dependencies.
- End-to-end tests validate real network behavior under Docker Compose.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
