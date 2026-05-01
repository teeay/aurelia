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

### Timeout Policy

- Every test must have a clear expectation for how long it should run.
- Any operation that waits on external signals (channels, sockets, notifies, I/O) must be wrapped
  in a timeout that exceeds the expected duration, so tests cannot hang indefinitely.
- Entire test suites must also be bounded by a timeout that exceeds the expected total runtime.
  Use the test harness scripts so the suite is always time-bounded.

### Test Harness Overview

The test harness supports A1/A2/A3 layers:

- Unit tests: fast, isolated, deterministic.
- Integration tests: mock all external dependencies while exercising real internal components.
- End-to-end tests: run small test applications that embed Aurelia in Docker Compose.

### Directory Layout

- `src/lib/src/tests/`: unit tests kept out of module files.
- `src/lib/tests/`: integration tests for the main crate.
- `src/crates/<crate-name>/src/tests/`: unit tests for supporting crates.
- `src/crates/<crate-name>/tests/`: integration tests for supporting crates.
- `src/crates/testing/`: shared mocks and test utilities.
- `testing/<crate-name>/apps/<app-name>/`: end-to-end test applications per crate.
- `scripts/`: all test-related scripts.
- `containers/`: Dockerfiles, Compose files, and container assets.
- `tmp/`: ephemeral test artifacts (git-ignored).

The shared harness utilities live in `src/crates/testing/`, with helper sinks and resolvers for
integration tests.

### Shared Mock Utilities

The `aurelia-testing` crate provides reusable integration helpers:

- `RecordingSink`: captures delivered messages for assertions.
- `BlockingSink`: blocks ingress until released.
- `RejectingSink`, `IngressFullSink`, `TimeoutSink`: fixed failure outcomes.
- `StaticRouteResolver`: returns a fixed peer address or `UnknownEndpoint` (blob resolution uses
  the same address).

### Unit Tests

- Cover core logic with deterministic inputs and outputs.
- No network or filesystem dependencies.
- Use the Rust built-in test framework with tests isolated in dedicated files.
- Test-only helpers must be gated with `#[cfg(test)]` or placed under test-only modules to avoid
  dead-code in library builds.

### Test Input Validation

Tests must validate all parameters that are part of the exercised path. Sinks, mocks, and harness
components should reject unexpected values instead of ignoring them (for example, validate
`msg_type`, taberna IDs, flags, and payload shape when they are part of the tested behavior).

### Integration Tests

- Exercise real component boundaries such as routing, peer management, ACK tracking, replay logic,
  and callis isolation with mocked external dependencies.
- Mocks are explicit and reusable across crates.
- Integration tests run without Docker.
- Shared mocks live in `src/crates/testing/`.

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

- `containers/_shared/Dockerfile.test-base`: base image for test apps.
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
