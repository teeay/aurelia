# Logging Utilities

Status: Developed

## Objectives

- Provide a system-wide, rate-limited logging facility with stable log IDs.
- Allow runtime updates to the log suppression interval via channel-based updates.
- Keep log reads synchronous and lock-free at call sites.

## Technical Details

### Logging Levels

- `info`: Operational milestones that establish progress or state (for example, connection
  established or handshake completed).
- `warn`: Errors that are handled without jeopardizing system functionality.
- `error`: Errors that jeopardize system functionality or require operator attention.
- `debug`: Major internal activities within a function that explain control flow and decisions.
- `trace`: All significant steps that are worth logging for deep troubleshooting.

### Limited Logging Registry

- Each Domus owns its own registry of `LogId` (`u32`) entries and last-log timestamps.
- Registries are created per Domus via `init_limited_logging(ids, interval)`, which returns a
  `LimitedLogContext` (registry + control handle).
- The registry uses a fixed `HashMap<LogId, AtomicU64>` to avoid mutation and locks at log time.
- Log timestamp checks are performed with atomic compare-exchange and `now_secs()`.

### Channel-Based Configuration Updates

- A `watch::Sender<u64>` carries the suppression interval in seconds.
- Updates call `LimitedLogControl::set_interval(Duration)`.
- Call sites read the interval synchronously through `watch::Receiver::borrow()`.
- Limited logging intentionally operates at whole-second resolution. `Duration::as_secs()` is the
  conversion boundary, so any sub-second component is truncated before storage.
- `Duration::ZERO` disables limited logging. Any non-zero interval shorter than one second also
  truncates to zero and therefore disables limited logging.
- Sub-second precision is not supported for limited logging intervals.

### Macros

- `info_limited!(REGISTRY, LOG_ID, ...)`
- `warn_limited!(REGISTRY, LOG_ID, ...)`
- `debug_limited!(REGISTRY, LOG_ID, ...)`
- `error_limited!(REGISTRY, LOG_ID, ...)`

Each macro emits `log_id = <id>` in structured output and only logs when the registry allows it.

### When To Use Limited Logging

- Limited logging is for high-frequency or repeating events that can flood logs.
- One-time or configuration-time warnings (for example, clamping a config value to enforced
  limits) should use standard `warn` logging, not the limited macros.

### Initialization and Updates

- Domus startup creates a per-Domus limited logging context with all known IDs and the configured interval.
- Config updates call `LimitedLogControl::set_interval` on the Domus-local control handle.
- The interval is configured via `DomusConfig::limited_log_interval` and may be updated through
  `DomusConfigAccess`. Operators should configure this value in whole seconds.

### Testing Scope

- Unit tests cover rate-limiting behavior and interval updates.
- Peering tests cover handshake admission control logs via the limited macros.

### Security and Reliability Considerations

- Default interval is 120 seconds to prevent log floods during thundering herd events.
- Logging IDs are centrally documented in `docs/ids.md`.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
