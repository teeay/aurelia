# Peering E2E Tests

Status: Developed

## Objectives

- Define peering end-to-end scenarios that validate real network behavior.
- Scenarios use only the Aurelia wrapper API surface.
- Graceful shutdown / restart behavior is deterministic and observable with `PeerUnavailable`.
- All test-driver control of peer test apps travels on a dedicated out-of-band (OOB) plane, never
  on the A1 transport that the test is exercising. Test infrastructure does not depend on the
  system under test for its own steering.
- Every scenario that crashes or shuts down a peer waits for the peer to advertise readiness on
  the OOB plane before its next dependent action.

## Testing Plan

- `bash scripts/testing/run-workspace-tests.sh`
- `bash scripts/testing/run-peering-app-tests.sh`
- `bash scripts/testing/run-peering-e2e.sh`

## Technical Details

### Topology

- Compose file: `containers/peering/docker-compose.yml` on `peering-net`.
- Peers: three domus containers plus a driver.
- Apps:
  - `testing/peering/apps/peering-domus`
  - `testing/peering/apps/peering-driver`
- Runner: `scripts/testing/run-peering-e2e.sh`.

### Graceful Shutdown Semantics

- When a domus begins shutdown, the listener stops accepting immediately.
- New inbound connections during shutdown are dropped at the socket level (no protocol
  negotiation, no error frames).
- Existing connections may drain; inflight waits are bounded by the configured timeout.
- Dialers observing shutdown receive `PeerUnavailable` (connection loss or dial failure).

### Scenario Matrix (E2E)

All scenario steps that drive the peer apps (set behaviors, crash, shutdown, reload-auth) travel
on the Out-of-Band (OOB) Test Control Plane defined below. No scenario uses the A1 transport for
test control. Network failure injection (`tc/netem`) is performed locally on the driver
container; the peer's `NET_ADMIN` capability is no longer used by tests.

| Scenario | Purpose | Real-World Replacement | Notes |
| --- | --- | --- | --- |
| `scenario_listener_originator` | Validate basic send/receive. | None. | Normal path. |
| `scenario_reconnect_replay` | Replay after temporary disconnect. | Apply temporary NetEm partition on the driver and let it auto-clear. | Partition window must be shorter than `send_timeout` when replay is expected. |
| `scenario_peer_restart` | Restart recovery. | OOB `crash` then Docker restarts the container. | Driver pings before, awaits `ready` on OOB, pings after. Verifies clean recovery without relying on aurelia internals (e.g., fresh-session error codes on inflight). |
| `scenario_smooth_rotation` | Reload auth material without disruption. | Reload during in-flight traffic. | Existing callis continues; sends across the reload all succeed. A subsequent fresh dial uses the new cert. |
| `scenario_graceful_close` | Graceful shutdown plus restart. | OOB `shutdown <ms>` command. | Use driver domus; expect `PeerUnavailable` during downtime. After downtime probes, the driver awaits `ready` on OOB. |
| `scenario_backpressure` | Send queue / inflight pressure. | None. | Uses short timeouts. |
| `scenario_taberna_errors` | Taberna rejection paths. | None. | Unchanged. |
| `scenario_receive_timeout` | Taberna receive timeout plus shutdown mapping. | None. | Local driver domus taberna `next` uses short timeout, then expects `domus-closed` after shutdown. |
| `scenario_unknown_taberna` | Unknown taberna handling. | None. | Unchanged. |
| `scenario_peer_unreachable` | Unreachable peer. | Dial to an unreachable port. | Expect `SendTimeout`. |
| `scenario_protocol_mismatch` | Protocol mismatch detection. | Raw TLS / protocol mismatch. | Expect protocol failure. |
| `scenario_half_open_keepalive` | Half-open detection. | NetEm partition (driver-side). | Use keepalive interval plus timeout tuned to partition. |

### Reconnect vs Restart

The two failure modes are tested as deliberately distinct scenarios:

- **Reconnect (network blip, peer alive throughout)** — `scenario_reconnect_replay`. Driver-side
  NetEm partition for a short window. Aurelia detects disconnect, reconnects after the window
  clears, replays the inflight on the same session. Send returns `Ok`.
- **Restart (peer dies, fresh process comes up)** — `scenario_peer_restart`. Driver crashes the
  peer via OOB; Docker restarts the container. Driver awaits `ready` on OOB (proof that A1 is
  up), then verifies a fresh ping succeeds. The test is bracketed cleanly across the restart
  boundary; it does not rely on aurelia's internal fresh-session error code on inflight messages.

### Graceful Shutdown Scenario Details

- Driver issues `oob_control(domus, "shutdown <downtime-ms>")`.
- Driver sets `send_timeout` and `accept_timeout` to `<= 1000ms` for fast probe failure.
- During the downtime window, the driver fires fresh probe domus dials at the peer; each is
  expected to fail with `PeerUnavailable`, `SendTimeout`, or `ConnectionLost` (any indicates the
  listener is closed). At least one such failure within the probe window proves the peer is in
  graceful-shutdown mode.
- After the downtime window, the driver calls `wait_for_peer_ready_oob` against the peer.
- Once `ready` succeeds, the driver retries a ping until it succeeds, then restores defaults.

The peer's runtime command handler spawns `domus.shutdown()` rather than awaiting it: aurelia's
`graceful_close` (which closes the listener) runs synchronously at the start of `shutdown`, so
the listener is unreachable by the time the downtime sleep begins. Awaiting the full
`wait_for_callis_zero` drain (up to `2 * send_timeout`) would block past the downtime window and
delay `process::exit`; the test instead lets the drain run while the downtime sleep proceeds and
the process exits at the end of the window.

### Timeouts

- All scenarios define explicit time bounds; no unbounded waits.
- Suites use the runner timeout harness to prevent hangs.

### Out-of-Band (OOB) Test Control Plane

End-to-end test apps do NOT accept driver control commands on the A1 transport that the test is
exercising. Routing test-driver control through the system under test couples the test
infrastructure's success to the system's correctness — when A1 is intentionally provoked into
failure, the test loses its ability to drive the system. Every peer test app exposes a dedicated
OOB plane on a separate TCP port. The driver uses it for all test control and for explicit
peer-readiness signalling.

#### Listener (peer side)

- Bind: `0.0.0.0:OOB_CONTROL_PORT` (env var, default `5001`). The driver reads the same env var
  and Compose threads the value through so both sides agree.
- Independent from the aurelia A1 listener: different port, different code path, no shared
  dispatch. Implemented as a top-level `tokio::spawn` in `main`, not as an aurelia taberna.
- Bind ordering (strict): `serve` is spawned only after `aurelia.domus_builder(...).build().await?`
  returns and the peer's response/app tabernas have been registered. A successful TCP connect to
  `OOB_CONTROL_PORT` is therefore a strict precondition that A1 is fully initialised. The peer
  enforces this ordering and documents the contract in code.
- OOB liveness vs. A1 liveness: the OOB listener is bound 1:1 to a single A1 initialisation.
  There is no in-process restart command on the OOB plane (`RuntimeCommand::Restart` does not
  exist). A peer process exit is the only path that affects OOB availability, and the new process
  binds OOB only after its A1 layer is up — so `ready` is never optimistic.
- Accept loop spawns one task per inbound TCP connection. Each connection is one-shot:
  read one line, dispatch, write response, close. The `crash` command is the only exception (see
  Command Set).
- Lifecycle: tied to the same `watch::Receiver<bool>` as the taberna tasks so OOB stops cleanly
  on Domus shutdown. The receiver is flipped before `process::exit` in the shutdown path, so the
  OOB listener does not accept new connections during the downtime window.

#### Wire Protocol

Line-based, UTF-8.

- Request: `<command>\n`. Maximum payload (including newline) is 1 KiB. Reads bounded by a 5 s
  deadline.
- Response: `OK\n` on success, `ERR <message>\n` on failure. `<message>` is a plain UTF-8 string
  up to 256 bytes.
- Connection closes after the single response, with one event-based exception for `crash`.

#### Command Set

- `ready` — no-op, returns `OK`. Implies the peer's A1 is fully initialised.
- `set app <behavior> [duration_ms]` — `Normal | Block | Reject | IngressFull | Busy | DecodeFailure`.
  Optional `duration_ms` schedules a peer-side restoration of `Normal` after the behavior
  duration. This is a behavior duration, not a synchronisation delay.
- `unblock app` — equivalent to `set app normal`.
- `shutdown <ms>` — graceful shutdown of A1, then `process::exit(2)` after the `<ms>` downtime
  window. The OOB listener also stops at the start of the shutdown sequence.
- `crash` — exit, with event-based delivery synchronisation. Sequence:
  1. Dispatcher writes `OK\n`, flushes the write buffer.
  2. Connection task half-closes the write side of the TCP stream (FIN).
  3. Connection task reads from the socket until EOF (the driver, having read `OK`, drops its
     end of the connection).
  4. Connection task calls `process::exit(2)`.
  No timer is used; the EOF read is the synchronisation event that proves the driver received
  `OK`.
- `reload-auth` — reload the Domus auth material from disk.

Unknown commands or argument parse errors return `ERR <reason>` and close the connection.

#### Driver Helpers

```rust
fn oob_control_port() -> u16 {
    std::env::var("OOB_CONTROL_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(5001)
}

async fn oob_control(domus_info: &DomusInfo, command: &str) -> Result<(), String> {
    let addr = SocketAddr::new(domus_info.primary.ip(), oob_control_port());
    let mut stream = timeout(Duration::from_secs(5), TcpStream::connect(addr))
        .await
        .map_err(|_| "oob control connect timeout".to_string())?
        .map_err(|err| format!("oob control connect: {err}"))?;
    stream
        .write_all(format!("{command}\n").as_bytes())
        .await
        .map_err(|err| format!("oob control write: {err}"))?;
    let mut buf = String::new();
    timeout(
        Duration::from_secs(5),
        BufReader::new(&mut stream).read_line(&mut buf),
    )
    .await
    .map_err(|_| "oob control response timeout".to_string())?
    .map_err(|err| format!("oob control read: {err}"))?;
    let trimmed = buf.trim_end_matches(&['\r', '\n'][..]);
    if trimmed == "OK" {
        Ok(())
    } else if let Some(rest) = trimmed.strip_prefix("ERR ") {
        Err(rest.to_string())
    } else {
        Err(format!("oob control malformed response: {trimmed}"))
    }
}

async fn wait_for_peer_ready_oob(
    domus_info: &DomusInfo,
    deadline: Instant,
) -> Result<(), String> {
    loop {
        if oob_control(domus_info, "ready").await.is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "peer {} never became ready via OOB",
                domus_info.name
            ));
        }
        // Polling cadence, not synchronisation delay: there is no peer-side event the driver
        // can subscribe to before the peer's listener is bound.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
```

Every OOB command, including `crash`, returns a strict success response because `crash`
synchronises on the client EOF before exiting.

#### Determinism Guarantees

- The OOB plane has zero dependency on aurelia A1. Disruption to A1 does not impair the driver's
  ability to drive the system.
- The `crash` command's `OK` is fully delivered to the driver before the runtime exits, because
  the peer waits for the driver's connection close (EOF) before calling `process::exit`. No
  timer participates in this handshake.
- After a peer crashes or completes graceful shutdown, the new container's listener binds
  `OOB_CONTROL_PORT` only after A1 is fully initialised. A successful `ready` is therefore a
  strict precondition for the next test step.
- Network failure simulation (`tc/netem`) is performed on the driver container. The OOB plane
  is never subject to a partition that would break its own request/response handshake.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
