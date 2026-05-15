// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::super::backend::AuthenticatedStream;
use super::super::callis::{
    cancel_waiters, drain_accept_waiters, drain_blob_callis_waiters, BlobAcceptWaiter,
    BlobCallisWaiter, InboundWaiter, MessageWaiter,
};
use super::*;
use crate::config::DomusConfigBuilder;
use crate::observability::{DomusReporting, DomusReportingEvent};
use crate::session::CancelReason;
use crate::transport::peer::{
    PeerState, PeerStateEffects, PeerStateMachine, PeerStateSnapshot, PrimaryDialState,
};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::time::Instant;

const PEER_STATE_TEST_TIMEOUT: Duration = Duration::from_secs(5);

type ListenerAcceptRx =
    Arc<Mutex<Option<mpsc::Receiver<AuthenticatedStream<tokio::io::DuplexStream, DomusAddr>>>>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EffectivePrimaryDial {
    Idle,
    Dialing,
    SuppressedByActivePrimary,
}

fn effective_primary_dial(state: &PeerState) -> EffectivePrimaryDial {
    if !state.primary.is_empty() && state.primary_dial != PrimaryDialState::Dialing {
        EffectivePrimaryDial::SuppressedByActivePrimary
    } else {
        match state.primary_dial {
            PrimaryDialState::Idle => EffectivePrimaryDial::Idle,
            PrimaryDialState::Dialing => EffectivePrimaryDial::Dialing,
        }
    }
}

#[derive(Clone)]
struct ListenerBackend {
    accept_tx: mpsc::Sender<AuthenticatedStream<tokio::io::DuplexStream, DomusAddr>>,
    accept_rx: ListenerAcceptRx,
    accepted: Arc<AtomicUsize>,
}

impl ListenerBackend {
    fn new() -> Self {
        let (accept_tx, accept_rx) = mpsc::channel(4);
        Self {
            accept_tx,
            accept_rx: Arc::new(Mutex::new(Some(accept_rx))),
            accepted: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait::async_trait]
impl TransportBackend for ListenerBackend {
    type Addr = DomusAddr;
    type Listener = mpsc::Receiver<AuthenticatedStream<tokio::io::DuplexStream, DomusAddr>>;
    type Stream = tokio::io::DuplexStream;

    async fn bind(&self, _local: &Self::Addr) -> Result<Self::Listener, AureliaError> {
        let mut guard = self.accept_rx.lock().await;
        Ok(guard.take().expect("listener already bound"))
    }

    async fn accept(
        &self,
        listener: &mut Self::Listener,
    ) -> Result<AuthenticatedStream<Self::Stream, Self::Addr>, AureliaError> {
        match listener.recv().await {
            Some(stream) => {
                self.accepted.fetch_add(1, Ordering::SeqCst);
                Ok(stream)
            }
            None => Err(AureliaError::new(ErrorId::PeerUnavailable)),
        }
    }

    async fn dial(
        &self,
        _peer: &Self::Addr,
    ) -> Result<AuthenticatedStream<Self::Stream, Self::Addr>, AureliaError> {
        Err(AureliaError::new(ErrorId::PeerUnavailable))
    }
}

fn test_authenticated_stream(
    peer_addr: DomusAddr,
) -> AuthenticatedStream<tokio::io::DuplexStream, DomusAddr> {
    let (stream, _peer) = tokio::io::duplex(64);
    AuthenticatedStream { stream, peer_addr }
}

fn empty_peer_state() -> PeerState {
    PeerState {
        role: PeerRole::Listener,
        had_primary: false,
        primary: VecDeque::new(),
        reconnect_attempt: 0,
        primary_dial: PrimaryDialState::Idle,
        dialing_blob: false,
        blob_reconnect_attempt: 0,
        closing: false,
        impaired_since: None,
    }
}

fn test_callis_handle() -> CallisHandle {
    let (shutdown, _shutdown_rx) = watch::channel(false);
    CallisHandle {
        id: next_callis_id(),
        tx: CallisTx::Primary,
        shutdown,
    }
}

fn test_peer_with_reporting(
    peer_addr: Option<DomusAddr>,
    config: DomusConfig,
) -> (
    Arc<PeerHandle<TestBackend>>,
    DomusReporting,
    watch::Receiver<PeerStateSnapshot>,
) {
    let backend = Arc::new(TestBackend);
    let registry = Arc::new(TabernaRegistry::new());
    let config: DomusConfigAccess = DomusConfigAccess::from_config(config);
    let limited_registry = config.limited_registry();
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let (listener_shutdown_tx, _listener_shutdown_rx) = watch::channel(false);
    let (reporting, observability) =
        crate::observability::new_observability(tokio::runtime::Handle::current());

    let (handle, snapshot_rx) = PeerHandle::new_for_tests(
        peer_addr,
        Arc::clone(&registry),
        config,
        Arc::new(BlobBufferTracker::default()),
        Arc::clone(&backend),
        HandshakeGate::new(limited_registry),
        observability,
        shutdown_rx,
        listener_shutdown_tx,
        tokio::runtime::Handle::current(),
    );
    let handle = Arc::new(handle);

    (handle, reporting, snapshot_rx)
}

#[tokio::test]
async fn ensure_peer_state_signal_coalesces_when_command_queue_is_full() {
    tokio::time::timeout(PEER_STATE_TEST_TIMEOUT, async {
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(PeerStateUpdate::EnsurePrimaryDial)
            .expect("fill command queue");

        try_signal_peer_state_ensure(&tx, PeerStateUpdate::EnsurePrimaryDial)
            .expect("full ensure queue should coalesce");

        assert!(matches!(
            rx.recv().await,
            Some(PeerStateUpdate::EnsurePrimaryDial)
        ));
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn peer_handle_shutdown_is_latched_for_state_task() {
    let peer_addr = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9400));
    let (handle, _reporting, _snapshot_rx) =
        test_peer_with_reporting(Some(peer_addr), DomusConfig::default());

    handle.shutdown().await;

    timeout(Duration::from_secs(1), async {
        loop {
            if handle.peer_state_tx.is_closed() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("state task should observe latched shutdown");
}

#[test]
fn peer_state_machine_primary_dial_branches_are_deterministic() {
    let mut state = empty_peer_state();

    let effects = PeerStateMachine::ensure_primary_dial(&mut state, false, true);
    assert_eq!(effects, PeerStateEffects::default());
    assert_eq!(state.primary_dial, PrimaryDialState::Idle);

    let effects = PeerStateMachine::ensure_primary_dial(&mut state, true, false);
    assert_eq!(effects, PeerStateEffects::default());
    assert_eq!(state.primary_dial, PrimaryDialState::Idle);

    state.closing = true;
    let effects = PeerStateMachine::ensure_primary_dial(&mut state, true, true);
    assert_eq!(effects, PeerStateEffects::default());
    assert_eq!(state.primary_dial, PrimaryDialState::Idle);
    state.closing = false;

    let effects = PeerStateMachine::ensure_primary_dial(&mut state, true, true);
    assert!(effects.spawn_primary_dial);
    assert!(effects.promote_listener_to_originator);
    assert_eq!(state.primary_dial, PrimaryDialState::Dialing);

    let effects = PeerStateMachine::ensure_primary_dial(&mut state, true, true);
    assert!(!effects.spawn_primary_dial);
    assert_eq!(state.primary_dial, PrimaryDialState::Dialing);

    let effects = PeerStateMachine::connected_primary(&mut state, false, false);
    assert!(effects.publish_snapshot);
    assert_eq!(state.primary_dial, PrimaryDialState::Idle);
    assert!(state.had_primary);
    assert_eq!(state.reconnect_attempt, 0);

    state.primary.push_back(test_callis_handle());
    let effects = PeerStateMachine::ensure_primary_dial(&mut state, true, true);
    assert!(!effects.spawn_primary_dial);
    assert_eq!(
        effective_primary_dial(&state),
        EffectivePrimaryDial::SuppressedByActivePrimary
    );

    let effects = PeerStateMachine::dial_failed_primary(&mut state, true, true);
    assert!(!effects.spawn_primary_dial);
    assert_eq!(
        effective_primary_dial(&state),
        EffectivePrimaryDial::SuppressedByActivePrimary
    );

    state.primary.clear();
    let effects = PeerStateMachine::dial_failed_primary(&mut state, false, true);
    assert!(!effects.spawn_primary_dial);
    assert_eq!(state.primary_dial, PrimaryDialState::Idle);

    let effects = PeerStateMachine::dial_failed_primary(&mut state, true, false);
    assert!(!effects.spawn_primary_dial);
    assert_eq!(state.primary_dial, PrimaryDialState::Idle);

    let effects = PeerStateMachine::dial_failed_primary(&mut state, true, true);
    assert!(effects.spawn_primary_dial);
    assert_eq!(state.primary_dial, PrimaryDialState::Dialing);
}

#[test]
fn peer_state_machine_connected_primary_branches_are_explicit() {
    let mut state = empty_peer_state();
    state.primary_dial = PrimaryDialState::Dialing;
    state.reconnect_attempt = 3;

    let effects = PeerStateMachine::connected_primary(&mut state, false, true);
    assert!(effects.publish_snapshot);
    assert!(effects.fail_blob_streams);
    assert!(effects.fail_tracked);
    assert!(!effects.close_new_primary);
    assert!(state.had_primary);
    assert_eq!(state.reconnect_attempt, 0);
    assert_eq!(state.primary_dial, PrimaryDialState::Idle);

    state.closing = true;
    let effects = PeerStateMachine::connected_primary(&mut state, false, false);
    assert!(effects.close_new_primary);
    assert!(!effects.publish_snapshot);

    let mut state = empty_peer_state();
    let effects = PeerStateMachine::connected_primary(&mut state, true, false);
    assert!(effects.close_new_primary);
    assert!(!state.had_primary);
}

#[test]
fn peer_state_machine_primary_removed_branches_are_explicit() {
    let mut state = empty_peer_state();
    state.primary.push_back(test_callis_handle());

    let effects = PeerStateMachine::primary_removed(&mut state, true, true);
    assert!(effects.publish_snapshot);
    assert!(!effects.spawn_primary_dial);
    assert_eq!(
        effective_primary_dial(&state),
        EffectivePrimaryDial::SuppressedByActivePrimary
    );

    state.primary.clear();
    state.primary_dial = PrimaryDialState::Idle;
    let effects = PeerStateMachine::primary_removed(&mut state, false, true);
    assert!(effects.publish_snapshot);
    assert!(!effects.spawn_primary_dial);
    assert_eq!(state.primary_dial, PrimaryDialState::Idle);

    let effects = PeerStateMachine::primary_removed(&mut state, true, false);
    assert!(effects.publish_snapshot);
    assert!(!effects.spawn_primary_dial);
    assert_eq!(state.primary_dial, PrimaryDialState::Idle);

    let effects = PeerStateMachine::primary_removed(&mut state, true, true);
    assert!(effects.publish_snapshot);
    assert!(effects.spawn_primary_dial);
    assert!(effects.promote_listener_to_originator);
    assert_eq!(state.primary_dial, PrimaryDialState::Dialing);

    state.role = PeerRole::Originator;
    state.primary_dial = PrimaryDialState::Idle;
    let effects = PeerStateMachine::primary_removed(&mut state, true, true);
    assert!(effects.spawn_primary_dial);
    assert!(!effects.promote_listener_to_originator);
}

#[test]
fn peer_state_machine_connected_blob_branches_are_explicit() {
    let mut state = empty_peer_state();
    state.dialing_blob = true;
    state.blob_reconnect_attempt = 4;

    let effects = PeerStateMachine::connected_blob(&mut state, true);
    assert_eq!(effects, PeerStateEffects::default());
    assert!(!state.dialing_blob);
    assert_eq!(state.blob_reconnect_attempt, 0);

    state.dialing_blob = true;
    state.blob_reconnect_attempt = 2;
    let effects = PeerStateMachine::connected_blob(&mut state, false);
    assert!(effects.close_new_blob);
    assert!(!state.dialing_blob);
    assert_eq!(state.blob_reconnect_attempt, 0);

    state.closing = true;
    state.dialing_blob = true;
    let effects = PeerStateMachine::connected_blob(&mut state, true);
    assert!(effects.close_new_blob);
    assert!(state.dialing_blob);
}

#[test]
fn peer_state_machine_blob_dial_branches_are_deterministic() {
    let mut state = empty_peer_state();

    let effects = PeerStateMachine::ensure_blob_dial(&mut state, false, false, true, true);
    assert_eq!(effects, PeerStateEffects::default());
    assert!(!state.dialing_blob);

    let effects = PeerStateMachine::ensure_blob_dial(&mut state, true, true, true, true);
    assert_eq!(effects, PeerStateEffects::default());
    assert!(!state.dialing_blob);

    let effects = PeerStateMachine::ensure_blob_dial(&mut state, true, false, false, true);
    assert_eq!(effects, PeerStateEffects::default());
    assert!(!state.dialing_blob);

    let effects = PeerStateMachine::ensure_blob_dial(&mut state, true, false, true, false);
    assert_eq!(effects, PeerStateEffects::default());
    assert!(!state.dialing_blob);

    let effects = PeerStateMachine::ensure_blob_dial(&mut state, true, false, true, true);
    assert!(effects.spawn_blob_dial);
    assert!(state.dialing_blob);

    let effects = PeerStateMachine::ensure_blob_dial(&mut state, true, false, true, true);
    assert_eq!(effects, PeerStateEffects::default());
    assert!(state.dialing_blob);
}

#[test]
fn peer_state_machine_blob_failure_and_removal_clear_dial_state() {
    let mut state = empty_peer_state();
    state.dialing_blob = true;

    let effects = PeerStateMachine::dial_failed_blob(&mut state, false, false, true, true);
    assert_eq!(effects, PeerStateEffects::default());
    assert!(!state.dialing_blob);

    state.dialing_blob = true;
    let effects = PeerStateMachine::dial_failed_blob(&mut state, true, false, true, true);
    assert!(effects.spawn_blob_dial);
    assert!(state.dialing_blob);

    state.dialing_blob = true;
    let effects = PeerStateMachine::blob_removed(&mut state, true, true, true, true);
    assert_eq!(effects, PeerStateEffects::default());
    assert!(!state.dialing_blob);

    state.dialing_blob = true;
    let effects = PeerStateMachine::blob_removed(&mut state, true, false, true, false);
    assert_eq!(effects, PeerStateEffects::default());
    assert!(!state.dialing_blob);

    state.dialing_blob = true;
    let effects = PeerStateMachine::blob_removed(&mut state, true, false, true, true);
    assert!(effects.spawn_blob_dial);
    assert!(state.dialing_blob);
}

#[test]
fn peer_state_machine_graceful_close_effects_are_explicit() {
    let mut state = empty_peer_state();
    state.primary_dial = PrimaryDialState::Dialing;
    state.dialing_blob = true;

    let effects = PeerStateMachine::graceful_close(&mut state);

    assert!(state.closing);
    assert_eq!(state.primary_dial, PrimaryDialState::Idle);
    assert!(!state.dialing_blob);
    assert!(effects.enqueue_primary_close);
    assert!(effects.fail_tracked);
    assert!(effects.fail_blob_streams);

    let effects = PeerStateMachine::graceful_close(&mut state);
    assert_eq!(effects, PeerStateEffects::default());
}

#[tokio::test]
async fn replacing_closed_peer_handle_does_not_stop_domus_listener() {
    // Regression: when peer_handle_for replaces a session-closing handle, the
    // outgoing handle is dropped. PeerHandle::Drop must not signal the
    // transport-wide listener_shutdown_tx — doing so refuses subsequent
    // inbound dials, including the peer's TCP callback, and breaks recovery
    // after a peer initiates a graceful close (see scenario_graceful_close).
    let backend = Arc::new(ListenerBackend::new());
    let registry = Arc::new(TabernaRegistry::new());
    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
    let local_addr = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0));
    let transport = Transport::bind_with_backend(
        local_addr,
        registry,
        config,
        crate::observability::new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        Arc::clone(&backend),
    )
    .await
    .expect("bind transport");

    let mut listener_shutdown_rx = transport.inner.listener_shutdown_tx.subscribe();
    let listener_handle = transport.start().await.expect("start listener");
    assert!(!listener_handle.is_finished());

    let peer_addr = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8003));

    // Mark the original handle's session as closing — emulates having received
    // MSG_CLOSE during a peer-initiated graceful close.
    let original = transport
        .inner
        .peer_handle(peer_addr.clone())
        .await
        .expect("original peer handle");
    original.session.begin_close();
    let original_weak = Arc::downgrade(&original);
    drop(original);

    // Re-acquire — peer_handle_for must remove the closing handle from the
    // map and create a fresh one. After this, the original Arc has no
    // strong references and PeerHandle::Drop runs.
    let replacement = transport
        .inner
        .peer_handle(peer_addr.clone())
        .await
        .expect("replacement peer handle");
    assert!(
        original_weak.upgrade().is_none(),
        "old peer handle should have been dropped after replacement"
    );

    // Give the dropped handle's tasks time to wind down so any spurious
    // listener_shutdown signal would land before we assert.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        timeout(Duration::from_millis(50), listener_shutdown_rx.changed())
            .await
            .is_err(),
        "dropping a peer handle must not signal listener shutdown"
    );
    assert!(
        !*listener_shutdown_rx.borrow(),
        "listener_shutdown_tx must remain unset after a peer handle is replaced"
    );
    assert!(
        !listener_handle.is_finished(),
        "listener must keep running after a peer handle is replaced"
    );

    backend
        .accept_tx
        .send(test_authenticated_stream(peer_addr))
        .await
        .expect("listener accepts after replacement");
    timeout(Duration::from_secs(1), async {
        loop {
            if backend.accepted.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("listener did not accept after peer handle replacement");

    drop(replacement);
    transport.shutdown().await;
    listener_handle.abort();
}

#[tokio::test]
async fn outbound_peer_handle_rejects_after_transport_shutdown_signal() {
    tokio::time::timeout(PEER_STATE_TEST_TIMEOUT, async {
        let backend = Arc::new(ListenerBackend::new());
        let registry = Arc::new(TabernaRegistry::new());
        let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
        let local_addr = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0));
        let transport = Transport::bind_with_backend(
            local_addr,
            registry,
            config,
            crate::observability::new_observability(tokio::runtime::Handle::current()).1,
            tokio::runtime::Handle::current(),
            Arc::clone(&backend),
        )
        .await
        .expect("bind transport");

        let peer_addr = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8004));
        transport.inner.shutdown_tx.send_replace(true);

        let err = match transport.inner.peer_handle(peer_addr.clone()).await {
            Ok(_) => panic!("outbound peer admission must reject after transport shutdown"),
            Err(err) => err,
        };
        assert_eq!(err.kind, ErrorId::PeerUnavailable);

        let guard = transport.inner.peers.lock().await;
        assert!(
            !guard.contains_key(&peer_addr),
            "shutdown-rejected outbound admission must not create a peer handle"
        );
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn transport_shutdown_waits_for_peer_callis_drain_concurrently() {
    let backend = Arc::new(ListenerBackend::new());
    let registry = Arc::new(TabernaRegistry::new());
    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig {
        send_timeout: Duration::from_millis(120),
        ..Default::default()
    });
    let local_addr = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0));
    let transport = Transport::bind_with_backend(
        local_addr,
        registry,
        config,
        crate::observability::new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        Arc::clone(&backend),
    )
    .await
    .expect("bind transport");

    for port in 8201..=8203 {
        let peer_addr = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port));
        let handle = transport
            .inner
            .peer_handle(peer_addr)
            .await
            .expect("peer handle");
        handle.open_callis_for_tests();
    }

    timeout(Duration::from_millis(500), transport.shutdown())
        .await
        .expect("shutdown drain waits should run concurrently across peers");
}

#[tokio::test]
async fn peer_graceful_close_does_not_stop_listener() {
    let backend = Arc::new(ListenerBackend::new());
    let registry = Arc::new(TabernaRegistry::new());
    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
    let local_addr = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0));
    let transport = Transport::bind_with_backend(
        local_addr,
        registry,
        config,
        crate::observability::new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        Arc::clone(&backend),
    )
    .await
    .expect("bind transport");

    let listener_handle = transport.start().await.expect("start listener");
    assert!(!listener_handle.is_finished());

    let peer_addr = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8001));
    let handle = transport
        .inner
        .peer_handle(peer_addr.clone())
        .await
        .expect("peer handle");

    handle.graceful_close().await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!listener_handle.is_finished());

    let send_result = backend
        .accept_tx
        .send(test_authenticated_stream(peer_addr))
        .await;
    assert!(send_result.is_ok());
    timeout(Duration::from_secs(1), async {
        loop {
            if backend.accepted.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("listener did not accept after peer close");

    transport.shutdown().await;
    listener_handle.abort();
}

#[tokio::test]
async fn primary_connected_observability_follows_snapshot_publication() {
    let peer_addr = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8210));
    let (handle, reporting, snapshot_rx) =
        test_peer_with_reporting(Some(peer_addr.clone()), DomusConfig::default());
    let mut events = reporting.subscribe_events();
    let (callis_shutdown_tx, _callis_shutdown_rx) = watch::channel(false);
    let callis_id = next_callis_id();
    handle
        .peer_state_tx
        .send(PeerStateUpdate::Connected {
            callis: CallisKind::Primary,
            info: ConnectionInfo {
                handle: CallisHandle {
                    id: callis_id,
                    tx: CallisTx::Primary,
                    shutdown: callis_shutdown_tx,
                },
                replay: Vec::new(),
                fresh_session: false,
                blob_settings: None,
                blob_resume: false,
            },
        })
        .await
        .expect("connect primary");

    timeout(Duration::from_secs(1), async {
        loop {
            let event = events.recv().await.expect("event");
            if matches!(
                event,
                DomusReportingEvent::PrimaryCallisConnectedEvent { callis_id: id, .. }
                    if id == callis_id
            ) {
                assert!(
                    snapshot_rx
                        .borrow()
                        .primary_handles
                        .iter()
                        .any(|handle| handle.id == callis_id),
                    "primary snapshot must be published before connected observability"
                );
                break;
            }
        }
    })
    .await
    .expect("primary connected event timeout");

    handle.shutdown().await;
}

#[tokio::test]
async fn fresh_primary_restart_observability_follows_stale_blob_cleanup() {
    let peer_addr = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8211));
    let config = DomusConfigBuilder::new()
        .send_timeout(Duration::from_millis(100))
        .callis_connect_timeout(Duration::from_millis(100))
        .accept_timeout(Duration::from_millis(100))
        .build()
        .expect("config");
    let (handle, reporting, mut snapshot_rx) =
        test_peer_with_reporting(Some(peer_addr.clone()), config);
    let mut events = reporting.subscribe_events();
    let (old_shutdown_tx, _old_shutdown_rx) = watch::channel(false);
    let old_primary_id = next_callis_id();
    handle
        .peer_state_tx
        .send(PeerStateUpdate::Connected {
            callis: CallisKind::Primary,
            info: ConnectionInfo {
                handle: CallisHandle {
                    id: old_primary_id,
                    tx: CallisTx::Primary,
                    shutdown: old_shutdown_tx,
                },
                replay: Vec::new(),
                fresh_session: false,
                blob_settings: None,
                blob_resume: false,
            },
        })
        .await
        .expect("connect old primary");

    timeout(Duration::from_secs(1), async {
        loop {
            if snapshot_rx
                .borrow()
                .primary_handles
                .iter()
                .any(|handle| handle.id == old_primary_id)
            {
                break;
            }
            snapshot_rx.changed().await.expect("snapshot changed");
        }
    })
    .await
    .expect("old primary snapshot timeout");

    while let Ok(Ok(_)) = timeout(Duration::from_millis(10), events.recv()).await {}

    let blob = handle.blob_for_tests();
    let settings = BlobCallisSettings {
        chunk_size: 8,
        ack_window_chunks: 4,
    };
    let (blob_shutdown_tx, _blob_shutdown_rx) = watch::channel(false);
    let blob_callis_id = next_callis_id();
    blob.add_callis(
        CallisHandle {
            id: blob_callis_id,
            tx: CallisTx::Blob,
            shutdown: blob_shutdown_tx,
        },
        settings,
        true,
    )
    .await;
    assert!(blob.reserve_outbound(920, 8, 1024).await);
    let (new_shutdown_tx, _new_shutdown_rx) = watch::channel(false);
    let new_primary_id = next_callis_id();
    handle
        .peer_state_tx
        .send(PeerStateUpdate::Connected {
            callis: CallisKind::Primary,
            info: ConnectionInfo {
                handle: CallisHandle {
                    id: new_primary_id,
                    tx: CallisTx::Primary,
                    shutdown: new_shutdown_tx,
                },
                replay: Vec::new(),
                fresh_session: true,
                blob_settings: None,
                blob_resume: false,
            },
        })
        .await
        .expect("connect fresh primary");

    timeout(Duration::from_secs(1), async {
        loop {
            let event = events.recv().await.expect("event");
            if matches!(
                event,
                DomusReportingEvent::PeerSessionRestartedEvent { peer, .. }
                    if peer == peer_addr
            ) {
                assert!(
                    snapshot_rx
                        .borrow()
                        .primary_handles
                        .iter()
                        .any(|handle| handle.id == new_primary_id),
                    "fresh primary snapshot must be visible before restart observability"
                );
                let snapshot = blob.lifecycle_snapshot().await;
                assert!(!snapshot.has_callis);
                assert!(!snapshot.has_active_streams);
                break;
            }
        }
    })
    .await
    .expect("fresh restart event timeout");

    handle.shutdown().await;
}

#[tokio::test]
async fn remote_peer_close_does_not_stop_domus_listener() {
    let backend = Arc::new(TestBackend);
    let registry = Arc::new(TabernaRegistry::new());
    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
    let limited_registry = config.limited_registry();
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let (listener_shutdown_tx, mut listener_shutdown_rx) = watch::channel(false);

    let handle = Arc::new(PeerHandle::new(
        None,
        Arc::clone(&registry),
        config,
        Arc::new(BlobBufferTracker::default()),
        Arc::clone(&backend),
        HandshakeGate::new(limited_registry),
        crate::observability::new_observability(tokio::runtime::Handle::current()).1,
        shutdown_rx,
        listener_shutdown_tx,
        tokio::runtime::Handle::current(),
    ));
    let (callis_shutdown_tx, _callis_shutdown_rx) = watch::channel(false);
    let callis_id = next_callis_id();
    handle
        .peer_state_tx
        .send(PeerStateUpdate::Connected {
            callis: CallisKind::Primary,
            info: ConnectionInfo {
                handle: CallisHandle {
                    id: callis_id,
                    tx: CallisTx::Primary,
                    shutdown: callis_shutdown_tx,
                },
                replay: Vec::new(),
                fresh_session: false,
                blob_settings: None,
                blob_resume: false,
            },
        })
        .await
        .expect("connect primary");

    handle
        .peer_state_tx
        .send(PeerStateUpdate::ConnectionClosed {
            callis: CallisKind::Primary,
            id: callis_id,
            reason: CancelReason::RemoteClose,
        })
        .await
        .expect("send connection closed");

    assert!(
        timeout(Duration::from_millis(50), listener_shutdown_rx.changed())
            .await
            .is_err(),
        "remote peer close must not stop the domus listener"
    );
    assert!(!*listener_shutdown_rx.borrow());

    handle.shutdown().await;
}

#[tokio::test]
async fn inbound_stream_dropped_during_shutdown_before_hello() {
    use tokio::io::AsyncReadExt;

    let backend = Arc::new(ListenerBackend::new());
    let registry = Arc::new(TabernaRegistry::new());
    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
    let local_addr = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0));
    let transport = Transport::bind_with_backend(
        local_addr,
        registry,
        config,
        crate::observability::new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        Arc::clone(&backend),
    )
    .await
    .expect("bind transport");

    let listener_handle = transport.start().await.expect("start listener");

    let peer_addr = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8101));
    let (stream, mut peer_end) = tokio::io::duplex(64);
    backend
        .accept_tx
        .send(AuthenticatedStream { stream, peer_addr })
        .await
        .expect("send stream");

    while backend.accepted.load(Ordering::SeqCst) < 1 {
        tokio::task::yield_now().await;
    }

    transport.shutdown().await;

    let mut buf = [0u8; 16];
    let n = timeout(Duration::from_secs(1), peer_end.read(&mut buf))
        .await
        .expect("read timeout")
        .expect("read");
    assert_eq!(
        n, 0,
        "inbound stream should be closed before hello is processed"
    );

    let _ = listener_handle.await;
}

#[tokio::test]
async fn remote_close_keeps_listener_accepting() {
    let backend = Arc::new(ListenerBackend::new());
    let registry = Arc::new(TabernaRegistry::new());
    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
    let local_addr = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0));
    let transport = Transport::bind_with_backend(
        local_addr,
        registry,
        config,
        crate::observability::new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        Arc::clone(&backend),
    )
    .await
    .expect("bind transport");

    let listener_handle = transport.start().await.expect("start listener");
    assert!(!listener_handle.is_finished());

    let peer_addr = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8002));
    let handle = transport
        .inner
        .peer_handle(peer_addr.clone())
        .await
        .expect("peer handle");

    handle
        .peer_state_tx
        .send(PeerStateUpdate::ConnectionClosed {
            callis: CallisKind::Primary,
            id: next_callis_id(),
            reason: CancelReason::RemoteClose,
        })
        .await
        .expect("send remote close");

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!listener_handle.is_finished());

    backend
        .accept_tx
        .send(test_authenticated_stream(peer_addr))
        .await
        .expect("send inbound after remote close");
    timeout(Duration::from_secs(1), async {
        loop {
            if backend.accepted.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("listener did not accept after remote close");

    transport.shutdown().await;
    listener_handle.abort();
}

#[tokio::test]
async fn teardown_after_send_timeout_marks_session_closing() {
    let backend = Arc::new(TestBackend);
    let registry = Arc::new(TabernaRegistry::new());
    let config = DomusConfigBuilder::new()
        .send_timeout(Duration::from_millis(50))
        .callis_connect_timeout(Duration::from_millis(50))
        .accept_timeout(Duration::from_millis(50))
        .build()
        .expect("config");
    let config: DomusConfigAccess = DomusConfigAccess::from_config(config);
    let limited_registry = config.limited_registry();
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let (listener_shutdown_tx, _listener_shutdown_rx) = watch::channel(false);

    let handle = Arc::new(PeerHandle::new(
        None,
        Arc::clone(&registry),
        config,
        Arc::new(BlobBufferTracker::default()),
        Arc::clone(&backend),
        HandshakeGate::new(limited_registry),
        crate::observability::new_observability(tokio::runtime::Handle::current()).1,
        shutdown_rx,
        listener_shutdown_tx,
        tokio::runtime::Handle::current(),
    ));
    let (callis_shutdown_tx, _callis_shutdown_rx) = watch::channel(false);
    let callis_id = next_callis_id();
    handle
        .peer_state_tx
        .send(PeerStateUpdate::Connected {
            callis: CallisKind::Primary,
            info: ConnectionInfo {
                handle: CallisHandle {
                    id: callis_id,
                    tx: CallisTx::Primary,
                    shutdown: callis_shutdown_tx,
                },
                replay: Vec::new(),
                fresh_session: false,
                blob_settings: None,
                blob_resume: false,
            },
        })
        .await
        .expect("connect callis");

    handle
        .peer_state_tx
        .send(PeerStateUpdate::ConnectionClosed {
            callis: CallisKind::Primary,
            id: callis_id,
            reason: CancelReason::ConnectionLost,
        })
        .await
        .expect("close callis");

    timeout(Duration::from_secs(1), async {
        loop {
            if handle.session.is_closing() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("teardown timeout");

    let err = timeout(
        Duration::from_secs(1),
        handle.send(1, crate::a3_message_type(0), Bytes::from_static(b"after")),
    )
    .await
    .expect("send timeout")
    .expect_err("expected peer unavailable");
    assert_eq!(err.kind, ErrorId::PeerUnavailable);
}

#[tokio::test]
async fn teardown_cancels_inbound_waiters_without_outcomes() {
    tokio::time::timeout(PEER_STATE_TEST_TIMEOUT, async {
        let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
        let allocator = Arc::new(PeerMessageIdAllocator::default());
        let session = Arc::new(PeerSession::new(
            Arc::clone(&allocator),
            config.clone(),
            tokio::runtime::Handle::current(),
            PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
        ));
        let blob = Arc::new(BlobManager::new(
            Arc::new(BlobBufferTracker::default()),
            Arc::new(Notify::new()),
            Arc::clone(&allocator),
            128,
        ));

        let (msg_tx, msg_rx) = oneshot::channel();
        let _ = msg_tx.send(Ok(()));

        let receiver_state = Arc::new(crate::transport::blob::BlobReceiverState {
            notify: Arc::new(Notify::new()),
            accepted: AtomicBool::new(false),
            completed: AtomicBool::new(false),
            error: Mutex::new(None),
            completion_ttl: Duration::from_secs(1),
            idle_timeout: Duration::from_secs(1),
        });
        let (blob_tx, blob_rx) = oneshot::channel();
        let _ = blob_tx.send(Ok(()));
        let (peer_state_tx, _peer_state_rx) = mpsc::channel(1);
        let pending_accept = super::super::BlobAcceptPending {
            dst_taberna: 2,
            accept_rx: blob_rx,
            receiver_state: Arc::clone(&receiver_state),
            send_timeout: Duration::from_secs(1),
            peer_state_tx: peer_state_tx.clone(),
        };

        let receiver_state_callis = Arc::new(crate::transport::blob::BlobReceiverState {
            notify: Arc::new(Notify::new()),
            accepted: AtomicBool::new(false),
            completed: AtomicBool::new(false),
            error: Mutex::new(None),
            completion_ttl: Duration::from_secs(1),
            idle_timeout: Duration::from_secs(1),
        });
        let (blob_callis_tx, blob_callis_rx) = oneshot::channel();
        let _ = blob_callis_tx.send(Ok(()));
        let pending_callis = super::super::BlobAcceptPending {
            dst_taberna: 3,
            accept_rx: blob_callis_rx,
            receiver_state: Arc::clone(&receiver_state_callis),
            send_timeout: Duration::from_secs(1),
            peer_state_tx,
        };

        let mut waiters = HashMap::new();
        waiters.insert(
            1,
            InboundWaiter::Message(MessageWaiter {
                dst_taberna: 1,
                accept_rx: msg_rx,
            }),
        );
        waiters.insert(
            2,
            InboundWaiter::BlobAccept(BlobAcceptWaiter {
                pending: pending_accept,
            }),
        );
        waiters.insert(
            3,
            InboundWaiter::BlobCallis(BlobCallisWaiter {
                pending: pending_callis,
                deadline: Instant::now() + Duration::from_secs(1),
            }),
        );

        cancel_waiters(&mut waiters, &session, &blob, CancelReason::LocalShutdown).await;
        assert!(waiters.is_empty());

        let outcomes = drain_accept_waiters(&mut waiters, &session, &blob).await;
        assert!(outcomes.is_empty());
        let outcomes = drain_blob_callis_waiters(&mut waiters, &session, &blob).await;
        assert!(outcomes.is_empty());
    })
    .await
    .expect("async test timed out");
}
