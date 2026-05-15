// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::observability::{HandshakePhase, ObservabilityHandle};
use std::future::Future;

fn callis_label(callis: CallisKind) -> &'static str {
    match callis {
        CallisKind::Primary => "primary",
        CallisKind::Blob => "blob",
    }
}

fn decode_wire_flags(context: &str, raw: u16) -> Result<WireFlags, AureliaError> {
    WireFlags::from_bits(raw).ok_or_else(|| {
        AureliaError::with_message(
            ErrorId::ProtocolViolation,
            format!("invalid {context} flags: 0x{raw:04x}"),
        )
    })
}

async fn read_hello_response<S>(
    stream: &mut S,
    deadline: Instant,
    max_payload_len: usize,
) -> Result<(WireFlags, HelloPayload), AureliaError>
where
    S: AsyncReadExt + Unpin,
{
    let response = tokio::time::timeout_at(deadline, read_frame(stream, max_payload_len))
        .await
        .map_err(|_| callis_connect_timeout_error())??
        .ok_or_else(|| {
            AureliaError::with_message(ErrorId::ConnectionLost, "missing hello-response")
        })?;
    let (header, payload) = response;
    if header.msg_type != MSG_HELLO_RESPONSE {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let flags = decode_wire_flags("hello-response", header.flags)?;
    let hello = HelloPayload::from_bytes(&payload)?;
    Ok((flags, hello))
}

async fn send_control_frame_until<S>(
    stream: &mut S,
    msg_type: MessageType,
    flags: u16,
    peer_msg_id: PeerMessageId,
    payload: &[u8],
    deadline: Instant,
) -> Result<(), AureliaError>
where
    S: AsyncWrite + Unpin,
{
    tokio::time::timeout_at(
        deadline,
        send_control_frame(stream, msg_type, flags, peer_msg_id, payload),
    )
    .await
    .map_err(|_| callis_connect_timeout_error())?
}

fn callis_connect_timeout_error() -> AureliaError {
    AureliaError::with_message(ErrorId::PeerUnavailable, "callis connect timeout")
}

async fn send_hello_response<S>(
    stream: &mut S,
    flags: WireFlags,
    peer_msg_id: PeerMessageId,
    response: &HelloPayload,
    deadline: Instant,
) -> Result<(), AureliaError>
where
    S: AsyncWrite + Unpin,
{
    send_control_frame_until(
        stream,
        MSG_HELLO_RESPONSE,
        flags.bits(),
        peer_msg_id,
        response.to_bytes().as_slice(),
        deadline,
    )
    .await
}

fn blob_hello_values(hello: &HelloPayload) -> Result<(u32, u32), AureliaError> {
    let HelloPayload::Blob {
        chunk_size: chunk,
        ack_window_chunks: window,
    } = *hello
    else {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    };
    if chunk == 0 || window == 0 {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    Ok((chunk, window))
}

fn basic_connection_info(
    handle: CallisHandle,
    blob_settings: Option<BlobCallisSettings>,
    blob_resume: bool,
) -> ConnectionInfo {
    ConnectionInfo {
        handle,
        replay: Vec::new(),
        fresh_session: false,
        blob_settings,
        blob_resume,
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_dial_task_scaffold<B, F, Fut, L>(
    dial_addr: DomusAddr,
    backend: Arc<B>,
    delay: Duration,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    callis: CallisKind,
    handshake_phase: HandshakePhase,
    config: DomusConfigAccess,
    observability: ObservabilityHandle,
    task_spawner: PeerTaskSpawner,
    establish: F,
    on_connected: L,
) where
    B: TransportBackend<Addr = DomusAddr> + 'static,
    F: FnOnce(B::Stream, Instant) -> Fut + Send + 'static,
    Fut: Future<Output = Result<ConnectionInfo, AureliaError>> + Send + 'static,
    L: FnOnce(&DomusAddr, &ConnectionInfo) + Send + 'static,
{
    let label = callis_label(callis);
    task_spawner.spawn(async move {
        if !delay.is_zero() {
            sleep(delay).await;
        }
        observability.dial_attempt();
        debug!(
            peer = %dial_addr,
            delay_ms = delay.as_millis(),
            "dialing {} callis",
            label
        );
        let connect_timeout = config.snapshot().await.callis_connect_timeout;
        let deadline = Instant::now() + connect_timeout;
        let dial_result = match tokio::time::timeout_at(deadline, backend.dial(&dial_addr)).await {
            Err(_) => Err(callis_connect_timeout_error()),
            Ok(result) => result,
        };
        let authenticated = match dial_result {
            Ok(value) => value,
            Err(err) => {
                observability.backend_failure(dial_addr.clone(), err.kind);
                observability.dial_failed(dial_addr.clone(), callis, err.kind);
                warn!(peer = %dial_addr, error = %err, "{} dial failed", label);
                let _ = peer_state_tx
                    .send(PeerStateUpdate::DialFailed(callis))
                    .await;
                return;
            }
        };
        let peer_identity = authenticated.peer_addr.clone();
        if let Err(err) = validate_backend_identity(&dial_addr, &peer_identity) {
            warn!(
                peer = %dial_addr,
                authenticated = %peer_identity,
                error = %err,
                "{} dial identity mismatch",
                label
            );
            observability.identity_mismatch(
                dial_addr.clone(),
                dial_addr.clone(),
                peer_identity.clone(),
            );
            observability.dial_failed(dial_addr.clone(), callis, err.kind);
            let _ = peer_state_tx
                .send(PeerStateUpdate::DialFailed(callis))
                .await;
            return;
        }
        match establish(authenticated.stream, deadline).await {
            Ok(info) => {
                on_connected(&dial_addr, &info);
                let _ = peer_state_tx
                    .send(PeerStateUpdate::Connected { callis, info })
                    .await;
            }
            Err(err) => {
                observability.dial_failed(dial_addr.clone(), callis, err.kind);
                if err.kind == ErrorId::ProtocolViolation {
                    observability.protocol_violation(dial_addr.clone(), err.kind);
                } else if err.kind == ErrorId::SendTimeout {
                    observability.handshake_timeout(dial_addr.clone(), handshake_phase);
                }
                warn!(peer = %dial_addr, error = %err, "{} handshake failed", label);
                let _ = peer_state_tx
                    .send(PeerStateUpdate::DialFailed(callis))
                    .await;
            }
        }
    });
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_dial_task<B>(
    dial_addr: DomusAddr,
    backend: Arc<B>,
    config: DomusConfigAccess,
    session: Arc<PeerSession>,
    blob: Arc<BlobManager>,
    registry: Arc<TabernaRegistry>,
    delay: Duration,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    primary_active: Arc<AtomicBool>,
    primary_dispatch: Arc<PrimaryDispatchManager>,
    callis_tracker: CallisTracker,
    observability: ObservabilityHandle,
    task_spawner: PeerTaskSpawner,
) where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    let peer_state_tx_establish = peer_state_tx.clone();
    let task_spawner_establish = task_spawner.clone();
    let peer_establish = dial_addr.clone();
    let observability_establish = observability.clone();
    spawn_dial_task_scaffold::<B, _, _, _>(
        dial_addr,
        backend,
        delay,
        peer_state_tx,
        CallisKind::Primary,
        HandshakePhase::OutboundPrimary,
        config.clone(),
        observability,
        task_spawner,
        move |stream, deadline| {
            establish_outbound_primary_with_observability(
                Some(peer_establish),
                config,
                session,
                blob,
                registry,
                stream,
                peer_state_tx_establish,
                primary_active,
                primary_dispatch,
                callis_tracker,
                observability_establish,
                task_spawner_establish,
                deadline,
            )
        },
        |peer, info| {
            info!(
                peer = %peer,
                callis_id = info.handle.id,
                fresh_session = info.fresh_session,
                "primary callis connected"
            );
        },
    );
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_blob_dial_task<B>(
    dial_addr: DomusAddr,
    backend: Arc<B>,
    config: DomusConfigAccess,
    session: Arc<PeerSession>,
    registry: Arc<TabernaRegistry>,
    blob: Arc<BlobManager>,
    delay: Duration,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    callis_tracker: CallisTracker,
    observability: ObservabilityHandle,
    task_spawner: PeerTaskSpawner,
) where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    let peer_state_tx_establish = peer_state_tx.clone();
    let task_spawner_establish = task_spawner.clone();
    let peer_establish = dial_addr.clone();
    let observability_establish = observability.clone();
    spawn_dial_task_scaffold::<B, _, _, _>(
        dial_addr,
        backend,
        delay,
        peer_state_tx,
        CallisKind::Blob,
        HandshakePhase::OutboundBlob,
        config.clone(),
        observability,
        task_spawner,
        move |stream, deadline| {
            establish_outbound_blob_with_observability(
                Some(peer_establish),
                config,
                session,
                registry,
                blob,
                stream,
                peer_state_tx_establish,
                callis_tracker,
                observability_establish,
                task_spawner_establish,
                deadline,
            )
        },
        |peer, info| {
            if let Some(settings) = info.blob_settings {
                info!(
                    peer = %peer,
                    callis_id = info.handle.id,
                    chunk_size = settings.chunk_size,
                    ack_window_chunks = settings.ack_window_chunks,
                    resume = info.blob_resume,
                    "blob callis connected"
                );
            } else {
                info!(peer = %peer, callis_id = info.handle.id, "blob callis connected");
            }
        },
    );
}

pub(super) fn validate_backend_identity(
    expected: &DomusAddr,
    authenticated: &DomusAddr,
) -> Result<(), AureliaError> {
    if expected == authenticated {
        Ok(())
    } else {
        Err(AureliaError::with_message(
            ErrorId::ProtocolViolation,
            format!(
                "peer identity mismatch: expected={} authenticated={}",
                expected, authenticated
            ),
        ))
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_callis_handle<S>(
    config: DomusConfigAccess,
    session: Arc<PeerSession>,
    blob: Arc<BlobManager>,
    registry: Arc<TabernaRegistry>,
    stream: S,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    callis: CallisKind,
    primary_dispatch: Option<Arc<PrimaryDispatchManager>>,
    availability_notify: Option<Arc<Notify>>,
    callis_tracker: CallisTracker,
    peer: Option<DomusAddr>,
    observability: ObservabilityHandle,
    task_spawner: PeerTaskSpawner,
) -> CallisHandle
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let callis_tx = match callis {
        CallisKind::Primary => CallisTx::Primary,
        CallisKind::Blob => CallisTx::Blob,
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let callis_id = next_callis_id();
    spawn_callis_task(
        config,
        session,
        blob,
        registry,
        stream,
        callis_id,
        primary_dispatch,
        shutdown_rx,
        peer_state_tx,
        callis,
        availability_notify,
        callis_tracker,
        peer,
        observability,
        task_spawner,
    );
    CallisHandle {
        id: callis_id,
        tx: callis_tx,
        shutdown: shutdown_tx,
    }
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(super) async fn establish_outbound_primary<S>(
    config: DomusConfigAccess,
    session: Arc<PeerSession>,
    blob: Arc<BlobManager>,
    registry: Arc<TabernaRegistry>,
    stream: S,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    primary_active: Arc<AtomicBool>,
    primary_dispatch: Arc<PrimaryDispatchManager>,
    callis_tracker: CallisTracker,
    task_spawner: PeerTaskSpawner,
    deadline: Instant,
) -> Result<ConnectionInfo, AureliaError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    establish_outbound_primary_with_observability(
        None,
        config,
        session,
        blob,
        registry,
        stream,
        peer_state_tx,
        primary_active,
        primary_dispatch,
        callis_tracker,
        ObservabilityHandle::noop(),
        task_spawner,
        deadline,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn establish_outbound_primary_with_observability<S>(
    peer: Option<DomusAddr>,
    config: DomusConfigAccess,
    session: Arc<PeerSession>,
    blob: Arc<BlobManager>,
    registry: Arc<TabernaRegistry>,
    mut stream: S,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    primary_active: Arc<AtomicBool>,
    primary_dispatch: Arc<PrimaryDispatchManager>,
    callis_tracker: CallisTracker,
    observability: ObservabilityHandle,
    task_spawner: PeerTaskSpawner,
    deadline: Instant,
) -> Result<ConnectionInfo, AureliaError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let reconnect = session.is_active();
    let hello = HelloPayload::Primary;
    let hello_flags = hello_header_flags(reconnect, false);
    send_control_frame_until(
        &mut stream,
        MSG_HELLO,
        hello_flags.bits(),
        0,
        hello.to_bytes().as_slice(),
        deadline,
    )
    .await?;
    let cfg = config.snapshot().await;
    let (response_flags, response_payload) =
        read_hello_response(&mut stream, deadline, cfg.max_payload_len).await?;
    if response_flags.contains(WireFlags::BLOB) {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    if response_payload != HelloPayload::Primary {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let mut replay = Vec::new();
    let mut fresh_session = false;
    if reconnect && response_flags.contains(WireFlags::RECONNECT) {
        replay = session.handle_hello_response(true).await;
    } else if reconnect && primary_active.load(Ordering::SeqCst) {
        // Simultaneous bidirectional dial can create an inbound primary before this
        // outbound dial receives its hello response. A missing RECONNECT echo in that
        // case is a duplicate primary, not evidence of peer restart.
    } else if reconnect && !response_flags.contains(WireFlags::RECONNECT) {
        // Reconnect disagreement: receiver no longer holds our session. Treat as a fresh
        // session on the same peer handle. Retained inflight will be invalidated by the
        // Connected arm in peer.rs.
        let _ = session.handle_hello_response(false).await;
        fresh_session = true;
    } else if !reconnect && response_flags.contains(WireFlags::RECONNECT) {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    session.set_active(true);
    let handle = spawn_callis_handle(
        config,
        session,
        blob,
        registry,
        stream,
        peer_state_tx,
        CallisKind::Primary,
        Some(primary_dispatch),
        None,
        callis_tracker,
        peer,
        observability,
        task_spawner,
    );
    Ok(ConnectionInfo {
        handle,
        replay,
        fresh_session,
        blob_settings: None,
        blob_resume: false,
    })
}

pub(super) fn validate_blob_hello_request(
    hello: &HelloPayload,
) -> Result<(u32, u32), AureliaError> {
    blob_hello_values(hello)
}

pub(super) fn validate_blob_hello_response(
    proposed_chunk: u32,
    proposed_window: u32,
    hello: &HelloPayload,
) -> Result<BlobCallisSettings, AureliaError> {
    let (agreed_chunk, agreed_window) = blob_hello_values(hello)?;
    if agreed_chunk > proposed_chunk || agreed_window > proposed_window {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    Ok(BlobCallisSettings {
        chunk_size: agreed_chunk,
        ack_window_chunks: agreed_window,
    })
}

pub(super) fn negotiate_blob_settings(
    proposed_chunk: u32,
    proposed_window: u32,
    cfg_chunk: u32,
    cfg_window: u32,
) -> BlobCallisSettings {
    BlobCallisSettings {
        chunk_size: proposed_chunk.min(cfg_chunk),
        ack_window_chunks: proposed_window.min(cfg_window),
    }
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(super) async fn establish_outbound_blob<S>(
    config: DomusConfigAccess,
    session: Arc<PeerSession>,
    registry: Arc<TabernaRegistry>,
    blob: Arc<BlobManager>,
    stream: S,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    callis_tracker: CallisTracker,
    task_spawner: PeerTaskSpawner,
    deadline: Instant,
) -> Result<ConnectionInfo, AureliaError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    establish_outbound_blob_with_observability(
        None,
        config,
        session,
        registry,
        blob,
        stream,
        peer_state_tx,
        callis_tracker,
        ObservabilityHandle::noop(),
        task_spawner,
        deadline,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn establish_outbound_blob_with_observability<S>(
    peer: Option<DomusAddr>,
    config: DomusConfigAccess,
    session: Arc<PeerSession>,
    registry: Arc<TabernaRegistry>,
    blob: Arc<BlobManager>,
    mut stream: S,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    callis_tracker: CallisTracker,
    observability: ObservabilityHandle,
    task_spawner: PeerTaskSpawner,
    deadline: Instant,
) -> Result<ConnectionInfo, AureliaError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let reconnect = blob.had_callis();
    let cfg = config.snapshot().await;
    let proposed_chunk = cfg.blob_window.chunk_size();
    let proposed_window = cfg.blob_window.ack_window();
    let hello = HelloPayload::Blob {
        chunk_size: proposed_chunk,
        ack_window_chunks: proposed_window,
    };
    let hello_flags = hello_header_flags(reconnect, true);
    send_control_frame_until(
        &mut stream,
        MSG_HELLO,
        hello_flags.bits(),
        0,
        hello.to_bytes().as_slice(),
        deadline,
    )
    .await?;

    let (response_flags, response_payload) =
        read_hello_response(&mut stream, deadline, cfg.max_payload_len).await?;
    if !response_flags.contains(WireFlags::BLOB) {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    if !reconnect && response_flags.contains(WireFlags::RECONNECT) {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let settings =
        validate_blob_hello_response(proposed_chunk, proposed_window, &response_payload)?;
    let resume = reconnect && response_flags.contains(WireFlags::RECONNECT);

    let work_notify = blob.work_handle();
    let handle = spawn_callis_handle(
        config,
        session,
        Arc::clone(&blob),
        registry,
        stream,
        peer_state_tx,
        CallisKind::Blob,
        None,
        Some(work_notify),
        callis_tracker,
        peer,
        observability,
        task_spawner,
    );

    Ok(basic_connection_info(handle, Some(settings), resume))
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(super) async fn accept_inbound<S>(
    config: DomusConfigAccess,
    session: Arc<PeerSession>,
    blob: Arc<BlobManager>,
    registry: Arc<TabernaRegistry>,
    primary_active: Arc<AtomicBool>,
    primary_dispatch: Arc<PrimaryDispatchManager>,
    stream: S,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    callis_tracker: CallisTracker,
    listener_shutdown_rx: watch::Receiver<bool>,
    task_spawner: PeerTaskSpawner,
) -> Result<(CallisKind, ConnectionInfo), AureliaError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    accept_inbound_with_observability(
        None,
        config,
        session,
        blob,
        registry,
        primary_active,
        primary_dispatch,
        stream,
        peer_state_tx,
        callis_tracker,
        listener_shutdown_rx,
        ObservabilityHandle::noop(),
        task_spawner,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn accept_inbound_with_observability<S>(
    peer: Option<DomusAddr>,
    config: DomusConfigAccess,
    session: Arc<PeerSession>,
    blob: Arc<BlobManager>,
    registry: Arc<TabernaRegistry>,
    primary_active: Arc<AtomicBool>,
    primary_dispatch: Arc<PrimaryDispatchManager>,
    mut stream: S,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    callis_tracker: CallisTracker,
    mut listener_shutdown_rx: watch::Receiver<bool>,
    observability: ObservabilityHandle,
    task_spawner: PeerTaskSpawner,
) -> Result<(CallisKind, ConnectionInfo), AureliaError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    if *listener_shutdown_rx.borrow() {
        let _ = stream.shutdown().await;
        return Err(AureliaError::new(ErrorId::DomusClosed));
    }
    let cfg = config.snapshot().await;
    let deadline = Instant::now() + cfg.callis_connect_timeout;
    let response = tokio::select! {
        biased;
        _ = listener_shutdown_rx.changed() => {
            let _ = stream.shutdown().await;
            return Err(AureliaError::new(ErrorId::DomusClosed));
        }
        res = tokio::time::timeout_at(deadline, read_frame(&mut stream, cfg.max_payload_len)) => {
            res.map_err(|_| callis_connect_timeout_error())??
                .ok_or_else(|| AureliaError::new(ErrorId::ConnectionLost))?
        }
    };
    let (header, payload) = response;
    if header.msg_type != MSG_HELLO {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let flags = decode_wire_flags("hello", header.flags)?;
    let hello = HelloPayload::from_bytes(&payload)?;
    let peer_msg_id = header.peer_msg_id;
    let max_parallel = cfg.max_parallel_callis_per_peer.max(1);
    if callis_tracker.count() >= max_parallel {
        warn!(
            log_id = aurelia_ids::LogId::CallisPerPeerLimit.as_u32(),
            active_callis = callis_tracker.count(),
            max_parallel,
            "rejecting inbound callis due to per-peer active callis limit"
        );
        let payload =
            ErrorPayload::new(ErrorId::PeerUnavailable.as_u32(), "callis limit reached").to_bytes();
        let _ =
            send_control_frame(&mut stream, MSG_ERROR, 0, peer_msg_id, payload.as_slice()).await;
        return Err(AureliaError::new(ErrorId::PeerUnavailable));
    }
    if flags.contains(WireFlags::BLOB) {
        if !primary_active.load(Ordering::SeqCst) {
            let payload = ErrorPayload::new(
                ErrorId::BlobCallisWithoutPrimary.as_u32(),
                "blob callis without primary",
            )
            .to_bytes();
            let _ = send_control_frame(&mut stream, MSG_ERROR, 0, peer_msg_id, payload.as_slice())
                .await;
            warn!(
                error_id = ErrorId::BlobCallisWithoutPrimary.as_u32(),
                "rejected blob callis without primary"
            );
            return Err(AureliaError::new(ErrorId::BlobCallisWithoutPrimary));
        }
        let info = accept_inbound_blob_with_observability(
            peer,
            config,
            session,
            blob,
            registry,
            stream,
            peer_state_tx,
            hello,
            peer_msg_id,
            flags,
            callis_tracker,
            observability,
            task_spawner,
            deadline,
        )
        .await?;
        Ok((CallisKind::Blob, info))
    } else {
        let info = accept_inbound_primary_with_observability(
            peer,
            config,
            session,
            blob,
            registry,
            primary_active,
            primary_dispatch,
            stream,
            peer_state_tx,
            hello,
            peer_msg_id,
            flags,
            callis_tracker,
            observability,
            task_spawner,
            deadline,
        )
        .await?;
        Ok((CallisKind::Primary, info))
    }
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(super) async fn accept_inbound_primary<S>(
    config: DomusConfigAccess,
    session: Arc<PeerSession>,
    blob: Arc<BlobManager>,
    registry: Arc<TabernaRegistry>,
    primary_active: Arc<AtomicBool>,
    primary_dispatch: Arc<PrimaryDispatchManager>,
    stream: S,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    hello: HelloPayload,
    peer_msg_id: PeerMessageId,
    flags: WireFlags,
    callis_tracker: CallisTracker,
    task_spawner: PeerTaskSpawner,
    deadline: Instant,
) -> Result<ConnectionInfo, AureliaError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    accept_inbound_primary_with_observability(
        None,
        config,
        session,
        blob,
        registry,
        primary_active,
        primary_dispatch,
        stream,
        peer_state_tx,
        hello,
        peer_msg_id,
        flags,
        callis_tracker,
        ObservabilityHandle::noop(),
        task_spawner,
        deadline,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn accept_inbound_primary_with_observability<S>(
    peer: Option<DomusAddr>,
    config: DomusConfigAccess,
    session: Arc<PeerSession>,
    blob: Arc<BlobManager>,
    registry: Arc<TabernaRegistry>,
    primary_active: Arc<AtomicBool>,
    primary_dispatch: Arc<PrimaryDispatchManager>,
    mut stream: S,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    hello: HelloPayload,
    peer_msg_id: PeerMessageId,
    flags: WireFlags,
    callis_tracker: CallisTracker,
    observability: ObservabilityHandle,
    task_spawner: PeerTaskSpawner,
    deadline: Instant,
) -> Result<ConnectionInfo, AureliaError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    if hello != HelloPayload::Primary {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let reconnect = flags.contains(WireFlags::RECONNECT);
    let primary_is_active = primary_active.load(Ordering::SeqCst);
    let a1_resume = if reconnect || (!primary_is_active && !session.is_active()) {
        session.accept_hello(reconnect).await
    } else {
        false
    };
    let response_flags = hello_header_flags(a1_resume, false);
    let response = HelloPayload::Primary;
    send_hello_response(
        &mut stream,
        response_flags,
        peer_msg_id,
        &response,
        deadline,
    )
    .await?;
    session.set_active(true);
    let handle = spawn_callis_handle(
        config,
        session,
        blob,
        registry,
        stream,
        peer_state_tx,
        CallisKind::Primary,
        Some(primary_dispatch),
        None,
        callis_tracker,
        peer,
        observability,
        task_spawner,
    );
    Ok(basic_connection_info(handle, None, false))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn accept_inbound_blob_with_observability<S>(
    peer: Option<DomusAddr>,
    config: DomusConfigAccess,
    session: Arc<PeerSession>,
    blob: Arc<BlobManager>,
    registry: Arc<TabernaRegistry>,
    mut stream: S,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    hello: HelloPayload,
    peer_msg_id: PeerMessageId,
    flags: WireFlags,
    callis_tracker: CallisTracker,
    observability: ObservabilityHandle,
    task_spawner: PeerTaskSpawner,
    deadline: Instant,
) -> Result<ConnectionInfo, AureliaError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (proposed_chunk, proposed_window) = validate_blob_hello_request(&hello)?;

    let cfg = config.snapshot().await;
    let settings = negotiate_blob_settings(
        proposed_chunk,
        proposed_window,
        cfg.blob_window.chunk_size(),
        cfg.blob_window.ack_window(),
    );

    let reconnect = flags.contains(WireFlags::RECONNECT);
    let had_callis = blob.had_callis();
    let resume = if reconnect {
        had_callis || blob.has_active_streams().await
    } else {
        false
    };
    let response_flags = hello_header_flags(resume, true);
    let response = HelloPayload::Blob {
        chunk_size: settings.chunk_size,
        ack_window_chunks: settings.ack_window_chunks,
    };
    send_hello_response(
        &mut stream,
        response_flags,
        peer_msg_id,
        &response,
        deadline,
    )
    .await?;

    let work_notify = blob.work_handle();
    let handle = spawn_callis_handle(
        config,
        session,
        Arc::clone(&blob),
        registry,
        stream,
        peer_state_tx,
        CallisKind::Blob,
        None,
        Some(work_notify),
        callis_tracker,
        peer,
        observability,
        task_spawner,
    );

    Ok(basic_connection_info(handle, Some(settings), resume))
}

pub(super) fn hello_header_flags(reconnect: bool, blob: bool) -> WireFlags {
    let mut flags = WireFlags::empty();
    if blob {
        flags |= WireFlags::BLOB;
    }
    if reconnect {
        flags |= WireFlags::RECONNECT;
    }
    flags
}
