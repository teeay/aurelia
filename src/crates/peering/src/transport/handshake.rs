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

#[allow(clippy::too_many_arguments)]
fn spawn_dial_task_scaffold<B, F, Fut, L>(
    dial_addr: DomusAddr,
    backend: Arc<B>,
    delay: Duration,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    callis: CallisKind,
    handshake_phase: HandshakePhase,
    observability: ObservabilityHandle,
    runtime_handle: tokio::runtime::Handle,
    establish: F,
    on_connected: L,
) where
    B: TransportBackend<Addr = DomusAddr> + 'static,
    F: FnOnce(B::Stream) -> Fut + Send + 'static,
    Fut: Future<Output = Result<ConnectionInfo, AureliaError>> + Send + 'static,
    L: FnOnce(&DomusAddr, &ConnectionInfo) + Send + 'static,
{
    let label = callis_label(callis);
    runtime_handle.spawn(async move {
        if !delay.is_zero() {
            sleep(delay).await;
        }
        observability.dial_attempt(dial_addr.clone(), callis).await;
        debug!(
            peer = %dial_addr,
            delay_ms = delay.as_millis(),
            "dialing {} callis",
            label
        );
        let authenticated = match backend.dial(&dial_addr).await {
            Ok(value) => value,
            Err(err) => {
                observability
                    .backend_failure(dial_addr.clone(), err.kind)
                    .await;
                observability
                    .dial_failed(dial_addr.clone(), callis, err.kind)
                    .await;
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
            observability
                .identity_mismatch(dial_addr.clone(), dial_addr.clone(), peer_identity.clone())
                .await;
            observability
                .dial_failed(dial_addr.clone(), callis, err.kind)
                .await;
            let _ = peer_state_tx
                .send(PeerStateUpdate::DialFailed(callis))
                .await;
            return;
        }
        match establish(authenticated.stream).await {
            Ok(info) => {
                on_connected(&dial_addr, &info);
                let _ = peer_state_tx
                    .send(PeerStateUpdate::Connected { callis, info })
                    .await;
            }
            Err(err) => {
                observability
                    .dial_failed(dial_addr.clone(), callis, err.kind)
                    .await;
                if err.kind == ErrorId::ProtocolViolation {
                    observability
                        .protocol_violation(dial_addr.clone(), err.kind)
                        .await;
                } else if err.kind == ErrorId::SendTimeout {
                    observability
                        .handshake_timeout(dial_addr.clone(), handshake_phase)
                        .await;
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
    primary_available: Arc<Notify>,
    primary_dispatch: Arc<PrimaryDispatchQueue>,
    callis_tracker: CallisTracker,
    observability: ObservabilityHandle,
    runtime_handle: tokio::runtime::Handle,
) where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    let peer_state_tx_establish = peer_state_tx.clone();
    spawn_dial_task_scaffold::<B, _, _, _>(
        dial_addr,
        backend,
        delay,
        peer_state_tx,
        CallisKind::Primary,
        HandshakePhase::OutboundPrimaryHello,
        observability,
        runtime_handle,
        move |stream| {
            establish_outbound_primary(
                config,
                session,
                blob,
                registry,
                stream,
                peer_state_tx_establish,
                primary_available,
                primary_dispatch,
                callis_tracker,
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
    runtime_handle: tokio::runtime::Handle,
) where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    let peer_state_tx_establish = peer_state_tx.clone();
    spawn_dial_task_scaffold::<B, _, _, _>(
        dial_addr,
        backend,
        delay,
        peer_state_tx,
        CallisKind::Blob,
        HandshakePhase::OutboundBlobHello,
        observability,
        runtime_handle,
        move |stream| {
            establish_outbound_blob(
                config,
                session,
                registry,
                blob,
                stream,
                peer_state_tx_establish,
                callis_tracker,
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
    primary_dispatch: Option<Arc<PrimaryDispatchQueue>>,
    availability_notify: Option<Arc<Notify>>,
    callis_tracker: CallisTracker,
) -> CallisHandle
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (tx, rx) = mpsc::channel(1);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let callis_id = next_callis_id();
    let available = Arc::new(AtomicBool::new(true));
    let runtime_handle = session.runtime_handle();
    spawn_callis_task(
        config,
        session,
        blob,
        registry,
        stream,
        callis_id,
        primary_dispatch,
        tx.clone(),
        rx,
        shutdown_rx,
        peer_state_tx,
        callis,
        Arc::clone(&available),
        availability_notify,
        callis_tracker,
        runtime_handle,
    );
    CallisHandle {
        id: callis_id,
        tx,
        shutdown: shutdown_tx,
        available,
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn establish_outbound_primary<S>(
    config: DomusConfigAccess,
    session: Arc<PeerSession>,
    blob: Arc<BlobManager>,
    registry: Arc<TabernaRegistry>,
    mut stream: S,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    primary_available: Arc<Notify>,
    primary_dispatch: Arc<PrimaryDispatchQueue>,
    callis_tracker: CallisTracker,
) -> Result<ConnectionInfo, AureliaError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let reconnect = session.is_active();
    let hello = HelloPayload {
        chunk_size: None,
        ack_window_chunks: None,
    };
    let hello_flags = hello_header_flags(reconnect, false);
    send_control_frame(
        &mut stream,
        MSG_HELLO,
        hello_flags.bits(),
        0,
        hello.to_bytes().as_slice(),
    )
    .await?;
    let cfg = config.snapshot().await;
    let response = timeout(
        cfg.send_timeout,
        read_frame(&mut stream, cfg.max_payload_len),
    )
    .await
    .map_err(|_| AureliaError::new(ErrorId::SendTimeout))??
    .ok_or_else(|| AureliaError::with_message(ErrorId::ConnectionLost, "missing hello-response"))?;
    let (header, payload) = response;
    if header.msg_type != MSG_HELLO_RESPONSE {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let response_flags = WireFlags::from_bits(header.flags).ok_or_else(|| {
        AureliaError::with_message(
            ErrorId::ProtocolViolation,
            format!("invalid hello-response flags: 0x{:04x}", header.flags),
        )
    })?;
    if response_flags.contains(WireFlags::BLOB) {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let response_payload = HelloPayload::from_bytes(&payload)?;
    if response_payload.chunk_size.is_some() || response_payload.ack_window_chunks.is_some() {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let mut replay = Vec::new();
    let mut fresh_session = false;
    if reconnect && response_flags.contains(WireFlags::RECONNECT) {
        replay = session.handle_hello_response(true).await;
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
        Some(primary_available),
        callis_tracker,
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
    let (Some(chunk), Some(window)) = (hello.chunk_size, hello.ack_window_chunks) else {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    };
    if chunk == 0 || window == 0 {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    Ok((chunk, window))
}

pub(super) fn validate_blob_hello_response(
    proposed_chunk: u32,
    proposed_window: u32,
    hello: &HelloPayload,
) -> Result<BlobCallisSettings, AureliaError> {
    let (Some(agreed_chunk), Some(agreed_window)) = (hello.chunk_size, hello.ack_window_chunks)
    else {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    };
    if agreed_chunk == 0 || agreed_window == 0 {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
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

pub(super) async fn establish_outbound_blob<S>(
    config: DomusConfigAccess,
    session: Arc<PeerSession>,
    registry: Arc<TabernaRegistry>,
    blob: Arc<BlobManager>,
    mut stream: S,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    callis_tracker: CallisTracker,
) -> Result<ConnectionInfo, AureliaError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let reconnect = blob.had_callis();
    let cfg = config.snapshot().await;
    let proposed_chunk = cfg.blob_chunk_size;
    let proposed_window = cfg.blob_ack_window;
    let hello = HelloPayload {
        chunk_size: Some(proposed_chunk),
        ack_window_chunks: Some(proposed_window),
    };
    let hello_flags = hello_header_flags(reconnect, true);
    send_control_frame(
        &mut stream,
        MSG_HELLO,
        hello_flags.bits(),
        0,
        hello.to_bytes().as_slice(),
    )
    .await?;

    let response = timeout(
        cfg.send_timeout,
        read_frame(&mut stream, cfg.max_payload_len),
    )
    .await
    .map_err(|_| AureliaError::new(ErrorId::SendTimeout))??
    .ok_or_else(|| AureliaError::with_message(ErrorId::ConnectionLost, "missing hello-response"))?;
    let (header, payload) = response;
    if header.msg_type != MSG_HELLO_RESPONSE {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let response_flags = WireFlags::from_bits(header.flags).ok_or_else(|| {
        AureliaError::with_message(
            ErrorId::ProtocolViolation,
            format!("invalid hello-response flags: 0x{:04x}", header.flags),
        )
    })?;
    if !response_flags.contains(WireFlags::BLOB) {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    if !reconnect && response_flags.contains(WireFlags::RECONNECT) {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let response_payload = HelloPayload::from_bytes(&payload)?;
    let settings =
        validate_blob_hello_response(proposed_chunk, proposed_window, &response_payload)?;
    let resume = reconnect && response_flags.contains(WireFlags::RECONNECT);

    let dispatch_notify = blob.dispatch_handle();
    let handle = spawn_callis_handle(
        config,
        session,
        Arc::clone(&blob),
        registry,
        stream,
        peer_state_tx,
        CallisKind::Blob,
        None,
        Some(dispatch_notify),
        callis_tracker,
    );

    Ok(ConnectionInfo {
        handle,
        replay: Vec::new(),
        fresh_session: false,
        blob_settings: Some(settings),
        blob_resume: resume,
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn accept_inbound<S>(
    config: DomusConfigAccess,
    session: Arc<PeerSession>,
    blob: Arc<BlobManager>,
    registry: Arc<TabernaRegistry>,
    primary_active: Arc<AtomicBool>,
    primary_available: Arc<Notify>,
    primary_dispatch: Arc<PrimaryDispatchQueue>,
    mut stream: S,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    callis_tracker: CallisTracker,
    mut listener_shutdown_rx: watch::Receiver<bool>,
) -> Result<(CallisKind, ConnectionInfo), AureliaError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    if *listener_shutdown_rx.borrow() {
        let _ = stream.shutdown().await;
        return Err(AureliaError::new(ErrorId::DomusClosed));
    }
    let cfg = config.snapshot().await;
    let response = tokio::select! {
        biased;
        _ = listener_shutdown_rx.changed() => {
            let _ = stream.shutdown().await;
            return Err(AureliaError::new(ErrorId::DomusClosed));
        }
        res = timeout(cfg.send_timeout, read_frame(&mut stream, cfg.max_payload_len)) => {
            res.map_err(|_| AureliaError::new(ErrorId::SendTimeout))??
                .ok_or_else(|| AureliaError::new(ErrorId::ConnectionLost))?
        }
    };
    let (header, payload) = response;
    if header.msg_type != MSG_HELLO {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let flags = WireFlags::from_bits(header.flags).ok_or_else(|| {
        AureliaError::with_message(
            ErrorId::ProtocolViolation,
            format!("invalid hello flags: 0x{:04x}", header.flags),
        )
    })?;
    let hello = HelloPayload::from_bytes(&payload)?;
    let peer_msg_id = header.peer_msg_id;
    let max_parallel = cfg.max_parallel_callis_per_peer.max(1);
    if callis_tracker.count() >= max_parallel {
        warn!(
            log_id = aurelia_logging::limited::log_ids::CALLIS_PER_PEER_LIMIT,
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
        let info = accept_inbound_blob(
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
        )
        .await?;
        Ok((CallisKind::Blob, info))
    } else {
        let info = accept_inbound_primary(
            config,
            session,
            blob,
            registry,
            primary_available,
            primary_dispatch,
            stream,
            peer_state_tx,
            hello,
            peer_msg_id,
            flags,
            callis_tracker,
        )
        .await?;
        Ok((CallisKind::Primary, info))
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn accept_inbound_primary<S>(
    config: DomusConfigAccess,
    session: Arc<PeerSession>,
    blob: Arc<BlobManager>,
    registry: Arc<TabernaRegistry>,
    primary_available: Arc<Notify>,
    primary_dispatch: Arc<PrimaryDispatchQueue>,
    mut stream: S,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    hello: HelloPayload,
    peer_msg_id: PeerMessageId,
    flags: WireFlags,
    callis_tracker: CallisTracker,
) -> Result<ConnectionInfo, AureliaError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    if hello.chunk_size.is_some() || hello.ack_window_chunks.is_some() {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let reconnect = flags.contains(WireFlags::RECONNECT);
    let a1_resume = session.accept_hello(reconnect).await;
    let response_flags = hello_header_flags(a1_resume, false);
    let response = HelloPayload {
        chunk_size: None,
        ack_window_chunks: None,
    };
    send_control_frame(
        &mut stream,
        MSG_HELLO_RESPONSE,
        response_flags.bits(),
        peer_msg_id,
        response.to_bytes().as_slice(),
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
        Some(primary_available),
        callis_tracker,
    );
    Ok(ConnectionInfo {
        handle,
        replay: Vec::new(),
        fresh_session: false,
        blob_settings: None,
        blob_resume: false,
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn accept_inbound_blob<S>(
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
) -> Result<ConnectionInfo, AureliaError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (proposed_chunk, proposed_window) = validate_blob_hello_request(&hello)?;

    let cfg = config.snapshot().await;
    let settings = negotiate_blob_settings(
        proposed_chunk,
        proposed_window,
        cfg.blob_chunk_size,
        cfg.blob_ack_window,
    );

    let reconnect = flags.contains(WireFlags::RECONNECT);
    let had_callis = blob.had_callis();
    let resume = if reconnect {
        had_callis || blob.has_active_streams().await
    } else {
        false
    };
    let response_flags = hello_header_flags(resume, true);
    let response = HelloPayload {
        chunk_size: Some(settings.chunk_size),
        ack_window_chunks: Some(settings.ack_window_chunks),
    };
    send_control_frame(
        &mut stream,
        MSG_HELLO_RESPONSE,
        response_flags.bits(),
        peer_msg_id,
        response.to_bytes().as_slice(),
    )
    .await?;

    let dispatch_notify = blob.dispatch_handle();
    let handle = spawn_callis_handle(
        config,
        session,
        Arc::clone(&blob),
        registry,
        stream,
        peer_state_tx,
        CallisKind::Blob,
        None,
        Some(dispatch_notify),
        callis_tracker,
    );

    Ok(ConnectionInfo {
        handle,
        replay: Vec::new(),
        fresh_session: false,
        blob_settings: Some(settings),
        blob_resume: resume,
    })
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
