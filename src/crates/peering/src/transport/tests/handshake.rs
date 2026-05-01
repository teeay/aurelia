// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::super::backend::AuthenticatedStream;
use super::super::handshake::establish_outbound_primary;
use super::*;
use crate::config::DomusConfigBuilder;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

#[derive(Clone)]
struct DialBackend {
    dial_rx: Arc<Mutex<mpsc::Receiver<AuthenticatedStream<tokio::io::DuplexStream, DomusAddr>>>>,
}

impl DialBackend {
    fn new(
        dial_rx: mpsc::Receiver<AuthenticatedStream<tokio::io::DuplexStream, DomusAddr>>,
    ) -> Self {
        Self {
            dial_rx: Arc::new(Mutex::new(dial_rx)),
        }
    }
}

#[async_trait::async_trait]
impl TransportBackend for DialBackend {
    type Addr = DomusAddr;
    type Listener = ();
    type Stream = tokio::io::DuplexStream;

    async fn bind(&self, _local: &Self::Addr) -> Result<Self::Listener, AureliaError> {
        Ok(())
    }

    async fn accept(
        &self,
        _listener: &mut Self::Listener,
    ) -> Result<AuthenticatedStream<Self::Stream, Self::Addr>, AureliaError> {
        Err(AureliaError::new(ErrorId::PeerUnavailable))
    }

    async fn dial(
        &self,
        _peer: &Self::Addr,
    ) -> Result<AuthenticatedStream<Self::Stream, Self::Addr>, AureliaError> {
        let mut guard = self.dial_rx.lock().await;
        guard
            .recv()
            .await
            .ok_or_else(|| AureliaError::new(ErrorId::PeerUnavailable))
    }
}

#[test]
fn blob_hello_request_requires_values() {
    let hello = HelloPayload {
        chunk_size: None,
        ack_window_chunks: None,
    };
    let err = validate_blob_hello_request(&hello).expect_err("expected error");
    assert_eq!(err.kind, ErrorId::ProtocolViolation);
}

#[test]
fn blob_hello_request_rejects_zero_values() {
    let hello = HelloPayload {
        chunk_size: Some(0),
        ack_window_chunks: Some(10),
    };
    let err = validate_blob_hello_request(&hello).expect_err("expected error");
    assert_eq!(err.kind, ErrorId::ProtocolViolation);
}

#[test]
fn blob_hello_response_requires_values() {
    let hello = HelloPayload {
        chunk_size: None,
        ack_window_chunks: None,
    };
    let err = validate_blob_hello_response(10, 10, &hello).expect_err("expected error");
    assert_eq!(err.kind, ErrorId::ProtocolViolation);
}

#[test]
fn blob_hello_response_rejects_excessive_values() {
    let hello = HelloPayload {
        chunk_size: Some(20),
        ack_window_chunks: Some(10),
    };
    let err = validate_blob_hello_response(10, 10, &hello).expect_err("expected error");
    assert_eq!(err.kind, ErrorId::ProtocolViolation);
}

#[test]
fn blob_hello_response_accepts_valid_values() {
    let hello = HelloPayload {
        chunk_size: Some(10),
        ack_window_chunks: Some(5),
    };
    let settings = validate_blob_hello_response(10, 10, &hello).expect("valid response");
    assert_eq!(settings.chunk_size, 10);
    assert_eq!(settings.ack_window_chunks, 5);
}

#[test]
fn negotiate_blob_settings_clamps_to_config() {
    let settings = negotiate_blob_settings(1200, 512, 1000, 256);
    assert_eq!(settings.chunk_size, 1000);
    assert_eq!(settings.ack_window_chunks, 256);
}

#[test]
fn backend_identity_mismatch_is_error() {
    let expected = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5000));
    let authenticated = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5001));
    let err = validate_backend_identity(&expected, &authenticated).expect_err("expected error");
    assert_eq!(err.kind, ErrorId::ProtocolViolation);
}

/// When the originator dials with `RECONNECT` set but the receiver does not echo it (its peer
/// session is gone), the originator continues on the **same** peer handle with `fresh_session`
/// flagged. The fresh-session handler in peer.rs is responsible for failing retained inflight
/// locally and re-dispatching queued non-A1 work.
#[tokio::test]
async fn reconnect_no_resume_returns_fresh_session_on_same_handle() {
    let (dial_tx, dial_rx) = mpsc::channel(1);
    let backend = Arc::new(DialBackend::new(dial_rx));
    let registry = Arc::new(TabernaRegistry::new());
    let config = DomusConfigBuilder::new()
        .send_timeout(Duration::from_millis(200))
        .accept_timeout(Duration::from_millis(200))
        .listener_delay(Duration::from_millis(0))
        .build()
        .expect("config");
    let config: DomusConfigAccess = DomusConfigAccess::from_config(config);
    let local_addr = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0));
    let transport = Transport::bind_with_backend(
        local_addr,
        registry,
        config.clone(),
        crate::observability::new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        Arc::clone(&backend),
    )
    .await
    .expect("bind transport");

    let peer_addr = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4001));
    let handle = transport
        .inner
        .peer_handle(peer_addr.clone())
        .await
        .expect("peer handle");
    handle.session.set_active(true);

    let (_message, waiter) = handle
        .session
        .create_outgoing(1, 2, 0x0001_0000, 0, Bytes::from_static(b"pending"))
        .await
        .expect("enqueue");

    let (client, mut server) = tokio::io::duplex(256);
    let config_server = config.clone();
    let server_task = tokio::spawn(async move {
        let cfg = config_server.snapshot().await;
        let (header, payload) = read_frame(&mut server, cfg.max_payload_len)
            .await
            .expect("read hello")
            .expect("hello frame");
        let flags = WireFlags::from_bits(header.flags).expect("flags");
        assert!(flags.contains(WireFlags::RECONNECT));
        let _hello = HelloPayload::from_bytes(&payload).expect("hello payload");
        let response = HelloPayload {
            chunk_size: None,
            ack_window_chunks: None,
        };
        // No RECONNECT echo — receiver's session is gone.
        send_control_frame(
            &mut server,
            MSG_HELLO_RESPONSE,
            0,
            header.peer_msg_id,
            response.to_bytes().as_slice(),
        )
        .await
        .expect("send hello-response");
    });

    let authenticated = AuthenticatedStream {
        stream: client,
        peer_addr: peer_addr.clone(),
    };
    dial_tx.send(authenticated).await.expect("queue dial");

    handle
        .peer_state_tx
        .send(PeerStateUpdate::EnsurePrimaryDial)
        .await
        .expect("ensure dial");

    // The retained inflight non-A1 message must fail locally with PeerRestarted because the
    // receiver did not echo RECONNECT.
    let err = timeout(Duration::from_secs(1), handle.session.wait_for_ack(waiter))
        .await
        .expect("ack timeout")
        .expect_err("expected error");
    assert_eq!(err.kind, ErrorId::PeerRestarted);

    // The peer handle is reused — no replacement was created.
    let same = transport
        .inner
        .peer_handle(peer_addr)
        .await
        .expect("peer handle");
    assert!(Arc::ptr_eq(&same, &handle));

    server_task.await.expect("server task");
}

#[tokio::test]
async fn inbound_primary_rejects_blob_settings_in_hello() {
    let registry = Arc::new(TabernaRegistry::new());
    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
    let allocator = Arc::new(PeerMessageIdAllocator::default());
    let session = Arc::new(PeerSession::new(
        Arc::clone(&allocator),
        config.clone(),
        tokio::runtime::Handle::current(),
    ));
    let blob = Arc::new(BlobManager::new(
        Arc::new(BlobBufferTracker::default()),
        Arc::new(Notify::new()),
        Arc::clone(&allocator),
    ));
    let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(1);
    let (stream, _peer) = tokio::io::duplex(64);
    let primary_dispatch = session.primary_dispatch();
    let hello = HelloPayload {
        chunk_size: Some(4),
        ack_window_chunks: Some(4),
    };
    let err = match super::super::handshake::accept_inbound_primary(
        config.clone(),
        session,
        blob,
        registry,
        Arc::new(Notify::new()),
        primary_dispatch,
        stream,
        events_tx,
        hello,
        1,
        WireFlags::empty(),
        CallisTracker::new(),
    )
    .await
    {
        Ok(_) => panic!("expected invalid hello"),
        Err(err) => err,
    };
    assert_eq!(err.kind, ErrorId::ProtocolViolation);
}

#[tokio::test]
async fn inbound_hello_rejects_unknown_flags() {
    let registry = Arc::new(TabernaRegistry::new());
    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
    let allocator = Arc::new(PeerMessageIdAllocator::default());
    let session = Arc::new(PeerSession::new(
        Arc::clone(&allocator),
        config.clone(),
        tokio::runtime::Handle::current(),
    ));
    let blob = Arc::new(BlobManager::new(
        Arc::new(BlobBufferTracker::default()),
        Arc::new(Notify::new()),
        Arc::clone(&allocator),
    ));
    let primary_active = Arc::new(AtomicBool::new(false));
    let primary_available = Arc::new(Notify::new());
    let primary_dispatch = session.primary_dispatch();
    let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(1);
    let (mut writer, reader) = tokio::io::duplex(64);
    let header = WireHeader {
        version: PROTOCOL_VERSION,
        flags: 0x8000,
        msg_type: MSG_HELLO,
        peer_msg_id: 1,
        src_taberna: 0,
        dst_taberna: 0,
        payload_len: 0,
    };
    writer
        .write_all(&header.encode())
        .await
        .expect("write header");

    let (_listener_shutdown_tx, listener_shutdown_rx) = watch::channel(false);
    let err = match super::super::handshake::accept_inbound(
        config,
        session,
        blob,
        registry,
        primary_active,
        primary_available,
        primary_dispatch,
        reader,
        events_tx,
        CallisTracker::new(),
        listener_shutdown_rx,
    )
    .await
    {
        Ok(_) => panic!("expected invalid flags"),
        Err(err) => err,
    };
    assert_eq!(err.kind, ErrorId::ProtocolViolation);
}

#[tokio::test]
async fn inbound_rejects_when_parallel_callis_limit_reached() {
    let registry = Arc::new(TabernaRegistry::new());
    let cfg = DomusConfig {
        max_parallel_callis_per_peer: 1,
        ..Default::default()
    };
    let config: DomusConfigAccess = DomusConfigAccess::from_config(cfg);
    let allocator = Arc::new(PeerMessageIdAllocator::default());
    let session = Arc::new(PeerSession::new(
        Arc::clone(&allocator),
        config.clone(),
        tokio::runtime::Handle::current(),
    ));
    let blob = Arc::new(BlobManager::new(
        Arc::new(BlobBufferTracker::default()),
        Arc::new(Notify::new()),
        Arc::clone(&allocator),
    ));
    let primary_active = Arc::new(AtomicBool::new(false));
    let primary_available = Arc::new(Notify::new());
    let primary_dispatch = session.primary_dispatch();
    let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(1);
    let (mut writer, reader) = tokio::io::duplex(256);

    let hello = HelloPayload {
        chunk_size: None,
        ack_window_chunks: None,
    };
    let payload = hello.to_bytes();
    let header = WireHeader {
        version: PROTOCOL_VERSION,
        flags: WireFlags::empty().bits(),
        msg_type: MSG_HELLO,
        peer_msg_id: 1,
        src_taberna: 0,
        dst_taberna: 0,
        payload_len: payload.len() as u32,
    };
    writer
        .write_all(&header.encode())
        .await
        .expect("write header");
    writer
        .write_all(payload.as_slice())
        .await
        .expect("write payload");

    let callis_tracker = CallisTracker::new();
    callis_tracker.open();

    let (_listener_shutdown_tx, listener_shutdown_rx) = watch::channel(false);
    let err = match super::super::handshake::accept_inbound(
        config,
        session,
        blob,
        registry,
        primary_active,
        primary_available,
        primary_dispatch,
        reader,
        events_tx,
        callis_tracker,
        listener_shutdown_rx,
    )
    .await
    {
        Ok(_) => panic!("expected callis limit rejection"),
        Err(err) => err,
    };
    assert_eq!(err.kind, ErrorId::PeerUnavailable);
}

// Reference unused symbol so the helper stays in scope for future expansion.
#[allow(dead_code)]
fn _refs() {
    let _ = establish_outbound_primary::<tokio::io::DuplexStream>;
}
