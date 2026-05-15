// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::super::backend::AuthenticatedStream;
use super::super::handshake::establish_outbound_primary;
use super::*;
use crate::config::DomusConfigBuilder;
use crate::transport::handshake::{
    negotiate_blob_settings, validate_backend_identity, validate_blob_hello_request,
    validate_blob_hello_response,
};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

const HANDSHAKE_TEST_TIMEOUT: Duration = Duration::from_secs(5);

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

fn session_and_blob(config: DomusConfigAccess) -> (Arc<PeerSession>, Arc<BlobManager>) {
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
        allocator,
        128,
    ));
    (session, blob)
}

#[test]
fn blob_hello_request_requires_values() {
    let hello = HelloPayload::Primary;
    let err = validate_blob_hello_request(&hello).expect_err("expected error");
    assert_eq!(err.kind, ErrorId::ProtocolViolation);
}

#[test]
fn blob_hello_request_rejects_zero_values() {
    let hello = HelloPayload::Blob {
        chunk_size: 0,
        ack_window_chunks: 10,
    };
    let err = validate_blob_hello_request(&hello).expect_err("expected error");
    assert_eq!(err.kind, ErrorId::ProtocolViolation);
}

#[test]
fn blob_hello_response_requires_values() {
    let hello = HelloPayload::Primary;
    let err = validate_blob_hello_response(10, 10, &hello).expect_err("expected error");
    assert_eq!(err.kind, ErrorId::ProtocolViolation);
}

#[test]
fn blob_hello_response_rejects_excessive_values() {
    let hello = HelloPayload::Blob {
        chunk_size: 20,
        ack_window_chunks: 10,
    };
    let err = validate_blob_hello_response(10, 10, &hello).expect_err("expected error");
    assert_eq!(err.kind, ErrorId::ProtocolViolation);
}

#[test]
fn blob_hello_response_accepts_valid_values() {
    let hello = HelloPayload::Blob {
        chunk_size: 10,
        ack_window_chunks: 5,
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
        .callis_connect_timeout(Duration::from_millis(200))
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
        .create_outgoing(
            1,
            2,
            crate::a3_message_type(0),
            0,
            Bytes::from_static(b"pending"),
        )
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
        let response = HelloPayload::Primary;
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
async fn outbound_primary_hello_response_uses_callis_connect_deadline() {
    let registry = Arc::new(TabernaRegistry::new());
    let config = DomusConfigBuilder::new()
        .send_timeout(Duration::from_secs(1))
        .accept_timeout(Duration::from_secs(1))
        .callis_connect_timeout(Duration::from_millis(50))
        .build()
        .expect("config");
    let config = DomusConfigAccess::from_config(config);
    let (session, blob) = session_and_blob(config.clone());
    let primary_dispatch = session.primary_dispatch();
    let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(1);
    let primary_active = Arc::new(AtomicBool::new(false));
    let callis_tracker = CallisTracker::new();
    let task_set = PeerTaskSet::new(&tokio::runtime::Handle::current());
    let (client, _held_open_peer) = tokio::io::duplex(256);

    let started = Instant::now();
    let result = establish_outbound_primary(
        config,
        session,
        blob,
        registry,
        client,
        events_tx,
        primary_active,
        primary_dispatch,
        callis_tracker,
        task_set.spawner(),
        started + Duration::from_millis(50),
    )
    .await;
    let err = match result {
        Ok(_) => panic!("expected primary hello-response timeout"),
        Err(err) => err,
    };
    assert_eq!(err.kind, ErrorId::PeerUnavailable);
    assert_eq!(err.message.as_deref(), Some("callis connect timeout"));
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "callis-connect deadline should fail before the outer send budget"
    );
}

#[tokio::test]
async fn outbound_primary_reconnect_hello_response_uses_callis_connect_deadline() {
    let registry = Arc::new(TabernaRegistry::new());
    let config = DomusConfigBuilder::new()
        .send_timeout(Duration::from_secs(1))
        .accept_timeout(Duration::from_secs(1))
        .callis_connect_timeout(Duration::from_millis(50))
        .build()
        .expect("config");
    let config = DomusConfigAccess::from_config(config);
    let (session, blob) = session_and_blob(config.clone());
    session.set_active(true);
    let primary_dispatch = session.primary_dispatch();
    let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(1);
    let primary_active = Arc::new(AtomicBool::new(false));
    let callis_tracker = CallisTracker::new();
    let task_set = PeerTaskSet::new(&tokio::runtime::Handle::current());
    let (client, _held_open_peer) = tokio::io::duplex(256);

    let started = Instant::now();
    let result = establish_outbound_primary(
        config,
        session,
        blob,
        registry,
        client,
        events_tx,
        primary_active,
        primary_dispatch,
        callis_tracker,
        task_set.spawner(),
        started + Duration::from_millis(50),
    )
    .await;
    let err = match result {
        Ok(_) => panic!("expected reconnect primary hello-response timeout"),
        Err(err) => err,
    };
    assert_eq!(err.kind, ErrorId::PeerUnavailable);
    assert_eq!(err.message.as_deref(), Some("callis connect timeout"));
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "callis-connect deadline should fail before the outer send budget"
    );
}

#[tokio::test]
async fn outbound_blob_hello_response_uses_callis_connect_deadline() {
    let registry = Arc::new(TabernaRegistry::new());
    let config = DomusConfigBuilder::new()
        .send_timeout(Duration::from_secs(1))
        .accept_timeout(Duration::from_secs(1))
        .callis_connect_timeout(Duration::from_millis(50))
        .build()
        .expect("config");
    let config = DomusConfigAccess::from_config(config);
    let (session, blob) = session_and_blob(config.clone());
    let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(1);
    let callis_tracker = CallisTracker::new();
    let task_set = PeerTaskSet::new(&tokio::runtime::Handle::current());
    let (client, _held_open_peer) = tokio::io::duplex(256);

    let started = Instant::now();
    let result = super::super::handshake::establish_outbound_blob(
        config,
        session,
        registry,
        blob,
        client,
        events_tx,
        callis_tracker,
        task_set.spawner(),
        started + Duration::from_millis(50),
    )
    .await;
    let err = match result {
        Ok(_) => panic!("expected blob hello-response timeout"),
        Err(err) => err,
    };
    assert_eq!(err.kind, ErrorId::PeerUnavailable);
    assert_eq!(err.message.as_deref(), Some("callis connect timeout"));
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "callis-connect deadline should fail before the outer send budget"
    );
}

#[tokio::test]
async fn outbound_blob_reconnect_hello_response_uses_callis_connect_deadline() {
    let registry = Arc::new(TabernaRegistry::new());
    let config = DomusConfigBuilder::new()
        .send_timeout(Duration::from_secs(1))
        .accept_timeout(Duration::from_secs(1))
        .callis_connect_timeout(Duration::from_millis(50))
        .build()
        .expect("config");
    let config = DomusConfigAccess::from_config(config);
    let (session, blob) = session_and_blob(config.clone());
    let (shutdown_tx, _shutdown_rx) = watch::channel(false);
    blob.add_callis(
        CallisHandle {
            id: 99,
            tx: CallisTx::Blob,
            shutdown: shutdown_tx,
        },
        BlobCallisSettings {
            chunk_size: 1024,
            ack_window_chunks: 16,
        },
        false,
    )
    .await;
    let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(1);
    let callis_tracker = CallisTracker::new();
    let task_set = PeerTaskSet::new(&tokio::runtime::Handle::current());
    let (client, _held_open_peer) = tokio::io::duplex(256);

    let started = Instant::now();
    let result = super::super::handshake::establish_outbound_blob(
        config,
        session,
        registry,
        blob,
        client,
        events_tx,
        callis_tracker,
        task_set.spawner(),
        started + Duration::from_millis(50),
    )
    .await;
    let err = match result {
        Ok(_) => panic!("expected reconnect blob hello-response timeout"),
        Err(err) => err,
    };
    assert_eq!(err.kind, ErrorId::PeerUnavailable);
    assert_eq!(err.message.as_deref(), Some("callis connect timeout"));
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "callis-connect deadline should fail before the outer send budget"
    );
}

#[tokio::test]
async fn inbound_primary_rejects_blob_settings_in_hello() {
    tokio::time::timeout(HANDSHAKE_TEST_TIMEOUT, async {
        let registry = Arc::new(TabernaRegistry::new());
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
        let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(1);
        let (stream, _peer) = tokio::io::duplex(64);
        let primary_dispatch = session.primary_dispatch();
        let hello = HelloPayload::Blob {
            chunk_size: 4,
            ack_window_chunks: 4,
        };
        let task_set = PeerTaskSet::new(&tokio::runtime::Handle::current());
        let err = match super::super::handshake::accept_inbound_primary(
            config.clone(),
            session,
            blob,
            registry,
            Arc::new(AtomicBool::new(false)),
            primary_dispatch,
            stream,
            events_tx,
            hello,
            1,
            WireFlags::empty(),
            CallisTracker::new(),
            task_set.spawner(),
            Instant::now() + Duration::from_secs(1),
        )
        .await
        {
            Ok(_) => panic!("expected invalid hello"),
            Err(err) => err,
        };
        assert_eq!(err.kind, ErrorId::ProtocolViolation);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn inbound_duplicate_primary_does_not_clear_pending_dispatch() {
    tokio::time::timeout(HANDSHAKE_TEST_TIMEOUT, async {
        let registry = Arc::new(TabernaRegistry::new());
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
        let primary_dispatch = session.primary_dispatch();
        session.set_active(true);
        let (message, waiter) = session
            .create_outgoing(
                1,
                2,
                crate::a3_message_type(1),
                0,
                Bytes::from_static(b"pending"),
            )
            .await
            .expect("create outgoing");

        let (mut writer, reader) = tokio::io::duplex(256);
        let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(1);
        let hello = HelloPayload::Primary;
        let task_set = PeerTaskSet::new(&tokio::runtime::Handle::current());
        let info = super::super::handshake::accept_inbound_primary(
            config,
            Arc::clone(&session),
            blob,
            registry,
            Arc::new(AtomicBool::new(true)),
            Arc::clone(&primary_dispatch),
            reader,
            events_tx,
            hello,
            1,
            WireFlags::empty(),
            CallisTracker::new(),
            task_set.spawner(),
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .expect("accept duplicate primary");

        let (header, _payload) = read_frame(&mut writer, 1024)
            .await
            .expect("read hello response")
            .expect("hello response");
        assert_eq!(header.msg_type, MSG_HELLO_RESPONSE);
        assert!(!WireFlags::from_bits(header.flags)
            .expect("flags")
            .contains(WireFlags::RECONNECT));
        assert!(
            primary_dispatch
                .message(message.peer_msg_id)
                .await
                .is_some(),
            "duplicate inbound primary must not clear pending dispatch"
        );

        let _ = info.handle.shutdown.send(true);
        session.handle_ack(message.peer_msg_id).await;
        let _ = waiter;
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn inbound_duplicate_primary_during_activation_does_not_clear_pending_dispatch() {
    tokio::time::timeout(HANDSHAKE_TEST_TIMEOUT, async {
        let registry = Arc::new(TabernaRegistry::new());
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
        let primary_dispatch = session.primary_dispatch();
        session.set_active(true);
        let (message, waiter) = session
            .create_outgoing(
                1,
                2,
                crate::a3_message_type(2),
                0,
                Bytes::from_static(b"pending"),
            )
            .await
            .expect("create outgoing");

        let (mut writer, reader) = tokio::io::duplex(256);
        let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(1);
        let hello = HelloPayload::Primary;
        let task_set = PeerTaskSet::new(&tokio::runtime::Handle::current());
        let info = super::super::handshake::accept_inbound_primary(
            config,
            Arc::clone(&session),
            blob,
            registry,
            Arc::new(AtomicBool::new(false)),
            Arc::clone(&primary_dispatch),
            reader,
            events_tx,
            hello,
            1,
            WireFlags::empty(),
            CallisTracker::new(),
            task_set.spawner(),
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .expect("accept duplicate primary during activation");

        let (header, _payload) = read_frame(&mut writer, 1024)
            .await
            .expect("read hello response")
            .expect("hello response");
        assert_eq!(header.msg_type, MSG_HELLO_RESPONSE);
        assert!(!WireFlags::from_bits(header.flags)
            .expect("flags")
            .contains(WireFlags::RECONNECT));
        assert!(
            primary_dispatch
                .message(message.peer_msg_id)
                .await
                .is_some(),
            "duplicate inbound primary during activation must not clear pending dispatch"
        );

        let _ = info.handle.shutdown.send(true);
        session.handle_ack(message.peer_msg_id).await;
        let _ = waiter;
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn inbound_hello_rejects_unknown_flags() {
    tokio::time::timeout(HANDSHAKE_TEST_TIMEOUT, async {
        let registry = Arc::new(TabernaRegistry::new());
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
        let primary_active = Arc::new(AtomicBool::new(false));
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
        let task_set = PeerTaskSet::new(&tokio::runtime::Handle::current());
        let err = match super::super::handshake::accept_inbound(
            config,
            session,
            blob,
            registry,
            primary_active,
            primary_dispatch,
            reader,
            events_tx,
            CallisTracker::new(),
            listener_shutdown_rx,
            task_set.spawner(),
        )
        .await
        {
            Ok(_) => panic!("expected invalid flags"),
            Err(err) => err,
        };
        assert_eq!(err.kind, ErrorId::ProtocolViolation);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn inbound_rejects_when_parallel_callis_limit_reached() {
    tokio::time::timeout(HANDSHAKE_TEST_TIMEOUT, async {
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
            PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
        ));
        let blob = Arc::new(BlobManager::new(
            Arc::new(BlobBufferTracker::default()),
            Arc::new(Notify::new()),
            Arc::clone(&allocator),
            128,
        ));
        let primary_active = Arc::new(AtomicBool::new(false));
        let primary_dispatch = session.primary_dispatch();
        let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(1);
        let (mut writer, reader) = tokio::io::duplex(256);

        let hello = HelloPayload::Primary;
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
        let task_set = PeerTaskSet::new(&tokio::runtime::Handle::current());
        let err = match super::super::handshake::accept_inbound(
            config,
            session,
            blob,
            registry,
            primary_active,
            primary_dispatch,
            reader,
            events_tx,
            callis_tracker,
            listener_shutdown_rx,
            task_set.spawner(),
        )
        .await
        {
            Ok(_) => panic!("expected callis limit rejection"),
            Err(err) => err,
        };
        assert_eq!(err.kind, ErrorId::PeerUnavailable);
    })
    .await
    .expect("async test timed out");
}

// Reference unused symbol so the helper stays in scope for future expansion.
#[allow(dead_code)]
fn _refs() {
    let _ = establish_outbound_primary::<tokio::io::DuplexStream>;
}
