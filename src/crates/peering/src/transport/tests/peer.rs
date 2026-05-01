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
use crate::session::CancelReason;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::time::Instant;

type ListenerAcceptRx =
    Arc<Mutex<Option<mpsc::Receiver<AuthenticatedStream<tokio::io::DuplexStream, DomusAddr>>>>>;

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

#[tokio::test]
async fn route_change_flushes_queued_send_on_connect() {
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let domus_addr = SocketAddr::new(domus_ip, 5555);
    let backend = Arc::new(TestBackend);

    let registry = Arc::new(TabernaRegistry::new());
    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let (listener_shutdown_tx, _listener_shutdown_rx) = watch::channel(false);

    let handle = Arc::new(PeerHandle::new(
        None,
        Arc::clone(&registry),
        config.clone(),
        Arc::new(BlobBufferTracker::default()),
        Arc::clone(&backend),
        HandshakeGate::new(config.limited_registry()),
        crate::observability::new_observability(tokio::runtime::Handle::current()).1,
        shutdown_rx,
        listener_shutdown_tx,
        tokio::runtime::Handle::current(),
    ));

    let send_handle = Arc::clone(&handle);
    let send_task = tokio::spawn(async move {
        send_handle
            .send(1, 42, Bytes::from_static(b"queued"))
            .await
            .map_err(|err| err.kind)
    });

    handle.update_dial_addr(DomusAddr::Tcp(domus_addr)).await;

    let (tx, mut rx) = mpsc::channel(8);
    let (callis_shutdown_tx, _callis_shutdown_rx) = watch::channel(false);
    let callis_id = next_callis_id();
    let available = Arc::new(AtomicBool::new(true));
    let info = ConnectionInfo {
        handle: CallisHandle {
            id: callis_id,
            tx,
            shutdown: callis_shutdown_tx,
            available,
        },
        replay: Vec::new(),
        fresh_session: false,
        blob_settings: None,
        blob_resume: false,
    };
    handle
        .peer_state_tx
        .send(PeerStateUpdate::Connected {
            callis: CallisKind::Primary,
            info,
        })
        .await
        .expect("send connected");

    let frame = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("frame timeout")
        .expect("frame");
    let msg_id = match frame {
        OutboundFrame::Message(sent) => {
            assert_eq!(sent.msg_type, 42);
            assert_eq!(sent.payload, Bytes::from_static(b"queued"));
            sent.peer_msg_id
        }
        _ => panic!("expected message frame"),
    };

    handle.session.handle_ack(msg_id).await;
    let send_result = timeout(Duration::from_secs(1), send_task)
        .await
        .expect("send task timeout")
        .expect("send task join");
    assert_eq!(send_result, Ok(()));

    handle.shutdown().await;
}

#[tokio::test]
async fn negotiated_close_stops_listener_and_rejects_inbound() {
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

    timeout(Duration::from_secs(1), listener_handle)
        .await
        .expect("listener shutdown timeout")
        .expect("listener join");

    let send_result = backend
        .accept_tx
        .send(test_authenticated_stream(peer_addr))
        .await;
    assert!(send_result.is_err());
    assert_eq!(backend.accepted.load(Ordering::SeqCst), 0);
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
async fn remote_close_stops_listener() {
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
        .peer_handle(peer_addr)
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

    timeout(Duration::from_secs(1), listener_handle)
        .await
        .expect("listener shutdown timeout")
        .expect("listener join");
    assert_eq!(backend.accepted.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn teardown_after_send_timeout_marks_session_closing() {
    let backend = Arc::new(TestBackend);
    let registry = Arc::new(TabernaRegistry::new());
    let config = DomusConfigBuilder::new()
        .send_timeout(Duration::from_millis(50))
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

    let (tx, _rx) = mpsc::channel(8);
    let (callis_shutdown_tx, _callis_shutdown_rx) = watch::channel(false);
    let available = Arc::new(AtomicBool::new(true));
    let callis_id = next_callis_id();
    handle
        .peer_state_tx
        .send(PeerStateUpdate::Connected {
            callis: CallisKind::Primary,
            info: ConnectionInfo {
                handle: CallisHandle {
                    id: callis_id,
                    tx,
                    shutdown: callis_shutdown_tx,
                    available,
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
        handle.send(1, 2, Bytes::from_static(b"after")),
    )
    .await
    .expect("send timeout")
    .expect_err("expected peer unavailable");
    assert_eq!(err.kind, ErrorId::PeerUnavailable);
}

#[tokio::test]
async fn stalled_peer_does_not_block_other_peer() {
    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
    let registry = Arc::new(TabernaRegistry::new());
    let backend = Arc::new(TestBackend);
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let (listener_shutdown_tx, _listener_shutdown_rx) = watch::channel(false);

    let blocked = Arc::new(PeerHandle::new(
        None,
        Arc::clone(&registry),
        config.clone(),
        Arc::new(BlobBufferTracker::default()),
        Arc::clone(&backend),
        HandshakeGate::new(config.limited_registry()),
        crate::observability::new_observability(tokio::runtime::Handle::current()).1,
        shutdown_rx.clone(),
        listener_shutdown_tx.clone(),
        tokio::runtime::Handle::current(),
    ));
    let fast = Arc::new(PeerHandle::new(
        None,
        Arc::clone(&registry),
        config.clone(),
        Arc::new(BlobBufferTracker::default()),
        Arc::clone(&backend),
        HandshakeGate::new(config.limited_registry()),
        crate::observability::new_observability(tokio::runtime::Handle::current()).1,
        shutdown_rx,
        listener_shutdown_tx,
        tokio::runtime::Handle::current(),
    ));

    let (blocked_tx, mut blocked_rx) = mpsc::channel(8);
    let (blocked_shutdown_tx, _blocked_shutdown_rx) = watch::channel(false);
    let blocked_callis_id = next_callis_id();
    let blocked_available = Arc::new(AtomicBool::new(true));
    blocked
        .peer_state_tx
        .send(PeerStateUpdate::Connected {
            callis: CallisKind::Primary,
            info: ConnectionInfo {
                handle: CallisHandle {
                    id: blocked_callis_id,
                    tx: blocked_tx,
                    shutdown: blocked_shutdown_tx,
                    available: Arc::clone(&blocked_available),
                },
                replay: Vec::new(),
                fresh_session: false,
                blob_settings: None,
                blob_resume: false,
            },
        })
        .await
        .expect("connect blocked handle");

    let (fast_tx, mut fast_rx) = mpsc::channel(8);
    let (fast_shutdown_tx, _fast_shutdown_rx) = watch::channel(false);
    let fast_callis_id = next_callis_id();
    let fast_available = Arc::new(AtomicBool::new(true));
    fast.peer_state_tx
        .send(PeerStateUpdate::Connected {
            callis: CallisKind::Primary,
            info: ConnectionInfo {
                handle: CallisHandle {
                    id: fast_callis_id,
                    tx: fast_tx,
                    shutdown: fast_shutdown_tx,
                    available: Arc::clone(&fast_available),
                },
                replay: Vec::new(),
                fresh_session: false,
                blob_settings: None,
                blob_resume: false,
            },
        })
        .await
        .expect("connect fast handle");

    let blocked_handle = Arc::clone(&blocked);
    let blocked_send = tokio::spawn(async move {
        blocked_handle
            .send(10, 20, Bytes::from_static(b"blocked"))
            .await
            .map_err(|err| err.kind)
    });
    let blocked_frame = timeout(Duration::from_secs(1), blocked_rx.recv())
        .await
        .expect("blocked frame timeout")
        .expect("blocked frame");
    let blocked_msg_id = match blocked_frame {
        OutboundFrame::Message(message) => message.peer_msg_id,
        _ => panic!("expected blocked message frame"),
    };

    let fast_handle = Arc::clone(&fast);
    let fast_send = tokio::spawn(async move {
        fast_handle
            .send(11, 21, Bytes::from_static(b"fast"))
            .await
            .map_err(|err| err.kind)
    });
    let fast_frame = timeout(Duration::from_secs(1), fast_rx.recv())
        .await
        .expect("fast frame timeout")
        .expect("fast frame");
    let fast_msg_id = match fast_frame {
        OutboundFrame::Message(message) => message.peer_msg_id,
        _ => panic!("expected fast message frame"),
    };

    fast.session.handle_ack(fast_msg_id).await;
    let fast_result = timeout(Duration::from_secs(1), fast_send)
        .await
        .expect("fast send timeout")
        .expect("fast send join");
    assert_eq!(fast_result, Ok(()));

    blocked.session.handle_ack(blocked_msg_id).await;
    let blocked_result = timeout(Duration::from_secs(1), blocked_send)
        .await
        .expect("blocked send timeout")
        .expect("blocked send join");
    assert_eq!(blocked_result, Ok(()));

    blocked.shutdown().await;
    fast.shutdown().await;
}

#[tokio::test]
async fn teardown_cancels_inbound_waiters_without_outcomes() {
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
}
