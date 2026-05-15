// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rcgen::{BasicConstraints, Certificate, CertificateParams, IsCa, SanType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::time::{timeout, timeout_at, Instant};
use tokio_rustls::rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName,
};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::auth::{Pkcs8AuthConfig, Pkcs8DerConfig};
use crate::config::{BlobWindowConfig, DomusConfig, DomusConfigAccess, DomusConfigBuilder};
use crate::observability::new_observability;
use crate::peering::RouteLocalRemote;
use crate::taberna::{TabernaInbox, TabernaRegistry};
use crate::transport::Transport;
use crate::wire::{ErrorPayload, HelloPayload, WireFlags, WireHeader, PROTOCOL_VERSION};
use crate::{a3_message_type, BlobReceiver, SendOptions, SendOutcome};
use aurelia_data::DomusAddr;
use aurelia_data::RouteResolver;
use aurelia_ids::TabernaId;
use aurelia_ids::{AureliaError, ErrorId};
use aurelia_ids::{MSG_ERROR, MSG_HELLO, MSG_HELLO_RESPONSE};

use super::{test_message, TestCodec};

const TRANSPORT_TEST_TIMEOUT: Duration = Duration::from_secs(10);

struct StaticResolver {
    routes: HashMap<TabernaId, DomusAddr>,
}

impl StaticResolver {
    fn new(taberna_id: TabernaId, addr: DomusAddr) -> Self {
        Self {
            routes: HashMap::from([(taberna_id, addr)]),
        }
    }

    fn from_routes(routes: impl IntoIterator<Item = (TabernaId, DomusAddr)>) -> Self {
        Self {
            routes: routes.into_iter().collect(),
        }
    }
}

#[async_trait::async_trait]
impl RouteResolver for StaticResolver {
    async fn resolve(&self, taberna_id: TabernaId) -> Result<DomusAddr, AureliaError> {
        self.routes
            .get(&taberna_id)
            .cloned()
            .ok_or_else(|| AureliaError::new(ErrorId::UnknownTaberna))
    }
}

#[tokio::test]
async fn static_resolver_accepts_configured_id_and_rejects_neighbor() {
    tokio::time::timeout(TRANSPORT_TEST_TIMEOUT, async {
        let addr = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5555));
        let resolver = StaticResolver::new(7, addr.clone());

        let resolved = resolver.resolve(7).await.expect("configured route");
        assert_eq!(resolved, addr);

        let err = resolver
            .resolve(8)
            .await
            .expect_err("unexpected taberna id");
        assert_eq!(err.kind, ErrorId::UnknownTaberna);
    })
    .await
    .expect("async test timed out");
}

struct RecordingSink {
    received: tokio::sync::Mutex<Vec<(u32, Bytes, Option<BlobReceiver>)>>,
    received_notify: Notify,
    expected_msg_types: Vec<u32>,
}

impl RecordingSink {
    fn new(expected_msg_types: Vec<u32>) -> Self {
        Self {
            received: tokio::sync::Mutex::new(Vec::new()),
            received_notify: Notify::new(),
            expected_msg_types,
        }
    }

    async fn take(&self) -> Vec<(u32, Bytes, Option<BlobReceiver>)> {
        let mut guard = self.received.lock().await;
        std::mem::take(&mut *guard)
    }

    async fn wait_for(
        &self,
        count: usize,
        timeout_duration: Duration,
    ) -> Vec<(u32, Bytes, Option<BlobReceiver>)> {
        let deadline = tokio::time::Instant::now() + timeout_duration;
        loop {
            if let Some(items) = {
                let mut guard = self.received.lock().await;
                if guard.len() >= count {
                    Some(std::mem::take(&mut *guard))
                } else {
                    None
                }
            } {
                return items;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("timeout waiting for {count} messages");
            }
            timeout_at(deadline, self.received_notify.notified())
                .await
                .unwrap_or_else(|_| panic!("timeout waiting for {count} messages"));
        }
    }
}

#[async_trait::async_trait]
impl TabernaInbox for RecordingSink {
    async fn enqueue(
        &self,
        msg_type: u32,
        payload: Bytes,
        blob_receiver: Option<BlobReceiver>,
        notify: Option<Arc<Notify>>,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<(), AureliaError>>, AureliaError> {
        assert!(
            self.expected_msg_types.contains(&msg_type),
            "unexpected msg_type {msg_type}; expected one of {:?}",
            self.expected_msg_types
        );
        self.received
            .lock()
            .await
            .push((msg_type, payload, blob_receiver));
        self.received_notify.notify_waiters();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = tx.send(Ok(()));
        if let Some(notify) = notify.as_ref() {
            notify.notify_one();
        }
        Ok(rx)
    }
}

struct BlockingInbox {
    started: Arc<Notify>,
    ready: Arc<Notify>,
    expected_msg_types: Vec<u32>,
}

impl BlockingInbox {
    fn new(expected_msg_types: Vec<u32>, started: Arc<Notify>, ready: Arc<Notify>) -> Self {
        Self {
            started,
            ready,
            expected_msg_types,
        }
    }
}

#[async_trait::async_trait]
impl TabernaInbox for BlockingInbox {
    async fn enqueue(
        &self,
        msg_type: u32,
        _payload: Bytes,
        _blob_receiver: Option<BlobReceiver>,
        notify: Option<Arc<Notify>>,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<(), AureliaError>>, AureliaError> {
        assert!(
            self.expected_msg_types.contains(&msg_type),
            "unexpected msg_type {msg_type}; expected one of {:?}",
            self.expected_msg_types
        );
        self.started.notify_waiters();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let ready = Arc::clone(&self.ready);
        let notify = notify.clone();
        tokio::spawn(async move {
            ready.notified().await;
            let _ = tx.send(Ok(()));
            if let Some(notify) = notify.as_ref() {
                notify.notify_one();
            }
        });
        Ok(rx)
    }
}

async fn read_blob(mut receiver: BlobReceiver) -> Vec<u8> {
    let mut out = Vec::new();
    let mut buf = [0u8; 64];
    loop {
        let n = receiver.read(&mut buf).await.expect("blob read");
        if n == 0 {
            break;
        }
        out.extend_from_slice(&buf[..n]);
    }
    out
}

async fn write_blob(mut sender: crate::BlobSender, chunks: Vec<Bytes>) -> std::io::Result<()> {
    for chunk in chunks {
        sender.write_all(&chunk).await?;
    }
    sender.shutdown().await
}

fn map_io_error(err: std::io::Error) -> ErrorId {
    map_io_error_message(&err.to_string())
}

fn map_io_error_message(message: &str) -> ErrorId {
    if message.contains("PeerRestarted") {
        ErrorId::PeerRestarted
    } else if message.contains("SendTimeout") {
        ErrorId::SendTimeout
    } else if message.contains("ProtocolViolation") {
        ErrorId::ProtocolViolation
    } else {
        ErrorId::PeerUnavailable
    }
}

async fn dump_observability(label: &str, reporting: &crate::observability::DomusReporting) {
    eprintln!("=== observability: {label} ===");
    let snapshot = reporting.snapshot().await;
    eprintln!("metrics: {snapshot:?}");
    let peers = reporting.connected_peers().await;
    eprintln!("connected peers: {peers:?}");
    match reporting.errors_since(0, 256).await {
        Ok(errors) if errors.is_empty() => {
            eprintln!("buffered errors: <none>");
        }
        Ok(errors) => {
            eprintln!("buffered errors:");
            for (seq, err) in errors {
                eprintln!("  - {seq}: {err}");
            }
        }
        Err(err) => {
            eprintln!("buffered errors unavailable: {err}");
        }
    }
}

fn spawn_event_capture(
    reporting: &crate::observability::DomusReporting,
) -> (
    tokio::task::JoinHandle<()>,
    Arc<tokio::sync::Mutex<Vec<crate::observability::DomusReportingEvent>>>,
) {
    let mut rx = reporting.subscribe_events();
    let events = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let events_clone = Arc::clone(&events);
    let handle = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    let mut guard = events_clone.lock().await;
                    guard.push(event);
                    let excess = guard.len().saturating_sub(256);
                    if excess > 0 {
                        guard.drain(0..excess);
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });
    (handle, events)
}

async fn dump_events(
    label: &str,
    events: &tokio::sync::Mutex<Vec<crate::observability::DomusReportingEvent>>,
) {
    let guard = events.lock().await;
    if guard.is_empty() {
        eprintln!("=== events: {label}: <none> ===");
        return;
    }
    eprintln!("=== events: {label} (latest {}) ===", guard.len());
    for event in guard.iter() {
        eprintln!("  - {event:?}");
    }
}

fn build_ca() -> Certificate {
    let mut params = CertificateParams::new(Vec::new());
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    Certificate::from_params(params).expect("ca cert")
}

fn build_domus_cert(ca: &Certificate, addr: SocketAddr) -> (Vec<u8>, Vec<u8>) {
    let mut params = CertificateParams::new(Vec::new());
    params.is_ca = IsCa::NoCa;
    let uri = format!("aurelia+tcp://{addr}");
    params.subject_alt_names.push(SanType::URI(uri));
    params.subject_alt_names.push(SanType::IpAddress(addr.ip()));
    let cert = Certificate::from_params(params).expect("domus cert");
    let cert_der = cert.serialize_der_with_signer(ca).expect("sign cert");
    let key_der = cert.serialize_private_key_der();
    (cert_der, key_der)
}

fn build_auth_and_client(ca: &Certificate, addr: SocketAddr) -> (Pkcs8AuthConfig, ClientConfig) {
    let (cert_der, key_der) = build_domus_cert(ca, addr);
    let ca_der = ca.serialize_der().expect("ca der");
    let auth = Pkcs8AuthConfig::Pkcs8Der(Pkcs8DerConfig {
        ca_der: ca_der.clone(),
        cert_der: cert_der.clone(),
        pkcs8_key_der: key_der.clone().into(),
    });

    let mut roots = RootCertStore::empty();
    roots.add(CertificateDer::from(ca_der)).expect("add root");

    let client_config = ClientConfig::builder()
        .with_root_certificates(roots.clone())
        .with_client_auth_cert(
            vec![CertificateDer::from(cert_der)],
            PrivateKeyDer::from(PrivatePkcs8KeyDer::from(key_der)),
        )
        .expect("client config");

    (auth, client_config)
}

fn build_server_config(ca: &Certificate, addr: SocketAddr) -> ServerConfig {
    let (cert_der, key_der) = build_domus_cert(ca, addr);
    let ca_der = ca.serialize_der().expect("ca der");
    let mut roots = RootCertStore::empty();
    roots.add(CertificateDer::from(ca_der)).expect("add root");
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .expect("verifier");
    let server_key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(key_der));
    ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![CertificateDer::from(cert_der)], server_key)
        .expect("server config")
}

fn build_auth(ca: &Certificate, addr: SocketAddr) -> Pkcs8AuthConfig {
    build_auth_and_client(ca, addr).0
}

fn pick_addr(ip: IpAddr) -> SocketAddr {
    TcpListener::bind(SocketAddr::new(ip, 0))
        .expect("bind temp listener")
        .local_addr()
        .expect("local addr")
}

async fn write_frame<S: AsyncWriteExt + Unpin>(stream: &mut S, header: WireHeader, payload: &[u8]) {
    let encoded = header.encode();
    stream.write_all(&encoded).await.expect("write header");
    if !payload.is_empty() {
        stream.write_all(payload).await.expect("write payload");
    }
    stream.flush().await.expect("flush");
}

async fn read_frame<S: AsyncReadExt + Unpin>(stream: &mut S) -> Option<(WireHeader, Vec<u8>)> {
    let mut header_buf = [0u8; WireHeader::LEN];
    match stream.read_exact(&mut header_buf).await {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return None,
        Err(err) => panic!("read header failed: {err}"),
    }
    let header = WireHeader::decode(&header_buf).expect("decode header");
    let mut payload = vec![0u8; header.payload_len as usize];
    if !payload.is_empty() {
        stream.read_exact(&mut payload).await.expect("read payload");
    }
    Some((header, payload))
}

const TCP_AUTH_INIT: u8 = 1;
const TCP_CALLBACK_INIT: u8 = 2;
const TCP_AUTH_CHALLENGE: u8 = 3;
const TCP_AUTH_PROOF: u8 = 4;
const TCP_NONCE_LEN: usize = 32;
const CONCURRENCY_SEND_TIMEOUT: Duration = Duration::from_millis(1500);
const CONCURRENCY_ACCEPT_TIMEOUT: Duration = Duration::from_millis(250);
const CONCURRENCY_SEND_JOIN_TIMEOUT: Duration = Duration::from_millis(1500);
const CONCURRENCY_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(1000);

fn fast_concurrency_config() -> DomusConfig {
    DomusConfig {
        listener_delay: Duration::from_millis(0),
        listener_reconnect_timeout: Duration::from_millis(100),
        send_timeout: CONCURRENCY_SEND_TIMEOUT,
        accept_timeout: CONCURRENCY_ACCEPT_TIMEOUT,
        ..Default::default()
    }
}

async fn await_send_join(
    src_peer: usize,
    dst_peer: usize,
    msg_type: u32,
    handle: tokio::task::JoinHandle<Result<(), AureliaError>>,
) {
    let result = timeout(CONCURRENCY_SEND_JOIN_TIMEOUT, handle)
        .await
        .unwrap_or_else(|_| {
            panic!("send join timeout src_peer={src_peer} dst_peer={dst_peer} msg_type={msg_type}")
        })
        .unwrap_or_else(|err| {
            panic!("send task panicked src_peer={src_peer} dst_peer={dst_peer} msg_type={msg_type}: {err}")
        });
    result.unwrap_or_else(|err| {
        panic!("send failed src_peer={src_peer} dst_peer={dst_peer} msg_type={msg_type}: {err}")
    });
}

async fn shutdown_transport_within(label: &str, transport: Arc<Transport>) {
    timeout(CONCURRENCY_SHUTDOWN_TIMEOUT, transport.shutdown())
        .await
        .unwrap_or_else(|_| panic!("shutdown timeout for {label}"));
}

async fn read_exact_array<S: AsyncReadExt + Unpin, const N: usize>(stream: &mut S) -> [u8; N] {
    let mut buf = [0u8; N];
    stream.read_exact(&mut buf).await.expect("read exact");
    buf
}

async fn write_type<S: AsyncWriteExt + Unpin>(stream: &mut S, value: u8) {
    stream.write_all(&[value]).await.expect("write type");
    stream.flush().await.expect("flush");
}

async fn write_auth_init<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    nonce_a_cb: [u8; TCP_NONCE_LEN],
) {
    write_type(stream, TCP_AUTH_INIT).await;
    stream
        .write_all(&nonce_a_cb)
        .await
        .expect("write nonce_a_cb");
    stream.flush().await.expect("flush");
}

async fn write_auth_proof<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    echo_nonce_b_cb: [u8; TCP_NONCE_LEN],
) {
    write_type(stream, TCP_AUTH_PROOF).await;
    stream
        .write_all(&echo_nonce_b_cb)
        .await
        .expect("write proof");
    stream.flush().await.expect("flush");
}

async fn accept_tcp_callback(
    listener: tokio::net::TcpListener,
    server_config: Arc<ServerConfig>,
    expected_nonce_a_cb: [u8; TCP_NONCE_LEN],
) -> [u8; TCP_NONCE_LEN] {
    let acceptor = TlsAcceptor::from(server_config);
    let (socket, _) = listener.accept().await.expect("callback accept");
    let mut stream = acceptor.accept(socket).await.expect("callback tls");
    let msg_type = read_exact_array::<_, 1>(&mut stream).await[0];
    assert_eq!(msg_type, TCP_CALLBACK_INIT);
    let nonce_b_cb = read_exact_array::<_, TCP_NONCE_LEN>(&mut stream).await;
    let echo_nonce_a_cb = read_exact_array::<_, TCP_NONCE_LEN>(&mut stream).await;
    assert_eq!(echo_nonce_a_cb, expected_nonce_a_cb);
    let _ = stream.shutdown().await;
    nonce_b_cb
}

#[tokio::test]
async fn transport_remote_delivery_with_ack() {
    tokio::time::timeout(TRANSPORT_TEST_TIMEOUT, async {
        let ca = build_ca();
        let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

        let registry_a = Arc::new(TabernaRegistry::new());
        let registry_b = Arc::new(TabernaRegistry::new());
        let msg_type = a3_message_type(100);
        let sink = Arc::new(RecordingSink::new(vec![msg_type]));
        let taberna_id = 42;
        registry_b.register(taberna_id, sink.clone()).await.unwrap();

        let cfg = DomusConfig {
            listener_delay: Duration::from_millis(0),
            ..Default::default()
        };
        let config_a = DomusConfigAccess::from_config(cfg.clone());
        let config_b = DomusConfigAccess::from_config(cfg);
        let config_a_access: DomusConfigAccess = config_a.clone();
        let config_b_access: DomusConfigAccess = config_b.clone();
        let addr_b = pick_addr(domus_ip);
        let addr_a = pick_addr(domus_ip);
        let transport_b = Transport::bind(
            DomusAddr::Tcp(addr_b),
            registry_b,
            config_b_access,
            new_observability(tokio::runtime::Handle::current()).1,
            tokio::runtime::Handle::current(),
            build_auth(&ca, addr_b),
        )
        .await
        .expect("bind transport b");
        let _handle_b = transport_b.start().await.expect("start b");
        let transport_b = Arc::new(transport_b);

        let transport_a = Transport::bind(
            DomusAddr::Tcp(addr_a),
            registry_a.clone(),
            config_a_access,
            new_observability(tokio::runtime::Handle::current()).1,
            tokio::runtime::Handle::current(),
            build_auth(&ca, addr_a),
        )
        .await
        .expect("bind transport a");
        transport_a.start().await.expect("start a");
        let transport_a = Arc::new(transport_a);

        let resolver = Arc::new(StaticResolver::new(taberna_id, DomusAddr::Tcp(addr_b)));

        let peering = Arc::new(RouteLocalRemote::new(
            config_a,
            registry_a,
            resolver,
            Arc::clone(&transport_a),
        ));

        let codec = TestCodec;
        let outcome = peering
            .send(
                &codec,
                taberna_id,
                &test_message(msg_type, b"hello"),
                SendOptions::MESSAGE_ONLY,
            )
            .await
            .expect("send");
        assert!(matches!(outcome, SendOutcome::MessageOnly));

        let received = sink.take().await;
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].0, msg_type);
        assert_eq!(received[0].1, Bytes::from_static(b"hello"));

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    })
    .await
    .expect("async test timed out");
}

/// Four senders concurrently dispatch one message each into a single taberna across one
/// primary callis. Asserts no head-of-line blocking and that all four deliveries complete
/// within a tight latency budget. Locks in the missed-notify fix from the
/// `concurrency.md` Pattern 1 migration of the per-callis receive loop.
#[tokio::test]
async fn transport_primary_parallel_4_senders_to_one_receiver() {
    tokio::time::timeout(TRANSPORT_TEST_TIMEOUT, async {
        let ca = build_ca();
        let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

        let registry_a = Arc::new(TabernaRegistry::new());
        let registry_b = Arc::new(TabernaRegistry::new());
        let msg_types: Vec<u32> = (200..204).map(a3_message_type).collect();
        let sink = Arc::new(RecordingSink::new(msg_types.clone()));
        let taberna_id: TabernaId = 200;
        registry_b.register(taberna_id, sink.clone()).await.unwrap();

        let cfg = DomusConfig {
            listener_delay: Duration::from_millis(0),
            ..Default::default()
        };
        let config_a = DomusConfigAccess::from_config(cfg.clone());
        let config_b = DomusConfigAccess::from_config(cfg);
        let addr_b = pick_addr(domus_ip);
        let addr_a = pick_addr(domus_ip);
        let transport_b = Transport::bind(
            DomusAddr::Tcp(addr_b),
            registry_b,
            config_b,
            new_observability(tokio::runtime::Handle::current()).1,
            tokio::runtime::Handle::current(),
            build_auth(&ca, addr_b),
        )
        .await
        .expect("bind transport b");
        transport_b.start().await.expect("start b");
        let transport_b = Arc::new(transport_b);

        let transport_a = Transport::bind(
            DomusAddr::Tcp(addr_a),
            registry_a.clone(),
            config_a.clone(),
            new_observability(tokio::runtime::Handle::current()).1,
            tokio::runtime::Handle::current(),
            build_auth(&ca, addr_a),
        )
        .await
        .expect("bind transport a");
        transport_a.start().await.expect("start a");
        let transport_a = Arc::new(transport_a);

        let resolver = Arc::new(StaticResolver::new(taberna_id, DomusAddr::Tcp(addr_b)));
        let peering = Arc::new(RouteLocalRemote::new(
            config_a,
            registry_a,
            resolver,
            Arc::clone(&transport_a),
        ));

        let started_at = tokio::time::Instant::now();
        let mut handles = Vec::new();
        for msg_type in msg_types.iter().copied() {
            let peering = Arc::clone(&peering);
            handles.push(tokio::spawn(async move {
                peering
                    .send(
                        &TestCodec,
                        taberna_id,
                        &test_message(msg_type, b"parallel"),
                        SendOptions::MESSAGE_ONLY,
                    )
                    .await
                    .map(|_| msg_type)
            }));
        }

        let mut delivered = Vec::new();
        for handle in handles {
            let msg_type = handle.await.expect("join").expect("send");
            delivered.push(msg_type);
        }
        let elapsed = started_at.elapsed();
        assert!(
            elapsed < CONCURRENCY_SEND_JOIN_TIMEOUT,
            "4 parallel sends should complete within the local loopback budget, took {elapsed:?}"
        );

        let received = sink.wait_for(4, Duration::from_millis(500)).await;
        let mut got: Vec<u32> = received.into_iter().map(|(t, _, _)| t).collect();
        got.sort();
        delivered.sort();
        assert_eq!(got, msg_types);
        assert_eq!(delivered, msg_types);

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    })
    .await
    .expect("async test timed out");
}

/// Same shape as `transport_primary_parallel_4_senders_to_one_receiver`, but uses A3
/// application message IDs and bounds send joins and shutdown so broken concurrent
/// dispatch cannot hide behind the default transport timeout.
#[tokio::test]
async fn transport_primary_parallel_4_senders_to_one_receiver_a3() {
    tokio::time::timeout(TRANSPORT_TEST_TIMEOUT, async {
        let ca = build_ca();
        let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

        let registry_a = Arc::new(TabernaRegistry::new());
        let registry_b = Arc::new(TabernaRegistry::new());
        let msg_types: Vec<u32> = (0..4).map(|offset| a3_message_type(200 + offset)).collect();
        let sink = Arc::new(RecordingSink::new(msg_types.clone()));
        let taberna_id: TabernaId = 200;
        registry_b.register(taberna_id, sink.clone()).await.unwrap();

        let cfg = fast_concurrency_config();
        let config_a = DomusConfigAccess::from_config(cfg.clone());
        let config_b = DomusConfigAccess::from_config(cfg);
        let addr_b = pick_addr(domus_ip);
        let addr_a = pick_addr(domus_ip);
        let transport_b = Transport::bind(
            DomusAddr::Tcp(addr_b),
            registry_b,
            config_b,
            new_observability(tokio::runtime::Handle::current()).1,
            tokio::runtime::Handle::current(),
            build_auth(&ca, addr_b),
        )
        .await
        .expect("bind transport b");
        transport_b.start().await.expect("start b");
        let transport_b = Arc::new(transport_b);

        let transport_a = Transport::bind(
            DomusAddr::Tcp(addr_a),
            registry_a.clone(),
            config_a.clone(),
            new_observability(tokio::runtime::Handle::current()).1,
            tokio::runtime::Handle::current(),
            build_auth(&ca, addr_a),
        )
        .await
        .expect("bind transport a");
        transport_a.start().await.expect("start a");
        let transport_a = Arc::new(transport_a);

        let resolver = Arc::new(StaticResolver::new(taberna_id, DomusAddr::Tcp(addr_b)));
        let peering = Arc::new(RouteLocalRemote::new(
            config_a,
            registry_a,
            resolver,
            Arc::clone(&transport_a),
        ));

        let started_at = tokio::time::Instant::now();
        let mut handles = Vec::new();
        for msg_type in msg_types.iter().copied() {
            let peering = Arc::clone(&peering);
            let handle = tokio::spawn(async move {
                peering
                    .send(
                        &TestCodec,
                        taberna_id,
                        &test_message(msg_type, b"parallel-a3"),
                        SendOptions::MESSAGE_ONLY,
                    )
                    .await
                    .map(|_| ())
            });
            handles.push((0, 1, msg_type, handle));
        }

        for (src_peer, dst_peer, msg_type, handle) in handles {
            await_send_join(src_peer, dst_peer, msg_type, handle).await;
        }
        let elapsed = started_at.elapsed();
        assert!(
        elapsed < CONCURRENCY_SEND_JOIN_TIMEOUT,
        "4 A3 parallel sends should complete within the local loopback budget, took {elapsed:?}"
    );

        let received = sink.wait_for(4, Duration::from_millis(500)).await;
        let mut got: Vec<u32> = received.into_iter().map(|(t, _, _)| t).collect();
        got.sort();
        assert_eq!(got, msg_types);

        shutdown_transport_within("peer-a", Arc::clone(&transport_a)).await;
        shutdown_transport_within("peer-b", Arc::clone(&transport_b)).await;
    })
    .await
    .expect("async test timed out");
}

/// Cold A3 send through a not-yet-connected peer. This isolates dial, TLS/auth,
/// A1 hello, snapshot publication, primary dispatch, delivery, and ACK for one
/// message without the additional contention from concurrent senders.
#[tokio::test]
async fn transport_primary_cold_dial_single_a3() {
    tokio::time::timeout(TRANSPORT_TEST_TIMEOUT, async {
        let ca = build_ca();
        let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

        let registry_a = Arc::new(TabernaRegistry::new());
        let registry_b = Arc::new(TabernaRegistry::new());
        let msg_type = a3_message_type(180);
        let sink = Arc::new(RecordingSink::new(vec![msg_type]));
        let taberna_id: TabernaId = 180;
        registry_b.register(taberna_id, sink.clone()).await.unwrap();

        let cfg = fast_concurrency_config();
        let config_a = DomusConfigAccess::from_config(cfg.clone());
        let config_b = DomusConfigAccess::from_config(cfg);
        let addr_b = pick_addr(domus_ip);
        let addr_a = pick_addr(domus_ip);
        let transport_b = Transport::bind(
            DomusAddr::Tcp(addr_b),
            registry_b,
            config_b,
            new_observability(tokio::runtime::Handle::current()).1,
            tokio::runtime::Handle::current(),
            build_auth(&ca, addr_b),
        )
        .await
        .expect("bind transport b");
        transport_b.start().await.expect("start b");
        let transport_b = Arc::new(transport_b);

        let transport_a = Transport::bind(
            DomusAddr::Tcp(addr_a),
            registry_a.clone(),
            config_a.clone(),
            new_observability(tokio::runtime::Handle::current()).1,
            tokio::runtime::Handle::current(),
            build_auth(&ca, addr_a),
        )
        .await
        .expect("bind transport a");
        transport_a.start().await.expect("start a");
        let transport_a = Arc::new(transport_a);

        let resolver = Arc::new(StaticResolver::new(taberna_id, DomusAddr::Tcp(addr_b)));
        let peering =
            RouteLocalRemote::new(config_a, registry_a, resolver, Arc::clone(&transport_a));

        let started_at = tokio::time::Instant::now();
        peering
            .send(
                &TestCodec,
                taberna_id,
                &test_message(msg_type, b"cold-a3"),
                SendOptions::MESSAGE_ONLY,
            )
            .await
            .expect("cold a3 send");
        let elapsed = started_at.elapsed();
        assert!(
            elapsed < CONCURRENCY_SEND_JOIN_TIMEOUT,
            "cold A3 send should complete within the local loopback budget, took {elapsed:?}"
        );

        let received = sink.wait_for(1, Duration::from_millis(500)).await;
        assert_eq!(received[0].0, msg_type);

        shutdown_transport_within("peer-a", Arc::clone(&transport_a)).await;
        shutdown_transport_within("peer-b", Arc::clone(&transport_b)).await;
    })
    .await
    .expect("async test timed out");
}

/// A3 parallel dispatch after the primary callis has already been established.
/// This isolates primary queue progress, callis writer availability, receiver
/// delivery, and ACK return from cold dial and handshake timing.
#[tokio::test]
async fn transport_primary_parallel_a3_after_warm_callis() {
    tokio::time::timeout(TRANSPORT_TEST_TIMEOUT, async {
    let ca = build_ca();
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let registry_a = Arc::new(TabernaRegistry::new());
    let registry_b = Arc::new(TabernaRegistry::new());
    let warmup_msg_type = a3_message_type(239);
    let msg_types: Vec<u32> = (0..4).map(|offset| a3_message_type(240 + offset)).collect();
    let mut expected = msg_types.clone();
    expected.push(warmup_msg_type);
    let sink = Arc::new(RecordingSink::new(expected));
    let taberna_id: TabernaId = 240;
    registry_b.register(taberna_id, sink.clone()).await.unwrap();

    let cfg = fast_concurrency_config();
    let config_a = DomusConfigAccess::from_config(cfg.clone());
    let config_b = DomusConfigAccess::from_config(cfg);
    let addr_b = pick_addr(domus_ip);
    let addr_a = pick_addr(domus_ip);
    let transport_b = Transport::bind(
        DomusAddr::Tcp(addr_b),
        registry_b,
        config_b,
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_b),
    )
    .await
    .expect("bind transport b");
    transport_b.start().await.expect("start b");
    let transport_b = Arc::new(transport_b);

    let transport_a = Transport::bind(
        DomusAddr::Tcp(addr_a),
        registry_a.clone(),
        config_a.clone(),
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_a),
    )
    .await
    .expect("bind transport a");
    transport_a.start().await.expect("start a");
    let transport_a = Arc::new(transport_a);

    let resolver = Arc::new(StaticResolver::new(taberna_id, DomusAddr::Tcp(addr_b)));
    let peering = Arc::new(RouteLocalRemote::new(
        config_a,
        registry_a,
        resolver,
        Arc::clone(&transport_a),
    ));

    peering
        .send(
            &TestCodec,
            taberna_id,
            &test_message(warmup_msg_type, b"warmup-a3"),
            SendOptions::MESSAGE_ONLY,
        )
        .await
        .expect("warmup a3 send");

    let started_at = tokio::time::Instant::now();
    let mut handles = Vec::new();
    for msg_type in msg_types.iter().copied() {
        let peering = Arc::clone(&peering);
        let handle = tokio::spawn(async move {
            peering
                .send(
                    &TestCodec,
                    taberna_id,
                    &test_message(msg_type, b"warm-a3"),
                    SendOptions::MESSAGE_ONLY,
                )
                .await
                .map(|_| ())
        });
        handles.push((0, 1, msg_type, handle));
    }

    for (src_peer, dst_peer, msg_type, handle) in handles {
        await_send_join(src_peer, dst_peer, msg_type, handle).await;
    }
    let elapsed = started_at.elapsed();
    assert!(
        elapsed < CONCURRENCY_SEND_JOIN_TIMEOUT,
        "4 warm A3 parallel sends should complete within the local loopback budget, took {elapsed:?}"
    );

    let received = sink.wait_for(5, Duration::from_millis(500)).await;
    let mut got: Vec<u32> = received.into_iter().map(|(t, _, _)| t).collect();
    got.sort();
    let mut expected = msg_types;
    expected.push(warmup_msg_type);
    expected.sort();
    assert_eq!(got, expected);

    shutdown_transport_within("peer-a", Arc::clone(&transport_a)).await;
    shutdown_transport_within("peer-b", Arc::clone(&transport_b)).await;

    })
    .await
    .expect("async test timed out");
}

/// Twenty serial sends through one peer's primary callis after warmup. Asserts the
/// primary transmitter progresses purely on retained outbound notifications
/// rather than waiting on the 200ms keepalive tick. If progress depended on the tick,
/// total time would cluster around N × 100ms; with proper signalling, total time is
/// well under 500ms.
#[tokio::test]
async fn transport_primary_progress_independent_of_keepalive_tick() {
    tokio::time::timeout(TRANSPORT_TEST_TIMEOUT, async {
        let ca = build_ca();
        let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

        let registry_a = Arc::new(TabernaRegistry::new());
        let registry_b = Arc::new(TabernaRegistry::new());
        const N: u32 = 20;
        let msg_types: Vec<u32> = (500..(500 + N + 1)).map(a3_message_type).collect(); // +1 for warmup at 500
        let sink = Arc::new(RecordingSink::new(msg_types.clone()));
        let taberna_id: TabernaId = 500;
        registry_b.register(taberna_id, sink.clone()).await.unwrap();

        let cfg = DomusConfig {
            listener_delay: Duration::from_millis(0),
            keepalive_interval: Duration::from_millis(200),
            ..fast_concurrency_config()
        };
        let config_a = DomusConfigAccess::from_config(cfg.clone());
        let config_b = DomusConfigAccess::from_config(cfg);
        let addr_b = pick_addr(domus_ip);
        let addr_a = pick_addr(domus_ip);
        let transport_b = Transport::bind(
            DomusAddr::Tcp(addr_b),
            registry_b,
            config_b,
            new_observability(tokio::runtime::Handle::current()).1,
            tokio::runtime::Handle::current(),
            build_auth(&ca, addr_b),
        )
        .await
        .expect("bind transport b");
        transport_b.start().await.expect("start b");
        let transport_b = Arc::new(transport_b);

        let transport_a = Transport::bind(
            DomusAddr::Tcp(addr_a),
            registry_a.clone(),
            config_a.clone(),
            new_observability(tokio::runtime::Handle::current()).1,
            tokio::runtime::Handle::current(),
            build_auth(&ca, addr_a),
        )
        .await
        .expect("bind transport a");
        transport_a.start().await.expect("start a");
        let transport_a = Arc::new(transport_a);

        let resolver = Arc::new(StaticResolver::new(taberna_id, DomusAddr::Tcp(addr_b)));
        let peering =
            RouteLocalRemote::new(config_a, registry_a, resolver, Arc::clone(&transport_a));

        // Warmup: establish the primary callis so the timed loop measures only dispatch.
        peering
            .send(
                &TestCodec,
                taberna_id,
                &test_message(a3_message_type(500), b"warmup"),
                SendOptions::MESSAGE_ONLY,
            )
            .await
            .expect("warmup send");

        let started_at = tokio::time::Instant::now();
        for i in 0..N {
            let msg_type = a3_message_type(501 + i);
            peering
                .send(
                    &TestCodec,
                    taberna_id,
                    &test_message(msg_type, b"burst"),
                    SendOptions::MESSAGE_ONLY,
                )
                .await
                .expect("send");
        }
        let elapsed = started_at.elapsed();
        assert!(
            elapsed < Duration::from_millis(500),
            "{N} serial sends after warmup should complete in <500ms, took {elapsed:?} \
         (latency would cluster at ~100ms/msg if progress depended on the keepalive tick)"
        );

        let received = sink
            .wait_for(N as usize + 1, Duration::from_millis(500))
            .await;
        assert_eq!(received.len(), N as usize + 1);

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    })
    .await
    .expect("async test timed out");
}

/// Sixteen concurrent sends through one peer's single primary callis. Asserts the
/// dispatcher does not head-of-line-block and the per-callis receive loop drains all
/// acks promptly.
#[tokio::test]
async fn transport_primary_burst_single_callis() {
    tokio::time::timeout(TRANSPORT_TEST_TIMEOUT, async {
        let ca = build_ca();
        let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

        const BURST: u32 = 16;
        let registry_a = Arc::new(TabernaRegistry::new());
        let registry_b = Arc::new(TabernaRegistry::new());
        let msg_types: Vec<u32> = (300..(300 + BURST)).map(a3_message_type).collect();
        let sink = Arc::new(RecordingSink::new(msg_types.clone()));
        let taberna_id: TabernaId = 300;
        registry_b.register(taberna_id, sink.clone()).await.unwrap();

        let cfg = DomusConfig {
            listener_delay: Duration::from_millis(0),
            ..fast_concurrency_config()
        };
        let config_a = DomusConfigAccess::from_config(cfg.clone());
        let config_b = DomusConfigAccess::from_config(cfg);
        let addr_b = pick_addr(domus_ip);
        let addr_a = pick_addr(domus_ip);
        let transport_b = Transport::bind(
            DomusAddr::Tcp(addr_b),
            registry_b,
            config_b,
            new_observability(tokio::runtime::Handle::current()).1,
            tokio::runtime::Handle::current(),
            build_auth(&ca, addr_b),
        )
        .await
        .expect("bind transport b");
        transport_b.start().await.expect("start b");
        let transport_b = Arc::new(transport_b);

        let transport_a = Transport::bind(
            DomusAddr::Tcp(addr_a),
            registry_a.clone(),
            config_a.clone(),
            new_observability(tokio::runtime::Handle::current()).1,
            tokio::runtime::Handle::current(),
            build_auth(&ca, addr_a),
        )
        .await
        .expect("bind transport a");
        transport_a.start().await.expect("start a");
        let transport_a = Arc::new(transport_a);

        let resolver = Arc::new(StaticResolver::new(taberna_id, DomusAddr::Tcp(addr_b)));
        let peering = Arc::new(RouteLocalRemote::new(
            config_a,
            registry_a,
            resolver,
            Arc::clone(&transport_a),
        ));

        let started_at = tokio::time::Instant::now();
        let mut handles = Vec::new();
        for msg_type in msg_types.iter().copied() {
            let peering = Arc::clone(&peering);
            handles.push(tokio::spawn(async move {
                peering
                    .send(
                        &TestCodec,
                        taberna_id,
                        &test_message(msg_type, b"burst"),
                        SendOptions::MESSAGE_ONLY,
                    )
                    .await
                    .map(|_| msg_type)
            }));
        }
        for handle in handles {
            handle.await.expect("join").expect("send");
        }
        let elapsed = started_at.elapsed();
        assert!(
        elapsed < CONCURRENCY_SEND_JOIN_TIMEOUT,
        "{BURST} parallel sends should complete within the local loopback budget, took {elapsed:?}"
    );

        let received = sink
            .wait_for(BURST as usize, Duration::from_millis(500))
            .await;
        assert_eq!(received.len(), BURST as usize);

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn transport_primary_parallel_shutdown_completes_promptly() {
    let ca = build_ca();
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let registry_a = Arc::new(TabernaRegistry::new());
    let registry_b = Arc::new(TabernaRegistry::new());
    let msg_type = a3_message_type(700);
    let sink = Arc::new(RecordingSink::new(vec![msg_type]));
    let taberna_id: TabernaId = 700;
    registry_b.register(taberna_id, sink.clone()).await.unwrap();

    let cfg = fast_concurrency_config();
    let config_a = DomusConfigAccess::from_config(cfg.clone());
    let config_b = DomusConfigAccess::from_config(cfg);
    let addr_b = pick_addr(domus_ip);
    let addr_a = pick_addr(domus_ip);
    let transport_b = Transport::bind(
        DomusAddr::Tcp(addr_b),
        registry_b,
        config_b,
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_b),
    )
    .await
    .expect("bind transport b");
    transport_b.start().await.expect("start b");
    let transport_b = Arc::new(transport_b);

    let transport_a = Transport::bind(
        DomusAddr::Tcp(addr_a),
        registry_a.clone(),
        config_a.clone(),
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_a),
    )
    .await
    .expect("bind transport a");
    transport_a.start().await.expect("start a");
    let transport_a = Arc::new(transport_a);

    let resolver = Arc::new(StaticResolver::new(taberna_id, DomusAddr::Tcp(addr_b)));
    let peering = RouteLocalRemote::new(config_a, registry_a, resolver, Arc::clone(&transport_a));

    let outcome = peering
        .send(
            &TestCodec,
            taberna_id,
            &test_message(msg_type, b"shutdown"),
            SendOptions::MESSAGE_ONLY,
        )
        .await
        .expect("send");
    assert!(matches!(outcome, SendOutcome::MessageOnly));

    let received = sink.wait_for(1, Duration::from_millis(500)).await;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].0, msg_type);

    let started_at = tokio::time::Instant::now();
    timeout(CONCURRENCY_SHUTDOWN_TIMEOUT, async {
        tokio::join!(transport_a.shutdown(), transport_b.shutdown());
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "shutdown waited longer than {CONCURRENCY_SHUTDOWN_TIMEOUT:?}; it must not exhaust \
             the 2 * send_timeout callis-drain fallback"
        )
    });
    assert!(
        started_at.elapsed() < CONCURRENCY_SEND_TIMEOUT,
        "shutdown should complete before send_timeout, took {:?}",
        started_at.elapsed()
    );
}

/// Full-mesh test: four peers, every peer sends one message to every other peer (12
/// messages). Locks in primary-only N-to-N concurrent delivery with a tight budget.
#[tokio::test]
#[allow(clippy::needless_range_loop)]
async fn transport_primary_parallel_full_mesh_4_peers() {
    tokio::time::timeout(TRANSPORT_TEST_TIMEOUT, async {
    let ca = build_ca();
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
    const N: usize = 4;

    let msg_types: Vec<u32> = (0..((N as u32) * (N as u32)))
        .map(|offset| a3_message_type(400 + offset))
        .collect();
    let taberna_base: TabernaId = 400;
    let mut registries: Vec<Arc<TabernaRegistry>> = Vec::with_capacity(N);
    let mut sinks: Vec<Arc<RecordingSink>> = Vec::with_capacity(N);
    let mut addrs: Vec<DomusAddr> = Vec::with_capacity(N);
    let mut transports: Vec<Arc<Transport>> = Vec::with_capacity(N);

    let cfg = fast_concurrency_config();

    for i in 0..N {
        let registry = Arc::new(TabernaRegistry::new());
        let sink = Arc::new(RecordingSink::new(msg_types.clone()));
        let taberna_id: TabernaId = taberna_base + i as TabernaId;
        registry.register(taberna_id, sink.clone()).await.unwrap();
        let addr = pick_addr(domus_ip);
        let transport = Transport::bind(
            DomusAddr::Tcp(addr),
            Arc::clone(&registry),
            DomusConfigAccess::from_config(cfg.clone()),
            new_observability(tokio::runtime::Handle::current()).1,
            tokio::runtime::Handle::current(),
            build_auth(&ca, addr),
        )
        .await
        .expect("bind");
        transport.start().await.expect("start");
        registries.push(registry);
        sinks.push(sink);
        addrs.push(DomusAddr::Tcp(addr));
        transports.push(Arc::new(transport));
    }

    struct TableResolver {
        table: Vec<(TabernaId, DomusAddr)>,
    }
    #[async_trait::async_trait]
    impl aurelia_data::RouteResolver for TableResolver {
        async fn resolve(
            &self,
            taberna_id: TabernaId,
        ) -> Result<DomusAddr, aurelia_ids::AureliaError> {
            self.table
                .iter()
                .find(|(id, _)| *id == taberna_id)
                .map(|(_, addr)| addr.clone())
                .ok_or_else(|| aurelia_ids::AureliaError::new(aurelia_ids::ErrorId::UnknownTaberna))
        }
    }
    let table: Vec<(TabernaId, DomusAddr)> = (0..N)
        .map(|j| (taberna_base + j as TabernaId, addrs[j].clone()))
        .collect();

    let mut peerings: Vec<Arc<crate::peering::RouteLocalRemote<TableResolver>>> =
        Vec::with_capacity(N);
    for i in 0..N {
        let resolver = Arc::new(TableResolver {
            table: table.clone(),
        });
        let peering = RouteLocalRemote::new(
            DomusConfigAccess::from_config(cfg.clone()),
            Arc::clone(&registries[i]),
            resolver,
            Arc::clone(&transports[i]),
        );
        peerings.push(Arc::new(peering));
    }

    let started_at = tokio::time::Instant::now();
    let mut handles = Vec::new();
    for i in 0..N {
        for j in 0..N {
            if i == j {
                continue;
            }
            let peering = Arc::clone(&peerings[i]);
            let dst_taberna: TabernaId = taberna_base + j as TabernaId;
            let msg_type = a3_message_type(400 + (i as u32) * (N as u32) + j as u32);
            let handle = tokio::spawn(async move {
                peering
                    .send(
                        &TestCodec,
                        dst_taberna,
                        &test_message(msg_type, b"mesh"),
                        SendOptions::MESSAGE_ONLY,
                    )
                    .await
                    .map(|_| ())
            });
            handles.push((i, j, msg_type, handle));
        }
    }

    for (src_peer, dst_peer, msg_type, handle) in handles {
        await_send_join(src_peer, dst_peer, msg_type, handle).await;
    }
    let elapsed = started_at.elapsed();
    assert!(
        elapsed < Duration::from_secs(3),
        "full-mesh 4-peer delivery should complete within the local loopback budget, took {elapsed:?}"
    );

    for j in 0..N {
        let got = sinks[j].wait_for(N - 1, Duration::from_millis(750)).await;
        assert_eq!(got.len(), N - 1, "peer {j} expected {} messages", N - 1);
    }

    for (peer, transport) in transports.into_iter().enumerate() {
        shutdown_transport_within(&format!("peer-{peer}"), transport).await;
    }

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
/// `reload_auth` swaps the originator's TLS material non-disruptively. Sends straddling the
/// reload all succeed on the same primary callis (no drop, no reconnect). The fresh cert
/// minted by the second `build_auth` proves that A1 admits a different (but still
/// CA-validated) certificate at the same peer address.
async fn transport_reload_auth_keeps_existing_connection_and_admits_new_cert() {
    let ca = build_ca();
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let registry_a = Arc::new(TabernaRegistry::new());
    let registry_b = Arc::new(TabernaRegistry::new());
    let msg_type = a3_message_type(90);
    let sink = Arc::new(RecordingSink::new(vec![msg_type]));
    let taberna_id = 900;
    registry_b.register(taberna_id, sink.clone()).await.unwrap();

    let cfg = DomusConfig {
        listener_delay: Duration::from_millis(0),
        ..Default::default()
    };
    let config_a = DomusConfigAccess::from_config(cfg.clone());
    let config_b = DomusConfigAccess::from_config(cfg);
    let config_a_access: DomusConfigAccess = config_a.clone();
    let config_b_access: DomusConfigAccess = config_b.clone();

    let addr_b = pick_addr(domus_ip);
    let addr_a = pick_addr(domus_ip);

    let transport_b = Transport::bind(
        DomusAddr::Tcp(addr_b),
        registry_b,
        config_b_access,
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_b),
    )
    .await
    .expect("bind transport b");
    let _handle_b = transport_b.start().await.expect("start b");
    let transport_b = Arc::new(transport_b);

    let transport_a = Transport::bind(
        DomusAddr::Tcp(addr_a),
        registry_a.clone(),
        config_a_access,
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_a),
    )
    .await
    .expect("bind transport a");
    transport_a.start().await.expect("start a");
    let transport_a = Arc::new(transport_a);

    let resolver = Arc::new(StaticResolver::new(taberna_id, DomusAddr::Tcp(addr_b)));
    let peering = RouteLocalRemote::new(config_a, registry_a, resolver, Arc::clone(&transport_a));

    let codec = TestCodec;
    let outcome = peering
        .send(
            &codec,
            taberna_id,
            &test_message(msg_type, b"first"),
            SendOptions::MESSAGE_ONLY,
        )
        .await
        .expect("first send");
    assert!(matches!(outcome, SendOutcome::MessageOnly));
    let received = sink.take().await;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].1, Bytes::from_static(b"first"));

    let new_auth = build_auth(&ca, addr_a);
    transport_a
        .reload_auth(new_auth)
        .await
        .expect("reload auth");

    let outcome = peering
        .send(
            &codec,
            taberna_id,
            &test_message(msg_type, b"second"),
            SendOptions::MESSAGE_ONLY,
        )
        .await
        .expect("second send");
    assert!(matches!(outcome, SendOutcome::MessageOnly));
    let received = sink.take().await;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].1, Bytes::from_static(b"second"));

    // A third send proves the rotation didn't tear down the session: if the breaker had
    // disconnected anything, the impaired window would still be visible here.
    let outcome = peering
        .send(
            &codec,
            taberna_id,
            &test_message(msg_type, b"third"),
            SendOptions::MESSAGE_ONLY,
        )
        .await
        .expect("third send");
    assert!(matches!(outcome, SendOutcome::MessageOnly));
    let received = sink.take().await;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].1, Bytes::from_static(b"third"));

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn transport_reconnect_replays_inflight() {
    let ca = build_ca();
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let registry_a = Arc::new(TabernaRegistry::new());
    let registry_b = Arc::new(TabernaRegistry::new());

    let started = Arc::new(Notify::new());
    let ready = Arc::new(Notify::new());
    let sink = Arc::new(BlockingInbox::new(
        vec![a3_message_type(90)],
        Arc::clone(&started),
        Arc::clone(&ready),
    ));
    let taberna_id = 55;
    registry_b.register(taberna_id, sink.clone()).await.unwrap();

    let cfg = fast_concurrency_config();
    let config_a = DomusConfigAccess::from_config(cfg.clone());
    let config_b = DomusConfigAccess::from_config(cfg.clone());
    let addr_b = pick_addr(domus_ip);
    let addr_a = pick_addr(domus_ip);

    let transport_b = Transport::bind(
        DomusAddr::Tcp(addr_b),
        Arc::clone(&registry_b),
        config_b.clone(),
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_b),
    )
    .await
    .expect("bind transport b");
    let handle_b = transport_b.start().await.expect("start b");
    let transport_b = Arc::new(transport_b);

    let transport_a = Transport::bind(
        DomusAddr::Tcp(addr_a),
        registry_a.clone(),
        config_a.clone(),
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_a),
    )
    .await
    .expect("bind transport a");
    transport_a.start().await.expect("start a");
    let transport_a = Arc::new(transport_a);

    let resolver = Arc::new(StaticResolver::new(taberna_id, DomusAddr::Tcp(addr_b)));

    let peering = Arc::new(RouteLocalRemote::new(
        config_a,
        registry_a,
        resolver,
        Arc::clone(&transport_a),
    ));

    let send_task = tokio::spawn(async move {
        let codec = TestCodec;
        peering
            .send(
                &codec,
                taberna_id,
                &test_message(a3_message_type(90), b"payload"),
                SendOptions::MESSAGE_ONLY,
            )
            .await
            .map_err(|err| err.kind)
    });

    timeout(Duration::from_millis(500), started.notified())
        .await
        .expect("started timeout");
    transport_b.shutdown().await;
    let _ = handle_b.await;
    drop(transport_b);

    let transport_b2 = Transport::bind(
        DomusAddr::Tcp(addr_b),
        Arc::clone(&registry_b),
        config_b.clone(),
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_b),
    )
    .await
    .expect("bind transport b2");
    transport_b2.start().await.expect("start b2");
    let transport_b2 = Arc::new(transport_b2);
    ready.notify_waiters();

    let result = timeout(Duration::from_secs(2), send_task)
        .await
        .expect("send timeout")
        .expect("join");
    assert!(matches!(
        result,
        Ok(SendOutcome::MessageOnly)
            | Err(ErrorId::SendTimeout)
            | Err(ErrorId::PeerRestarted)
            | Err(ErrorId::PeerUnavailable)
    ));

    transport_a.shutdown().await;
    transport_b2.shutdown().await;
}

#[tokio::test]
async fn transport_backpressure_send_queue_limit() {
    let ca = build_ca();
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let registry_a = Arc::new(TabernaRegistry::new());
    let registry_b = Arc::new(TabernaRegistry::new());

    let started = Arc::new(Notify::new());
    let ready = Arc::new(Notify::new());
    let first_msg_type = a3_message_type(20);
    let second_msg_type = a3_message_type(21);
    let sink = Arc::new(BlockingInbox::new(
        vec![first_msg_type, second_msg_type],
        Arc::clone(&started),
        Arc::clone(&ready),
    ));
    let taberna_id = 77;
    registry_b.register(taberna_id, sink.clone()).await.unwrap();

    let cfg = DomusConfig {
        send_timeout: Duration::from_millis(500),
        send_queue_size: 2,
        listener_delay: Duration::from_millis(0),
        ..Default::default()
    };
    let config_a = DomusConfigAccess::from_config(cfg.clone());
    let config_b = DomusConfigAccess::from_config(cfg.clone());
    let addr_b = pick_addr(domus_ip);
    let addr_a = pick_addr(domus_ip);

    let transport_b = Transport::bind(
        DomusAddr::Tcp(addr_b),
        registry_b,
        config_b,
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_b),
    )
    .await
    .expect("bind transport b");
    let _handle_b = transport_b.start().await.expect("start b");
    let transport_b = Arc::new(transport_b);

    let transport_a = Transport::bind(
        DomusAddr::Tcp(addr_a),
        registry_a.clone(),
        config_a.clone(),
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_a),
    )
    .await
    .expect("bind transport a");
    transport_a.start().await.expect("start a");
    let transport_a = Arc::new(transport_a);

    let resolver = Arc::new(StaticResolver::new(taberna_id, DomusAddr::Tcp(addr_b)));

    let peering = RouteLocalRemote::new(
        config_a.clone(),
        registry_a.clone(),
        resolver.clone(),
        Arc::clone(&transport_a),
    );
    let peering_send =
        RouteLocalRemote::new(config_a, registry_a, resolver, Arc::clone(&transport_a));

    let send_one = tokio::spawn(async move {
        let codec = TestCodec;
        peering_send
            .send(
                &codec,
                taberna_id,
                &test_message(first_msg_type, b"one"),
                SendOptions::MESSAGE_ONLY,
            )
            .await
            .map_err(|err| err.kind)
    });

    timeout(Duration::from_millis(500), started.notified())
        .await
        .expect("started timeout");
    let codec = TestCodec;
    let result_two = peering
        .send(
            &codec,
            taberna_id,
            &test_message(second_msg_type, b"two"),
            SendOptions::MESSAGE_ONLY,
        )
        .await;
    assert_eq!(result_two.unwrap_err().kind, ErrorId::SendTimeout);

    ready.notify_waiters();
    let _ = send_one.await;

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn transport_blocked_taberna_does_not_block_sibling_taberna() {
    let ca = build_ca();
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let registry_a = Arc::new(TabernaRegistry::new());
    let registry_b = Arc::new(TabernaRegistry::new());

    let started = Arc::new(Notify::new());
    let ready = Arc::new(Notify::new());
    let blocked_msg_type = a3_message_type(30);
    let fast_msg_type = a3_message_type(31);
    let blocked_sink = Arc::new(BlockingInbox::new(
        vec![blocked_msg_type],
        Arc::clone(&started),
        Arc::clone(&ready),
    ));
    let fast_sink = Arc::new(RecordingSink::new(vec![fast_msg_type]));
    let blocked_taberna = 333;
    let fast_taberna = 334;
    registry_b
        .register(blocked_taberna, blocked_sink)
        .await
        .unwrap();
    registry_b
        .register(fast_taberna, fast_sink.clone())
        .await
        .unwrap();

    let cfg = DomusConfig {
        send_timeout: Duration::from_millis(1000),
        accept_timeout: Duration::from_millis(250),
        listener_delay: Duration::from_millis(0),
        ..Default::default()
    };
    let config_a = DomusConfigAccess::from_config(cfg.clone());
    let config_b = DomusConfigAccess::from_config(cfg);
    let addr_b = pick_addr(domus_ip);
    let addr_a = pick_addr(domus_ip);

    let transport_b = Transport::bind(
        DomusAddr::Tcp(addr_b),
        registry_b,
        config_b,
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_b),
    )
    .await
    .expect("bind transport b");
    transport_b.start().await.expect("start b");
    let transport_b = Arc::new(transport_b);

    let transport_a = Transport::bind(
        DomusAddr::Tcp(addr_a),
        registry_a.clone(),
        config_a.clone(),
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_a),
    )
    .await
    .expect("bind transport a");
    transport_a.start().await.expect("start a");
    let transport_a = Arc::new(transport_a);

    let resolver = Arc::new(StaticResolver::from_routes([
        (blocked_taberna, DomusAddr::Tcp(addr_b)),
        (fast_taberna, DomusAddr::Tcp(addr_b)),
    ]));

    let peering = RouteLocalRemote::new(
        config_a.clone(),
        registry_a.clone(),
        resolver.clone(),
        Arc::clone(&transport_a),
    );
    let peering_blocked =
        RouteLocalRemote::new(config_a, registry_a, resolver, Arc::clone(&transport_a));

    let blocked_send = tokio::spawn(async move {
        let codec = TestCodec;
        peering_blocked
            .send(
                &codec,
                blocked_taberna,
                &test_message(blocked_msg_type, b"blocked"),
                SendOptions::MESSAGE_ONLY,
            )
            .await
            .map_err(|err| err.kind)
    });

    timeout(Duration::from_millis(500), started.notified())
        .await
        .expect("blocked taberna start");

    let codec = TestCodec;
    let outcome = peering
        .send(
            &codec,
            fast_taberna,
            &test_message(fast_msg_type, b"fast"),
            SendOptions::MESSAGE_ONLY,
        )
        .await
        .expect("fast taberna send");
    assert!(matches!(outcome, SendOutcome::MessageOnly));

    let received = fast_sink.take().await;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].0, fast_msg_type);
    assert_eq!(received[0].1, Bytes::from_static(b"fast"));

    ready.notify_waiters();
    let result = timeout(Duration::from_millis(1000), blocked_send)
        .await
        .expect("blocked send timeout")
        .expect("blocked send join");
    assert!(matches!(result, Ok(SendOutcome::MessageOnly)));

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn transport_peer_restart_invalidates_inflight() {
    let ca = build_ca();
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let registry_a = Arc::new(TabernaRegistry::new());
    let registry_b = Arc::new(TabernaRegistry::new());

    let started = Arc::new(Notify::new());
    let ready = Arc::new(Notify::new());
    let msg_type = a3_message_type(12);
    let sink = Arc::new(BlockingInbox::new(
        vec![msg_type],
        Arc::clone(&started),
        Arc::clone(&ready),
    ));
    let taberna_id = 222;
    registry_b.register(taberna_id, sink.clone()).await.unwrap();

    let cfg = DomusConfig {
        send_timeout: Duration::from_millis(1500),
        accept_timeout: Duration::from_millis(250),
        listener_delay: Duration::from_millis(0),
        ..Default::default()
    };
    let config_a = DomusConfigAccess::from_config(cfg.clone());
    let config_b = DomusConfigAccess::from_config(cfg.clone());
    let addr_b = pick_addr(domus_ip);
    let addr_a = pick_addr(domus_ip);

    let transport_b = Transport::bind(
        DomusAddr::Tcp(addr_b),
        registry_b,
        config_b,
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_b),
    )
    .await
    .expect("bind transport b");
    let handle_b = transport_b.start().await.expect("start b");
    let transport_b = Arc::new(transport_b);
    let peer_addr = addr_b;

    let transport_a = Transport::bind(
        DomusAddr::Tcp(addr_a),
        registry_a.clone(),
        config_a.clone(),
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_a),
    )
    .await
    .expect("bind transport a");
    transport_a.start().await.expect("start a");
    let transport_a = Arc::new(transport_a);

    let resolver = Arc::new(StaticResolver::new(taberna_id, DomusAddr::Tcp(peer_addr)));

    let peering = RouteLocalRemote::new(
        config_a.clone(),
        registry_a.clone(),
        resolver.clone(),
        Arc::clone(&transport_a),
    );

    let send_task = tokio::spawn(async move {
        let codec = TestCodec;
        peering
            .send(
                &codec,
                taberna_id,
                &test_message(msg_type, b"restart"),
                SendOptions::MESSAGE_ONLY,
            )
            .await
            .map_err(|err| err.kind)
    });

    timeout(Duration::from_millis(500), started.notified())
        .await
        .expect("started timeout");
    transport_b.shutdown().await;
    let _ = handle_b.await;
    drop(transport_b);

    let registry_b2 = Arc::new(TabernaRegistry::new());
    let transport_b2 = Transport::bind(
        DomusAddr::Tcp(peer_addr),
        registry_b2,
        DomusConfigAccess::from_config(cfg),
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, peer_addr),
    )
    .await
    .expect("bind transport b2");
    transport_b2.start().await.expect("start b2");

    let result = timeout(Duration::from_secs(2), send_task)
        .await
        .expect("send timeout")
        .expect("send join");
    assert!(matches!(
        result,
        Err(ErrorId::PeerRestarted) | Err(ErrorId::SendTimeout) | Err(ErrorId::PeerUnavailable)
    ));

    ready.notify_waiters();
    transport_a.shutdown().await;
    transport_b2.shutdown().await;
}

#[tokio::test]
async fn transport_blob_callis_rejected_without_primary() {
    tokio::time::timeout(TRANSPORT_TEST_TIMEOUT, async {
        let ca = build_ca();
        let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

        let registry_b = Arc::new(TabernaRegistry::new());
        let config_b = DomusConfigAccess::from_config(DomusConfig::default());
        let addr_b = pick_addr(domus_ip);
        let transport_b = Transport::bind(
            DomusAddr::Tcp(addr_b),
            registry_b,
            config_b,
            new_observability(tokio::runtime::Handle::current()).1,
            tokio::runtime::Handle::current(),
            build_auth(&ca, addr_b),
        )
        .await
        .expect("bind transport b");
        transport_b.start().await.expect("start b");
        let transport_b = Arc::new(transport_b);

        let addr_a = pick_addr(domus_ip);
        let (_auth_a, client_a) = build_auth_and_client(&ca, addr_a);
        let server_config_a = Arc::new(build_server_config(&ca, addr_a));
        let callback_listener = tokio::net::TcpListener::bind(addr_a)
            .await
            .expect("callback bind");
        let connector = TlsConnector::from(Arc::new(client_a));
        let tcp = TcpStream::connect(addr_b).await.expect("tcp connect");
        let server_name = ServerName::IpAddress(addr_b.ip().into());
        let mut stream = connector
            .connect(server_name, tcp)
            .await
            .expect("tls connect");

        let nonce_a_cb = [2u8; TCP_NONCE_LEN];
        let callback_task = tokio::spawn(accept_tcp_callback(
            callback_listener,
            Arc::clone(&server_config_a),
            nonce_a_cb,
        ));
        write_auth_init(&mut stream, nonce_a_cb).await;
        let nonce_b_cb = callback_task.await.expect("callback task");
        let msg_type = read_exact_array::<_, 1>(&mut stream).await[0];
        assert_eq!(msg_type, TCP_AUTH_CHALLENGE);
        write_auth_proof(&mut stream, nonce_b_cb).await;

        let hello = HelloPayload::Blob {
            chunk_size: 4,
            ack_window_chunks: 4,
        };
        let payload = hello.to_bytes();
        let header = WireHeader {
            version: PROTOCOL_VERSION,
            flags: WireFlags::BLOB.bits(),
            msg_type: MSG_HELLO,
            peer_msg_id: 1,
            src_taberna: 0,
            dst_taberna: 0,
            payload_len: payload.len() as u32,
        };
        write_frame(&mut stream, header, &payload).await;
        let (resp_header, resp_payload) = read_frame(&mut stream).await.expect("hello response");
        assert_eq!(resp_header.msg_type, MSG_ERROR);
        let error = ErrorPayload::from_bytes(&resp_payload).expect("error payload");
        assert_eq!(error.error_id, ErrorId::BlobCallisWithoutPrimary.as_u32());

        transport_b.shutdown().await;
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn tcp_pre_a1_connect_back_success() {
    tokio::time::timeout(TRANSPORT_TEST_TIMEOUT, async {
        let ca = build_ca();
        let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let registry_b = Arc::new(TabernaRegistry::new());
        let config_b = DomusConfigAccess::from_config(DomusConfig::default());
        let addr_b = pick_addr(domus_ip);
        let transport_b = Transport::bind(
            DomusAddr::Tcp(addr_b),
            registry_b,
            config_b,
            new_observability(tokio::runtime::Handle::current()).1,
            tokio::runtime::Handle::current(),
            build_auth(&ca, addr_b),
        )
        .await
        .expect("bind transport b");
        transport_b.start().await.expect("start b");
        let transport_b = Arc::new(transport_b);

        let addr_a = pick_addr(domus_ip);
        let (_auth_a, client_a) = build_auth_and_client(&ca, addr_a);
        let server_config_a = Arc::new(build_server_config(&ca, addr_a));
        let callback_listener = tokio::net::TcpListener::bind(addr_a)
            .await
            .expect("callback bind");
        let connector = TlsConnector::from(Arc::new(client_a));
        let tcp = TcpStream::connect(addr_b).await.expect("tcp connect");
        let server_name = ServerName::IpAddress(addr_b.ip().into());
        let mut stream = connector
            .connect(server_name, tcp)
            .await
            .expect("tls connect");

        let nonce_a_cb = [4u8; TCP_NONCE_LEN];
        let callback_task = tokio::spawn(accept_tcp_callback(
            callback_listener,
            Arc::clone(&server_config_a),
            nonce_a_cb,
        ));
        write_auth_init(&mut stream, nonce_a_cb).await;
        let nonce_b_cb = callback_task.await.expect("callback task");
        let msg_type = read_exact_array::<_, 1>(&mut stream).await[0];
        assert_eq!(msg_type, TCP_AUTH_CHALLENGE);
        write_auth_proof(&mut stream, nonce_b_cb).await;

        let hello = HelloPayload::Primary;
        let payload = hello.to_bytes();
        let header = WireHeader {
            version: PROTOCOL_VERSION,
            flags: 0,
            msg_type: MSG_HELLO,
            peer_msg_id: 1,
            src_taberna: 0,
            dst_taberna: 0,
            payload_len: payload.len() as u32,
        };
        write_frame(&mut stream, header, &payload).await;
        let (resp_header, resp_payload) = read_frame(&mut stream).await.expect("hello response");
        assert_eq!(resp_header.msg_type, MSG_HELLO_RESPONSE);
        let response = HelloPayload::from_bytes(&resp_payload).expect("hello payload");
        assert_eq!(response, HelloPayload::Primary);

        transport_b.shutdown().await;
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn tcp_pre_a1_callback_timeout_fails() {
    let ca = build_ca();
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let registry_b = Arc::new(TabernaRegistry::new());
    let cfg = DomusConfigBuilder::new()
        .tcp_callback_timeout(Duration::from_millis(50))
        .tcp_handshake_timeout(Duration::from_millis(200))
        .build()
        .expect("valid domus config");
    let config_b = DomusConfigAccess::from_config(cfg);
    let addr_b = pick_addr(domus_ip);
    let transport_b = Transport::bind(
        DomusAddr::Tcp(addr_b),
        registry_b,
        config_b,
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_b),
    )
    .await
    .expect("bind transport b");
    transport_b.start().await.expect("start b");
    let transport_b = Arc::new(transport_b);

    let addr_a = pick_addr(domus_ip);
    let (_auth_a, client_a) = build_auth_and_client(&ca, addr_a);
    let connector = TlsConnector::from(Arc::new(client_a));
    let tcp = TcpStream::connect(addr_b).await.expect("tcp connect");
    let server_name = ServerName::IpAddress(addr_b.ip().into());
    let mut stream = connector
        .connect(server_name, tcp)
        .await
        .expect("tls connect");

    let nonce_a_cb = [10u8; TCP_NONCE_LEN];
    write_auth_init(&mut stream, nonce_a_cb).await;

    let mut buf = [0u8; 1];
    let result = timeout(Duration::from_millis(400), stream.read_exact(&mut buf)).await;
    assert!(result.is_err() || result.unwrap().is_err());

    transport_b.shutdown().await;
}

#[tokio::test]
async fn tcp_pre_a1_callback_port_mismatch_fails() {
    let ca = build_ca();
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let registry_b = Arc::new(TabernaRegistry::new());
    let cfg = DomusConfigBuilder::new()
        .tcp_callback_timeout(Duration::from_millis(50))
        .tcp_handshake_timeout(Duration::from_millis(200))
        .build()
        .expect("valid domus config");
    let config_b = DomusConfigAccess::from_config(cfg);
    let addr_b = pick_addr(domus_ip);
    let transport_b = Transport::bind(
        DomusAddr::Tcp(addr_b),
        registry_b,
        config_b,
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_b),
    )
    .await
    .expect("bind transport b");
    transport_b.start().await.expect("start b");
    let transport_b = Arc::new(transport_b);

    let claimed_addr = pick_addr(domus_ip);
    let mut listener_addr = pick_addr(domus_ip);
    while listener_addr.port() == claimed_addr.port() {
        listener_addr = pick_addr(domus_ip);
    }

    let (_auth_a, client_a) = build_auth_and_client(&ca, claimed_addr);
    let server_config_a = Arc::new(build_server_config(&ca, claimed_addr));
    let callback_listener = tokio::net::TcpListener::bind(listener_addr)
        .await
        .expect("callback bind");
    let callback_task = tokio::spawn(async move {
        timeout(Duration::from_millis(200), async {
            let acceptor = TlsAcceptor::from(server_config_a);
            let (socket, _) = callback_listener.accept().await.expect("callback accept");
            let mut stream = acceptor.accept(socket).await.expect("callback tls");
            let msg_type = read_exact_array::<_, 1>(&mut stream).await[0];
            let nonce_b_cb = read_exact_array::<_, TCP_NONCE_LEN>(&mut stream).await;
            let echo_nonce_a_cb = read_exact_array::<_, TCP_NONCE_LEN>(&mut stream).await;
            let _ = stream.shutdown().await;
            (msg_type, nonce_b_cb, echo_nonce_a_cb)
        })
        .await
    });

    let connector = TlsConnector::from(Arc::new(client_a));
    let tcp = TcpStream::connect(addr_b).await.expect("tcp connect");
    let server_name = ServerName::IpAddress(addr_b.ip().into());
    let mut stream = connector
        .connect(server_name, tcp)
        .await
        .expect("tls connect");

    let nonce_a_cb = [14u8; TCP_NONCE_LEN];
    write_auth_init(&mut stream, nonce_a_cb).await;

    let mut buf = [0u8; 1];
    let result = timeout(Duration::from_millis(400), stream.read_exact(&mut buf)).await;
    assert!(result.is_err() || result.unwrap().is_err());

    let callback_result = callback_task.await.expect("callback task");
    assert!(callback_result.is_err());

    transport_b.shutdown().await;
}

#[tokio::test]
async fn tcp_pre_a1_rejects_removed_message_type_five() {
    let ca = build_ca();
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let registry_b = Arc::new(TabernaRegistry::new());
    let config_b = DomusConfigAccess::from_config(DomusConfig::default());
    let addr_b = pick_addr(domus_ip);
    let transport_b = Transport::bind(
        DomusAddr::Tcp(addr_b),
        registry_b,
        config_b,
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_b),
    )
    .await
    .expect("bind transport b");
    transport_b.start().await.expect("start b");
    let transport_b = Arc::new(transport_b);

    let addr_a = pick_addr(domus_ip);
    let (_auth_a, client_a) = build_auth_and_client(&ca, addr_a);
    let connector = TlsConnector::from(Arc::new(client_a));
    let tcp = TcpStream::connect(addr_b).await.expect("tcp connect");
    let server_name = ServerName::IpAddress(addr_b.ip().into());
    let mut stream = connector
        .connect(server_name, tcp)
        .await
        .expect("tls connect");

    write_type(&mut stream, 5).await;
    let mut buf = [0u8; 1];
    let result = timeout(Duration::from_millis(400), stream.read_exact(&mut buf)).await;
    assert!(result.is_err() || result.unwrap().is_err());

    transport_b.shutdown().await;
}

#[tokio::test]
async fn transport_blob_request_fails_when_blob_callis_unreachable() {
    let ca = build_ca();
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let registry_a = Arc::new(TabernaRegistry::new());
    let registry_b = Arc::new(TabernaRegistry::new());
    let msg_type = a3_message_type(55);
    let sink = Arc::new(RecordingSink::new(vec![msg_type]));
    let taberna_id = 210;
    registry_b.register(taberna_id, sink.clone()).await.unwrap();

    let config_a = DomusConfigBuilder::new()
        .send_timeout(Duration::from_secs(2))
        .callis_connect_timeout(Duration::from_secs(2))
        .accept_timeout(Duration::from_secs(2))
        .build()
        .expect("valid domus config");
    let config_b = DomusConfigBuilder::new()
        .send_timeout(Duration::from_millis(200))
        .callis_connect_timeout(Duration::from_millis(200))
        .accept_timeout(Duration::from_millis(200))
        .listener_delay(Duration::from_millis(0))
        .build()
        .expect("valid domus config");
    let config_a = DomusConfigAccess::from_config(config_a);
    let config_b = DomusConfigAccess::from_config(config_b);
    let addr_b = pick_addr(domus_ip);
    let addr_a = pick_addr(domus_ip);

    let transport_b = Transport::bind(
        DomusAddr::Tcp(addr_b),
        Arc::clone(&registry_b),
        config_b.clone(),
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_b),
    )
    .await
    .expect("bind transport b");
    let _handle_b = transport_b.start().await.expect("start b");
    let transport_b = Arc::new(transport_b);

    let transport_a = Transport::bind(
        DomusAddr::Tcp(addr_a),
        registry_a.clone(),
        config_a.clone(),
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_a),
    )
    .await
    .expect("bind transport a");
    let transport_a = Arc::new(transport_a);
    // intentionally do not start transport_a listener

    let resolver = Arc::new(StaticResolver::new(taberna_id, DomusAddr::Tcp(addr_b)));
    let peering = Arc::new(RouteLocalRemote::new(
        config_a,
        registry_a,
        resolver,
        Arc::clone(&transport_a),
    ));

    let result = peering
        .send(
            &TestCodec,
            taberna_id,
            &test_message(msg_type, b"blob"),
            SendOptions::BLOB,
        )
        .await
        .map_err(|err| err.kind);
    assert!(matches!(result, Err(ErrorId::SendTimeout)));

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn transport_blob_transfers_in_parallel() {
    tokio::time::timeout(TRANSPORT_TEST_TIMEOUT, async {
    let ca = build_ca();
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let registry_a = Arc::new(TabernaRegistry::new());
    let registry_b = Arc::new(TabernaRegistry::new());
    let blob_a_msg_type = a3_message_type(71);
    let blob_b_msg_type = a3_message_type(72);
    let sink = Arc::new(RecordingSink::new(vec![blob_a_msg_type, blob_b_msg_type]));
    let taberna_id = 311;
    registry_b.register(taberna_id, sink.clone()).await.unwrap();

    let config_a = DomusConfigAccess::from_config(DomusConfig::default());
    let config_b = DomusConfigAccess::from_config(DomusConfig::default());
    let addr_b = pick_addr(domus_ip);
    let addr_a = pick_addr(domus_ip);

    let (reporting_b, obs_b) = new_observability(tokio::runtime::Handle::current());
    let transport_b = Transport::bind(
        DomusAddr::Tcp(addr_b),
        Arc::clone(&registry_b),
        config_b.clone(),
        obs_b,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_b),
    )
    .await
    .expect("bind transport b");
    let _handle_b = transport_b.start().await.expect("start b");
    let transport_b = Arc::new(transport_b);

    let (reporting_a, obs_a) = new_observability(tokio::runtime::Handle::current());
    let transport_a = Transport::bind(
        DomusAddr::Tcp(addr_a),
        registry_a.clone(),
        config_a.clone(),
        obs_a,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_a),
    )
    .await
    .expect("bind transport a");
    transport_a.start().await.expect("start a");
    let transport_a = Arc::new(transport_a);

    let (_events_task_a, events_a) = spawn_event_capture(&reporting_a);
    let (_events_task_b, events_b) = spawn_event_capture(&reporting_b);
    let resolver = Arc::new(StaticResolver::new(taberna_id, DomusAddr::Tcp(addr_b)));
    let peering = Arc::new(RouteLocalRemote::new(
        config_a,
        registry_a,
        resolver,
        Arc::clone(&transport_a),
    ));

    let started_at = Instant::now();
    let task_a = {
        let peering = Arc::clone(&peering);
        tokio::spawn(async move {
            let send_started = Instant::now();
            let outcome = peering
                .send(
                    &TestCodec,
                    taberna_id,
                    &test_message(blob_a_msg_type, b"blob-a"),
                    SendOptions::BLOB,
                )
                .await
                .map_err(|err| {
                    AureliaError::with_message(
                        err.kind,
                        format!("send(blob-a) after {:?}: {err}", send_started.elapsed()),
                    )
                })?;
            match outcome {
                SendOutcome::Blob { sender } => {
                    let blob_started = Instant::now();
                    write_blob(sender, vec![Bytes::from_static(b"alpha")])
                        .await
                        .map_err(|err| {
                            let message = err.to_string();
                            AureliaError::with_message(
                                map_io_error_message(&message),
                                format!(
                                    "write_blob(blob-a) after {:?}: {message}",
                                    blob_started.elapsed()
                                ),
                            )
                        })?;
                    Ok(())
                }
                SendOutcome::MessageOnly => Err(AureliaError::new(ErrorId::ProtocolViolation)),
            }
        })
    };
    let task_b = {
        let peering = Arc::clone(&peering);
        tokio::spawn(async move {
            let send_started = Instant::now();
            let outcome = peering
                .send(
                    &TestCodec,
                    taberna_id,
                    &test_message(blob_b_msg_type, b"blob-b"),
                    SendOptions::BLOB,
                )
                .await
                .map_err(|err| {
                    AureliaError::with_message(
                        err.kind,
                        format!("send(blob-b) after {:?}: {err}", send_started.elapsed()),
                    )
                })?;
            match outcome {
                SendOutcome::Blob { sender } => {
                    let blob_started = Instant::now();
                    write_blob(sender, vec![Bytes::from_static(b"bravo")])
                        .await
                        .map_err(|err| {
                            let message = err.to_string();
                            AureliaError::with_message(
                                map_io_error_message(&message),
                                format!(
                                    "write_blob(blob-b) after {:?}: {message}",
                                    blob_started.elapsed()
                                ),
                            )
                        })?;
                    Ok(())
                }
                SendOutcome::MessageOnly => Err(AureliaError::new(ErrorId::ProtocolViolation)),
            }
        })
    };

    let result_a = task_a.await.expect("task a");
    let result_b = task_b.await.expect("task b");
    if result_a.is_err() || result_b.is_err() {
        eprintln!(
            "transport_blob_transfers_in_parallel failed after {:?}: result_a={result_a:?} result_b={result_b:?}",
            started_at.elapsed()
        );
        dump_observability("transport-a", &reporting_a).await;
        dump_events("transport-a", &events_a).await;
        dump_observability("transport-b", &reporting_b).await;
        dump_events("transport-b", &events_b).await;
        panic!("parallel blob transfer failed");
    }

    let received = sink.wait_for(2, Duration::from_secs(2)).await;
    let mut blobs = std::collections::HashMap::new();
    for (msg_type, _payload, receiver) in received {
        let receiver = receiver.expect("expected blob receiver");
        let data = read_blob(receiver).await;
        blobs.insert(msg_type, data);
    }
    assert_eq!(
        blobs.get(&blob_a_msg_type).map(|v| v.as_slice()),
        Some(b"alpha".as_slice())
    );
    assert_eq!(
        blobs.get(&blob_b_msg_type).map(|v| v.as_slice()),
        Some(b"bravo".as_slice())
    );

    transport_a.shutdown().await;
    transport_b.shutdown().await;

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn transport_blob_transfer_end_to_end() {
    tokio::time::timeout(TRANSPORT_TEST_TIMEOUT, async {
        let ca = build_ca();
        let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

        let registry_a = Arc::new(TabernaRegistry::new());
        let registry_b = Arc::new(TabernaRegistry::new());
        let msg_type = a3_message_type(77);
        let sink = Arc::new(RecordingSink::new(vec![msg_type]));
        let taberna_id = 55;
        registry_b.register(taberna_id, sink.clone()).await.unwrap();

        let config_a = DomusConfigAccess::from_config(DomusConfig::default());
        let config_b = DomusConfigAccess::from_config(DomusConfig::default());
        let addr_b = pick_addr(domus_ip);
        let addr_a = pick_addr(domus_ip);

        let transport_b = Transport::bind(
            DomusAddr::Tcp(addr_b),
            Arc::clone(&registry_b),
            config_b.clone(),
            new_observability(tokio::runtime::Handle::current()).1,
            tokio::runtime::Handle::current(),
            build_auth(&ca, addr_b),
        )
        .await
        .expect("bind transport b");
        let _handle_b = transport_b.start().await.expect("start b");
        let transport_b = Arc::new(transport_b);

        let transport_a = Transport::bind(
            DomusAddr::Tcp(addr_a),
            registry_a.clone(),
            config_a.clone(),
            new_observability(tokio::runtime::Handle::current()).1,
            tokio::runtime::Handle::current(),
            build_auth(&ca, addr_a),
        )
        .await
        .expect("bind transport a");
        transport_a.start().await.expect("start a");
        let transport_a = Arc::new(transport_a);

        let resolver = Arc::new(StaticResolver::new(taberna_id, DomusAddr::Tcp(addr_b)));
        let peering =
            RouteLocalRemote::new(config_a, registry_a, resolver, Arc::clone(&transport_a));

        let outcome = peering
            .send(
                &TestCodec,
                taberna_id,
                &test_message(msg_type, b"blob"),
                SendOptions::BLOB,
            )
            .await
            .expect("send blob");
        match outcome {
            SendOutcome::Blob { sender } => {
                write_blob(
                    sender,
                    vec![Bytes::from_static(b"alpha"), Bytes::from_static(b"beta")],
                )
                .await
                .expect("write blob");
            }
            SendOutcome::MessageOnly => panic!("expected blob sender"),
        }

        let mut received = sink.take().await;
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].0, msg_type);
        assert_eq!(received[0].1, Bytes::from_static(b"blob"));

        let receiver = received.remove(0).2.expect("blob receiver");
        let data = read_blob(receiver).await;
        assert_eq!(data, b"alphabeta");

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn transport_blob_negotiates_chunk_size() {
    tokio::time::timeout(TRANSPORT_TEST_TIMEOUT, async {
        let ca = build_ca();
        let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

        let registry_a = Arc::new(TabernaRegistry::new());
        let registry_b = Arc::new(TabernaRegistry::new());
        let taberna_id = 90;
        let msg_type = a3_message_type(12);
        let sink = Arc::new(RecordingSink::new(vec![msg_type]));
        registry_b.register(taberna_id, sink.clone()).await.unwrap();

        let config_a = DomusConfigBuilder::new()
            .blob_window(8, 1024)
            .build()
            .expect("valid domus config");
        let config_b = DomusConfigBuilder::new()
            .blob_window(3, 1024)
            .build()
            .expect("valid domus config");
        let config_a = DomusConfigAccess::from_config(config_a);
        let config_b = DomusConfigAccess::from_config(config_b);
        let addr_b = pick_addr(domus_ip);
        let addr_a = pick_addr(domus_ip);

        let transport_b = Transport::bind(
            DomusAddr::Tcp(addr_b),
            Arc::clone(&registry_b),
            config_b.clone(),
            new_observability(tokio::runtime::Handle::current()).1,
            tokio::runtime::Handle::current(),
            build_auth(&ca, addr_b),
        )
        .await
        .expect("bind transport b");
        let _handle_b = transport_b.start().await.expect("start b");
        let transport_b = Arc::new(transport_b);

        let transport_a = Transport::bind(
            DomusAddr::Tcp(addr_a),
            registry_a.clone(),
            config_a.clone(),
            new_observability(tokio::runtime::Handle::current()).1,
            tokio::runtime::Handle::current(),
            build_auth(&ca, addr_a),
        )
        .await
        .expect("bind transport a");
        transport_a.start().await.expect("start a");
        let transport_a = Arc::new(transport_a);

        let resolver = Arc::new(StaticResolver::new(taberna_id, DomusAddr::Tcp(addr_b)));
        let peering =
            RouteLocalRemote::new(config_a, registry_a, resolver, Arc::clone(&transport_a));

        let outcome = peering
            .send(
                &TestCodec,
                taberna_id,
                &test_message(msg_type, b"chunked"),
                SendOptions::BLOB,
            )
            .await
            .expect("send blob");
        match outcome {
            SendOutcome::Blob { sender } => {
                write_blob(sender, vec![Bytes::from_static(b"abcdefghi")])
                    .await
                    .expect("write blob");
            }
            SendOutcome::MessageOnly => panic!("expected blob sender"),
        }

        let mut received = sink.wait_for(1, Duration::from_secs(2)).await;
        let receiver = received.remove(0).2.expect("expected blob receiver");
        let data = read_blob(receiver).await;
        assert_eq!(data, b"abcdefghi");

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn transport_blob_transfer_exceeds_ack_window_without_stalling() {
    let ca = build_ca();
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let registry_a = Arc::new(TabernaRegistry::new());
    let registry_b = Arc::new(TabernaRegistry::new());
    let taberna_id = 91;
    let msg_type = a3_message_type(13);
    let sink = Arc::new(RecordingSink::new(vec![msg_type]));
    registry_b.register(taberna_id, sink.clone()).await.unwrap();

    let config_a = DomusConfigBuilder::new()
        .send_timeout(Duration::from_secs(5))
        .callis_connect_timeout(Duration::from_secs(5))
        .listener_delay(Duration::from_millis(0))
        .blob_window(4, 2)
        .build()
        .expect("valid domus config");
    let config_b = DomusConfigBuilder::new()
        .send_timeout(Duration::from_secs(5))
        .callis_connect_timeout(Duration::from_secs(5))
        .listener_delay(Duration::from_millis(0))
        .blob_window(4, 2)
        .build()
        .expect("valid domus config");
    let config_a = DomusConfigAccess::from_config(config_a);
    let config_b = DomusConfigAccess::from_config(config_b);
    let addr_b = pick_addr(domus_ip);
    let addr_a = pick_addr(domus_ip);

    let transport_b = Transport::bind(
        DomusAddr::Tcp(addr_b),
        Arc::clone(&registry_b),
        config_b.clone(),
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_b),
    )
    .await
    .expect("bind transport b");
    let _handle_b = transport_b.start().await.expect("start b");
    let transport_b = Arc::new(transport_b);

    let transport_a = Transport::bind(
        DomusAddr::Tcp(addr_a),
        registry_a.clone(),
        config_a.clone(),
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_a),
    )
    .await
    .expect("bind transport a");
    transport_a.start().await.expect("start a");
    let transport_a = Arc::new(transport_a);

    let resolver = Arc::new(StaticResolver::new(taberna_id, DomusAddr::Tcp(addr_b)));
    let peering = RouteLocalRemote::new(config_a, registry_a, resolver, Arc::clone(&transport_a));

    let outcome = peering
        .send(
            &TestCodec,
            taberna_id,
            &test_message(msg_type, b"over-window"),
            SendOptions::BLOB,
        )
        .await
        .expect("send blob");
    let write_task = match outcome {
        SendOutcome::Blob { sender } => tokio::spawn(write_blob(
            sender,
            vec![Bytes::from_static(b"abcdefghijkl")],
        )),
        SendOutcome::MessageOnly => panic!("expected blob sender"),
    };

    let mut received = sink.wait_for(1, Duration::from_secs(2)).await;
    let receiver = received.remove(0).2.expect("expected blob receiver");
    let data = read_blob(receiver).await;
    assert_eq!(data, b"abcdefghijkl");
    timeout(Duration::from_secs(5), write_task)
        .await
        .expect("blob write stalled")
        .expect("blob write join")
        .expect("write blob");

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn transport_blob_reconnect_replays_chunks() {
    let ca = build_ca();
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let registry_a = Arc::new(TabernaRegistry::new());
    let registry_b = Arc::new(TabernaRegistry::new());
    let msg_type = a3_message_type(81);
    let sink = Arc::new(RecordingSink::new(vec![msg_type]));
    let started = Arc::new(Notify::new());
    let ready = Arc::new(Notify::new());
    let taberna_id = 140;
    registry_b.register(taberna_id, sink.clone()).await.unwrap();

    let cfg = DomusConfigBuilder::new()
        .send_timeout(Duration::from_secs(5))
        .callis_connect_timeout(Duration::from_secs(5))
        .listener_delay(Duration::from_millis(0))
        .blob_window(4, 1024)
        .build()
        .expect("valid domus config");
    let config_a = DomusConfigAccess::from_config(cfg.clone());
    let config_b = DomusConfigAccess::from_config(cfg);
    let addr_b = pick_addr(domus_ip);
    let addr_a = pick_addr(domus_ip);

    let transport_b = Transport::bind(
        DomusAddr::Tcp(addr_b),
        Arc::clone(&registry_b),
        config_b.clone(),
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_b),
    )
    .await
    .expect("bind transport b");
    let handle_b = transport_b.start().await.expect("start b");
    let transport_b = Arc::new(transport_b);

    let transport_a = Transport::bind(
        DomusAddr::Tcp(addr_a),
        registry_a.clone(),
        config_a.clone(),
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_a),
    )
    .await
    .expect("bind transport a");
    transport_a.start().await.expect("start a");
    let transport_a = Arc::new(transport_a);

    let resolver = Arc::new(StaticResolver::new(taberna_id, DomusAddr::Tcp(addr_b)));
    let peering = RouteLocalRemote::new(config_a, registry_a, resolver, Arc::clone(&transport_a));

    let started_notify = Arc::clone(&started);
    let ready_notify = Arc::clone(&ready);
    let send_task = tokio::spawn(async move {
        let outcome = peering
            .send(
                &TestCodec,
                taberna_id,
                &test_message(msg_type, b"blob"),
                SendOptions::BLOB,
            )
            .await
            .map_err(|err| err.kind)?;
        match outcome {
            SendOutcome::Blob { mut sender } => {
                sender.write_all(b"first").await.map_err(map_io_error)?;
                started_notify.notify_waiters();
                ready_notify.notified().await;
                sender.write_all(b"second").await.map_err(map_io_error)?;
                sender.shutdown().await.map_err(map_io_error)?;
                Ok(())
            }
            SendOutcome::MessageOnly => Err(ErrorId::ProtocolViolation),
        }
    });

    timeout(Duration::from_secs(5), started.notified())
        .await
        .expect("chunk start timeout");
    transport_b.shutdown().await;
    let _ = handle_b.await;
    drop(transport_b);

    let transport_b2 = Transport::bind(
        DomusAddr::Tcp(addr_b),
        registry_b,
        config_b.clone(),
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_b),
    )
    .await
    .expect("bind transport b2");
    transport_b2.start().await.expect("start b2");
    let transport_b2 = Arc::new(transport_b2);
    ready.notify_waiters();

    let result = timeout(TRANSPORT_TEST_TIMEOUT, send_task)
        .await
        .expect("send timeout")
        .expect("send join");
    match result {
        Ok(()) => {
            let mut received = sink.wait_for(1, Duration::from_secs(5)).await;
            let receiver = received.remove(0).2.expect("expected blob receiver");
            let data = read_blob(receiver).await;
            assert_eq!(data, b"firstsecond");
        }
        Err(ErrorId::SendTimeout) | Err(ErrorId::PeerRestarted) => {}
        Err(other) => panic!("unexpected blob error: {other:?}"),
    }

    transport_a.shutdown().await;
    transport_b2.shutdown().await;
}

#[tokio::test]
async fn transport_blob_reconnect_setting_change_fails_stream() {
    let ca = build_ca();
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let registry_a = Arc::new(TabernaRegistry::new());
    let registry_b = Arc::new(TabernaRegistry::new());
    let msg_type = a3_message_type(82);
    let sink = Arc::new(RecordingSink::new(vec![msg_type]));
    let started = Arc::new(Notify::new());
    let ready = Arc::new(Notify::new());
    let taberna_id = 141;
    registry_b.register(taberna_id, sink.clone()).await.unwrap();

    let cfg = DomusConfigBuilder::new()
        .send_timeout(Duration::from_secs(5))
        .callis_connect_timeout(Duration::from_secs(5))
        .listener_delay(Duration::from_millis(0))
        .blob_window(4, 8)
        .build()
        .expect("valid domus config");
    let config_a = DomusConfigAccess::from_config(cfg.clone());
    let config_b = DomusConfigAccess::from_config(cfg);
    let addr_b = pick_addr(domus_ip);
    let addr_a = pick_addr(domus_ip);

    let config_b_access: DomusConfigAccess = config_b.clone();
    let transport_b = Transport::bind(
        DomusAddr::Tcp(addr_b),
        Arc::clone(&registry_b),
        config_b_access,
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_b),
    )
    .await
    .expect("bind transport b");
    let handle_b = transport_b.start().await.expect("start b");
    let transport_b = Arc::new(transport_b);

    let config_a_access: DomusConfigAccess = config_a.clone();
    let transport_a = Transport::bind(
        DomusAddr::Tcp(addr_a),
        registry_a.clone(),
        config_a_access,
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_a),
    )
    .await
    .expect("bind transport a");
    transport_a.start().await.expect("start a");
    let transport_a = Arc::new(transport_a);

    let resolver = Arc::new(StaticResolver::new(taberna_id, DomusAddr::Tcp(addr_b)));
    let peering = RouteLocalRemote::new(
        config_a.clone(),
        registry_a,
        resolver,
        Arc::clone(&transport_a),
    );

    let started_notify = Arc::clone(&started);
    let ready_notify = Arc::clone(&ready);
    let send_task = tokio::spawn(async move {
        let outcome = peering
            .send(
                &TestCodec,
                taberna_id,
                &test_message(msg_type, b"blob"),
                SendOptions::BLOB,
            )
            .await
            .map_err(|err| err.kind)?;
        match outcome {
            SendOutcome::Blob { mut sender } => {
                sender.write_all(b"restart").await.map_err(map_io_error)?;
                started_notify.notify_waiters();
                ready_notify.notified().await;
                sender.shutdown().await.map_err(map_io_error)?;
                Ok(())
            }
            SendOutcome::MessageOnly => Err(ErrorId::ProtocolViolation),
        }
    });

    timeout(Duration::from_secs(5), started.notified())
        .await
        .expect("chunk start timeout");
    let mut updated = config_b.snapshot().await;
    updated.blob_window = BlobWindowConfig::new(updated.blob_window.chunk_size(), 2);
    config_b.update(updated).await.expect("valid domus config");
    transport_b.shutdown().await;
    let _ = handle_b.await;
    drop(transport_b);

    let transport_b2 = Transport::bind(
        DomusAddr::Tcp(addr_b),
        registry_b,
        config_b.clone(),
        new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        build_auth(&ca, addr_b),
    )
    .await
    .expect("bind transport b2");
    transport_b2.start().await.expect("start b2");
    let transport_b2 = Arc::new(transport_b2);
    ready.notify_waiters();

    let result = timeout(TRANSPORT_TEST_TIMEOUT, send_task)
        .await
        .expect("send timeout")
        .expect("send join");
    assert!(matches!(
        result,
        Err(ErrorId::PeerRestarted) | Err(ErrorId::SendTimeout)
    ));

    transport_a.shutdown().await;
    transport_b2.shutdown().await;
}
