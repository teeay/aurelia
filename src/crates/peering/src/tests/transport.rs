// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rcgen::{BasicConstraints, Certificate, CertificateParams, IsCa, SanType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::time::{sleep, timeout};
use tokio_rustls::rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName,
};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::address::DomusAddr;
use crate::auth::{DomusAuthConfig, Pkcs8AuthConfig, Pkcs8DerConfig};
use crate::config::{DomusConfig, DomusConfigAccess, DomusConfigBuilder};
use crate::observability::new_observability;
use crate::peering::RouteLocalRemoteBuilder;
use crate::routing::RouteResolver;
use crate::taberna::{TabernaInbox, TabernaRegistry};
use crate::transport::Transport;
use crate::wire::{ErrorPayload, HelloPayload, WireFlags, WireHeader, PROTOCOL_VERSION};
use crate::{BlobReceiver, SendOptions, SendOutcome};
use aurelia_ids::{AureliaError, ErrorId};
use aurelia_ids::{MSG_ERROR, MSG_HELLO, MSG_HELLO_RESPONSE};

use super::{test_message, TestCodec};

struct StaticResolver {
    addr: DomusAddr,
}

#[async_trait::async_trait]
impl RouteResolver for StaticResolver {
    async fn resolve(&self, _taberna_id: u64) -> Result<DomusAddr, AureliaError> {
        Ok(self.addr.clone())
    }
}

struct RecordingSink {
    received: tokio::sync::Mutex<Vec<(u32, Bytes, Option<BlobReceiver>)>>,
    expected_msg_types: Vec<u32>,
}

impl RecordingSink {
    fn new(expected_msg_types: Vec<u32>) -> Self {
        Self {
            received: tokio::sync::Mutex::new(Vec::new()),
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
            sleep(Duration::from_millis(10)).await;
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
        if !self.expected_msg_types.contains(&msg_type) {
            return Err(AureliaError::new(ErrorId::RemoteTabernaRejected));
        }
        self.received
            .lock()
            .await
            .push((msg_type, payload, blob_receiver));
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
        if !self.expected_msg_types.contains(&msg_type) {
            return Err(AureliaError::new(ErrorId::RemoteTabernaRejected));
        }
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
    let message = err.to_string();
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

fn build_auth_and_client(ca: &Certificate, addr: SocketAddr) -> (DomusAuthConfig, ClientConfig) {
    let (cert_der, key_der) = build_domus_cert(ca, addr);
    let ca_der = ca.serialize_der().expect("ca der");
    let auth = DomusAuthConfig::Pkcs8(Pkcs8AuthConfig::Pkcs8Der(Pkcs8DerConfig {
        ca_der: ca_der.clone(),
        cert_der: cert_der.clone(),
        pkcs8_key_der: key_der.clone(),
    }));

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

fn build_auth(ca: &Certificate, addr: SocketAddr) -> DomusAuthConfig {
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
const TCP_AUTH_RESUME: u8 = 5;
const TCP_NONCE_LEN: usize = 32;
const TCP_SESSION_NONCE_LEN: usize = TCP_NONCE_LEN * 4;

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
    nonce_a: [u8; TCP_NONCE_LEN],
    nonce_a_cb: [u8; TCP_NONCE_LEN],
) {
    write_type(stream, TCP_AUTH_INIT).await;
    stream.write_all(&nonce_a).await.expect("write nonce_a");
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

async fn write_auth_resume<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    session_nonce: [u8; TCP_SESSION_NONCE_LEN],
) {
    write_type(stream, TCP_AUTH_RESUME).await;
    stream
        .write_all(&session_nonce)
        .await
        .expect("write resume");
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

fn build_session_nonce(
    nonce_a: [u8; TCP_NONCE_LEN],
    nonce_b: [u8; TCP_NONCE_LEN],
    nonce_a_cb: [u8; TCP_NONCE_LEN],
    nonce_b_cb: [u8; TCP_NONCE_LEN],
) -> [u8; TCP_SESSION_NONCE_LEN] {
    let mut out = [0u8; TCP_SESSION_NONCE_LEN];
    out[0..TCP_NONCE_LEN].copy_from_slice(&nonce_a);
    out[TCP_NONCE_LEN..TCP_NONCE_LEN * 2].copy_from_slice(&nonce_b);
    out[TCP_NONCE_LEN * 2..TCP_NONCE_LEN * 3].copy_from_slice(&nonce_a_cb);
    out[TCP_NONCE_LEN * 3..TCP_NONCE_LEN * 4].copy_from_slice(&nonce_b_cb);
    out
}

#[tokio::test]
async fn transport_remote_delivery_with_ack() {
    let ca = build_ca();
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let registry_a = Arc::new(TabernaRegistry::new());
    let registry_b = Arc::new(TabernaRegistry::new());
    let sink = Arc::new(RecordingSink::new(vec![100]));
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

    let resolver = Arc::new(StaticResolver {
        addr: DomusAddr::Tcp(addr_b),
    });

    let peering = Arc::new(
        RouteLocalRemoteBuilder::new(config_a, registry_a, resolver, Arc::clone(&transport_a))
            .build(),
    );

    let codec = TestCodec;
    let outcome = peering
        .send(
            &codec,
            taberna_id,
            &test_message(100, b"hello"),
            SendOptions::MESSAGE_ONLY,
        )
        .await
        .expect("send");
    assert!(matches!(outcome, SendOutcome::MessageOnly));

    let received = sink.take().await;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].0, 100);
    assert_eq!(received[0].1, Bytes::from_static(b"hello"));

    transport_a.shutdown().await;
    transport_b.shutdown().await;
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
    let sink = Arc::new(RecordingSink::new(vec![90]));
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

    let resolver = Arc::new(StaticResolver {
        addr: DomusAddr::Tcp(addr_b),
    });
    let peering =
        RouteLocalRemoteBuilder::new(config_a, registry_a, resolver, Arc::clone(&transport_a))
            .build();

    let codec = TestCodec;
    let outcome = peering
        .send(
            &codec,
            taberna_id,
            &test_message(90, b"first"),
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
            &test_message(90, b"second"),
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
            &test_message(90, b"third"),
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
        vec![90],
        Arc::clone(&started),
        Arc::clone(&ready),
    ));
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

    let resolver = Arc::new(StaticResolver {
        addr: DomusAddr::Tcp(addr_b),
    });

    let peering = Arc::new(
        RouteLocalRemoteBuilder::new(config_a, registry_a, resolver, Arc::clone(&transport_a))
            .build(),
    );

    let send_task = tokio::spawn(async move {
        let codec = TestCodec;
        peering
            .send(
                &codec,
                taberna_id,
                &test_message(90, b"payload"),
                SendOptions::MESSAGE_ONLY,
            )
            .await
            .map_err(|err| err.kind)
    });

    timeout(Duration::from_secs(10), started.notified())
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

    let result = timeout(Duration::from_secs(5), send_task)
        .await
        .expect("send timeout")
        .expect("join");
    assert!(matches!(
        result,
        Ok(SendOutcome::MessageOnly) | Err(ErrorId::SendTimeout) | Err(ErrorId::PeerRestarted)
    ));

    transport_a.shutdown().await;
    transport_b2.shutdown().await;
}

#[tokio::test]
async fn transport_backpressure_inflight_window() {
    let ca = build_ca();
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let registry_a = Arc::new(TabernaRegistry::new());
    let registry_b = Arc::new(TabernaRegistry::new());

    let started = Arc::new(Notify::new());
    let ready = Arc::new(Notify::new());
    let sink = Arc::new(BlockingInbox::new(
        vec![20, 21],
        Arc::clone(&started),
        Arc::clone(&ready),
    ));
    let taberna_id = 77;
    registry_b.register(taberna_id, sink.clone()).await.unwrap();

    let cfg = DomusConfig {
        send_timeout: Duration::from_millis(500),
        inflight_window: 1,
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

    let resolver = Arc::new(StaticResolver {
        addr: DomusAddr::Tcp(addr_b),
    });

    let peering = RouteLocalRemoteBuilder::new(
        config_a.clone(),
        registry_a.clone(),
        resolver.clone(),
        Arc::clone(&transport_a),
    )
    .build();
    let peering_send =
        RouteLocalRemoteBuilder::new(config_a, registry_a, resolver, Arc::clone(&transport_a))
            .build();

    let send_one = tokio::spawn(async move {
        let codec = TestCodec;
        peering_send
            .send(
                &codec,
                taberna_id,
                &test_message(20, b"one"),
                SendOptions::MESSAGE_ONLY,
            )
            .await
            .map_err(|err| err.kind)
    });

    timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("started timeout");
    let codec = TestCodec;
    let result_two = peering
        .send(
            &codec,
            taberna_id,
            &test_message(21, b"two"),
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
    let blocked_sink = Arc::new(BlockingInbox::new(
        vec![30],
        Arc::clone(&started),
        Arc::clone(&ready),
    ));
    let fast_sink = Arc::new(RecordingSink::new(vec![31]));
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
        send_timeout: Duration::from_secs(3),
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

    let resolver = Arc::new(StaticResolver {
        addr: DomusAddr::Tcp(addr_b),
    });

    let peering = RouteLocalRemoteBuilder::new(
        config_a.clone(),
        registry_a.clone(),
        resolver.clone(),
        Arc::clone(&transport_a),
    )
    .build();
    let peering_blocked =
        RouteLocalRemoteBuilder::new(config_a, registry_a, resolver, Arc::clone(&transport_a))
            .build();

    let blocked_send = tokio::spawn(async move {
        let codec = TestCodec;
        peering_blocked
            .send(
                &codec,
                blocked_taberna,
                &test_message(30, b"blocked"),
                SendOptions::MESSAGE_ONLY,
            )
            .await
            .map_err(|err| err.kind)
    });

    timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("blocked taberna start");

    let codec = TestCodec;
    let outcome = peering
        .send(
            &codec,
            fast_taberna,
            &test_message(31, b"fast"),
            SendOptions::MESSAGE_ONLY,
        )
        .await
        .expect("fast taberna send");
    assert!(matches!(outcome, SendOutcome::MessageOnly));

    let received = fast_sink.take().await;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].0, 31);
    assert_eq!(received[0].1, Bytes::from_static(b"fast"));

    ready.notify_waiters();
    let result = timeout(Duration::from_secs(2), blocked_send)
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
    let sink = Arc::new(BlockingInbox::new(
        vec![12],
        Arc::clone(&started),
        Arc::clone(&ready),
    ));
    let taberna_id = 222;
    registry_b.register(taberna_id, sink.clone()).await.unwrap();

    let cfg = DomusConfig {
        send_timeout: Duration::from_secs(5),
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

    let resolver = Arc::new(StaticResolver {
        addr: DomusAddr::Tcp(peer_addr),
    });

    let peering = RouteLocalRemoteBuilder::new(
        config_a.clone(),
        registry_a.clone(),
        resolver.clone(),
        Arc::clone(&transport_a),
    )
    .build();

    let send_task = tokio::spawn(async move {
        let codec = TestCodec;
        peering
            .send(
                &codec,
                taberna_id,
                &test_message(12, b"restart"),
                SendOptions::MESSAGE_ONLY,
            )
            .await
            .map_err(|err| err.kind)
    });

    timeout(Duration::from_secs(2), started.notified())
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

    let result = timeout(Duration::from_secs(6), send_task)
        .await
        .expect("send timeout")
        .expect("send join");
    assert!(matches!(
        result,
        Err(ErrorId::PeerRestarted) | Err(ErrorId::SendTimeout)
    ));

    ready.notify_waiters();
    transport_a.shutdown().await;
    transport_b2.shutdown().await;
}

#[tokio::test]
async fn transport_blob_callis_rejected_without_primary() {
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

    let nonce_a = [1u8; TCP_NONCE_LEN];
    let nonce_a_cb = [2u8; TCP_NONCE_LEN];
    let callback_task = tokio::spawn(accept_tcp_callback(
        callback_listener,
        Arc::clone(&server_config_a),
        nonce_a_cb,
    ));
    write_auth_init(&mut stream, nonce_a, nonce_a_cb).await;
    let nonce_b_cb = callback_task.await.expect("callback task");
    let msg_type = read_exact_array::<_, 1>(&mut stream).await[0];
    assert_eq!(msg_type, TCP_AUTH_CHALLENGE);
    let _nonce_b = read_exact_array::<_, TCP_NONCE_LEN>(&mut stream).await;
    write_auth_proof(&mut stream, nonce_b_cb).await;

    let hello = HelloPayload {
        chunk_size: Some(4),
        ack_window_chunks: Some(4),
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
}

#[tokio::test]
async fn tcp_pre_a1_connect_back_success() {
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

    let nonce_a = [3u8; TCP_NONCE_LEN];
    let nonce_a_cb = [4u8; TCP_NONCE_LEN];
    let callback_task = tokio::spawn(accept_tcp_callback(
        callback_listener,
        Arc::clone(&server_config_a),
        nonce_a_cb,
    ));
    write_auth_init(&mut stream, nonce_a, nonce_a_cb).await;
    let nonce_b_cb = callback_task.await.expect("callback task");
    let msg_type = read_exact_array::<_, 1>(&mut stream).await[0];
    assert_eq!(msg_type, TCP_AUTH_CHALLENGE);
    let _nonce_b = read_exact_array::<_, TCP_NONCE_LEN>(&mut stream).await;
    write_auth_proof(&mut stream, nonce_b_cb).await;

    let hello = HelloPayload {
        chunk_size: None,
        ack_window_chunks: None,
    };
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
    assert!(response.chunk_size.is_none());
    assert!(response.ack_window_chunks.is_none());

    transport_b.shutdown().await;
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

    let nonce_a = [9u8; TCP_NONCE_LEN];
    let nonce_a_cb = [10u8; TCP_NONCE_LEN];
    write_auth_init(&mut stream, nonce_a, nonce_a_cb).await;

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

    let nonce_a = [13u8; TCP_NONCE_LEN];
    let nonce_a_cb = [14u8; TCP_NONCE_LEN];
    write_auth_init(&mut stream, nonce_a, nonce_a_cb).await;

    let mut buf = [0u8; 1];
    let result = timeout(Duration::from_millis(400), stream.read_exact(&mut buf)).await;
    assert!(result.is_err() || result.unwrap().is_err());

    let callback_result = callback_task.await.expect("callback task");
    assert!(callback_result.is_err());

    transport_b.shutdown().await;
}

#[tokio::test]
async fn tcp_pre_a1_rejects_mismatched_cert_on_resume() {
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
    let (_auth_a1, client_a1) = build_auth_and_client(&ca, addr_a);
    let server_config_a1 = Arc::new(build_server_config(&ca, addr_a));
    let callback_listener = tokio::net::TcpListener::bind(addr_a)
        .await
        .expect("callback bind");
    let connector_a1 = TlsConnector::from(Arc::new(client_a1));
    let tcp = TcpStream::connect(addr_b).await.expect("tcp connect");
    let server_name = ServerName::IpAddress(addr_b.ip().into());
    let mut stream1 = connector_a1
        .connect(server_name.clone(), tcp)
        .await
        .expect("tls connect");

    let nonce_a = [11u8; TCP_NONCE_LEN];
    let nonce_a_cb = [12u8; TCP_NONCE_LEN];
    let callback_task = tokio::spawn(accept_tcp_callback(
        callback_listener,
        Arc::clone(&server_config_a1),
        nonce_a_cb,
    ));
    write_auth_init(&mut stream1, nonce_a, nonce_a_cb).await;
    let nonce_b_cb = callback_task.await.expect("callback task");
    let msg_type = read_exact_array::<_, 1>(&mut stream1).await[0];
    assert_eq!(msg_type, TCP_AUTH_CHALLENGE);
    let nonce_b = read_exact_array::<_, TCP_NONCE_LEN>(&mut stream1).await;
    write_auth_proof(&mut stream1, nonce_b_cb).await;
    let session_nonce = build_session_nonce(nonce_a, nonce_b, nonce_a_cb, nonce_b_cb);

    let hello = HelloPayload {
        chunk_size: None,
        ack_window_chunks: None,
    };
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
    write_frame(&mut stream1, header, &payload).await;
    let (resp_header, _resp_payload) = read_frame(&mut stream1).await.expect("hello response");
    assert_eq!(resp_header.msg_type, MSG_HELLO_RESPONSE);

    let (_auth_a2, client_a2) = build_auth_and_client(&ca, addr_a);
    let connector_a2 = TlsConnector::from(Arc::new(client_a2));
    let tcp2 = TcpStream::connect(addr_b).await.expect("tcp connect");
    let mut stream2 = connector_a2
        .connect(server_name, tcp2)
        .await
        .expect("tls connect");
    write_auth_resume(&mut stream2, session_nonce).await;
    let mut buf = [0u8; 1];
    let result = timeout(Duration::from_millis(400), stream2.read_exact(&mut buf)).await;
    assert!(result.is_err() || result.unwrap().is_err());

    transport_b.shutdown().await;
}

#[tokio::test]
async fn transport_blob_request_fails_when_blob_callis_unreachable() {
    let ca = build_ca();
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let registry_a = Arc::new(TabernaRegistry::new());
    let registry_b = Arc::new(TabernaRegistry::new());
    let sink = Arc::new(RecordingSink::new(vec![55]));
    let taberna_id = 210;
    registry_b.register(taberna_id, sink.clone()).await.unwrap();

    let config_a = DomusConfigBuilder::new()
        .send_timeout(Duration::from_secs(2))
        .accept_timeout(Duration::from_secs(2))
        .build()
        .expect("valid domus config");
    let config_b = DomusConfigBuilder::new()
        .send_timeout(Duration::from_millis(200))
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

    let resolver = Arc::new(StaticResolver {
        addr: DomusAddr::Tcp(addr_b),
    });
    let peering = Arc::new(
        RouteLocalRemoteBuilder::new(config_a, registry_a, resolver, Arc::clone(&transport_a))
            .build(),
    );

    let result = peering
        .send(
            &TestCodec,
            taberna_id,
            &test_message(55, b"blob"),
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
    let ca = build_ca();
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let registry_a = Arc::new(TabernaRegistry::new());
    let registry_b = Arc::new(TabernaRegistry::new());
    let sink = Arc::new(RecordingSink::new(vec![71, 72]));
    let taberna_id = 311;
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

    let resolver = Arc::new(StaticResolver {
        addr: DomusAddr::Tcp(addr_b),
    });
    let peering = Arc::new(
        RouteLocalRemoteBuilder::new(config_a, registry_a, resolver, Arc::clone(&transport_a))
            .build(),
    );

    let task_a = {
        let peering = Arc::clone(&peering);
        tokio::spawn(async move {
            let outcome = peering
                .send(
                    &TestCodec,
                    taberna_id,
                    &test_message(71, b"blob-a"),
                    SendOptions::BLOB,
                )
                .await
                .map_err(|err| err.kind)?;
            match outcome {
                SendOutcome::Blob { sender } => {
                    write_blob(sender, vec![Bytes::from_static(b"alpha")])
                        .await
                        .map_err(|_| ErrorId::PeerUnavailable)?;
                    Ok(())
                }
                SendOutcome::MessageOnly => Err(ErrorId::ProtocolViolation),
            }
        })
    };
    let task_b = {
        let peering = Arc::clone(&peering);
        tokio::spawn(async move {
            let outcome = peering
                .send(
                    &TestCodec,
                    taberna_id,
                    &test_message(72, b"blob-b"),
                    SendOptions::BLOB,
                )
                .await
                .map_err(|err| err.kind)?;
            match outcome {
                SendOutcome::Blob { sender } => {
                    write_blob(sender, vec![Bytes::from_static(b"bravo")])
                        .await
                        .map_err(|_| ErrorId::PeerUnavailable)?;
                    Ok(())
                }
                SendOutcome::MessageOnly => Err(ErrorId::ProtocolViolation),
            }
        })
    };

    let result_a = task_a.await.expect("task a");
    let result_b = task_b.await.expect("task b");
    assert_eq!(result_a, Ok(()));
    assert_eq!(result_b, Ok(()));

    let received = sink.wait_for(2, Duration::from_secs(2)).await;
    let mut blobs = std::collections::HashMap::new();
    for (msg_type, _payload, receiver) in received {
        let receiver = receiver.expect("expected blob receiver");
        let data = read_blob(receiver).await;
        blobs.insert(msg_type, data);
    }
    assert_eq!(
        blobs.get(&71).map(|v| v.as_slice()),
        Some(b"alpha".as_slice())
    );
    assert_eq!(
        blobs.get(&72).map(|v| v.as_slice()),
        Some(b"bravo".as_slice())
    );

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn transport_blob_transfer_end_to_end() {
    let ca = build_ca();
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let registry_a = Arc::new(TabernaRegistry::new());
    let registry_b = Arc::new(TabernaRegistry::new());
    let sink = Arc::new(RecordingSink::new(vec![77]));
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

    let resolver = Arc::new(StaticResolver {
        addr: DomusAddr::Tcp(addr_b),
    });
    let peering =
        RouteLocalRemoteBuilder::new(config_a, registry_a, resolver, Arc::clone(&transport_a))
            .build();

    let outcome = peering
        .send(
            &TestCodec,
            taberna_id,
            &test_message(77, b"blob"),
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
    assert_eq!(received[0].0, 77);
    assert_eq!(received[0].1, Bytes::from_static(b"blob"));

    let receiver = received.remove(0).2.expect("blob receiver");
    let data = read_blob(receiver).await;
    assert_eq!(data, b"alphabeta");

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn transport_blob_negotiates_chunk_size() {
    let ca = build_ca();
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let registry_a = Arc::new(TabernaRegistry::new());
    let registry_b = Arc::new(TabernaRegistry::new());
    let taberna_id = 90;
    let sink = Arc::new(RecordingSink::new(vec![12]));
    registry_b.register(taberna_id, sink.clone()).await.unwrap();

    let config_a = DomusConfigBuilder::new()
        .blob_chunk_size(8)
        .build()
        .expect("valid domus config");
    let config_b = DomusConfigBuilder::new()
        .blob_chunk_size(3)
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

    let resolver = Arc::new(StaticResolver {
        addr: DomusAddr::Tcp(addr_b),
    });
    let peering =
        RouteLocalRemoteBuilder::new(config_a, registry_a, resolver, Arc::clone(&transport_a))
            .build();

    let outcome = peering
        .send(
            &TestCodec,
            taberna_id,
            &test_message(12, b"chunked"),
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
}

#[tokio::test]
async fn transport_blob_reconnect_replays_chunks() {
    let ca = build_ca();
    let domus_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let registry_a = Arc::new(TabernaRegistry::new());
    let registry_b = Arc::new(TabernaRegistry::new());
    let sink = Arc::new(RecordingSink::new(vec![81]));
    let started = Arc::new(Notify::new());
    let ready = Arc::new(Notify::new());
    let taberna_id = 140;
    registry_b.register(taberna_id, sink.clone()).await.unwrap();

    let cfg = DomusConfigBuilder::new()
        .send_timeout(Duration::from_secs(5))
        .listener_delay(Duration::from_millis(0))
        .blob_chunk_size(4)
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

    let resolver = Arc::new(StaticResolver {
        addr: DomusAddr::Tcp(addr_b),
    });
    let peering =
        RouteLocalRemoteBuilder::new(config_a, registry_a, resolver, Arc::clone(&transport_a))
            .build();

    let started_notify = Arc::clone(&started);
    let ready_notify = Arc::clone(&ready);
    let send_task = tokio::spawn(async move {
        let outcome = peering
            .send(
                &TestCodec,
                taberna_id,
                &test_message(81, b"blob"),
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

    let result = timeout(Duration::from_secs(10), send_task)
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
    let sink = Arc::new(RecordingSink::new(vec![82]));
    let started = Arc::new(Notify::new());
    let ready = Arc::new(Notify::new());
    let taberna_id = 141;
    registry_b.register(taberna_id, sink.clone()).await.unwrap();

    let cfg = DomusConfigBuilder::new()
        .send_timeout(Duration::from_secs(5))
        .listener_delay(Duration::from_millis(0))
        .blob_ack_window(8)
        .blob_chunk_size(4)
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

    let resolver = Arc::new(StaticResolver {
        addr: DomusAddr::Tcp(addr_b),
    });
    let peering = RouteLocalRemoteBuilder::new(
        config_a.clone(),
        registry_a,
        resolver,
        Arc::clone(&transport_a),
    )
    .build();

    let started_notify = Arc::clone(&started);
    let ready_notify = Arc::clone(&ready);
    let send_task = tokio::spawn(async move {
        let outcome = peering
            .send(
                &TestCodec,
                taberna_id,
                &test_message(82, b"blob"),
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
    updated.blob_ack_window = 2;
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

    let result = timeout(Duration::from_secs(10), send_task)
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
