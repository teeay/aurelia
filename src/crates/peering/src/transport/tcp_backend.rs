// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio::time::timeout;
use tokio_rustls::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use tokio_rustls::rustls::sign::{CertifiedKey, SingleCertAndKey};
use tokio_rustls::rustls::{ClientConfig, CommonState, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector, TlsStream};

use ring::rand::{SecureRandom, SystemRandom};

use crate::auth::Pkcs8AuthConfig;
use crate::config::DomusConfigAccess;
use aurelia_data::DomusAddr;
use aurelia_ids::{AureliaError, ErrorId};

use super::backend::{AuthenticatedStream, TransportBackend};
use super::callback_rendezvous::{CallbackRendezvous, CallbackSnapshot};
use super::pkcs8::parse_pkcs8_auth_material;
use super::tls::{verify_peer_cert_uri_inbound, verify_peer_cert_uri_outbound};

const MSG_AUTH_INIT: u8 = 1;
const MSG_CALLBACK_INIT: u8 = 2;
const MSG_AUTH_CHALLENGE: u8 = 3;
const MSG_AUTH_PROOF: u8 = 4;
const NONCE_LEN: usize = 32;
type TcpAuthenticatedStream = AuthenticatedStream<TlsStream<TcpStream>, DomusAddr>;
type TcpAcceptedResult = Result<TcpAuthenticatedStream, AureliaError>;

struct TcpAuthMaterial {
    client: Arc<ClientConfig>,
    server: Arc<ServerConfig>,
}

#[derive(Clone)]
pub struct TcpBackend {
    client: Arc<RwLock<Arc<ClientConfig>>>,
    server: Arc<RwLock<Arc<ServerConfig>>>,
    config: DomusConfigAccess,
    preauth_gate: Arc<super::limits::PreAuthGate>,
    pending_callbacks: Arc<TcpCallbackRendezvous>,
    accepted_tx: mpsc::Sender<TcpAcceptedResult>,
    accepted_rx: Arc<Mutex<mpsc::Receiver<TcpAcceptedResult>>>,
    rng: SystemRandom,
    runtime_handle: tokio::runtime::Handle,
}

#[derive(Debug)]
struct ExpectedCallback {
    expected_addr: std::net::SocketAddr,
    expected_cert: Bytes,
}

#[derive(Debug)]
struct CallbackInfo {
    nonce_b_cb: [u8; NONCE_LEN],
}

#[derive(Debug)]
struct TcpCallbackRendezvous {
    inner: CallbackRendezvous<[u8; NONCE_LEN], ExpectedCallback, CallbackInfo>,
}

impl TcpCallbackRendezvous {
    fn new() -> Self {
        Self {
            inner: CallbackRendezvous::new(),
        }
    }

    async fn register(
        &self,
        nonce_a_cb: [u8; NONCE_LEN],
        expected_addr: std::net::SocketAddr,
        expected_cert: Bytes,
    ) -> (oneshot::Receiver<CallbackInfo>, CallbackSnapshot) {
        CallbackRendezvous::register(
            &self.inner,
            nonce_a_cb,
            ExpectedCallback {
                expected_addr,
                expected_cert,
            },
        )
        .await
    }

    async fn cleanup(&self, nonce_a_cb: &[u8; NONCE_LEN]) -> CallbackSnapshot {
        CallbackRendezvous::cleanup(&self.inner, *nonce_a_cb).await
    }

    async fn fulfill(
        &self,
        peer_addr: std::net::SocketAddr,
        cert_bytes: Bytes,
        nonce_b_cb: [u8; NONCE_LEN],
        echo_nonce_a_cb: [u8; NONCE_LEN],
    ) -> Result<CallbackSnapshot, AureliaError> {
        CallbackRendezvous::fulfill(
            &self.inner,
            echo_nonce_a_cb,
            |expected| {
                expected.expected_addr == peer_addr
                    && expected.expected_cert.as_ref() == cert_bytes.as_ref()
            },
            CallbackInfo { nonce_b_cb },
        )
        .await
    }
}

#[cfg(test)]
impl TcpCallbackRendezvous {
    async fn pending_len(&self) -> usize {
        self.inner.pending_len().await
    }
}

fn parse_pkcs8_auth(auth: Pkcs8AuthConfig) -> Result<TcpAuthMaterial, AureliaError> {
    let material = parse_pkcs8_auth_material(auth)?;
    let roots = material.roots;
    let certs = material.certs;
    let key = material.key_der;

    let mut root_store = RootCertStore::empty();
    for root in roots.iter().cloned() {
        root_store
            .add(root)
            .map_err(|_| AureliaError::new(ErrorId::ProtocolViolation))?;
    }

    let key_der = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(key.as_slice())).clone_key();
    let provider = ClientConfig::builder().crypto_provider().clone();
    let certified_key = Arc::new(
        CertifiedKey::from_der(certs, key_der, &provider)
            .map_err(|_| AureliaError::new(ErrorId::ProtocolViolation))?,
    );
    let client_resolver = Arc::new(SingleCertAndKey::from(Arc::clone(&certified_key)));
    let server_resolver = Arc::new(SingleCertAndKey::from(certified_key));

    let client_config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|_| AureliaError::new(ErrorId::ProtocolViolation))?
        .with_root_certificates(root_store.clone())
        .with_client_cert_resolver(client_resolver);

    let verifier = tokio_rustls::rustls::server::WebPkiClientVerifier::builder(root_store.into())
        .build()
        .map_err(|_| AureliaError::new(ErrorId::ProtocolViolation))?;
    let server_config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|_| AureliaError::new(ErrorId::ProtocolViolation))?
        .with_client_cert_verifier(verifier)
        .with_cert_resolver(server_resolver);

    Ok(TcpAuthMaterial {
        client: Arc::new(client_config),
        server: Arc::new(server_config),
    })
}

impl TcpBackend {
    pub fn new(
        auth: Pkcs8AuthConfig,
        config: DomusConfigAccess,
        runtime_handle: tokio::runtime::Handle,
    ) -> Result<Self, AureliaError> {
        let auth = parse_pkcs8_auth(auth)?;

        let (accepted_tx, accepted_rx) = mpsc::channel(128);

        Ok(Self {
            client: Arc::new(RwLock::new(auth.client)),
            server: Arc::new(RwLock::new(auth.server)),
            config,
            preauth_gate: Arc::new(super::limits::PreAuthGate::new()),
            pending_callbacks: Arc::new(TcpCallbackRendezvous::new()),
            accepted_tx,
            accepted_rx: Arc::new(Mutex::new(accepted_rx)),
            rng: SystemRandom::new(),
            runtime_handle,
        })
    }

    pub async fn reload_auth(&self, auth: Pkcs8AuthConfig) -> Result<(), AureliaError> {
        let auth = parse_pkcs8_auth(auth)?;

        let mut client_guard = self.client.write().await;
        *client_guard = auth.client;
        let mut server_guard = self.server.write().await;
        *server_guard = auth.server;

        Ok(())
    }

    async fn register_pending_callback(
        &self,
        nonce_a_cb: [u8; NONCE_LEN],
        expected_addr: std::net::SocketAddr,
        expected_cert: Bytes,
    ) -> oneshot::Receiver<CallbackInfo> {
        let (rx, _snapshot) = self
            .pending_callbacks
            .register(nonce_a_cb, expected_addr, expected_cert)
            .await;
        rx
    }

    async fn clear_pending_callback(&self, nonce_a_cb: &[u8; NONCE_LEN]) {
        let _snapshot = self.pending_callbacks.cleanup(nonce_a_cb).await;
    }

    async fn fulfill_callback(
        &self,
        peer_addr: std::net::SocketAddr,
        cert_bytes: Bytes,
        nonce_b_cb: [u8; NONCE_LEN],
        echo_nonce_a_cb: [u8; NONCE_LEN],
    ) -> Result<(), AureliaError> {
        self.pending_callbacks
            .fulfill(peer_addr, cert_bytes, nonce_b_cb, echo_nonce_a_cb)
            .await
            .map(|_snapshot| ())
    }

    async fn accept_inbound(
        &self,
        stream: tokio_rustls::server::TlsStream<TcpStream>,
    ) -> Result<Option<AuthenticatedStream<TlsStream<TcpStream>, DomusAddr>>, AureliaError> {
        self.accept_inbound_inner(stream).await
    }

    async fn accept_inbound_inner(
        &self,
        mut stream: tokio_rustls::server::TlsStream<TcpStream>,
    ) -> Result<Option<AuthenticatedStream<TlsStream<TcpStream>, DomusAddr>>, AureliaError> {
        let peer_addr =
            stream.get_ref().0.peer_addr().map_err(|err| {
                AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string())
            })?;
        let peer_identity = verify_peer_cert_uri_inbound(&stream, peer_addr)?;
        let cert_bytes = peer_cert_bytes_from_server(&stream)?;
        let msg = read_auth_message(&mut stream).await?;
        match msg {
            AuthMessage::CallbackInit {
                nonce_b_cb,
                echo_nonce_a_cb,
            } => {
                self.fulfill_callback(peer_identity, cert_bytes, nonce_b_cb, echo_nonce_a_cb)
                    .await?;
                let _ = stream.shutdown().await;
                Ok(None)
            }
            AuthMessage::AuthInit { nonce_a_cb } => {
                let authenticated = self
                    .handle_auth_init(stream, peer_identity, nonce_a_cb)
                    .await?;
                Ok(Some(authenticated))
            }
            _ => Err(AureliaError::new(ErrorId::ProtocolViolation)),
        }
    }

    async fn handle_auth_init(
        &self,
        mut stream: tokio_rustls::server::TlsStream<TcpStream>,
        peer_addr: std::net::SocketAddr,
        nonce_a_cb: [u8; NONCE_LEN],
    ) -> Result<AuthenticatedStream<TlsStream<TcpStream>, DomusAddr>, AureliaError> {
        let nonce_b_cb = random_nonce(&self.rng)?;
        self.send_callback(peer_addr, nonce_b_cb, nonce_a_cb)
            .await?;
        write_auth_challenge(&mut stream).await?;
        let proof = read_auth_message(&mut stream).await?;
        let echo_nonce_b_cb = match proof {
            AuthMessage::AuthProof { echo_nonce_b_cb } => echo_nonce_b_cb,
            _ => return Err(AureliaError::new(ErrorId::ProtocolViolation)),
        };
        if echo_nonce_b_cb != nonce_b_cb {
            return Err(AureliaError::new(ErrorId::ProtocolViolation));
        }
        Ok(AuthenticatedStream {
            stream: TlsStream::from(stream),
            peer_addr: DomusAddr::Tcp(peer_addr),
        })
    }

    async fn outbound_handshake(
        &self,
        mut stream: tokio_rustls::client::TlsStream<TcpStream>,
        peer_addr: std::net::SocketAddr,
    ) -> Result<AuthenticatedStream<TlsStream<TcpStream>, DomusAddr>, AureliaError> {
        let cert_bytes = peer_cert_bytes_from_client(&stream)?;
        let nonce_a_cb = random_nonce(&self.rng)?;
        let callback_rx = self
            .register_pending_callback(nonce_a_cb, peer_addr, cert_bytes)
            .await;
        write_auth_init(&mut stream, nonce_a_cb).await?;

        let callback_timeout = self.config.snapshot().await.tcp_callback_timeout;
        let callback = match timeout(callback_timeout, callback_rx).await {
            Ok(Ok(value)) => value,
            _ => {
                self.clear_pending_callback(&nonce_a_cb).await;
                return Err(AureliaError::with_message(
                    ErrorId::PeerUnavailable,
                    "tcp callback timeout",
                ));
            }
        };

        let challenge = read_auth_message(&mut stream).await?;
        if !matches!(challenge, AuthMessage::AuthChallenge) {
            return Err(AureliaError::new(ErrorId::ProtocolViolation));
        }
        write_auth_proof(&mut stream, callback.nonce_b_cb).await?;

        Ok(AuthenticatedStream {
            stream: TlsStream::from(stream),
            peer_addr: DomusAddr::Tcp(peer_addr),
        })
    }

    async fn send_callback(
        &self,
        peer_addr: std::net::SocketAddr,
        nonce_b_cb: [u8; NONCE_LEN],
        echo_nonce_a_cb: [u8; NONCE_LEN],
    ) -> Result<(), AureliaError> {
        let timeout_duration = self.config.snapshot().await.tcp_callback_timeout;
        let client = self.client.read().await.clone();
        let connector = TlsConnector::from(client);
        let server_name = ServerName::IpAddress(peer_addr.ip().into());
        let result = timeout(timeout_duration, async {
            let tcp = TcpStream::connect(peer_addr).await.map_err(|err| {
                AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string())
            })?;
            let mut stream = connector.connect(server_name, tcp).await.map_err(|err| {
                AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string())
            })?;
            write_callback_init(&mut stream, nonce_b_cb, echo_nonce_a_cb).await?;
            let mut buf = [0u8; 1];
            let _ = stream.read(&mut buf).await;
            Ok::<_, AureliaError>(())
        })
        .await;
        result.map_err(|_| {
            AureliaError::with_message(ErrorId::PeerUnavailable, "tcp callback timeout")
        })?
    }

    fn spawn_accept_socket(&self, socket: TcpStream) {
        let backend = self.clone();
        super::accept::InboundAuthContext::new(
            &self.runtime_handle,
            Arc::clone(&self.preauth_gate),
            self.config.clone(),
            self.accepted_tx.clone(),
            "tcp handshake timeout",
        )
        .spawn(
            socket,
            |config| config.tcp_handshake_timeout,
            move |socket| async move {
                let server = backend.server.read().await.clone();
                let acceptor = TlsAcceptor::from(server);
                let stream = acceptor.accept(socket).await.map_err(|err| {
                    AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string())
                })?;
                backend.accept_inbound(stream).await
            },
        );
    }
}

#[async_trait::async_trait]
impl TransportBackend for TcpBackend {
    type Addr = DomusAddr;
    type Listener = TcpListener;
    type Stream = TlsStream<TcpStream>;

    async fn bind(&self, local: &Self::Addr) -> Result<Self::Listener, AureliaError> {
        let DomusAddr::Tcp(addr) = local else {
            return Err(AureliaError::with_message(
                ErrorId::PeerUnavailable,
                "tcp backend cannot bind non-tcp address",
            ));
        };
        TcpListener::bind(addr)
            .await
            .map_err(|err| AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string()))
    }

    async fn accept(
        &self,
        listener: &mut Self::Listener,
    ) -> Result<AuthenticatedStream<Self::Stream, Self::Addr>, AureliaError> {
        loop {
            tokio::select! {
                queued = async {
                    let mut guard = self.accepted_rx.lock().await;
                    guard.recv().await
                } => {
                    if let Some(result) = queued {
                        return result;
                    }
                }
                accepted = listener.accept() => {
                    let (socket, _) = accepted.map_err(|err| {
                        AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string())
                    })?;
                    self.spawn_accept_socket(socket);
                }
            }
        }
    }

    async fn dial(
        &self,
        peer: &Self::Addr,
    ) -> Result<AuthenticatedStream<Self::Stream, Self::Addr>, AureliaError> {
        let handshake_timeout = self.config.snapshot().await.tcp_handshake_timeout;
        timeout(handshake_timeout, self.dial_inner(peer))
            .await
            .map_err(|_| {
                AureliaError::with_message(ErrorId::PeerUnavailable, "tcp handshake timeout")
            })?
    }
}

impl TcpBackend {
    async fn dial_inner(
        &self,
        peer: &DomusAddr,
    ) -> Result<AuthenticatedStream<TlsStream<TcpStream>, DomusAddr>, AureliaError> {
        let DomusAddr::Tcp(peer_addr) = peer else {
            return Err(AureliaError::with_message(
                ErrorId::PeerUnavailable,
                "tcp backend cannot dial non-tcp address",
            ));
        };
        let tcp = TcpStream::connect(peer_addr)
            .await
            .map_err(|err| AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string()))?;
        let server_name = ServerName::IpAddress(peer_addr.ip().into());
        let client = self.client.read().await.clone();
        let connector = TlsConnector::from(client);
        let stream = connector
            .connect(server_name, tcp)
            .await
            .map_err(|err| AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string()))?;
        verify_peer_cert_uri_outbound(&stream, *peer_addr)?;
        self.outbound_handshake(stream, *peer_addr).await
    }
}

enum AuthMessage {
    AuthInit {
        nonce_a_cb: [u8; NONCE_LEN],
    },
    CallbackInit {
        nonce_b_cb: [u8; NONCE_LEN],
        echo_nonce_a_cb: [u8; NONCE_LEN],
    },
    AuthChallenge,
    AuthProof {
        echo_nonce_b_cb: [u8; NONCE_LEN],
    },
}

fn random_nonce(rng: &SystemRandom) -> Result<[u8; NONCE_LEN], AureliaError> {
    let mut buf = [0u8; NONCE_LEN];
    rng.fill(&mut buf)
        .map_err(|_| AureliaError::new(ErrorId::ProtocolViolation))?;
    Ok(buf)
}

fn peer_cert_bytes_from_common_state(session: &CommonState) -> Result<Bytes, AureliaError> {
    let Some(certs) = session.peer_certificates() else {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    };
    certs
        .first()
        .map(|cert| Bytes::copy_from_slice(cert.as_ref()))
        .ok_or_else(|| AureliaError::new(ErrorId::ProtocolViolation))
}

fn peer_cert_bytes_from_server(
    stream: &tokio_rustls::server::TlsStream<TcpStream>,
) -> Result<Bytes, AureliaError> {
    let (_, session) = stream.get_ref();
    peer_cert_bytes_from_common_state(session)
}

fn peer_cert_bytes_from_client(
    stream: &tokio_rustls::client::TlsStream<TcpStream>,
) -> Result<Bytes, AureliaError> {
    let (_, session) = stream.get_ref();
    peer_cert_bytes_from_common_state(session)
}

#[cfg(test)]
#[path = "tests/leaf/tcp_backend.rs"]
mod tests;

async fn read_auth_message<S: AsyncReadExt + Unpin>(
    stream: &mut S,
) -> Result<AuthMessage, AureliaError> {
    let msg_type = read_type(stream).await?;
    match msg_type {
        MSG_AUTH_INIT => {
            let nonce_a_cb = read_exact_array::<_, NONCE_LEN>(stream).await?;
            Ok(AuthMessage::AuthInit { nonce_a_cb })
        }
        MSG_CALLBACK_INIT => {
            let nonce_b_cb = read_exact_array::<_, NONCE_LEN>(stream).await?;
            let echo_nonce_a_cb = read_exact_array::<_, NONCE_LEN>(stream).await?;
            Ok(AuthMessage::CallbackInit {
                nonce_b_cb,
                echo_nonce_a_cb,
            })
        }
        MSG_AUTH_CHALLENGE => Ok(AuthMessage::AuthChallenge),
        MSG_AUTH_PROOF => {
            let echo_nonce_b_cb = read_exact_array::<_, NONCE_LEN>(stream).await?;
            Ok(AuthMessage::AuthProof { echo_nonce_b_cb })
        }
        _ => Err(AureliaError::new(ErrorId::ProtocolViolation)),
    }
}

async fn read_type<S: AsyncReadExt + Unpin>(stream: &mut S) -> Result<u8, AureliaError> {
    let mut buf = [0u8; 1];
    stream
        .read_exact(&mut buf)
        .await
        .map_err(|err| AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string()))?;
    Ok(buf[0])
}

async fn read_exact_array<S: AsyncReadExt + Unpin, const N: usize>(
    stream: &mut S,
) -> Result<[u8; N], AureliaError> {
    let mut buf = [0u8; N];
    stream
        .read_exact(&mut buf)
        .await
        .map_err(|err| AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string()))?;
    Ok(buf)
}

async fn write_auth_init<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    nonce_a_cb: [u8; NONCE_LEN],
) -> Result<(), AureliaError> {
    write_type(stream, MSG_AUTH_INIT).await?;
    write_all(stream, &nonce_a_cb).await?;
    Ok(())
}

async fn write_callback_init<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    nonce_b_cb: [u8; NONCE_LEN],
    echo_nonce_a_cb: [u8; NONCE_LEN],
) -> Result<(), AureliaError> {
    write_type(stream, MSG_CALLBACK_INIT).await?;
    write_all(stream, &nonce_b_cb).await?;
    write_all(stream, &echo_nonce_a_cb).await?;
    Ok(())
}

async fn write_auth_challenge<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
) -> Result<(), AureliaError> {
    write_type(stream, MSG_AUTH_CHALLENGE).await?;
    Ok(())
}

async fn write_auth_proof<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    echo_nonce_b_cb: [u8; NONCE_LEN],
) -> Result<(), AureliaError> {
    write_type(stream, MSG_AUTH_PROOF).await?;
    write_all(stream, &echo_nonce_b_cb).await?;
    Ok(())
}

async fn write_type<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    value: u8,
) -> Result<(), AureliaError> {
    write_all(stream, &[value]).await
}

async fn write_all<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    buf: &[u8],
) -> Result<(), AureliaError> {
    stream
        .write_all(buf)
        .await
        .map_err(|err| AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string()))?;
    stream
        .flush()
        .await
        .map_err(|err| AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string()))?;
    Ok(())
}
