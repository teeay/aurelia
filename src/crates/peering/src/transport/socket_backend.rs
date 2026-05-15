// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio::time::timeout;
use tokio_rustls::rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, UnixTime,
};
use tokio_rustls::rustls::server::danger::ClientCertVerifier;
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::RootCertStore;

use asn1_rs::Oid;
use ring::rand::{SecureRandom, SystemRandom};
use ring::signature;
use x509_parser::oid_registry::{
    OID_EC_P256, OID_KEY_TYPE_EC_PUBLIC_KEY, OID_NIST_EC_P384, OID_PKCS1_RSAENCRYPTION,
    OID_SIG_ED25519,
};

use crate::auth::Pkcs8AuthConfig;
use crate::config::{DomusConfigAccess, MAX_INBOUND_HANDSHAKE_LIMIT_TOTAL};
use aurelia_data::DomusAddr;
use aurelia_ids::{AureliaError, ErrorId};

use super::backend::{AuthenticatedStream, TransportBackend};
use super::callback_rendezvous::{CallbackRendezvous, CallbackSnapshot};
use super::frame::wire_payload_len;
use super::pkcs8::parse_pkcs8_auth_material;

const AUTH_VERSION: u8 = 1;
const MSG_AUTH_INIT: u8 = 1;
const MSG_AUTH_CHALLENGE: u8 = 2;
const MSG_AUTH_PROOF: u8 = 3;
const MSG_CALLBACK_INIT: u8 = 4;
const MAX_FRAME_LEN: usize = 64 * 1024;
const NONCE_LEN: usize = 32;
const PATH_MAX_LEN: usize = libc::PATH_MAX as usize;
const SOCKET_FS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
const MAX_CERT_CHAIN_LEN: usize = 16;
type SocketAuthenticatedStream = AuthenticatedStream<UnixStream, DomusAddr>;
type SocketAcceptedResult = Result<SocketAuthenticatedStream, AureliaError>;

#[derive(Clone)]
pub struct SocketBackend {
    auth: Arc<RwLock<Arc<SocketAuthMaterial>>>,
    config: DomusConfigAccess,
    local_path: Arc<Mutex<Option<PathBuf>>>,
    preauth_gate: Arc<super::limits::PreAuthGate>,
    pending_callbacks: Arc<SocketCallbackRendezvous>,
    accepted_tx: mpsc::Sender<SocketAcceptedResult>,
    accepted_rx: Arc<Mutex<mpsc::Receiver<SocketAcceptedResult>>>,
    rng: SystemRandom,
    runtime_handle: tokio::runtime::Handle,
}

struct SocketAuthMaterial {
    cert_chain_der: Vec<Vec<u8>>,
    signer: LocalSigner,
    verifier: Arc<dyn ClientCertVerifier>,
}

#[derive(Debug)]
struct CallbackInfo {
    origin_path: PathBuf,
    destination_path: PathBuf,
    nonce_b_cb: Vec<u8>,
}

#[derive(Debug)]
struct ExpectedSocketCallback {
    expected_origin_path: PathBuf,
    expected_destination_path: PathBuf,
}

#[derive(Debug)]
struct SocketCallbackRendezvous {
    inner: CallbackRendezvous<Vec<u8>, ExpectedSocketCallback, CallbackInfo>,
}

enum LocalSigner {
    Rsa(signature::RsaKeyPair),
    EcdsaP256(signature::EcdsaKeyPair),
    EcdsaP384(signature::EcdsaKeyPair),
    Ed25519(signature::Ed25519KeyPair),
}

impl SocketCallbackRendezvous {
    fn new() -> Self {
        Self {
            inner: CallbackRendezvous::new(),
        }
    }

    async fn register(
        &self,
        nonce_a_cb: Vec<u8>,
        expected_origin_path: PathBuf,
        expected_destination_path: PathBuf,
    ) -> (oneshot::Receiver<CallbackInfo>, CallbackSnapshot) {
        CallbackRendezvous::register(
            &self.inner,
            nonce_a_cb,
            ExpectedSocketCallback {
                expected_origin_path,
                expected_destination_path,
            },
        )
        .await
    }

    async fn cleanup(&self, nonce_a_cb: &[u8]) -> CallbackSnapshot {
        CallbackRendezvous::cleanup(&self.inner, nonce_a_cb.to_vec()).await
    }

    async fn fulfill(
        &self,
        nonce_a_cb: &[u8],
        info: CallbackInfo,
    ) -> Result<CallbackSnapshot, AureliaError> {
        let origin_path = info.origin_path.clone();
        let destination_path = info.destination_path.clone();
        CallbackRendezvous::fulfill(
            &self.inner,
            nonce_a_cb.to_vec(),
            |expected| {
                expected.expected_origin_path == origin_path
                    && expected.expected_destination_path == destination_path
            },
            info,
        )
        .await
    }
}

#[cfg(test)]
impl SocketCallbackRendezvous {
    async fn pending_len(&self) -> usize {
        self.inner.pending_len().await
    }
}

impl SocketBackend {
    async fn with_fs_timeout<T>(
        op: impl Future<Output = std::io::Result<T>>,
    ) -> Result<std::io::Result<T>, AureliaError> {
        timeout(SOCKET_FS_TIMEOUT, op).await.map_err(|_| {
            AureliaError::with_message(ErrorId::PeerUnavailable, "socket filesystem timeout")
        })
    }

    async fn path_exists(path: &Path) -> Result<bool, AureliaError> {
        match Self::with_fs_timeout(tokio::fs::metadata(path)).await? {
            Ok(_) => Ok(true),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(err) => Err(AureliaError::with_message(
                ErrorId::ProtocolViolation,
                err.to_string(),
            )),
        }
    }

    async fn canonicalize_path(path: &Path) -> Result<PathBuf, AureliaError> {
        match Self::with_fs_timeout(tokio::fs::canonicalize(path)).await? {
            Ok(canonical) => Ok(canonical),
            Err(err) => Err(AureliaError::with_message(
                ErrorId::ProtocolViolation,
                err.to_string(),
            )),
        }
    }

    async fn remove_socket_file(path: &Path) -> Result<(), AureliaError> {
        match Self::with_fs_timeout(tokio::fs::remove_file(path)).await? {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(AureliaError::with_message(
                ErrorId::PeerUnavailable,
                err.to_string(),
            )),
        }
    }

    pub fn new(
        auth: Pkcs8AuthConfig,
        config: DomusConfigAccess,
        runtime_handle: tokio::runtime::Handle,
    ) -> Result<Self, AureliaError> {
        let auth = parse_pkcs8_auth(auth)?;
        let (accepted_tx, accepted_rx) = mpsc::channel(MAX_INBOUND_HANDSHAKE_LIMIT_TOTAL);
        Ok(Self {
            auth: Arc::new(RwLock::new(Arc::new(auth))),
            config,
            local_path: Arc::new(Mutex::new(None)),
            preauth_gate: Arc::new(super::limits::PreAuthGate::new()),
            pending_callbacks: Arc::new(SocketCallbackRendezvous::new()),
            accepted_tx,
            accepted_rx: Arc::new(Mutex::new(accepted_rx)),
            rng: SystemRandom::new(),
            runtime_handle,
        })
    }

    pub async fn reload_auth(&self, auth: Pkcs8AuthConfig) -> Result<(), AureliaError> {
        let auth = parse_pkcs8_auth(auth)?;
        let mut guard = self.auth.write().await;
        *guard = Arc::new(auth);
        Ok(())
    }

    pub(super) async fn canonicalize_socket_path(path: &Path) -> Result<PathBuf, AureliaError> {
        if !path.is_absolute() {
            return Err(AureliaError::with_message(
                ErrorId::ProtocolViolation,
                "socket path must be absolute",
            ));
        }
        let raw = path.to_str().ok_or_else(|| {
            AureliaError::with_message(ErrorId::ProtocolViolation, "socket path not utf8")
        })?;
        if raw.is_empty() || raw.len() > PATH_MAX_LEN {
            return Err(AureliaError::with_message(
                ErrorId::ProtocolViolation,
                "socket path length invalid",
            ));
        }
        if Self::path_exists(path).await? {
            let canonical = Self::canonicalize_path(path).await?;
            let canonical_str = canonical.to_str().ok_or_else(|| {
                AureliaError::with_message(ErrorId::ProtocolViolation, "socket path not utf8")
            })?;
            if canonical_str.is_empty() || canonical_str.len() > PATH_MAX_LEN {
                return Err(AureliaError::with_message(
                    ErrorId::ProtocolViolation,
                    "socket path length invalid",
                ));
            }
            return Ok(canonical);
        }
        let parent = path.parent().ok_or_else(|| {
            AureliaError::with_message(ErrorId::ProtocolViolation, "socket path missing parent")
        })?;
        let parent = Self::canonicalize_path(parent).await?;
        if parent.to_str().is_none() {
            return Err(AureliaError::with_message(
                ErrorId::ProtocolViolation,
                "socket path not utf8",
            ));
        }
        let file_name = path.file_name().ok_or_else(|| {
            AureliaError::with_message(ErrorId::ProtocolViolation, "socket path missing filename")
        })?;
        Ok(parent.join(file_name))
    }

    async fn local_path(&self) -> Result<PathBuf, AureliaError> {
        let guard = self.local_path.lock().await;
        guard.as_ref().cloned().ok_or_else(|| {
            AureliaError::with_message(ErrorId::PeerUnavailable, "socket backend not bound")
        })
    }

    async fn register_pending_callback(
        &self,
        nonce_a_cb: Vec<u8>,
        expected_origin_path: PathBuf,
        expected_destination_path: PathBuf,
    ) -> oneshot::Receiver<CallbackInfo> {
        let (rx, _snapshot) = self
            .pending_callbacks
            .register(nonce_a_cb, expected_origin_path, expected_destination_path)
            .await;
        rx
    }

    async fn clear_pending_callback(&self, nonce_a_cb: &[u8]) {
        let _snapshot = self.pending_callbacks.cleanup(nonce_a_cb).await;
    }

    async fn fulfill_callback(
        &self,
        nonce_a_cb: &[u8],
        info: CallbackInfo,
    ) -> Result<(), AureliaError> {
        self.pending_callbacks
            .fulfill(nonce_a_cb, info)
            .await
            .map(|_snapshot| ())
    }

    async fn read_first_message(&self, stream: &mut UnixStream) -> Result<Vec<u8>, AureliaError> {
        read_framed(stream).await
    }

    async fn handle_callback_message(&self, payload: &[u8]) -> Result<(), AureliaError> {
        let msg = parse_callback_init(payload).await?;
        let local = self.local_path().await?;
        if msg.destination_path != local {
            return Err(AureliaError::new(ErrorId::ProtocolViolation));
        }
        let info = CallbackInfo {
            origin_path: msg.origin_path,
            destination_path: msg.destination_path,
            nonce_b_cb: msg.nonce_b_cb,
        };
        self.fulfill_callback(&msg.echo_nonce_a_cb, info).await
    }

    async fn accept_inbound(
        &self,
        mut stream: UnixStream,
    ) -> Result<Option<AuthenticatedStream<UnixStream, DomusAddr>>, AureliaError> {
        let payload = self.read_first_message(&mut stream).await?;
        let msg_type = payload
            .first()
            .copied()
            .ok_or_else(|| AureliaError::new(ErrorId::ProtocolViolation))?;
        if msg_type == MSG_CALLBACK_INIT {
            self.handle_callback_message(&payload).await?;
            return Ok(None);
        }
        match msg_type {
            MSG_AUTH_INIT => self.handle_auth_init(stream, payload).await.map(Some),
            _ => Err(AureliaError::new(ErrorId::ProtocolViolation)),
        }
    }

    async fn handle_auth_init(
        &self,
        mut stream: UnixStream,
        payload: Vec<u8>,
    ) -> Result<AuthenticatedStream<UnixStream, DomusAddr>, AureliaError> {
        let msg = parse_auth_init(&payload).await?;
        let local = self.local_path().await?;
        let auth = self.auth.read().await.clone();
        if msg.destination_path != local {
            return Err(AureliaError::new(ErrorId::ProtocolViolation));
        }
        self.verify_peer_cert(&auth, &msg.cert_chain_der, &msg.origin_path)
            .await?;
        let nonce_b = random_bytes(&self.rng, NONCE_LEN)?;
        let nonce_b_cb = random_bytes(&self.rng, NONCE_LEN)?;
        self.send_callback(&msg.origin_path, &local, &nonce_b_cb, &msg.nonce_a_cb)
            .await?;
        let signature = self.sign_message(
            &auth,
            &auth_challenge_sig(&msg.nonce_a, &nonce_b, &local, &msg.origin_path)?,
        )?;
        let challenge = AuthChallenge {
            origin_path: local.clone(),
            destination_path: msg.origin_path.clone(),
            cert_chain_der: auth.cert_chain_der.clone(),
            nonce_b: nonce_b.clone(),
            signature,
        };
        write_framed(&mut stream, &encode_auth_challenge(&challenge)?).await?;
        let proof_payload = read_framed(&mut stream).await?;
        let proof = parse_auth_proof(&proof_payload)?;
        if proof.echo_nonce_b_cb != nonce_b_cb {
            return Err(AureliaError::new(ErrorId::ProtocolViolation));
        }
        let proof_message = auth_proof_sig(
            &nonce_b,
            &msg.nonce_a,
            &nonce_b_cb,
            &msg.origin_path,
            &local,
        )?;
        self.verify_signature(&msg.cert_chain_der[0], &proof_message, &proof.signature)?;
        Ok(AuthenticatedStream {
            stream,
            peer_addr: DomusAddr::Socket(msg.origin_path),
        })
    }

    async fn send_callback(
        &self,
        peer_path: &Path,
        local_path: &Path,
        nonce_b_cb: &[u8],
        echo_nonce_a_cb: &[u8],
    ) -> Result<(), AureliaError> {
        let timeout_duration = self.config.snapshot().await.socket_callback_timeout;
        let stream = timeout(timeout_duration, UnixStream::connect(peer_path))
            .await
            .map_err(|_| {
                AureliaError::with_message(ErrorId::PeerUnavailable, "socket callback timeout")
            })?
            .map_err(|err| AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string()))?;
        let msg = CallbackInit {
            origin_path: local_path.to_path_buf(),
            destination_path: peer_path.to_path_buf(),
            nonce_b_cb: nonce_b_cb.to_vec(),
            echo_nonce_a_cb: echo_nonce_a_cb.to_vec(),
        };
        let mut stream = stream;
        write_framed(&mut stream, &encode_callback_init(&msg)?).await?;
        let _ = stream.shutdown().await;
        Ok(())
    }

    async fn outbound_handshake(
        &self,
        stream: UnixStream,
        peer_path: PathBuf,
    ) -> Result<AuthenticatedStream<UnixStream, DomusAddr>, AureliaError> {
        self.outbound_connect_back_handshake(stream, peer_path)
            .await
    }

    async fn outbound_connect_back_handshake(
        &self,
        mut stream: UnixStream,
        peer_path: PathBuf,
    ) -> Result<AuthenticatedStream<UnixStream, DomusAddr>, AureliaError> {
        let auth = self.auth.read().await.clone();
        let local = self.local_path().await?;
        let nonce_a = random_bytes(&self.rng, NONCE_LEN)?;
        let nonce_a_cb = random_bytes(&self.rng, NONCE_LEN)?;
        let auth_init = AuthInit {
            origin_path: local.clone(),
            destination_path: peer_path.clone(),
            cert_chain_der: auth.cert_chain_der.clone(),
            nonce_a: nonce_a.clone(),
            nonce_a_cb: nonce_a_cb.clone(),
        };
        let callback_rx = self
            .register_pending_callback(nonce_a_cb.clone(), peer_path.clone(), local.clone())
            .await;
        write_framed(&mut stream, &encode_auth_init(&auth_init)?).await?;
        let callback_timeout = self.config.snapshot().await.socket_callback_timeout;
        let callback = match timeout(callback_timeout, callback_rx).await {
            Ok(Ok(value)) => value,
            _ => {
                self.clear_pending_callback(&nonce_a_cb).await;
                return Err(AureliaError::with_message(
                    ErrorId::PeerUnavailable,
                    "socket callback timeout",
                ));
            }
        };
        let challenge_payload = read_framed(&mut stream).await.map_err(|err| {
            AureliaError::with_message(err.kind, format!("read auth challenge: {err}"))
        })?;
        let challenge = parse_auth_challenge(&challenge_payload)
            .await
            .map_err(|err| {
                AureliaError::with_message(err.kind, format!("parse auth challenge: {err}"))
            })?;
        if challenge.origin_path != callback.origin_path {
            return Err(AureliaError::with_message(
                ErrorId::ProtocolViolation,
                "challenge origin mismatch",
            ));
        }
        if challenge.destination_path != local {
            return Err(AureliaError::with_message(
                ErrorId::ProtocolViolation,
                "challenge destination mismatch",
            ));
        }
        self.verify_peer_cert(&auth, &challenge.cert_chain_der, &challenge.origin_path)
            .await
            .map_err(|err| {
                AureliaError::with_message(err.kind, format!("verify peer cert: {err}"))
            })?;
        let challenge_message = auth_challenge_sig(
            &nonce_a,
            &challenge.nonce_b,
            &challenge.origin_path,
            &challenge.destination_path,
        )
        .map_err(|err| {
            AureliaError::with_message(err.kind, format!("challenge sig input: {err}"))
        })?;
        self.verify_signature(
            &challenge.cert_chain_der[0],
            &challenge_message,
            &challenge.signature,
        )
        .map_err(|err| {
            AureliaError::with_message(err.kind, format!("verify challenge signature: {err}"))
        })?;
        let proof_signature = self
            .sign_message(
                &auth,
                &auth_proof_sig(
                    &challenge.nonce_b,
                    &nonce_a,
                    &callback.nonce_b_cb,
                    &local,
                    &challenge.origin_path,
                )?,
            )
            .map_err(|err| AureliaError::with_message(err.kind, format!("sign proof: {err}")))?;
        let proof = AuthProof {
            echo_nonce_b_cb: callback.nonce_b_cb.clone(),
            signature: proof_signature,
        };
        write_framed(&mut stream, &encode_auth_proof(&proof)?)
            .await
            .map_err(|err| {
                AureliaError::with_message(err.kind, format!("send auth proof: {err}"))
            })?;
        let _ = nonce_a;
        let _ = nonce_a_cb;
        let _ = callback.nonce_b_cb;
        Ok(AuthenticatedStream {
            stream,
            peer_addr: DomusAddr::Socket(peer_path),
        })
    }

    async fn verify_peer_cert(
        &self,
        auth: &SocketAuthMaterial,
        cert_chain_der: &[Vec<u8>],
        expected_path: &Path,
    ) -> Result<(), AureliaError> {
        let leaf = cert_chain_der
            .first()
            .ok_or_else(|| AureliaError::new(ErrorId::ProtocolViolation))?;
        let cert = CertificateDer::from(leaf.clone());
        let intermediates = cert_chain_der
            .iter()
            .skip(1)
            .cloned()
            .map(CertificateDer::from)
            .collect::<Vec<_>>();
        auth.verifier
            .verify_client_cert(&cert, &intermediates, UnixTime::now())
            .map_err(|_| {
                AureliaError::with_message(ErrorId::ProtocolViolation, "cert verify failed")
            })?;
        let peer_path = extract_peer_uri_san_socket(leaf).await?;
        if peer_path != expected_path {
            return Err(AureliaError::with_message(
                ErrorId::ProtocolViolation,
                "cert SAN mismatch",
            ));
        }
        Ok(())
    }

    fn sign_message(
        &self,
        auth: &SocketAuthMaterial,
        message: &[u8],
    ) -> Result<Vec<u8>, AureliaError> {
        match &auth.signer {
            LocalSigner::Rsa(key) => {
                let mut sig = vec![0u8; key.public().modulus_len()];
                key.sign(&signature::RSA_PSS_SHA256, &self.rng, message, &mut sig)
                    .map_err(|_| AureliaError::new(ErrorId::ProtocolViolation))?;
                Ok(sig)
            }
            LocalSigner::EcdsaP256(key) => {
                let sig = key
                    .sign(&self.rng, message)
                    .map_err(|_| AureliaError::new(ErrorId::ProtocolViolation))?;
                Ok(sig.as_ref().to_vec())
            }
            LocalSigner::EcdsaP384(key) => {
                let sig = key
                    .sign(&self.rng, message)
                    .map_err(|_| AureliaError::new(ErrorId::ProtocolViolation))?;
                Ok(sig.as_ref().to_vec())
            }
            LocalSigner::Ed25519(key) => Ok(key.sign(message).as_ref().to_vec()),
        }
    }

    fn verify_signature(
        &self,
        cert_der: &[u8],
        message: &[u8],
        signature_bytes: &[u8],
    ) -> Result<(), AureliaError> {
        let (_, cert) = x509_parser::parse_x509_certificate(cert_der)
            .map_err(|_| AureliaError::new(ErrorId::ProtocolViolation))?;
        let spki = &cert.tbs_certificate.subject_pki;
        // Socket proof signatures bind peer identity through the validated leaf certificate:
        // certificate validation proves the public key belongs to the expected socket URI SAN,
        // and this check proves the peer owns the matching private key.
        let algo = signature_algorithm_for_cert(spki)?;
        let public_key_bytes = spki.subject_public_key.data.as_ref();
        let public_key = signature::UnparsedPublicKey::new(algo, public_key_bytes);
        public_key.verify(message, signature_bytes).map_err(|_| {
            AureliaError::with_message(ErrorId::ProtocolViolation, "signature verify failed")
        })
    }
}

#[cfg(test)]
impl SocketBackend {
    fn accepted_queue_max_capacity(&self) -> usize {
        self.accepted_tx.max_capacity()
    }
}

#[async_trait::async_trait]
impl TransportBackend for SocketBackend {
    type Addr = DomusAddr;
    type Listener = UnixListener;
    type Stream = UnixStream;

    async fn bind(&self, local: &Self::Addr) -> Result<Self::Listener, AureliaError> {
        let DomusAddr::Socket(path) = local else {
            return Err(AureliaError::with_message(
                ErrorId::PeerUnavailable,
                "socket backend cannot bind non-socket address",
            ));
        };
        let canonical = Self::canonicalize_socket_path(path).await?;
        {
            let mut guard = self.local_path.lock().await;
            if let Some(existing) = guard.as_ref() {
                if existing != &canonical {
                    return Err(AureliaError::new(ErrorId::ProtocolViolation));
                }
            } else {
                *guard = Some(canonical.clone());
            }
        }
        if Self::path_exists(&canonical).await? {
            Self::remove_socket_file(&canonical).await?;
        }
        UnixListener::bind(&canonical)
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
                    let (stream, _) = accepted.map_err(|err| {
                        AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string())
                    })?;
                    self.spawn_accept_socket(stream);
                }
            }
        }
    }

    async fn dial(
        &self,
        peer: &Self::Addr,
    ) -> Result<AuthenticatedStream<Self::Stream, Self::Addr>, AureliaError> {
        let handshake_timeout = self.config.snapshot().await.socket_handshake_timeout;
        timeout(handshake_timeout, self.dial_inner(peer))
            .await
            .map_err(|_| {
                AureliaError::with_message(ErrorId::PeerUnavailable, "socket handshake timeout")
            })?
    }
}

impl SocketBackend {
    fn spawn_accept_socket(&self, stream: UnixStream) {
        let backend = self.clone();
        super::accept::InboundAuthContext::new(
            &self.runtime_handle,
            Arc::clone(&self.preauth_gate),
            self.config.clone(),
            self.accepted_tx.clone(),
            "socket handshake timeout",
        )
        .spawn(
            stream,
            |config| config.socket_handshake_timeout,
            move |stream| async move { backend.accept_inbound(stream).await },
        );
    }

    async fn dial_inner(
        &self,
        peer: &DomusAddr,
    ) -> Result<AuthenticatedStream<UnixStream, DomusAddr>, AureliaError> {
        let DomusAddr::Socket(path) = peer else {
            return Err(AureliaError::with_message(
                ErrorId::PeerUnavailable,
                "socket backend cannot dial non-socket address",
            ));
        };
        let peer_path = Self::canonicalize_socket_path(path).await?;
        let stream = UnixStream::connect(&peer_path)
            .await
            .map_err(|err| AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string()))?;
        self.outbound_handshake(stream, peer_path).await
    }
}

fn parse_pkcs8_auth(auth: Pkcs8AuthConfig) -> Result<SocketAuthMaterial, AureliaError> {
    let material = parse_pkcs8_auth_material(auth)?;
    let roots = material.roots;
    let certs = material.certs;
    let key_bytes = material.key_der;
    let mut root_store = RootCertStore::empty();
    for root in roots.iter().cloned() {
        root_store
            .add(root)
            .map_err(|_| AureliaError::new(ErrorId::ProtocolViolation))?;
    }
    let verifier = WebPkiClientVerifier::builder(Arc::new(root_store))
        .build()
        .map_err(|_| AureliaError::new(ErrorId::ProtocolViolation))?;
    let key_der = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(key_bytes.as_slice()));
    let signer = build_local_signer(&key_der)
        .ok_or_else(|| AureliaError::new(ErrorId::ProtocolViolation))?;
    if certs.len() > MAX_CERT_CHAIN_LEN {
        return Err(AureliaError::with_message(
            ErrorId::ProtocolViolation,
            "certificate chain too long",
        ));
    }
    let cert_chain_der = certs
        .into_iter()
        .map(|cert| cert.as_ref().to_vec())
        .collect::<Vec<_>>();
    if cert_chain_der.is_empty() {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    Ok(SocketAuthMaterial {
        cert_chain_der,
        signer,
        verifier,
    })
}

fn build_local_signer(key_der: &PrivateKeyDer<'_>) -> Option<LocalSigner> {
    if let PrivateKeyDer::Pkcs8(pkcs8) = key_der {
        if let Ok(key) = signature::Ed25519KeyPair::from_pkcs8(pkcs8.secret_pkcs8_der()) {
            return Some(LocalSigner::Ed25519(key));
        }
        let rng = SystemRandom::new();
        if let Ok(key) = signature::EcdsaKeyPair::from_pkcs8(
            &signature::ECDSA_P256_SHA256_ASN1_SIGNING,
            pkcs8.secret_pkcs8_der(),
            &rng,
        ) {
            return Some(LocalSigner::EcdsaP256(key));
        }
        if let Ok(key) = signature::EcdsaKeyPair::from_pkcs8(
            &signature::ECDSA_P384_SHA384_ASN1_SIGNING,
            pkcs8.secret_pkcs8_der(),
            &rng,
        ) {
            return Some(LocalSigner::EcdsaP384(key));
        }
        if let Ok(key) = signature::RsaKeyPair::from_pkcs8(pkcs8.secret_pkcs8_der()) {
            return Some(LocalSigner::Rsa(key));
        }
    }
    if let PrivateKeyDer::Pkcs1(pkcs1) = key_der {
        if let Ok(key) = signature::RsaKeyPair::from_der(pkcs1.secret_pkcs1_der()) {
            return Some(LocalSigner::Rsa(key));
        }
    }
    None
}

async fn extract_peer_uri_san_socket(cert_der: &[u8]) -> Result<PathBuf, AureliaError> {
    let (_, cert) = x509_parser::parse_x509_certificate(cert_der)
        .map_err(|_| AureliaError::new(ErrorId::ProtocolViolation))?;
    let san = cert
        .subject_alternative_name()
        .map_err(|_| AureliaError::new(ErrorId::ProtocolViolation))?;
    let san = san.ok_or_else(|| AureliaError::new(ErrorId::ProtocolViolation))?;
    let mut found: Option<PathBuf> = None;
    for entry in san.value.general_names.iter() {
        if let x509_parser::extensions::GeneralName::URI(uri) = entry {
            if let Some(path) = parse_aurelia_unix_uri(uri).await? {
                if let Some(existing) = &found {
                    if existing != &path {
                        return Err(AureliaError::new(ErrorId::ProtocolViolation));
                    }
                } else {
                    found = Some(path);
                }
            }
        }
    }
    found.ok_or_else(|| AureliaError::new(ErrorId::ProtocolViolation))
}

async fn parse_aurelia_unix_uri(uri: &str) -> Result<Option<PathBuf>, AureliaError> {
    const PREFIX: &str = "aurelia+unix://";
    let Some(rest) = uri.strip_prefix(PREFIX) else {
        return Ok(None);
    };
    let path = PathBuf::from(rest);
    let canonical = SocketBackend::canonicalize_socket_path(&path).await?;
    if canonical != path {
        return Err(AureliaError::with_message(
            ErrorId::ProtocolViolation,
            "SAN path not canonical",
        ));
    }
    Ok(Some(path))
}

fn signature_algorithm_for_cert(
    spki: &x509_parser::x509::SubjectPublicKeyInfo<'_>,
) -> Result<&'static dyn signature::VerificationAlgorithm, AureliaError> {
    if spki.algorithm.algorithm == OID_PKCS1_RSAENCRYPTION {
        // Socket RSA proofs are intentionally PSS/SHA-256. PKCS#1 v1.5 signatures are not
        // accepted for the socket proof protocol.
        return Ok(&signature::RSA_PSS_2048_8192_SHA256);
    }
    if spki.algorithm.algorithm == OID_KEY_TYPE_EC_PUBLIC_KEY {
        let params = spki
            .algorithm
            .parameters
            .as_ref()
            .ok_or_else(|| AureliaError::new(ErrorId::ProtocolViolation))?;
        let curve_oid =
            Oid::try_from(params).map_err(|_| AureliaError::new(ErrorId::ProtocolViolation))?;
        if curve_oid == OID_EC_P256 {
            return Ok(&signature::ECDSA_P256_SHA256_ASN1);
        }
        if curve_oid == OID_NIST_EC_P384 {
            return Ok(&signature::ECDSA_P384_SHA384_ASN1);
        }
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    if spki.algorithm.algorithm == OID_SIG_ED25519 {
        return Ok(&signature::ED25519);
    }
    Err(AureliaError::new(ErrorId::ProtocolViolation))
}

fn random_bytes(rng: &SystemRandom, len: usize) -> Result<Vec<u8>, AureliaError> {
    let mut buf = vec![0u8; len];
    rng.fill(&mut buf)
        .map_err(|_| AureliaError::new(ErrorId::ProtocolViolation))?;
    Ok(buf)
}

fn auth_challenge_sig(
    nonce_a: &[u8],
    nonce_b: &[u8],
    origin_path: &Path,
    destination_path: &Path,
) -> Result<Vec<u8>, AureliaError> {
    if nonce_a.len() != NONCE_LEN || nonce_b.len() != NONCE_LEN {
        return Err(AureliaError::with_message(
            ErrorId::ProtocolViolation,
            "invalid challenge nonce length",
        ));
    }
    let origin = path_bytes(origin_path)?;
    let dest = path_bytes(destination_path)?;
    let mut data = Vec::new();
    data.extend_from_slice(nonce_a);
    data.extend_from_slice(nonce_b);
    put_u16(&mut data, origin.len() as u16);
    put_u16(&mut data, dest.len() as u16);
    data.extend_from_slice(&origin);
    data.extend_from_slice(&dest);
    Ok(data)
}

fn auth_proof_sig(
    nonce_b: &[u8],
    nonce_a: &[u8],
    nonce_b_cb: &[u8],
    origin_path: &Path,
    destination_path: &Path,
) -> Result<Vec<u8>, AureliaError> {
    if nonce_a.len() != NONCE_LEN || nonce_b.len() != NONCE_LEN || nonce_b_cb.len() != NONCE_LEN {
        return Err(AureliaError::with_message(
            ErrorId::ProtocolViolation,
            "invalid proof nonce length",
        ));
    }
    let origin = path_bytes(origin_path)?;
    let dest = path_bytes(destination_path)?;
    let mut data = Vec::new();
    data.extend_from_slice(nonce_b);
    data.extend_from_slice(nonce_a);
    data.extend_from_slice(nonce_b_cb);
    put_u16(&mut data, origin.len() as u16);
    put_u16(&mut data, dest.len() as u16);
    data.extend_from_slice(&origin);
    data.extend_from_slice(&dest);
    Ok(data)
}

fn path_bytes(path: &Path) -> Result<Vec<u8>, AureliaError> {
    let text = path
        .to_str()
        .ok_or_else(|| AureliaError::with_message(ErrorId::ProtocolViolation, "path not utf8"))?;
    let bytes = text.as_bytes();
    if bytes.is_empty() || bytes.len() > PATH_MAX_LEN || bytes.len() > u16::MAX as usize {
        return Err(AureliaError::with_message(
            ErrorId::ProtocolViolation,
            "path length out of bounds",
        ));
    }
    Ok(bytes.to_vec())
}

async fn read_framed(stream: &mut UnixStream) -> Result<Vec<u8>, AureliaError> {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|err| AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string()))?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len == 0 || len > MAX_FRAME_LEN {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let mut payload = vec![0u8; len];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|err| AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string()))?;
    Ok(payload)
}

async fn write_framed(stream: &mut UnixStream, payload: &[u8]) -> Result<(), AureliaError> {
    if payload.is_empty() || payload.len() > MAX_FRAME_LEN {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let len = wire_payload_len(payload.len())?.to_be_bytes();
    stream
        .write_all(&len)
        .await
        .map_err(|err| AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string()))?;
    stream
        .write_all(payload)
        .await
        .map_err(|err| AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string()))?;
    stream
        .flush()
        .await
        .map_err(|err| AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string()))?;
    Ok(())
}

struct AuthInit {
    origin_path: PathBuf,
    destination_path: PathBuf,
    cert_chain_der: Vec<Vec<u8>>,
    nonce_a: Vec<u8>,
    nonce_a_cb: Vec<u8>,
}

struct CallbackInit {
    origin_path: PathBuf,
    destination_path: PathBuf,
    nonce_b_cb: Vec<u8>,
    echo_nonce_a_cb: Vec<u8>,
}

struct AuthChallenge {
    origin_path: PathBuf,
    destination_path: PathBuf,
    cert_chain_der: Vec<Vec<u8>>,
    nonce_b: Vec<u8>,
    signature: Vec<u8>,
}

struct AuthProof {
    echo_nonce_b_cb: Vec<u8>,
    signature: Vec<u8>,
}

fn encode_auth_init(msg: &AuthInit) -> Result<Vec<u8>, AureliaError> {
    if msg.nonce_a.len() != NONCE_LEN || msg.nonce_a_cb.len() != NONCE_LEN {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let origin = path_bytes(&msg.origin_path)?;
    let dest = path_bytes(&msg.destination_path)?;
    let mut buf = Vec::new();
    buf.push(MSG_AUTH_INIT);
    buf.push(AUTH_VERSION);
    put_u16(&mut buf, origin.len() as u16);
    put_u16(&mut buf, dest.len() as u16);
    put_cert_chain_header(&mut buf, &msg.cert_chain_der)?;
    put_u16(&mut buf, msg.nonce_a.len() as u16);
    put_u16(&mut buf, msg.nonce_a_cb.len() as u16);
    buf.extend_from_slice(&origin);
    buf.extend_from_slice(&dest);
    put_cert_chain_body(&mut buf, &msg.cert_chain_der);
    buf.extend_from_slice(&msg.nonce_a);
    buf.extend_from_slice(&msg.nonce_a_cb);
    Ok(buf)
}

async fn parse_auth_init(payload: &[u8]) -> Result<AuthInit, AureliaError> {
    let mut cursor = Cursor::new(payload);
    let msg_type = cursor.read_u8()?;
    let version = cursor.read_u8()?;
    if msg_type != MSG_AUTH_INIT || version != AUTH_VERSION {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let origin_len = cursor.read_u16()? as usize;
    let dest_len = cursor.read_u16()? as usize;
    let cert_lens = cursor.read_cert_lens()?;
    let nonce_len = cursor.read_u16()? as usize;
    let callback_len = cursor.read_u16()? as usize;
    if origin_len == 0
        || dest_len == 0
        || origin_len > PATH_MAX_LEN
        || dest_len > PATH_MAX_LEN
        || nonce_len != NONCE_LEN
        || callback_len != NONCE_LEN
    {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let origin = cursor.read_bytes(origin_len)?;
    let dest = cursor.read_bytes(dest_len)?;
    let cert_chain = cursor.read_cert_chain(&cert_lens)?;
    let nonce_a = cursor.read_bytes(nonce_len)?;
    let nonce_a_cb = cursor.read_bytes(callback_len)?;
    if cursor.has_remaining() {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let origin_path = parse_path(origin).await?;
    let destination_path = parse_path(dest).await?;
    Ok(AuthInit {
        origin_path,
        destination_path,
        cert_chain_der: cert_chain,
        nonce_a,
        nonce_a_cb,
    })
}

fn encode_callback_init(msg: &CallbackInit) -> Result<Vec<u8>, AureliaError> {
    if msg.nonce_b_cb.len() != NONCE_LEN || msg.echo_nonce_a_cb.len() != NONCE_LEN {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let origin = path_bytes(&msg.origin_path)?;
    let dest = path_bytes(&msg.destination_path)?;
    let mut buf = Vec::new();
    buf.push(MSG_CALLBACK_INIT);
    buf.push(AUTH_VERSION);
    put_u16(&mut buf, origin.len() as u16);
    put_u16(&mut buf, dest.len() as u16);
    put_u16(&mut buf, msg.nonce_b_cb.len() as u16);
    put_u16(&mut buf, msg.echo_nonce_a_cb.len() as u16);
    buf.extend_from_slice(&origin);
    buf.extend_from_slice(&dest);
    buf.extend_from_slice(&msg.nonce_b_cb);
    buf.extend_from_slice(&msg.echo_nonce_a_cb);
    Ok(buf)
}

async fn parse_callback_init(payload: &[u8]) -> Result<CallbackInit, AureliaError> {
    let mut cursor = Cursor::new(payload);
    let msg_type = cursor.read_u8()?;
    let version = cursor.read_u8()?;
    if msg_type != MSG_CALLBACK_INIT || version != AUTH_VERSION {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let origin_len = cursor.read_u16()? as usize;
    let dest_len = cursor.read_u16()? as usize;
    let nonce_len = cursor.read_u16()? as usize;
    let echo_len = cursor.read_u16()? as usize;
    if origin_len == 0
        || dest_len == 0
        || origin_len > PATH_MAX_LEN
        || dest_len > PATH_MAX_LEN
        || nonce_len != NONCE_LEN
        || echo_len != NONCE_LEN
    {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let origin = cursor.read_bytes(origin_len)?;
    let dest = cursor.read_bytes(dest_len)?;
    let nonce_b_cb = cursor.read_bytes(nonce_len)?;
    let echo_nonce_a_cb = cursor.read_bytes(echo_len)?;
    if cursor.has_remaining() {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let origin_path = parse_path(origin).await?;
    let destination_path = parse_path(dest).await?;
    Ok(CallbackInit {
        origin_path,
        destination_path,
        nonce_b_cb,
        echo_nonce_a_cb,
    })
}

fn encode_auth_challenge(msg: &AuthChallenge) -> Result<Vec<u8>, AureliaError> {
    if msg.nonce_b.len() != NONCE_LEN {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let origin = path_bytes(&msg.origin_path)?;
    let dest = path_bytes(&msg.destination_path)?;
    let mut buf = Vec::new();
    buf.push(MSG_AUTH_CHALLENGE);
    buf.push(AUTH_VERSION);
    put_u16(&mut buf, origin.len() as u16);
    put_u16(&mut buf, dest.len() as u16);
    put_cert_chain_header(&mut buf, &msg.cert_chain_der)?;
    put_u16(&mut buf, msg.nonce_b.len() as u16);
    put_u32(&mut buf, wire_payload_len(msg.signature.len())?);
    buf.extend_from_slice(&origin);
    buf.extend_from_slice(&dest);
    put_cert_chain_body(&mut buf, &msg.cert_chain_der);
    buf.extend_from_slice(&msg.nonce_b);
    buf.extend_from_slice(&msg.signature);
    Ok(buf)
}

async fn parse_auth_challenge(payload: &[u8]) -> Result<AuthChallenge, AureliaError> {
    let mut cursor = Cursor::new(payload);
    let msg_type = cursor.read_u8()?;
    let version = cursor.read_u8()?;
    if msg_type != MSG_AUTH_CHALLENGE || version != AUTH_VERSION {
        return Err(AureliaError::with_message(
            ErrorId::ProtocolViolation,
            "auth_challenge header mismatch",
        ));
    }
    let origin_len = cursor.read_u16()? as usize;
    let dest_len = cursor.read_u16()? as usize;
    let cert_lens = cursor.read_cert_lens()?;
    let nonce_len = cursor.read_u16()? as usize;
    let signature_len = cursor.read_u32()? as usize;
    if origin_len == 0
        || dest_len == 0
        || origin_len > PATH_MAX_LEN
        || dest_len > PATH_MAX_LEN
        || nonce_len != NONCE_LEN
    {
        return Err(AureliaError::with_message(
            ErrorId::ProtocolViolation,
            "auth_challenge length invalid",
        ));
    }
    let origin = cursor.read_bytes(origin_len)?;
    let dest = cursor.read_bytes(dest_len)?;
    let cert_chain = cursor.read_cert_chain(&cert_lens)?;
    let nonce_b = cursor.read_bytes(nonce_len)?;
    let signature = cursor.read_bytes(signature_len)?;
    if cursor.has_remaining() {
        return Err(AureliaError::with_message(
            ErrorId::ProtocolViolation,
            "auth_challenge trailing bytes",
        ));
    }
    let origin_path = parse_path(origin).await?;
    let destination_path = parse_path(dest).await?;
    Ok(AuthChallenge {
        origin_path,
        destination_path,
        cert_chain_der: cert_chain,
        nonce_b,
        signature,
    })
}

fn encode_auth_proof(msg: &AuthProof) -> Result<Vec<u8>, AureliaError> {
    if msg.echo_nonce_b_cb.len() != NONCE_LEN {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let mut buf = Vec::new();
    buf.push(MSG_AUTH_PROOF);
    buf.push(AUTH_VERSION);
    put_u16(&mut buf, msg.echo_nonce_b_cb.len() as u16);
    put_u32(&mut buf, wire_payload_len(msg.signature.len())?);
    buf.extend_from_slice(&msg.echo_nonce_b_cb);
    buf.extend_from_slice(&msg.signature);
    Ok(buf)
}

fn parse_auth_proof(payload: &[u8]) -> Result<AuthProof, AureliaError> {
    let mut cursor = Cursor::new(payload);
    let msg_type = cursor.read_u8()?;
    let version = cursor.read_u8()?;
    if msg_type != MSG_AUTH_PROOF || version != AUTH_VERSION {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let echo_len = cursor.read_u16()? as usize;
    let signature_len = cursor.read_u32()? as usize;
    if echo_len != NONCE_LEN {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let echo_nonce_b_cb = cursor.read_bytes(echo_len)?;
    let signature = cursor.read_bytes(signature_len)?;
    if cursor.has_remaining() {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    Ok(AuthProof {
        echo_nonce_b_cb,
        signature,
    })
}

struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, AureliaError> {
        if self.pos + 1 > self.data.len() {
            return Err(AureliaError::with_message(
                ErrorId::ProtocolViolation,
                "cursor underrun",
            ));
        }
        let value = self.data[self.pos];
        self.pos += 1;
        Ok(value)
    }

    fn read_u16(&mut self) -> Result<u16, AureliaError> {
        if self.pos + 2 > self.data.len() {
            return Err(AureliaError::with_message(
                ErrorId::ProtocolViolation,
                "cursor underrun",
            ));
        }
        let value = u16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(value)
    }

    fn read_u32(&mut self) -> Result<u32, AureliaError> {
        if self.pos + 4 > self.data.len() {
            return Err(AureliaError::with_message(
                ErrorId::ProtocolViolation,
                "cursor underrun",
            ));
        }
        let value = u32::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(value)
    }

    fn read_cert_lens(&mut self) -> Result<Vec<usize>, AureliaError> {
        let count = self.read_u16()? as usize;
        if count == 0 || count > MAX_CERT_CHAIN_LEN {
            return Err(AureliaError::with_message(
                ErrorId::ProtocolViolation,
                "certificate chain length invalid",
            ));
        }
        let mut lens = Vec::with_capacity(count);
        for _ in 0..count {
            let len = self.read_u32()? as usize;
            if len == 0 {
                return Err(AureliaError::with_message(
                    ErrorId::ProtocolViolation,
                    "certificate length invalid",
                ));
            }
            lens.push(len);
        }
        Ok(lens)
    }

    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>, AureliaError> {
        if self.pos + len > self.data.len() {
            return Err(AureliaError::with_message(
                ErrorId::ProtocolViolation,
                "cursor underrun",
            ));
        }
        let value = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(value)
    }

    fn read_cert_chain(&mut self, lens: &[usize]) -> Result<Vec<Vec<u8>>, AureliaError> {
        lens.iter()
            .map(|len| self.read_bytes(*len))
            .collect::<Result<Vec<_>, _>>()
    }

    fn has_remaining(&self) -> bool {
        self.pos != self.data.len()
    }
}

fn put_cert_chain_header(buf: &mut Vec<u8>, cert_chain: &[Vec<u8>]) -> Result<(), AureliaError> {
    if cert_chain.is_empty() || cert_chain.len() > MAX_CERT_CHAIN_LEN {
        return Err(AureliaError::with_message(
            ErrorId::ProtocolViolation,
            "certificate chain length invalid",
        ));
    }
    put_u16(buf, cert_chain.len() as u16);
    for cert in cert_chain {
        if cert.is_empty() {
            return Err(AureliaError::with_message(
                ErrorId::ProtocolViolation,
                "certificate length invalid",
            ));
        }
        put_u32(buf, wire_payload_len(cert.len())?);
    }
    Ok(())
}

fn put_cert_chain_body(buf: &mut Vec<u8>, cert_chain: &[Vec<u8>]) {
    for cert in cert_chain {
        buf.extend_from_slice(cert);
    }
}

fn put_u16(buf: &mut Vec<u8>, value: u16) {
    buf.extend_from_slice(&value.to_be_bytes());
}

fn put_u32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_be_bytes());
}

async fn parse_path(value: Vec<u8>) -> Result<PathBuf, AureliaError> {
    if value.is_empty() || value.len() > PATH_MAX_LEN {
        return Err(AureliaError::with_message(
            ErrorId::ProtocolViolation,
            "path length invalid",
        ));
    }
    let text = std::str::from_utf8(&value)
        .map_err(|_| AureliaError::with_message(ErrorId::ProtocolViolation, "path not utf8"))?;
    let path = PathBuf::from(text);
    let canonical = SocketBackend::canonicalize_socket_path(&path).await?;
    if canonical != path {
        return Err(AureliaError::with_message(
            ErrorId::ProtocolViolation,
            "path not canonical",
        ));
    }
    Ok(path)
}

#[cfg(test)]
#[path = "tests/leaf/socket_backend.rs"]
mod tests;
