// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::auth::{Pkcs8AuthConfig, Pkcs8DerConfig, Pkcs8PemConfig};
use crate::config::{DomusConfig, DomusConfigAccess, MAX_INBOUND_HANDSHAKE_LIMIT_TOTAL};
use crate::transport::callback_rendezvous::CallbackTransition;
use aurelia_data::DomusAddr;
use rcgen::{BasicConstraints, Certificate, CertificateParams, IsCa, SanType};

const SOCKET_BACKEND_TEST_TIMEOUT: Duration = Duration::from_secs(10);
static TEMP_DIR_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

struct TempSocketDir {
    path: PathBuf,
}

impl std::ops::Deref for TempSocketDir {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl Drop for TempSocketDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn build_ca() -> Certificate {
    let mut params = CertificateParams::new(Vec::new());
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    Certificate::from_params(params).expect("ca cert")
}

fn build_domus_cert(ca: &Certificate, path: &Path) -> (Vec<u8>, Vec<u8>) {
    let mut params = CertificateParams::new(Vec::new());
    let uri = format!("aurelia+unix://{}", path.to_string_lossy());
    params.subject_alt_names.push(SanType::URI(uri));
    let cert = Certificate::from_params(params).expect("domus cert");
    let cert_der = cert.serialize_der_with_signer(ca).expect("sign cert");
    let key_der = cert.serialize_private_key_der();
    (cert_der, key_der)
}

fn build_domus_cert_with_uris(ca: &Certificate, uris: Vec<String>) -> (Vec<u8>, Vec<u8>) {
    let mut params = CertificateParams::new(Vec::new());
    params
        .subject_alt_names
        .extend(uris.into_iter().map(SanType::URI));
    let cert = Certificate::from_params(params).expect("domus cert");
    let cert_der = cert.serialize_der_with_signer(ca).expect("sign cert");
    let key_der = cert.serialize_private_key_der();
    (cert_der, key_der)
}

fn build_auth(ca: &Certificate, path: &Path) -> Pkcs8AuthConfig {
    let (cert_der, key_der) = build_domus_cert(ca, path);
    Pkcs8AuthConfig::Pkcs8Der(Pkcs8DerConfig {
        ca_der: ca.serialize_der().expect("ca der"),
        cert_der,
        pkcs8_key_der: key_der.into(),
    })
}

fn build_intermediate_ca(root: &Certificate) -> Certificate {
    let mut params = CertificateParams::new(Vec::new());
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let cert = Certificate::from_params(params).expect("intermediate cert");
    let _ = cert
        .serialize_der_with_signer(root)
        .expect("intermediate signs with root");
    cert
}

fn build_pem_chain_auth(
    root: &Certificate,
    intermediate: &Certificate,
    path: &Path,
) -> Pkcs8AuthConfig {
    let mut params = CertificateParams::new(Vec::new());
    let uri = format!("aurelia+unix://{}", path.to_string_lossy());
    params.subject_alt_names.push(SanType::URI(uri));
    let leaf = Certificate::from_params(params).expect("leaf cert");
    let leaf_pem = leaf
        .serialize_pem_with_signer(intermediate)
        .expect("leaf pem");
    let intermediate_pem = intermediate
        .serialize_pem_with_signer(root)
        .expect("intermediate pem");
    Pkcs8AuthConfig::Pkcs8Pem(Pkcs8PemConfig {
        ca_pem: root.serialize_pem().expect("root pem").into_bytes(),
        cert_pem: format!("{leaf_pem}{intermediate_pem}").into_bytes(),
        pkcs8_key_pem: leaf.serialize_private_key_pem().into_bytes().into(),
    })
}

fn build_leaf_signed_by_intermediate(
    intermediate: &Certificate,
    path: &Path,
) -> (Vec<u8>, Vec<u8>) {
    let mut params = CertificateParams::new(Vec::new());
    let uri = format!("aurelia+unix://{}", path.to_string_lossy());
    params.subject_alt_names.push(SanType::URI(uri));
    let leaf = Certificate::from_params(params).expect("leaf cert");
    (
        leaf.serialize_der_with_signer(intermediate)
            .expect("leaf der"),
        leaf.serialize_private_key_der(),
    )
}

fn temp_dir(name: &str) -> TempSocketDir {
    let count = TEMP_DIR_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let _ = name;
    let dir = PathBuf::from("/tmp").join(format!("au-sock-{}-{count}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("create temp dir");
    TempSocketDir {
        path: fs::canonicalize(&dir).expect("canonicalize temp dir"),
    }
}

#[tokio::test]
async fn socket_connect_back_success() {
    tokio::time::timeout(SOCKET_BACKEND_TEST_TIMEOUT, async {
        let dir = temp_dir("connect-back-success");
        let path_a = dir.join("domus-a.sock");
        let path_b = dir.join("domus-b.sock");

        let ca = build_ca();
        let auth_a = build_auth(&ca, &path_a);
        let auth_b = build_auth(&ca, &path_b);

        let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
        let backend_a = Arc::new(
            SocketBackend::new(auth_a, config.clone(), tokio::runtime::Handle::current())
                .expect("backend a"),
        );
        let backend_b = Arc::new(
            SocketBackend::new(auth_b, config.clone(), tokio::runtime::Handle::current())
                .expect("backend b"),
        );

        let mut listener_a = backend_a
            .bind(&DomusAddr::Socket(path_a.clone()))
            .await
            .expect("bind a");
        let mut listener_b = backend_b
            .bind(&DomusAddr::Socket(path_b.clone()))
            .await
            .expect("bind b");

        let backend_a_accept = Arc::clone(&backend_a);
        let accept_a = tokio::spawn(async move {
            let _ = backend_a_accept.accept(&mut listener_a).await;
        });
        let backend_b_accept = Arc::clone(&backend_b);
        let accept_b = tokio::spawn(async move {
            backend_b_accept
                .accept(&mut listener_b)
                .await
                .expect("accept b")
        });

        let outbound = backend_a
            .dial(&DomusAddr::Socket(path_b.clone()))
            .await
            .expect("dial");
        let addr = outbound.peer_addr;
        drop(outbound.stream);
        let inbound = accept_b.await.expect("accept task");
        let peer_addr = inbound.peer_addr;
        assert_eq!(addr, DomusAddr::Socket(path_b));
        assert_eq!(peer_addr, DomusAddr::Socket(path_a));

        accept_a.abort();
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn socket_accepted_queue_uses_max_handshake_limit_capacity() {
    tokio::time::timeout(SOCKET_BACKEND_TEST_TIMEOUT, async {
        let dir = temp_dir("accepted-queue-capacity");
        let path = dir.join("domus.sock");
        let ca = build_ca();
        let auth = build_auth(&ca, &path);
        let backend = SocketBackend::new(
            auth,
            DomusConfigAccess::from_config(DomusConfig::default()),
            tokio::runtime::Handle::current(),
        )
        .expect("backend");

        assert_eq!(
            backend.accepted_queue_max_capacity(),
            MAX_INBOUND_HANDSHAKE_LIMIT_TOTAL
        );
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn socket_write_framed_rejects_oversized_payload() {
    tokio::time::timeout(SOCKET_BACKEND_TEST_TIMEOUT, async {
        let (mut client, _server) = UnixStream::pair().expect("socket pair");
        let payload = vec![0u8; MAX_FRAME_LEN + 1];

        let err = write_framed(&mut client, &payload)
            .await
            .expect_err("expected oversized frame rejection");

        assert_eq!(err.kind, ErrorId::ProtocolViolation);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn socket_stalled_auth_does_not_block_second_valid_auth() {
    let dir = temp_dir("stalled-does-not-block-valid");
    let path_a = dir.join("domus-a.sock");
    let path_b = dir.join("domus-b.sock");

    let ca = build_ca();
    let auth_a = build_auth(&ca, &path_a);
    let auth_b = build_auth(&ca, &path_b);

    let cfg = DomusConfig {
        socket_handshake_timeout: Duration::from_secs(2),
        inbound_handshake_limit_total: 2,
        ..Default::default()
    };
    let config: DomusConfigAccess = DomusConfigAccess::from_config(cfg);
    let backend_a = Arc::new(
        SocketBackend::new(auth_a, config.clone(), tokio::runtime::Handle::current())
            .expect("backend a"),
    );
    let backend_b = Arc::new(
        SocketBackend::new(auth_b, config.clone(), tokio::runtime::Handle::current())
            .expect("backend b"),
    );

    let mut listener_a = backend_a
        .bind(&DomusAddr::Socket(path_a.clone()))
        .await
        .expect("bind a");
    let mut listener_b = backend_b
        .bind(&DomusAddr::Socket(path_b.clone()))
        .await
        .expect("bind b");

    let backend_a_accept = Arc::clone(&backend_a);
    let accept_a = tokio::spawn(async move {
        let _ = backend_a_accept.accept(&mut listener_a).await;
    });
    let backend_b_accept = Arc::clone(&backend_b);
    let accept_b = tokio::spawn(async move {
        backend_b_accept
            .accept(&mut listener_b)
            .await
            .expect("valid accept")
    });

    let stalled = UnixStream::connect(&path_b).await.expect("connect stalled");
    let outbound = backend_a
        .dial(&DomusAddr::Socket(path_b.clone()))
        .await
        .expect("valid dial should not wait for stalled auth");
    assert_eq!(outbound.peer_addr, DomusAddr::Socket(path_b));
    drop(outbound.stream);

    let inbound = tokio::time::timeout(Duration::from_secs(1), accept_b)
        .await
        .expect("valid accept should complete")
        .expect("accept join");
    assert_eq!(inbound.peer_addr, DomusAddr::Socket(path_a));
    drop(stalled);
    accept_a.abort();
}

#[tokio::test]
async fn socket_callback_only_accept_does_not_complete_backend_accept() {
    tokio::time::timeout(SOCKET_BACKEND_TEST_TIMEOUT, async {
        let dir = temp_dir("callback-only-no-accept");
        let path_a = dir.join("domus-a.sock");
        let path_b = dir.join("domus-b.sock");

        let ca = build_ca();
        let auth_a = build_auth(&ca, &path_a);
        let auth_b = build_auth(&ca, &path_b);
        let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
        let backend_a = Arc::new(
            SocketBackend::new(auth_a, config.clone(), tokio::runtime::Handle::current())
                .expect("backend a"),
        );
        let backend_b = Arc::new(
            SocketBackend::new(auth_b, config.clone(), tokio::runtime::Handle::current())
                .expect("backend b"),
        );

        let mut listener_a = backend_a
            .bind(&DomusAddr::Socket(path_a.clone()))
            .await
            .expect("bind a");
        let mut listener_b = backend_b
            .bind(&DomusAddr::Socket(path_b.clone()))
            .await
            .expect("bind b");

        let backend_b_accept = Arc::clone(&backend_b);
        let accept_b = tokio::spawn(async move { backend_b_accept.accept(&mut listener_b).await });

        let echo_nonce = vec![9u8; NONCE_LEN];
        let _callback_rx = backend_b
            .register_pending_callback(echo_nonce.clone(), path_a.clone(), path_b.clone())
            .await;
        let callback = CallbackInit {
            origin_path: path_a.clone(),
            destination_path: path_b.clone(),
            nonce_b_cb: vec![10u8; NONCE_LEN],
            echo_nonce_a_cb: echo_nonce,
        };
        let mut callback_stream = UnixStream::connect(&path_b)
            .await
            .expect("connect callback");
        write_framed(
            &mut callback_stream,
            &encode_callback_init(&callback).expect("callback encode"),
        )
        .await
        .expect("write callback");
        let _ = callback_stream.shutdown().await;

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !accept_b.is_finished(),
            "callback-only connection must not complete backend accept"
        );

        let backend_a_accept = Arc::clone(&backend_a);
        let accept_a = tokio::spawn(async move {
            let _ = backend_a_accept.accept(&mut listener_a).await;
        });
        let outbound = backend_a
            .dial(&DomusAddr::Socket(path_b.clone()))
            .await
            .expect("valid dial");
        drop(outbound.stream);
        let inbound = accept_b
            .await
            .expect("accept join")
            .expect("valid accept after callback-only event");
        assert_eq!(inbound.peer_addr, DomusAddr::Socket(path_a));
        accept_a.abort();
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn socket_connect_back_accepts_intermediate_certificate_chain() {
    tokio::time::timeout(SOCKET_BACKEND_TEST_TIMEOUT, async {
        let dir = temp_dir("connect-back-chain");
        let path_a = dir.join("domus-a.sock");
        let path_b = dir.join("domus-b.sock");

        let root = build_ca();
        let intermediate = build_intermediate_ca(&root);
        let auth_a = build_pem_chain_auth(&root, &intermediate, &path_a);
        let auth_b = build_pem_chain_auth(&root, &intermediate, &path_b);

        let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
        let backend_a = Arc::new(
            SocketBackend::new(auth_a, config.clone(), tokio::runtime::Handle::current())
                .expect("backend a"),
        );
        let backend_b = Arc::new(
            SocketBackend::new(auth_b, config.clone(), tokio::runtime::Handle::current())
                .expect("backend b"),
        );

        assert_eq!(backend_a.auth.read().await.cert_chain_der.len(), 2);
        assert_eq!(backend_b.auth.read().await.cert_chain_der.len(), 2);

        let mut listener_a = backend_a
            .bind(&DomusAddr::Socket(path_a.clone()))
            .await
            .expect("bind a");
        let mut listener_b = backend_b
            .bind(&DomusAddr::Socket(path_b.clone()))
            .await
            .expect("bind b");

        let backend_a_accept = Arc::clone(&backend_a);
        let accept_a = tokio::spawn(async move {
            let _ = backend_a_accept.accept(&mut listener_a).await;
        });
        let backend_b_accept = Arc::clone(&backend_b);
        let accept_b = tokio::spawn(async move {
            backend_b_accept
                .accept(&mut listener_b)
                .await
                .expect("accept b")
        });

        let outbound = backend_a
            .dial(&DomusAddr::Socket(path_b.clone()))
            .await
            .expect("dial with chain");
        drop(outbound.stream);
        let inbound = accept_b.await.expect("accept join");
        assert_eq!(inbound.peer_addr, DomusAddr::Socket(path_a));
        accept_a.abort();
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn socket_chain_requires_presented_intermediate() {
    tokio::time::timeout(SOCKET_BACKEND_TEST_TIMEOUT, async {
        let dir = temp_dir("chain-missing-intermediate");
        let local_path = dir.join("local.sock");
        let peer_path = dir.join("peer.sock");

        let root = build_ca();
        let intermediate = build_intermediate_ca(&root);
        let auth = build_auth(&root, &local_path);
        let backend = SocketBackend::new(
            auth,
            DomusConfigAccess::from_config(DomusConfig::default()),
            tokio::runtime::Handle::current(),
        )
        .expect("backend");
        let (leaf_der, _) = build_leaf_signed_by_intermediate(&intermediate, &peer_path);
        let intermediate_der = intermediate
            .serialize_der_with_signer(&root)
            .expect("intermediate der");
        let auth = backend.auth.read().await.clone();

        backend
            .verify_peer_cert(&auth, &[leaf_der.clone(), intermediate_der], &peer_path)
            .await
            .expect("chain with intermediate verifies");
        let err = backend
            .verify_peer_cert(&auth, &[leaf_der], &peer_path)
            .await
            .expect_err("missing intermediate should fail");
        assert_eq!(err.kind, ErrorId::ProtocolViolation);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn socket_uri_san_accepts_duplicate_identical_and_rejects_conflict() {
    tokio::time::timeout(SOCKET_BACKEND_TEST_TIMEOUT, async {
        let dir = temp_dir("duplicate-uri-san");
        let path_a = dir.join("domus-a.sock");
        let path_b = dir.join("domus-b.sock");
        let ca = build_ca();
        let uri_a = format!("aurelia+unix://{}", path_a.to_string_lossy());
        let uri_b = format!("aurelia+unix://{}", path_b.to_string_lossy());

        let (duplicate_der, _) = build_domus_cert_with_uris(&ca, vec![uri_a.clone(), uri_a]);
        let peer_path = extract_peer_uri_san_socket(&duplicate_der)
            .await
            .expect("duplicate identical URI SAN accepted");
        assert_eq!(peer_path, path_a);

        let (conflicting_der, _) = build_domus_cert_with_uris(
            &ca,
            vec![
                uri_b,
                format!("aurelia+unix://{}", path_a.to_string_lossy()),
            ],
        );
        let err = extract_peer_uri_san_socket(&conflicting_der)
            .await
            .expect_err("conflicting URI SAN rejected");
        assert_eq!(err.kind, ErrorId::ProtocolViolation);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn socket_repeated_callis_use_full_connect_back() {
    tokio::time::timeout(SOCKET_BACKEND_TEST_TIMEOUT, async {
        let dir = temp_dir("repeated-full-connect-back");
        let path_a = dir.join("domus-a.sock");
        let path_b = dir.join("domus-b.sock");

        let ca = build_ca();
        let auth_a = build_auth(&ca, &path_a);
        let auth_b = build_auth(&ca, &path_b);

        let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
        let backend_a = Arc::new(
            SocketBackend::new(auth_a, config.clone(), tokio::runtime::Handle::current())
                .expect("backend a"),
        );
        let backend_b = Arc::new(
            SocketBackend::new(auth_b, config.clone(), tokio::runtime::Handle::current())
                .expect("backend b"),
        );

        let mut listener_a = backend_a
            .bind(&DomusAddr::Socket(path_a.clone()))
            .await
            .expect("bind a");
        let mut listener_b = backend_b
            .bind(&DomusAddr::Socket(path_b.clone()))
            .await
            .expect("bind b");

        let backend_a_accept = Arc::clone(&backend_a);
        let accept_a = tokio::spawn(async move {
            let _ = backend_a_accept.accept(&mut listener_a).await;
        });
        let backend_b_accept = Arc::clone(&backend_b);
        let accept_b = tokio::spawn(async move {
            let first = backend_b_accept
                .accept(&mut listener_b)
                .await
                .expect("first accept");
            let second = backend_b_accept
                .accept(&mut listener_b)
                .await
                .expect("second accept");
            let third = backend_b_accept
                .accept(&mut listener_b)
                .await
                .expect("third accept");
            vec![first.peer_addr, second.peer_addr, third.peer_addr]
        });

        for label in ["first", "second", "third"] {
            let outbound = backend_a
                .dial(&DomusAddr::Socket(path_b.clone()))
                .await
                .unwrap_or_else(|err| panic!("{label} dial failed: {err:?}"));
            assert_eq!(outbound.peer_addr, DomusAddr::Socket(path_b.clone()));
            drop(outbound.stream);
        }

        let inbound_peers = accept_b.await.expect("accept b");
        assert_eq!(
            inbound_peers,
            vec![
                DomusAddr::Socket(path_a.clone()),
                DomusAddr::Socket(path_a.clone()),
                DomusAddr::Socket(path_a),
            ]
        );

        accept_a.abort();
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn socket_removed_message_types_five_and_six_are_rejected() {
    tokio::time::timeout(SOCKET_BACKEND_TEST_TIMEOUT, async {
        for (name, msg_type) in [("removed-type-five", 5u8), ("removed-type-six", 6u8)] {
            let dir = temp_dir(name);
            let path_b = dir.join("domus-b.sock");

            let ca = build_ca();
            let auth_b = build_auth(&ca, &path_b);
            let backend_b = Arc::new(
                SocketBackend::new(
                    auth_b,
                    DomusConfigAccess::from_config(DomusConfig::default()),
                    tokio::runtime::Handle::current(),
                )
                .expect("backend b"),
            );
            let mut listener_b = backend_b
                .bind(&DomusAddr::Socket(path_b.clone()))
                .await
                .expect("bind b");
            let backend_b_accept = Arc::clone(&backend_b);
            let accept_b =
                tokio::spawn(async move { backend_b_accept.accept(&mut listener_b).await });
            let mut stream = UnixStream::connect(&path_b)
                .await
                .expect("connect removed message type");
            write_framed(&mut stream, &[msg_type, AUTH_VERSION])
                .await
                .expect("write removed message type");
            let err = match accept_b.await.expect("accept join") {
                Ok(_) => panic!("removed message type rejected"),
                Err(err) => err,
            };
            assert_eq!(err.kind, ErrorId::ProtocolViolation);
        }
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn socket_callback_timeout_fails() {
    tokio::time::timeout(SOCKET_BACKEND_TEST_TIMEOUT, async {
        let dir = temp_dir("callback-timeout");
        let path_a = dir.join("domus-a.sock");
        let path_b = dir.join("domus-b.sock");

        let ca = build_ca();
        let auth_a = build_auth(&ca, &path_a);
        let auth_b = build_auth(&ca, &path_b);

        let cfg = DomusConfig {
            socket_callback_timeout: Duration::from_millis(50),
            ..Default::default()
        };
        let config: DomusConfigAccess = DomusConfigAccess::from_config(cfg);
        let backend_a = Arc::new(
            SocketBackend::new(auth_a, config.clone(), tokio::runtime::Handle::current())
                .expect("backend a"),
        );
        let backend_b = Arc::new(
            SocketBackend::new(auth_b, config.clone(), tokio::runtime::Handle::current())
                .expect("backend b"),
        );

        let _listener_a = backend_a
            .bind(&DomusAddr::Socket(path_a.clone()))
            .await
            .expect("bind a");
        let mut listener_b = backend_b
            .bind(&DomusAddr::Socket(path_b.clone()))
            .await
            .expect("bind b");

        let backend_b_accept = Arc::clone(&backend_b);
        let accept_b = tokio::spawn(async move { backend_b_accept.accept(&mut listener_b).await });

        let result = backend_a.dial(&DomusAddr::Socket(path_b)).await;
        let err = match result {
            Ok(_) => panic!("callback timeout should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind, ErrorId::PeerUnavailable);
        assert_eq!(err.message.as_deref(), Some("socket callback timeout"));

        let _ = accept_b.await;
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn socket_stalled_first_auth_frame_times_out_and_releases_preauth() {
    let dir = temp_dir("stalled-first-auth");
    let path = dir.join("domus.sock");
    let ca = build_ca();
    let auth = build_auth(&ca, &path);
    let cfg = DomusConfig {
        socket_handshake_timeout: Duration::from_millis(50),
        inbound_handshake_limit_total: 1,
        ..Default::default()
    };
    let config: DomusConfigAccess = DomusConfigAccess::from_config(cfg);
    let backend = Arc::new(
        SocketBackend::new(auth, config.clone(), tokio::runtime::Handle::current())
            .expect("backend"),
    );
    let mut listener = backend
        .bind(&DomusAddr::Socket(path.clone()))
        .await
        .expect("bind");

    let backend_accept = Arc::clone(&backend);
    let accept = tokio::spawn(async move { backend_accept.accept(&mut listener).await });
    let stalled = UnixStream::connect(&path).await.expect("connect stalled");

    let result = tokio::time::timeout(Duration::from_secs(1), accept)
        .await
        .expect("accept timeout")
        .expect("accept join");
    let err = match result {
        Ok(_) => panic!("stalled auth should fail"),
        Err(err) => err,
    };
    assert_eq!(err.kind, ErrorId::PeerUnavailable);
    assert_eq!(err.message.as_deref(), Some("socket handshake timeout"));
    drop(stalled);

    let permit = backend.preauth_gate.try_acquire(&config).await;
    assert!(permit.is_some(), "preauth permit should be released");
}

#[tokio::test]
async fn callback_rendezvous_accepts_callback_before_wait() {
    tokio::time::timeout(SOCKET_BACKEND_TEST_TIMEOUT, async {
        let rendezvous = SocketCallbackRendezvous::new();
        let nonce = vec![1u8; NONCE_LEN];
        let nonce_b = vec![2u8; NONCE_LEN];
        let peer = PathBuf::from("/tmp/aurelia-peer.sock");
        let local = PathBuf::from("/tmp/aurelia-local.sock");
        let (rx, registered) = rendezvous
            .register(nonce.clone(), peer.clone(), local.clone())
            .await;
        assert_eq!(registered.transition, CallbackTransition::PendingRegistered);

        let arrived = rendezvous
            .fulfill(
                &nonce,
                CallbackInfo {
                    origin_path: peer.clone(),
                    destination_path: local.clone(),
                    nonce_b_cb: nonce_b.clone(),
                },
            )
            .await
            .expect("callback");
        assert_eq!(arrived.transition, CallbackTransition::CallbackArrived);
        assert_eq!(arrived.pending_len, 0);

        let info = rx.await.expect("latched callback");
        assert_eq!(info.origin_path, peer);
        assert_eq!(info.destination_path, local);
        assert_eq!(info.nonce_b_cb, nonce_b);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn callback_rendezvous_accepts_callback_after_wait() {
    tokio::time::timeout(SOCKET_BACKEND_TEST_TIMEOUT, async {
        let rendezvous = SocketCallbackRendezvous::new();
        let nonce = vec![7u8; NONCE_LEN];
        let nonce_b = vec![8u8; NONCE_LEN];
        let peer = PathBuf::from("/tmp/aurelia-peer-after.sock");
        let local = PathBuf::from("/tmp/aurelia-local-after.sock");
        let (rx, _) = rendezvous
            .register(nonce.clone(), peer.clone(), local.clone())
            .await;
        let waiter = tokio::spawn(async move { rx.await.expect("latched callback") });

        tokio::task::yield_now().await;
        rendezvous
            .fulfill(
                &nonce,
                CallbackInfo {
                    origin_path: peer,
                    destination_path: local,
                    nonce_b_cb: nonce_b.clone(),
                },
            )
            .await
            .expect("callback");

        let info = waiter.await.expect("join");
        assert_eq!(info.nonce_b_cb, nonce_b);
        assert_eq!(rendezvous.pending_len().await, 0);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn callback_rendezvous_cleans_up_timeout_and_rejects_stale_callback() {
    tokio::time::timeout(SOCKET_BACKEND_TEST_TIMEOUT, async {
        let rendezvous = SocketCallbackRendezvous::new();
        let nonce = vec![3u8; NONCE_LEN];
        let peer = PathBuf::from("/tmp/aurelia-peer-timeout.sock");
        let local = PathBuf::from("/tmp/aurelia-local-timeout.sock");
        let (_rx, _) = rendezvous
            .register(nonce.clone(), peer.clone(), local.clone())
            .await;

        let cleanup = rendezvous.cleanup(&nonce).await;
        assert_eq!(cleanup.transition, CallbackTransition::Cleanup);
        assert_eq!(cleanup.pending_len, 0);

        let err = rendezvous
            .fulfill(
                &nonce,
                CallbackInfo {
                    origin_path: peer,
                    destination_path: local,
                    nonce_b_cb: vec![4u8; NONCE_LEN],
                },
            )
            .await
            .expect_err("stale callback rejected");
        assert_eq!(err.kind, ErrorId::ProtocolViolation);
        assert_eq!(rendezvous.pending_len().await, 0);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn callback_rendezvous_rejects_path_mismatch_once() {
    tokio::time::timeout(SOCKET_BACKEND_TEST_TIMEOUT, async {
        let rendezvous = SocketCallbackRendezvous::new();
        let nonce = vec![5u8; NONCE_LEN];
        let peer = PathBuf::from("/tmp/aurelia-peer-mismatch.sock");
        let local = PathBuf::from("/tmp/aurelia-local-mismatch.sock");
        let (_rx, _) = rendezvous
            .register(nonce.clone(), peer, local.clone())
            .await;

        let err = rendezvous
            .fulfill(
                &nonce,
                CallbackInfo {
                    origin_path: PathBuf::from("/tmp/aurelia-wrong-peer.sock"),
                    destination_path: local,
                    nonce_b_cb: vec![6u8; NONCE_LEN],
                },
            )
            .await
            .expect_err("path mismatch rejected");
        assert_eq!(err.kind, ErrorId::ProtocolViolation);
        assert_eq!(rendezvous.pending_len().await, 0);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn auth_challenge_rejects_signature_length_overflow() {
    tokio::time::timeout(SOCKET_BACKEND_TEST_TIMEOUT, async {
        let msg = AuthChallenge {
            origin_path: PathBuf::from("/tmp/aurelia-auth-a.sock"),
            destination_path: PathBuf::from("/tmp/aurelia-auth-b.sock"),
            cert_chain_der: vec![vec![1, 2, 3]],
            nonce_b: vec![0u8; NONCE_LEN],
            signature: vec![9; 4],
        };
        let mut payload = encode_auth_challenge(&msg).expect("encode");
        let sig_len_offset = 14;
        let bad_len = msg.signature.len() as u32 + 10;
        payload[sig_len_offset..sig_len_offset + 4].copy_from_slice(&bad_len.to_be_bytes());

        let err = match parse_auth_challenge(&payload).await {
            Ok(_) => panic!("expected error"),
            Err(err) => err,
        };
        assert_eq!(err.kind, ErrorId::ProtocolViolation);
    })
    .await
    .expect("async test timed out");
}

#[test]
fn auth_proof_rejects_signature_length_overflow() {
    let msg = AuthProof {
        echo_nonce_b_cb: vec![0u8; NONCE_LEN],
        signature: vec![3; 5],
    };
    let mut payload = encode_auth_proof(&msg).expect("encode");
    let sig_len_offset = 4;
    let bad_len = msg.signature.len() as u32 + 7;
    payload[sig_len_offset..sig_len_offset + 4].copy_from_slice(&bad_len.to_be_bytes());

    let err = match parse_auth_proof(&payload) {
        Ok(_) => panic!("expected error"),
        Err(err) => err,
    };
    assert_eq!(err.kind, ErrorId::ProtocolViolation);
}
