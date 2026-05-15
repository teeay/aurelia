// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;

use crate::auth::{Pkcs8AuthConfig, Pkcs8DerConfig};
use crate::config::{DomusConfig, DomusConfigAccess};
use crate::transport::callback_rendezvous::CallbackTransition;
use aurelia_data::DomusAddr;
use rcgen::{BasicConstraints, Certificate, CertificateParams, IsCa, SanType};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream as TokioTcpStream;
use tokio::time::timeout;

const TCP_BACKEND_TEST_TIMEOUT: Duration = Duration::from_secs(10);

fn addr(port: u16) -> std::net::SocketAddr {
    std::net::SocketAddr::from(([127, 0, 0, 1], port))
}

fn pick_loopback_addr() -> SocketAddr {
    StdTcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .expect("bind temp listener")
        .local_addr()
        .expect("local addr")
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

fn build_auth(ca: &Certificate, addr: SocketAddr) -> Pkcs8AuthConfig {
    let (cert_der, key_der) = build_domus_cert(ca, addr);
    Pkcs8AuthConfig::Pkcs8Der(Pkcs8DerConfig {
        ca_der: ca.serialize_der().expect("ca der"),
        cert_der,
        pkcs8_key_der: key_der.into(),
    })
}

#[tokio::test]
async fn callback_rendezvous_accepts_callback_before_wait() {
    tokio::time::timeout(TCP_BACKEND_TEST_TIMEOUT, async {
        let rendezvous = TcpCallbackRendezvous::new();
        let nonce = [1u8; NONCE_LEN];
        let nonce_b = [2u8; NONCE_LEN];
        let cert = Bytes::from_static(&[3, 4, 5]);
        let (rx, registered) = rendezvous.register(nonce, addr(1001), cert.clone()).await;
        assert_eq!(registered.transition, CallbackTransition::PendingRegistered);

        let arrived = rendezvous
            .fulfill(addr(1001), cert, nonce_b, nonce)
            .await
            .expect("callback");
        assert_eq!(arrived.transition, CallbackTransition::CallbackArrived);
        assert_eq!(arrived.pending_len, 0);

        let info = rx.await.expect("latched callback");
        assert_eq!(info.nonce_b_cb, nonce_b);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn callback_rendezvous_accepts_callback_after_wait() {
    tokio::time::timeout(TCP_BACKEND_TEST_TIMEOUT, async {
        let rendezvous = TcpCallbackRendezvous::new();
        let nonce = [11u8; NONCE_LEN];
        let nonce_b = [12u8; NONCE_LEN];
        let cert = Bytes::from_static(&[13, 14, 15]);
        let (rx, _) = rendezvous.register(nonce, addr(1011), cert.clone()).await;
        let waiter = tokio::spawn(async move { rx.await.expect("latched callback") });

        tokio::task::yield_now().await;
        rendezvous
            .fulfill(addr(1011), cert, nonce_b, nonce)
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
    tokio::time::timeout(TCP_BACKEND_TEST_TIMEOUT, async {
        let rendezvous = TcpCallbackRendezvous::new();
        let nonce = [7u8; NONCE_LEN];
        let (_rx, _) = rendezvous
            .register(nonce, addr(1002), Bytes::from_static(&[1]))
            .await;

        let cleanup = rendezvous.cleanup(&nonce).await;
        assert_eq!(cleanup.transition, CallbackTransition::Cleanup);
        assert_eq!(cleanup.pending_len, 0);

        let err = rendezvous
            .fulfill(
                addr(1002),
                Bytes::from_static(&[1]),
                [8u8; NONCE_LEN],
                nonce,
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
async fn stalled_tcp_tls_accept_times_out_and_releases_preauth() {
    let ca = build_ca();
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let config = DomusConfig {
        tcp_handshake_timeout: Duration::from_millis(50),
        inbound_handshake_limit_total: 1,
        ..Default::default()
    };
    let config = DomusConfigAccess::from_config(config);
    let backend = Arc::new(
        TcpBackend::new(
            build_auth(&ca, bind_addr),
            config.clone(),
            tokio::runtime::Handle::current(),
        )
        .expect("backend"),
    );
    let mut listener = backend
        .bind(&DomusAddr::Tcp(bind_addr))
        .await
        .expect("bind");
    let actual_addr = listener.local_addr().expect("local addr");

    let accepting = {
        let backend = Arc::clone(&backend);
        tokio::spawn(async move { backend.accept(&mut listener).await })
    };
    let _stalled = TokioTcpStream::connect(actual_addr)
        .await
        .expect("connect stalled peer");

    let result = timeout(Duration::from_secs(1), accepting)
        .await
        .expect("accept should be bounded")
        .expect("accept task");
    let err = match result {
        Ok(_) => panic!("expected stalled TLS accept to fail"),
        Err(err) => err,
    };
    assert_eq!(err.kind, ErrorId::PeerUnavailable);
    assert_eq!(
        err.message.as_deref(),
        Some("tcp handshake timeout"),
        "A0 timeout should own stalled TLS accept"
    );
    assert!(
        backend.preauth_gate.try_acquire(&config).await.is_some(),
        "timed-out TCP accept must release pre-authentication capacity"
    );
}

#[tokio::test]
async fn tcp_repeated_callis_use_full_connect_back() {
    tokio::time::timeout(TCP_BACKEND_TEST_TIMEOUT, async {
        let ca = build_ca();
        let addr_a = pick_loopback_addr();
        let addr_b = pick_loopback_addr();
        let config = DomusConfigAccess::from_config(DomusConfig::default());
        let backend_a = Arc::new(
            TcpBackend::new(
                build_auth(&ca, addr_a),
                config.clone(),
                tokio::runtime::Handle::current(),
            )
            .expect("backend a"),
        );
        let backend_b = Arc::new(
            TcpBackend::new(
                build_auth(&ca, addr_b),
                config,
                tokio::runtime::Handle::current(),
            )
            .expect("backend b"),
        );

        let mut listener_a = backend_a
            .bind(&DomusAddr::Tcp(addr_a))
            .await
            .expect("bind a");
        let mut listener_b = backend_b
            .bind(&DomusAddr::Tcp(addr_b))
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
                .dial(&DomusAddr::Tcp(addr_b))
                .await
                .unwrap_or_else(|err| panic!("{label} dial failed: {err:?}"));
            assert_eq!(outbound.peer_addr, DomusAddr::Tcp(addr_b));
            drop(outbound.stream);
        }

        let inbound_peers = accept_b.await.expect("accept b");
        assert_eq!(
            inbound_peers,
            vec![
                DomusAddr::Tcp(addr_a),
                DomusAddr::Tcp(addr_a),
                DomusAddr::Tcp(addr_a),
            ]
        );

        accept_a.abort();
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn callback_rendezvous_rejects_address_and_cert_mismatch_once() {
    tokio::time::timeout(TCP_BACKEND_TEST_TIMEOUT, async {
        let rendezvous = TcpCallbackRendezvous::new();
        let nonce = [9u8; NONCE_LEN];
        let (_rx, _) = rendezvous
            .register(nonce, addr(1003), Bytes::from_static(&[1]))
            .await;

        let err = rendezvous
            .fulfill(
                addr(1004),
                Bytes::from_static(&[1]),
                [10u8; NONCE_LEN],
                nonce,
            )
            .await
            .expect_err("address mismatch");
        assert_eq!(err.kind, ErrorId::ProtocolViolation);
        assert_eq!(rendezvous.pending_len().await, 0);

        let (_rx, _) = rendezvous
            .register(nonce, addr(1003), Bytes::from_static(&[1]))
            .await;
        let err = rendezvous
            .fulfill(
                addr(1003),
                Bytes::from_static(&[2]),
                [10u8; NONCE_LEN],
                nonce,
            )
            .await
            .expect_err("cert mismatch");
        assert_eq!(err.kind, ErrorId::ProtocolViolation);
        assert_eq!(rendezvous.pending_len().await, 0);
    })
    .await
    .expect("async test timed out");
}
