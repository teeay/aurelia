// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::super::pkcs8::parse_pkcs8_auth_material;
use super::super::tls::{extract_peer_uri_san_addr, parse_aurelia_tcp_uri};
use super::*;
use crate::auth::{Pkcs8AuthConfig, Pkcs8DerConfig, Pkcs8PemConfig};
use tokio_rustls::rustls::pki_types::CertificateDer;
use zeroize::Zeroizing;

fn assert_message_contains(err: &AureliaError, expected: &str) {
    assert_eq!(err.kind, ErrorId::ProtocolViolation);
    assert!(
        err.message
            .as_deref()
            .is_some_and(|message| message.contains(expected)),
        "expected error message to contain {expected:?}, got {:?}",
        err.message
    );
}

#[test]
fn pkcs8_auth_debug_redacts_private_key_bytes() {
    let auth = Pkcs8AuthConfig::Pkcs8Der(Pkcs8DerConfig {
        ca_der: b"ca-public".to_vec(),
        cert_der: b"cert-public".to_vec(),
        pkcs8_key_der: b"super-secret-private-key".to_vec().into(),
    });

    let debug = format!("{auth:?}");

    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("super-secret-private-key"));
}

#[test]
fn pkcs8_private_key_accepts_zeroizing_vec() {
    let auth = Pkcs8AuthConfig::Pkcs8Der(Pkcs8DerConfig {
        ca_der: b"ca-public".to_vec(),
        cert_der: b"cert-public".to_vec(),
        pkcs8_key_der: Zeroizing::new(b"zeroizing-private-key".to_vec()).into(),
    });

    let debug = format!("{auth:?}");

    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("zeroizing-private-key"));
}

#[test]
fn pkcs8_pem_parse_error_includes_context() {
    let auth = Pkcs8AuthConfig::Pkcs8Pem(Pkcs8PemConfig {
        ca_pem: b"-----BEGIN CERTIFICATE-----\n!!!!\n-----END CERTIFICATE-----\n".to_vec(),
        cert_pem: Vec::new(),
        pkcs8_key_pem: Vec::new().into(),
    });

    let err = match parse_pkcs8_auth_material(auth) {
        Ok(_) => panic!("expected invalid PEM"),
        Err(err) => err,
    };

    assert_message_contains(&err, "invalid CA PEM");
}

#[test]
fn pkcs8_missing_key_includes_context() {
    let auth = Pkcs8AuthConfig::Pkcs8Pem(Pkcs8PemConfig {
        ca_pem: Vec::new(),
        cert_pem: Vec::new(),
        pkcs8_key_pem: Vec::new().into(),
    });

    let err = match parse_pkcs8_auth_material(auth) {
        Ok(_) => panic!("expected missing key"),
        Err(err) => err,
    };

    assert_message_contains(&err, "missing PKCS#8 private key");
}

#[test]
fn tls_empty_certificate_chain_includes_context() {
    let certs: Vec<CertificateDer<'static>> = Vec::new();

    let err = extract_peer_uri_san_addr(&certs).expect_err("expected empty certificate chain");

    assert_message_contains(&err, "empty peer certificate chain");
}

#[test]
fn tls_invalid_aurelia_uri_includes_context() {
    let err = parse_aurelia_tcp_uri("aurelia+tcp://not-a-socket-addr")
        .expect_err("expected invalid aurelia TCP URI");

    assert_message_contains(&err, "invalid aurelia TCP URI address");
}
