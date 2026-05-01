// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rcgen::{BasicConstraints, Certificate, CertificateParams, IsCa, SanType};

use aurelia_peering::{
    AureliaError, DomusAddr, DomusAuthConfig, DomusBuilder, DomusConfig, EncodedMessage, ErrorId,
    MessageCodec, Pkcs8AuthConfig, Pkcs8DerConfig, SimpleResolver, TabernaId,
};

struct TestCodec;

impl MessageCodec for TestCodec {
    type AppMessage = Bytes;

    fn encode_app(&self, msg: &Self::AppMessage) -> Result<EncodedMessage, AureliaError> {
        Ok(EncodedMessage::new(100, msg.clone()))
    }

    fn decode_app(&self, _msg_type: u32, payload: &[u8]) -> Result<Self::AppMessage, AureliaError> {
        Ok(Bytes::copy_from_slice(payload))
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

fn build_auth(ca: &Certificate, path: &Path) -> DomusAuthConfig {
    let (cert_der, key_der) = build_domus_cert(ca, path);
    DomusAuthConfig::Pkcs8(Pkcs8AuthConfig::Pkcs8Der(Pkcs8DerConfig {
        ca_der: ca.serialize_der().expect("ca der"),
        cert_der,
        pkcs8_key_der: key_der,
    }))
}

fn temp_dir(name: &str) -> PathBuf {
    let root = workspace_root().join("tmp/peering-taberna-next");
    let dir = root.join(name);
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("create temp dir");
    fs::canonicalize(&dir).expect("canonicalize temp dir")
}

fn workspace_root() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(|dir| dir.parent())
        .and_then(|dir| dir.parent())
        .map(PathBuf::from)
        .expect("workspace root")
}

#[tokio::test]
async fn taberna_next_timeout_and_shutdown_map_errors() {
    let dir = temp_dir("next-timeout");
    let path = dir.join("domus.sock");
    let ca = build_ca();
    let auth = build_auth(&ca, &path);
    let resolver = Arc::new(SimpleResolver::new());
    let domus = DomusBuilder::new(
        DomusConfig::default(),
        DomusAddr::Socket(path),
        auth,
        resolver,
        tokio::runtime::Handle::current(),
    )
    .build()
    .await
    .expect("build domus");

    let taberna_id: TabernaId = 7001;
    let taberna = domus.taberna(taberna_id, TestCodec).await.expect("taberna");

    let err = match taberna.next(Some(Duration::from_millis(20))).await {
        Ok(_) => panic!("expected receive-timeout"),
        Err(err) => err,
    };
    assert_eq!(err.kind, ErrorId::ReceiveTimeout);

    domus.shutdown().await;
    let err = match taberna.next(Some(Duration::from_millis(20))).await {
        Ok(_) => panic!("expected domus-closed"),
        Err(err) => err,
    };
    assert_eq!(err.kind, ErrorId::DomusClosed);
}
