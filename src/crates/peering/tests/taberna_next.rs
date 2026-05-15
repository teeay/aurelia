// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rcgen::{BasicConstraints, Certificate, CertificateParams, IsCa, SanType};

use aurelia_data::{DomusAddr, RouteResolver};
use aurelia_peering::{
    a3_message_type, AureliaError, DomusBuilder, DomusConfig, EncodedMessage, ErrorId,
    MessageCodec, MessageType, Pkcs8AuthConfig, Pkcs8DerConfig, TabernaId,
};

const TABERNA_NEXT_TEST_TIMEOUT: Duration = Duration::from_secs(1);
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

struct TestCodec;

struct EmptyResolver;

#[async_trait::async_trait]
impl RouteResolver for EmptyResolver {
    async fn resolve(&self, _taberna_id: TabernaId) -> Result<DomusAddr, AureliaError> {
        Err(AureliaError::new(ErrorId::UnknownTaberna))
    }
}

impl MessageCodec for TestCodec {
    type AppMessage = Bytes;

    fn encode_app(&self, msg: &Self::AppMessage) -> Result<EncodedMessage, AureliaError> {
        Ok(EncodedMessage::new(a3_message_type(100), msg.clone()))
    }

    fn decode_app(
        &self,
        _msg_type: MessageType,
        payload: &[u8],
    ) -> Result<Self::AppMessage, AureliaError> {
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

fn build_auth(ca: &Certificate, path: &Path) -> Pkcs8AuthConfig {
    let (cert_der, key_der) = build_domus_cert(ca, path);
    Pkcs8AuthConfig::Pkcs8Der(Pkcs8DerConfig {
        ca_der: ca.serialize_der().expect("ca der"),
        cert_der,
        pkcs8_key_der: key_der.into(),
    })
}

fn temp_dir(name: &str) -> TempSocketDir {
    let count = TEMP_DIR_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let _ = name;
    let dir = PathBuf::from("/tmp").join(format!("au-next-{}-{count}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("create temp dir");
    TempSocketDir {
        path: fs::canonicalize(&dir).expect("canonicalize temp dir"),
    }
}

async fn expect_invalid_config_build(config: DomusConfig, name: &str, reporting: bool) {
    let dir = temp_dir(name);
    let path = dir.join("domus.sock");
    let ca = build_ca();
    let auth = build_auth(&ca, &path);
    let resolver = Arc::new(EmptyResolver);
    let builder = DomusBuilder::new(config, DomusAddr::Socket(path), auth, resolver);
    let err = if reporting {
        builder
            .build_with_reporting()
            .await
            .map(|_| ())
            .expect_err("expected invalid config")
    } else {
        builder
            .build()
            .await
            .map(|_| ())
            .expect_err("expected invalid config")
    };
    assert_eq!(err.kind, ErrorId::InvalidConfig);
}

#[tokio::test]
async fn domus_builder_rejects_direct_invalid_send_queue_size() {
    tokio::time::timeout(TABERNA_NEXT_TEST_TIMEOUT, async {
        expect_invalid_config_build(
            DomusConfig {
                send_queue_size: 4097,
                ..Default::default()
            },
            "invalid-send-queue",
            false,
        )
        .await;
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn domus_builder_rejects_direct_invalid_max_payload_len() {
    tokio::time::timeout(TABERNA_NEXT_TEST_TIMEOUT, async {
        expect_invalid_config_build(
            DomusConfig {
                max_payload_len: 64 * 1024 * 1024 + 1,
                ..Default::default()
            },
            "invalid-max-payload",
            false,
        )
        .await;
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn domus_builder_with_reporting_rejects_direct_invalid_config() {
    tokio::time::timeout(TABERNA_NEXT_TEST_TIMEOUT, async {
        expect_invalid_config_build(
            DomusConfig {
                send_queue_size: 4097,
                ..Default::default()
            },
            "invalid-reporting",
            true,
        )
        .await;
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn taberna_next_timeout_and_shutdown_map_errors() {
    tokio::time::timeout(TABERNA_NEXT_TEST_TIMEOUT, async {
        let dir = temp_dir("next-timeout");
        let path = dir.join("domus.sock");
        let ca = build_ca();
        let auth = build_auth(&ca, &path);
        let resolver = Arc::new(EmptyResolver);
        let domus = DomusBuilder::new(
            DomusConfig::default(),
            DomusAddr::Socket(path),
            auth,
            resolver,
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
    })
    .await
    .expect("async test timed out");
}
