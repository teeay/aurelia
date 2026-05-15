// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rcgen::{BasicConstraints, Certificate, CertificateParams, IsCa, SanType};
use tokio::sync::RwLock;
use tokio::time::timeout;

use aurelia_data::{DomusAddr, RouteResolver};
use aurelia_peering::{
    a3_message_type, AureliaError, EncodedMessage, Pkcs8AuthConfig, Pkcs8DerConfig,
};
use aurelia_peering::{
    DomusBuilder, DomusConfig, DomusReportingEvent, ErrorId, MessageCodec, MessageType,
    SendOptions, TabernaId,
};

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

#[derive(Default)]
struct TestResolver {
    routes: RwLock<HashMap<TabernaId, DomusAddr>>,
}

impl TestResolver {
    async fn insert(&self, taberna_id: TabernaId, domus: DomusAddr) {
        let mut guard = self.routes.write().await;
        guard.insert(taberna_id, domus);
    }
}

#[async_trait::async_trait]
impl RouteResolver for TestResolver {
    async fn resolve(&self, taberna_id: TabernaId) -> Result<DomusAddr, AureliaError> {
        let guard = self.routes.read().await;
        guard
            .get(&taberna_id)
            .cloned()
            .ok_or_else(|| AureliaError::new(ErrorId::UnknownTaberna))
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
    let dir = PathBuf::from("/tmp").join(format!("au-obs-{}-{count}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("create temp dir");
    TempSocketDir {
        path: fs::canonicalize(&dir).expect("canonicalize temp dir"),
    }
}

#[tokio::test]
async fn domus_reporting_connected_peers_and_events() {
    let dir = temp_dir("connected-peers");
    let path_a = dir.join("domus-a.sock");
    let path_b = dir.join("domus-b.sock");

    let ca = build_ca();
    let auth_a = build_auth(&ca, &path_a);
    let auth_b = build_auth(&ca, &path_b);

    let resolver_a = Arc::new(TestResolver::default());
    let resolver_b = Arc::new(TestResolver::default());
    let taberna_id: TabernaId = 42;
    resolver_a
        .insert(taberna_id, DomusAddr::Socket(path_b.clone()))
        .await;

    let config = DomusConfig {
        listener_delay: Duration::from_millis(0),
        ..Default::default()
    };
    let (domus_a, mut feeds) = DomusBuilder::new(
        config.clone(),
        DomusAddr::Socket(path_a.clone()),
        auth_a,
        resolver_a,
    )
    .build_with_reporting()
    .await
    .expect("build domus a");

    let domus_b = DomusBuilder::new(
        config,
        DomusAddr::Socket(path_b.clone()),
        auth_b,
        resolver_b,
    )
    .build()
    .await
    .expect("build domus b");

    let taberna = domus_b
        .taberna(taberna_id, TestCodec)
        .await
        .expect("taberna");

    let taberna_task = tokio::spawn(async move {
        let request = taberna
            .next(Some(Duration::from_secs(30)))
            .await
            .expect("taberna next");
        request.accept();
    });

    domus_a
        .send(
            &TestCodec,
            taberna_id,
            &Bytes::from_static(b"ping"),
            SendOptions::MESSAGE_ONLY,
        )
        .await
        .expect("send");

    let connected = timeout(Duration::from_secs(5), async {
        loop {
            let event = feeds.events.recv().await.expect("event recv");
            if matches!(event, DomusReportingEvent::PeerConnectedEvent { .. }) {
                return event;
            }
        }
    })
    .await
    .expect("peer connected timeout");

    assert!(matches!(
        connected,
        DomusReportingEvent::PeerConnectedEvent { .. }
    ));

    let peers = domus_a
        .reporting()
        .connected_peer_identities()
        .await
        .expect("connected peer identities");
    assert!(peers.contains(&DomusAddr::Socket(path_b.clone())));

    let _ = taberna_task.await;

    domus_a.shutdown().await;
    domus_b.shutdown().await;
}

#[tokio::test]
async fn domus_reporting_emits_address_mismatch_error() {
    let dir = temp_dir("identity-mismatch");
    let path_a = dir.join("domus-a.sock");
    let ca = build_ca();
    let auth_a = build_auth(&ca, &path_a);

    let resolver_a = Arc::new(TestResolver::default());
    let taberna_id: TabernaId = 7;
    resolver_a
        .insert(
            taberna_id,
            DomusAddr::Tcp("127.0.0.1:5555".parse().expect("tcp addr")),
        )
        .await;

    let config = DomusConfig {
        listener_delay: Duration::from_millis(0),
        ..Default::default()
    };
    let (domus_a, mut feeds) = DomusBuilder::new(
        config.clone(),
        DomusAddr::Socket(path_a.clone()),
        auth_a,
        resolver_a,
    )
    .build_with_reporting()
    .await
    .expect("build domus a");

    let send_result = domus_a
        .send(
            &TestCodec,
            taberna_id,
            &Bytes::from_static(b"mismatch"),
            SendOptions::MESSAGE_ONLY,
        )
        .await
        .expect_err("expected address mismatch");
    assert_eq!(send_result.kind, ErrorId::AddressMismatch);

    let mismatch = timeout(Duration::from_secs(5), async {
        loop {
            let error = feeds.errors.recv().await.expect("error recv");
            if error.1.kind == ErrorId::AddressMismatch {
                return error;
            }
        }
    })
    .await
    .expect("address mismatch timeout");

    assert_eq!(mismatch.1.kind, ErrorId::AddressMismatch);

    domus_a.shutdown().await;
}
