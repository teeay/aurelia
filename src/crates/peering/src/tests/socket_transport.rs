// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use rcgen::{BasicConstraints, Certificate, CertificateParams, IsCa, SanType};
use tokio::sync::{Notify, RwLock};

use crate::auth::{Pkcs8AuthConfig, Pkcs8DerConfig};
use crate::config::{DomusConfig, DomusConfigAccess};
use crate::observability::new_observability;
use crate::peering::RouteLocalRemote;
use crate::taberna::{TabernaInbox, TabernaRegistry};
use crate::transport::Transport;
use crate::{BlobReceiver, MessageType, SendOptions, TabernaId};
use aurelia_data::DomusAddr;
use aurelia_data::RouteResolver;
use aurelia_ids::{AureliaError, ErrorId};

use super::{test_message, TestCodec};
use crate::a3_message_type;

const SOCKET_TRANSPORT_TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
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

struct RecordingInbox {
    received: tokio::sync::Mutex<Vec<(MessageType, Bytes)>>,
    blobs: tokio::sync::Mutex<Vec<BlobReceiver>>,
}

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

#[async_trait::async_trait]
impl TabernaInbox for RecordingInbox {
    async fn enqueue(
        &self,
        msg_type: MessageType,
        payload: Bytes,
        blob_receiver: Option<BlobReceiver>,
        notify: Option<Arc<Notify>>,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<(), AureliaError>>, AureliaError> {
        self.received.lock().await.push((msg_type, payload));
        if let Some(receiver) = blob_receiver {
            self.blobs.lock().await.push(receiver);
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = tx.send(Ok(()));
        if let Some(notify) = notify.as_ref() {
            notify.notify_one();
        }
        Ok(rx)
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
    let dir = PathBuf::from("/tmp").join(format!("au-st-{}-{count}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("create temp dir");
    TempSocketDir {
        path: fs::canonicalize(&dir).expect("canonicalize temp dir"),
    }
}

#[tokio::test]
async fn socket_primary_and_blob_delivery() {
    tokio::time::timeout(SOCKET_TRANSPORT_TEST_TIMEOUT, async {
        let dir = temp_dir("primary-and-blob");
        let path_a = dir.join("domus-a.sock");
        let path_b = dir.join("domus-b.sock");

        let ca = build_ca();
        let auth_a = build_auth(&ca, &path_a);
        let auth_b = build_auth(&ca, &path_b);

        let config_a = DomusConfigAccess::from_config(DomusConfig::default());
        let config_b = DomusConfigAccess::from_config(DomusConfig::default());
        let config_a_dyn: DomusConfigAccess = config_a.clone();
        let config_b_dyn: DomusConfigAccess = config_b.clone();

        let registry_a = Arc::new(TabernaRegistry::default());
        let registry_b = Arc::new(TabernaRegistry::default());
        let (_reporting_a, observability_a) = new_observability(tokio::runtime::Handle::current());
        let (_reporting_b, observability_b) = new_observability(tokio::runtime::Handle::current());

        let transport_a = Transport::bind(
            DomusAddr::Socket(path_a.clone()),
            Arc::clone(&registry_a),
            config_a_dyn.clone(),
            observability_a,
            tokio::runtime::Handle::current(),
            auth_a,
        )
        .await
        .expect("bind a");
        let transport_b = Transport::bind(
            DomusAddr::Socket(path_b.clone()),
            Arc::clone(&registry_b),
            config_b_dyn.clone(),
            observability_b,
            tokio::runtime::Handle::current(),
            auth_b,
        )
        .await
        .expect("bind b");

        let transport_a = Arc::new(transport_a);
        let transport_b = Arc::new(transport_b);
        let _handle_a = transport_a.start().await.expect("start a");
        let _handle_b = transport_b.start().await.expect("start b");

        let resolver_a = Arc::new(TestResolver::default());
        let resolver_b = Arc::new(TestResolver::default());

        let taberna_id: TabernaId = 42;
        resolver_a
            .insert(taberna_id, DomusAddr::Socket(path_b.clone()))
            .await;
        resolver_b
            .insert(taberna_id, DomusAddr::Socket(path_a.clone()))
            .await;

        let sink = Arc::new(RecordingInbox {
            received: tokio::sync::Mutex::new(Vec::new()),
            blobs: tokio::sync::Mutex::new(Vec::new()),
        });
        let sink_dyn: Arc<dyn TabernaInbox> = sink.clone();
        registry_b
            .register(taberna_id, sink_dyn)
            .await
            .expect("register sink");

        let peering_a = RouteLocalRemote::new(
            config_a.clone(),
            Arc::clone(&registry_a),
            Arc::clone(&resolver_a),
            Arc::clone(&transport_a),
        );
        let _peering_b = RouteLocalRemote::new(
            config_b.clone(),
            Arc::clone(&registry_b),
            Arc::clone(&resolver_b),
            Arc::clone(&transport_b),
        );
        let codec = TestCodec;
        let ping_msg_type = a3_message_type(100);

        peering_a
            .send(
                &codec,
                taberna_id,
                &test_message(ping_msg_type, b"ping"),
                SendOptions::MESSAGE_ONLY,
            )
            .await
            .expect("send");
        let received = sink.received.lock().await.clone();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].0, ping_msg_type);
        assert_eq!(received[0].1, Bytes::from_static(b"ping"));

        let blob_msg_type = a3_message_type(101);
        let outcome = peering_a
            .send(
                &codec,
                taberna_id,
                &test_message(blob_msg_type, b"blob-meta"),
                SendOptions::BLOB,
            )
            .await
            .expect("send blob");
        let mut sender = match outcome {
            crate::SendOutcome::Blob { sender } => sender,
            crate::SendOutcome::MessageOnly => panic!("expected blob sender"),
        };
        use tokio::io::AsyncWriteExt;
        sender.write_all(b"blob").await.expect("write blob");
        sender.shutdown().await.expect("shutdown blob sender");

        let mut blobs = sink.blobs.lock().await;
        assert_eq!(blobs.len(), 1);
        let mut receiver = blobs.pop().expect("blob receiver");
        drop(blobs);
        use tokio::io::AsyncReadExt;
        let mut data = Vec::new();
        receiver.read_to_end(&mut data).await.expect("read blob");
        assert_eq!(data, b"blob");
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
/// Smooth rotation over the socket backend: reload swaps auth material non-disruptively;
/// sends straddling the reload all succeed; the new cert produced by the second `build_auth`
/// is implicitly accepted (different keypair, same CA).
async fn socket_reload_auth_keeps_existing_connection_and_admits_new_cert() {
    let dir = temp_dir("reload-auth");
    let path_a = dir.join("domus-a.sock");
    let path_b = dir.join("domus-b.sock");

    let ca = build_ca();
    let auth_a = build_auth(&ca, &path_a);
    let auth_b = build_auth(&ca, &path_b);

    let config_a = DomusConfigAccess::from_config(DomusConfig::default());
    let config_b = DomusConfigAccess::from_config(DomusConfig::default());
    let config_a_dyn: DomusConfigAccess = config_a.clone();
    let config_b_dyn: DomusConfigAccess = config_b.clone();

    let registry_a = Arc::new(TabernaRegistry::default());
    let registry_b = Arc::new(TabernaRegistry::default());
    let (_reporting_a, observability_a) = new_observability(tokio::runtime::Handle::current());
    let (_reporting_b, observability_b) = new_observability(tokio::runtime::Handle::current());

    let transport_a = Transport::bind(
        DomusAddr::Socket(path_a.clone()),
        Arc::clone(&registry_a),
        config_a_dyn.clone(),
        observability_a,
        tokio::runtime::Handle::current(),
        auth_a,
    )
    .await
    .expect("bind a");
    let transport_b = Transport::bind(
        DomusAddr::Socket(path_b.clone()),
        Arc::clone(&registry_b),
        config_b_dyn.clone(),
        observability_b,
        tokio::runtime::Handle::current(),
        auth_b,
    )
    .await
    .expect("bind b");

    let _handle_a = transport_a.start().await.expect("start a");
    let _handle_b = transport_b.start().await.expect("start b");
    let transport_a = Arc::new(transport_a);
    let transport_b = Arc::new(transport_b);

    let resolver_a = Arc::new(TestResolver::default());
    let resolver_b = Arc::new(TestResolver::default());

    let taberna_id: TabernaId = 99;
    resolver_a
        .insert(taberna_id, DomusAddr::Socket(path_b.clone()))
        .await;
    resolver_b
        .insert(taberna_id, DomusAddr::Socket(path_a.clone()))
        .await;

    let sink = Arc::new(RecordingInbox {
        received: tokio::sync::Mutex::new(Vec::new()),
        blobs: tokio::sync::Mutex::new(Vec::new()),
    });
    let sink_dyn: Arc<dyn TabernaInbox> = sink.clone();
    registry_b
        .register(taberna_id, sink_dyn)
        .await
        .expect("register sink");

    let peering_a = RouteLocalRemote::new(
        config_a.clone(),
        Arc::clone(&registry_a),
        Arc::clone(&resolver_a),
        Arc::clone(&transport_a),
    );
    let _peering_b = RouteLocalRemote::new(
        config_b.clone(),
        Arc::clone(&registry_b),
        Arc::clone(&resolver_b),
        Arc::clone(&transport_b),
    );
    let codec = TestCodec;
    let msg_type = a3_message_type(200);

    peering_a
        .send(
            &codec,
            taberna_id,
            &test_message(msg_type, b"first"),
            SendOptions::MESSAGE_ONLY,
        )
        .await
        .expect("send first");
    let received = sink.received.lock().await.clone();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].1, Bytes::from_static(b"first"));

    let new_auth = build_auth(&ca, &path_a);
    transport_a
        .reload_auth(new_auth)
        .await
        .expect("reload auth");

    peering_a
        .send(
            &codec,
            taberna_id,
            &test_message(msg_type, b"second"),
            SendOptions::MESSAGE_ONLY,
        )
        .await
        .expect("send second");
    let received = sink.received.lock().await.clone();
    assert_eq!(received.len(), 2);
    assert_eq!(received[1].1, Bytes::from_static(b"second"));

    peering_a
        .send(
            &codec,
            taberna_id,
            &test_message(msg_type, b"third"),
            SendOptions::MESSAGE_ONLY,
        )
        .await
        .expect("send third");
    let received = sink.received.lock().await.clone();
    assert_eq!(received.len(), 3);
    assert_eq!(received[2].1, Bytes::from_static(b"third"));
}
