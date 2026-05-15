// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

#![cfg(feature = "actix")]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use actix::{Actor, Context, Handler};
use bytes::Bytes;
use rcgen::{BasicConstraints, Certificate, CertificateParams, IsCa, SanType};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};

use aurelia_data::{DomusAddr, RouteResolver};
use aurelia_ids::{a3_message_type, AureliaError, ErrorId, MessageType, TabernaId};
use aurelia_peering::{
    ActixTabernaDelivery, DomusBuilder, DomusConfig, EncodedMessage, MessageCodec, Pkcs8AuthConfig,
    Pkcs8DerConfig, SendOptions, SendOutcome,
};

const ACTIX_TABERNA_TEST_TIMEOUT: Duration = Duration::from_secs(5);
const TEST_MSG_TYPE: MessageType = a3_message_type(101);
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

struct TestDomus {
    domus: aurelia_peering::Domus<EmptyResolver>,
    _dir: TempSocketDir,
}

impl std::ops::Deref for TestDomus {
    type Target = aurelia_peering::Domus<EmptyResolver>;

    fn deref(&self) -> &Self::Target {
        &self.domus
    }
}

struct TestCodec;

impl MessageCodec for TestCodec {
    type AppMessage = Bytes;

    fn encode_app(&self, msg: &Self::AppMessage) -> Result<EncodedMessage, AureliaError> {
        Ok(EncodedMessage::new(TEST_MSG_TYPE, msg.clone()))
    }

    fn decode_app(
        &self,
        _msg_type: MessageType,
        payload: &[u8],
    ) -> Result<Self::AppMessage, AureliaError> {
        Ok(Bytes::copy_from_slice(payload))
    }
}

struct EmptyResolver;

#[async_trait::async_trait]
impl RouteResolver for EmptyResolver {
    async fn resolve(&self, _taberna_id: TabernaId) -> Result<DomusAddr, AureliaError> {
        Err(AureliaError::new(ErrorId::UnknownTaberna))
    }
}

struct AcceptActor;

impl Actor for AcceptActor {
    type Context = Context<Self>;
}

impl Handler<ActixTabernaDelivery<Bytes>> for AcceptActor {
    type Result = ();

    fn handle(
        &mut self,
        _delivery: ActixTabernaDelivery<Bytes>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
    }
}

struct RecordingActor {
    tx: mpsc::UnboundedSender<Bytes>,
}

impl Actor for RecordingActor {
    type Context = Context<Self>;
}

impl Handler<ActixTabernaDelivery<Bytes>> for RecordingActor {
    type Result = ();

    fn handle(
        &mut self,
        delivery: ActixTabernaDelivery<Bytes>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let _ = self.tx.send(delivery.message);
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
    let dir = PathBuf::from("/tmp").join(format!("au-actix-{}-{count}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("create temp dir");
    TempSocketDir {
        path: fs::canonicalize(&dir).expect("canonicalize temp dir"),
    }
}

async fn build_test_domus(name: &str) -> TestDomus {
    let dir = temp_dir(name);
    let path = dir.join("domus.sock");
    let ca = build_ca();
    let auth = build_auth(&ca, &path);
    let domus = DomusBuilder::new(
        DomusConfig::default(),
        DomusAddr::Socket(path),
        auth,
        Arc::new(EmptyResolver),
    )
    .build()
    .await
    .expect("build domus");
    TestDomus { domus, _dir: dir }
}

#[actix::test]
async fn actix_taberna_handle_unregisters_on_drop() {
    tokio::time::timeout(ACTIX_TABERNA_TEST_TIMEOUT, async {
        let domus = build_test_domus("drop-unregister").await;

        let taberna_id: TabernaId = 7002;
        let recipient = AcceptActor.start().recipient();
        let handle = domus
            .actix_taberna(taberna_id, TestCodec, recipient.clone())
            .await
            .expect("register actix taberna");

        let err = match domus
            .actix_taberna(taberna_id, TestCodec, recipient.clone())
            .await
        {
            Ok(_) => panic!("duplicate registration must fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind, ErrorId::TabernaAlreadyRegistered);

        drop(handle);

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match domus
                .actix_taberna(taberna_id, TestCodec, recipient.clone())
                .await
            {
                Ok(_handle) => break,
                Err(err) if err.kind == ErrorId::TabernaAlreadyRegistered => {
                    assert!(
                        Instant::now() < deadline,
                        "actix taberna did not unregister"
                    );
                    sleep(Duration::from_millis(10)).await;
                }
                Err(err) => panic!("unexpected registration error: {err:?}"),
            }
        }

        domus.shutdown().await;
    })
    .await
    .expect("async test timed out");
}

#[actix::test]
async fn actix_taberna_local_send_moves_message_to_recipient() {
    let domus = build_test_domus("local-send").await;
    let taberna_id: TabernaId = 7003;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let recipient = RecordingActor { tx }.start().recipient();
    let _handle = domus
        .actix_taberna(taberna_id, TestCodec, recipient)
        .await
        .expect("register actix taberna");

    let message = Bytes::from_static(b"hello-actix");
    let outcome = domus
        .send(&TestCodec, taberna_id, &message, SendOptions::MESSAGE_ONLY)
        .await
        .expect("send to actix taberna");
    assert!(matches!(outcome, SendOutcome::MessageOnly));

    let delivered = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("delivery timeout")
        .expect("delivery channel closed");
    assert_eq!(delivered, message);

    domus.shutdown().await;
}
