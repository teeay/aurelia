// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, Mutex};

use actix::{Actor, ActorContext, Context, Handler};
use bytes::Bytes;
use tokio::sync::oneshot;
use tokio::time::{timeout, Duration};

use super::*;
use crate::taberna::TabernaRequest;
use crate::{a3_message_type, EncodedMessage};
use aurelia_ids::{AureliaError, MessageType, TabernaId};

const ACTIX_BRIDGE_UNIT_TEST_TIMEOUT: Duration = Duration::from_secs(1);
const TEST_MSG_TYPE: MessageType = a3_message_type(7);

struct TestMessage(String);

#[derive(Clone, Copy)]
struct TestCodec;

impl MessageCodec for TestCodec {
    type AppMessage = TestMessage;

    fn encode_app(&self, msg: &Self::AppMessage) -> Result<EncodedMessage, AureliaError> {
        Ok(EncodedMessage::new(
            TEST_MSG_TYPE,
            Bytes::from(msg.0.clone()),
        ))
    }

    fn decode_app(
        &self,
        msg_type: MessageType,
        payload: &[u8],
    ) -> Result<Self::AppMessage, AureliaError> {
        if msg_type != TEST_MSG_TYPE {
            return Err(AureliaError::with_message(
                ErrorId::DecodeFailure,
                "unexpected msg_type",
            ));
        }
        let payload = String::from_utf8(payload.to_vec())
            .map_err(|err| AureliaError::with_message(ErrorId::DecodeFailure, err.to_string()))?;
        Ok(TestMessage(payload))
    }
}

struct RecordingActor {
    received: Arc<Mutex<Vec<(String, bool)>>>,
}

impl Actor for RecordingActor {
    type Context = Context<Self>;
}

impl Handler<ActixTabernaDelivery<TestMessage>> for RecordingActor {
    type Result = ();

    fn handle(
        &mut self,
        delivery: ActixTabernaDelivery<TestMessage>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        self.received
            .lock()
            .expect("received lock")
            .push((delivery.message.0, delivery.blob_receiver.is_some()));
    }
}

struct StopActor;

impl Actor for StopActor {
    type Context = Context<Self>;
}

impl Handler<ActixTabernaDelivery<TestMessage>> for StopActor {
    type Result = ();

    fn handle(
        &mut self,
        _delivery: ActixTabernaDelivery<TestMessage>,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        ctx.stop();
    }
}

struct FullActor;

impl Actor for FullActor {
    type Context = Context<Self>;
}

impl Handler<ActixTabernaDelivery<TestMessage>> for FullActor {
    type Result = ();

    fn handle(
        &mut self,
        _delivery: ActixTabernaDelivery<TestMessage>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
    }
}

fn request(
    message: &str,
    blob_receiver: Option<BlobReceiver>,
) -> (
    TabernaRequest<TestCodec>,
    oneshot::Receiver<Result<(), AureliaError>>,
) {
    let (response, rx) = oneshot::channel();
    let request = TabernaRequest::new(
        TestMessage(message.to_string()),
        blob_receiver,
        response,
        None,
    );
    (request, rx)
}

#[actix::test]
async fn actix_bridge_try_send_success_accepts_request() {
    let received = Arc::new(Mutex::new(Vec::new()));
    let recipient = RecordingActor {
        received: Arc::clone(&received),
    }
    .start()
    .recipient();
    let (request, rx) = request("hello", None);

    let parts = request.into_parts();
    let delivery = ActixTabernaDelivery {
        message: parts.message,
        blob_receiver: parts.blob_receiver,
    };
    recipient.try_send(delivery).expect("try_send");
    parts.completion.accept();

    rx.await.expect("accept recv").expect("accepted");
    timeout(Duration::from_secs(1), async {
        loop {
            if !received.lock().expect("received lock").is_empty() {
                break;
            }
            actix::clock::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("actor delivery");
    let received = received.lock().expect("received lock");
    assert_eq!(received.as_slice(), [("hello".to_string(), false)]);
}

#[actix::test]
async fn actix_bridge_preserves_blob_receiver() {
    let received = Arc::new(Mutex::new(Vec::new()));
    let recipient = RecordingActor {
        received: Arc::clone(&received),
    }
    .start()
    .recipient();
    let receiver = BlobReceiver::new(Box::new(tokio::io::empty()));
    let (request, rx) = request("blob", Some(receiver));

    let parts = request.into_parts();
    let delivery = ActixTabernaDelivery {
        message: parts.message,
        blob_receiver: parts.blob_receiver,
    };
    recipient.try_send(delivery).expect("try_send");
    parts.completion.accept();

    rx.await.expect("accept recv").expect("accepted");
    timeout(Duration::from_secs(1), async {
        loop {
            if !received.lock().expect("received lock").is_empty() {
                break;
            }
            actix::clock::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("actor delivery");
    let received = received.lock().expect("received lock");
    assert_eq!(received.as_slice(), [("blob".to_string(), true)]);
}

#[actix::test]
async fn actix_bridge_closed_recipient_maps_taberna_shutdown() {
    tokio::time::timeout(ACTIX_BRIDGE_UNIT_TEST_TIMEOUT, async {
        let addr = StopActor.start();
        addr.do_send(ActixTabernaDelivery {
            message: TestMessage("stop".into()),
            blob_receiver: None,
        });
        let recipient = addr.recipient();
        actix::clock::sleep(Duration::from_millis(20)).await;
        let (request, rx) = request("after-stop", None);

        let parts = request.into_parts();
        let delivery = ActixTabernaDelivery {
            message: parts.message,
            blob_receiver: parts.blob_receiver,
        };
        match recipient.try_send(delivery) {
            Err(SendError::Closed(_delivery)) => parts.completion.taberna_shutdown(),
            other => panic!("expected closed recipient, got {other:?}"),
        }

        let err = rx
            .await
            .expect("accept recv")
            .expect_err("expected shutdown");
        assert_eq!(err.kind, ErrorId::TabernaShutdown);
    })
    .await
    .expect("async test timed out");
}

#[actix::test]
async fn actix_bridge_full_recipient_maps_taberna_busy() {
    tokio::time::timeout(ACTIX_BRIDGE_UNIT_TEST_TIMEOUT, async {
        let addr = FullActor::create(|ctx| {
            ctx.set_mailbox_capacity(1);
            FullActor
        });
        let recipient = addr.recipient();
        recipient
            .try_send(ActixTabernaDelivery {
                message: TestMessage("queued-1".into()),
                blob_receiver: None,
            })
            .expect("first queued");

        let (request, rx) = request("full", None);
        let parts = request.into_parts();
        let delivery = ActixTabernaDelivery {
            message: parts.message,
            blob_receiver: parts.blob_receiver,
        };
        match recipient.try_send(delivery) {
            Err(SendError::Full(_delivery)) => parts.completion.busy(),
            other => panic!("expected full recipient, got {other:?}"),
        }

        let err = rx.await.expect("accept recv").expect_err("expected busy");
        assert_eq!(err.kind, ErrorId::TabernaBusy);
    })
    .await
    .expect("async test timed out");
}

#[test]
fn actix_delivery_message_type_returns_unit() {
    fn assert_message<M>()
    where
        M: actix::Message<Result = ()>,
    {
    }

    assert_message::<ActixTabernaDelivery<TestMessage>>();
}

#[test]
fn actix_delivery_keeps_public_message_type_alias() {
    let _: MessageType = TEST_MSG_TYPE;
    let _: TabernaId = 7;
}
