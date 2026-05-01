// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use actix::prelude::Recipient;
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::{oneshot, Notify};

#[cfg(test)]
use crate::codec::decode_error;
use crate::codec::MessageCodec;
use crate::taberna::TabernaInbox;
use crate::BlobReceiver;
use aurelia_ids::{AureliaError, ErrorId};

pub struct ActixTabernaSink<C>
where
    C: MessageCodec,
    C::AppMessage: actix::Message + Send + 'static,
    <C::AppMessage as actix::Message>::Result: Send,
{
    codec: C,
    recipient: Recipient<C::AppMessage>,
    runtime_handle: tokio::runtime::Handle,
}

impl<C> ActixTabernaSink<C>
where
    C: MessageCodec,
    C::AppMessage: actix::Message + Send + 'static,
    <C::AppMessage as actix::Message>::Result: Send,
{
    pub fn new(
        codec: C,
        recipient: Recipient<C::AppMessage>,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            codec,
            recipient,
            runtime_handle,
        }
    }
}

#[async_trait::async_trait]
impl<C> TabernaInbox for ActixTabernaSink<C>
where
    C: MessageCodec,
    C::AppMessage: actix::Message + Send + 'static,
    <C::AppMessage as actix::Message>::Result: Send,
{
    async fn enqueue(
        &self,
        msg_type: u32,
        payload: Bytes,
        _blob_receiver: Option<BlobReceiver>,
        notify: Option<Arc<Notify>>,
    ) -> Result<oneshot::Receiver<Result<(), AureliaError>>, AureliaError> {
        let message = self
            .codec
            .decode_app(msg_type, &payload)
            .map_err(|err| AureliaError::with_message(ErrorId::DecodeFailure, err.to_string()))?;
        let recipient = self.recipient.clone();
        let runtime_handle = self.runtime_handle.clone();
        let (tx, rx) = oneshot::channel();
        runtime_handle.spawn(async move {
            let result = recipient
                .send(message)
                .await
                .map(|_| ())
                .map_err(|_| AureliaError::new(ErrorId::RemoteTabernaRejected));
            let _ = tx.send(result);
            if let Some(notify) = notify.as_ref() {
                notify.notify_one();
            }
        });
        Ok(rx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use actix::{Actor, ActorContext, Context, Handler};

    use super::*;
    use crate::EncodedMessage;

    #[derive(actix::Message)]
    #[rtype(result = "()")]
    struct TestMessage(String);

    #[derive(Clone, Copy)]
    struct TestCodec;

    impl MessageCodec for TestCodec {
        type AppMessage = TestMessage;

        fn encode_app(&self, msg: &Self::AppMessage) -> Result<EncodedMessage, AureliaError> {
            Ok(EncodedMessage::new(7, Bytes::from(msg.0.clone())))
        }

        fn decode_app(
            &self,
            msg_type: u32,
            payload: &[u8],
        ) -> Result<Self::AppMessage, AureliaError> {
            if msg_type != 7 {
                return Err(decode_error("unexpected msg_type"));
            }
            let payload =
                String::from_utf8(payload.to_vec()).map_err(|err| decode_error(err.to_string()))?;
            Ok(TestMessage(payload))
        }
    }

    struct RecordingActor {
        received: Arc<Mutex<Vec<String>>>,
    }

    impl Actor for RecordingActor {
        type Context = Context<Self>;
    }

    impl Handler<TestMessage> for RecordingActor {
        type Result = ();

        fn handle(&mut self, msg: TestMessage, _ctx: &mut Self::Context) -> Self::Result {
            self.received.lock().expect("received lock").push(msg.0);
        }
    }

    struct StopActor;

    impl Actor for StopActor {
        type Context = Context<Self>;
    }

    impl Handler<TestMessage> for StopActor {
        type Result = ();

        fn handle(&mut self, _msg: TestMessage, ctx: &mut Self::Context) -> Self::Result {
            ctx.stop();
        }
    }

    #[actix::test]
    async fn actix_taberna_sink_delivers_decoded_message() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let recipient = RecordingActor {
            received: Arc::clone(&received),
        }
        .start()
        .recipient();
        let sink = ActixTabernaSink::new(TestCodec, recipient, tokio::runtime::Handle::current());

        let rx = sink
            .enqueue(7, Bytes::from_static(b"hello"), None, None)
            .await
            .expect("actix enqueue");
        rx.await.expect("accept recv").expect("actix accept");

        let received = received.lock().expect("received lock");
        assert_eq!(received.as_slice(), ["hello"]);
    }

    #[actix::test]
    async fn actix_taberna_sink_maps_decode_failures() {
        let recipient = RecordingActor {
            received: Arc::new(Mutex::new(Vec::new())),
        }
        .start()
        .recipient();
        let sink = ActixTabernaSink::new(TestCodec, recipient, tokio::runtime::Handle::current());

        let err = sink
            .enqueue(90, Bytes::from_static(b"bad"), None, None)
            .await
            .expect_err("expected decode failure");
        assert_eq!(err.kind, ErrorId::DecodeFailure);
        let message = err.message.expect("decode message");
        assert!(message.contains("unexpected msg_type"));
    }

    #[actix::test]
    async fn actix_taberna_sink_maps_mailbox_failures() {
        let addr = StopActor.start();
        addr.do_send(TestMessage("stop".into()));
        let recipient = addr.recipient();
        actix::clock::sleep(std::time::Duration::from_millis(20)).await;
        let sink = ActixTabernaSink::new(TestCodec, recipient, tokio::runtime::Handle::current());

        let rx = sink
            .enqueue(7, Bytes::from_static(b"after-stop"), None, None)
            .await
            .expect("enqueue");
        let err = rx
            .await
            .expect("accept recv")
            .expect_err("expected mailbox failure");
        assert_eq!(err.kind, ErrorId::RemoteTabernaRejected);
    }
}
