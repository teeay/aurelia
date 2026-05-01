// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;

use crate::config::{DomusConfig, DomusConfigAccess};
use crate::taberna::TabernaInbox;
use bytes::Bytes;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::sync::{mpsc, watch, Notify};
use tokio::time::timeout;

#[derive(Clone, Default)]
struct TestBackend;

#[async_trait::async_trait]
impl TransportBackend for TestBackend {
    type Addr = DomusAddr;
    type Listener = ();
    type Stream = tokio::io::DuplexStream;

    async fn bind(&self, _local: &Self::Addr) -> Result<Self::Listener, AureliaError> {
        Ok(())
    }

    async fn accept(
        &self,
        _listener: &mut Self::Listener,
    ) -> Result<super::backend::AuthenticatedStream<Self::Stream, Self::Addr>, AureliaError> {
        Err(AureliaError::new(ErrorId::PeerUnavailable))
    }

    async fn dial(
        &self,
        _peer: &Self::Addr,
    ) -> Result<super::backend::AuthenticatedStream<Self::Stream, Self::Addr>, AureliaError> {
        Err(AureliaError::new(ErrorId::PeerUnavailable))
    }
}

type TestPeerEvent = PeerStateUpdate;

async fn spawn_blob_dispatcher(blob: Arc<BlobManager>) -> mpsc::Sender<TestPeerEvent> {
    let (events_tx, _events_rx) = mpsc::channel(8);
    let notify = blob.dispatch_handle();
    let dispatch_tx = events_tx.clone();
    tokio::spawn(async move {
        loop {
            notify.notified().await;
            dispatch_blob(&blob, &dispatch_tx, &notify).await;
        }
    });
    events_tx
}

struct RecordingSink {
    received: Mutex<Vec<(MessageType, Bytes, Option<crate::BlobReceiver>)>>,
    expected_msg_types: Vec<MessageType>,
}

impl RecordingSink {
    fn new(expected_msg_types: Vec<MessageType>) -> Self {
        Self {
            received: Mutex::new(Vec::new()),
            expected_msg_types,
        }
    }

    async fn take(&self) -> Vec<(MessageType, Bytes, Option<crate::BlobReceiver>)> {
        let mut guard = self.received.lock().await;
        std::mem::take(&mut *guard)
    }
}

#[async_trait::async_trait]
impl TabernaInbox for RecordingSink {
    async fn enqueue(
        &self,
        msg_type: MessageType,
        payload: Bytes,
        blob_receiver: Option<crate::BlobReceiver>,
        notify: Option<Arc<Notify>>,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<(), AureliaError>>, AureliaError> {
        if !self.expected_msg_types.contains(&msg_type) {
            return Err(AureliaError::new(ErrorId::RemoteTabernaRejected));
        }
        self.received
            .lock()
            .await
            .push((msg_type, payload, blob_receiver));
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = tx.send(Ok(()));
        if let Some(notify) = notify.as_ref() {
            notify.notify_one();
        }
        Ok(rx)
    }
}

mod backend;
mod blob;
mod callis;
mod handshake;
mod limits;
mod peer;
mod primary;
