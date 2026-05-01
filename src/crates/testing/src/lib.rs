// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{oneshot, Mutex, Notify};

use aurelia_peering::{
    AureliaError, BlobReceiver, DomusAddr, ErrorId, RouteResolver, TabernaInbox,
};

pub struct RecordingSink {
    received: Mutex<Vec<(u32, Bytes, Option<BlobReceiver>)>>,
    expected_msg_types: Vec<u32>,
}

impl RecordingSink {
    pub fn new(expected_msg_type: u32) -> Self {
        Self::new_multi(vec![expected_msg_type])
    }

    pub fn new_multi(expected_msg_types: Vec<u32>) -> Self {
        Self {
            received: Mutex::new(Vec::new()),
            expected_msg_types,
        }
    }

    pub async fn take(&self) -> Vec<(u32, Bytes, Option<BlobReceiver>)> {
        let mut guard = self.received.lock().await;
        std::mem::take(&mut *guard)
    }
}

#[async_trait::async_trait]
impl TabernaInbox for RecordingSink {
    async fn enqueue(
        &self,
        msg_type: u32,
        payload: Bytes,
        blob_receiver: Option<BlobReceiver>,
        notify: Option<Arc<Notify>>,
    ) -> Result<oneshot::Receiver<Result<(), AureliaError>>, AureliaError> {
        if !self.expected_msg_types.contains(&msg_type) {
            return Err(AureliaError::new(ErrorId::RemoteTabernaRejected));
        }
        self.received
            .lock()
            .await
            .push((msg_type, payload, blob_receiver));
        if let Some(notify) = notify.as_ref() {
            notify.notify_one();
        }
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Ok(()));
        Ok(rx)
    }
}

pub struct BlockingSink {
    started: Arc<Notify>,
    ready: Arc<Notify>,
    expected_msg_type: u32,
}

impl BlockingSink {
    pub fn new(expected_msg_type: u32, started: Arc<Notify>, ready: Arc<Notify>) -> Self {
        Self {
            started,
            ready,
            expected_msg_type,
        }
    }
}

#[async_trait::async_trait]
impl TabernaInbox for BlockingSink {
    async fn enqueue(
        &self,
        msg_type: u32,
        _payload: Bytes,
        _blob_receiver: Option<BlobReceiver>,
        notify: Option<Arc<Notify>>,
    ) -> Result<oneshot::Receiver<Result<(), AureliaError>>, AureliaError> {
        if msg_type != self.expected_msg_type {
            return Err(AureliaError::new(ErrorId::RemoteTabernaRejected));
        }
        if let Some(notify) = notify.as_ref() {
            notify.notify_one();
        }
        self.started.notify_waiters();
        let (tx, rx) = oneshot::channel();
        let ready = Arc::clone(&self.ready);
        tokio::spawn(async move {
            ready.notified().await;
            let _ = tx.send(Ok(()));
        });
        Ok(rx)
    }
}

pub struct RejectingSink {
    expected_msg_type: u32,
}

impl RejectingSink {
    pub fn new(expected_msg_type: u32) -> Self {
        Self { expected_msg_type }
    }
}

#[async_trait::async_trait]
impl TabernaInbox for RejectingSink {
    async fn enqueue(
        &self,
        msg_type: u32,
        _payload: Bytes,
        _blob_receiver: Option<BlobReceiver>,
        notify: Option<Arc<Notify>>,
    ) -> Result<oneshot::Receiver<Result<(), AureliaError>>, AureliaError> {
        if msg_type != self.expected_msg_type {
            return Err(AureliaError::new(ErrorId::RemoteTabernaRejected));
        }
        if let Some(notify) = notify.as_ref() {
            notify.notify_one();
        }
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Err(AureliaError::new(ErrorId::RemoteTabernaRejected)));
        Ok(rx)
    }
}

pub struct IngressFullSink {
    expected_msg_type: u32,
}

impl IngressFullSink {
    pub fn new(expected_msg_type: u32) -> Self {
        Self { expected_msg_type }
    }
}

#[async_trait::async_trait]
impl TabernaInbox for IngressFullSink {
    async fn enqueue(
        &self,
        msg_type: u32,
        _payload: Bytes,
        _blob_receiver: Option<BlobReceiver>,
        notify: Option<Arc<Notify>>,
    ) -> Result<oneshot::Receiver<Result<(), AureliaError>>, AureliaError> {
        if msg_type != self.expected_msg_type {
            return Err(AureliaError::new(ErrorId::RemoteTabernaRejected));
        }
        if let Some(notify) = notify.as_ref() {
            notify.notify_one();
        }
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Err(AureliaError::new(ErrorId::LocalQueueFull)));
        Ok(rx)
    }
}

pub struct TimeoutSink {
    expected_msg_type: u32,
}

impl TimeoutSink {
    pub fn new(expected_msg_type: u32) -> Self {
        Self { expected_msg_type }
    }
}

#[async_trait::async_trait]
impl TabernaInbox for TimeoutSink {
    async fn enqueue(
        &self,
        msg_type: u32,
        _payload: Bytes,
        _blob_receiver: Option<BlobReceiver>,
        notify: Option<Arc<Notify>>,
    ) -> Result<oneshot::Receiver<Result<(), AureliaError>>, AureliaError> {
        if msg_type != self.expected_msg_type {
            return Err(AureliaError::new(ErrorId::RemoteTabernaRejected));
        }
        if let Some(notify) = notify.as_ref() {
            notify.notify_one();
        }
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Err(AureliaError::new(ErrorId::TabernaBusy)));
        Ok(rx)
    }
}

pub struct StaticRouteResolver {
    addr: Option<DomusAddr>,
}

impl StaticRouteResolver {
    pub fn new(addr: Option<DomusAddr>) -> Self {
        Self { addr }
    }

    pub fn with_addr(addr: DomusAddr) -> Self {
        Self { addr: Some(addr) }
    }
}

#[async_trait::async_trait]
impl RouteResolver for StaticRouteResolver {
    async fn resolve(&self, _taberna_id: u64) -> Result<DomusAddr, AureliaError> {
        self.addr
            .clone()
            .ok_or(AureliaError::new(ErrorId::UnknownTaberna))
    }
}

pub fn shared_sink(expected_msg_type: u32) -> Arc<RecordingSink> {
    Arc::new(RecordingSink::new(expected_msg_type))
}
