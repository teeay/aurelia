// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::timeout;

use super::{BlobManager, BlobReceiverState};
use crate::ring_buffer::OutboundRingBuffer;
use aurelia_ids::PeerMessageId;
use aurelia_ids::{AureliaError, ErrorId};

pub(crate) struct BlobReceiverStream {
    blob: std::sync::Arc<BlobManager>,
    stream_id: PeerMessageId,
    receiver: std::sync::Arc<BlobReceiverState>,
    current: Option<Bytes>,
    offset: usize,
    pending: Option<Pin<Box<dyn Future<Output = ReadOutcome> + Send>>>,
    runtime_handle: tokio::runtime::Handle,
}

enum ReadOutcome {
    Chunk(Bytes),
    Eof,
    Error(AureliaError),
}

impl BlobReceiverStream {
    pub(crate) fn new(
        blob: std::sync::Arc<BlobManager>,
        stream_id: PeerMessageId,
        receiver: std::sync::Arc<BlobReceiverState>,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            blob,
            stream_id,
            receiver,
            current: None,
            offset: 0,
            pending: None,
            runtime_handle,
        }
    }

    fn start_read(&mut self) {
        let blob = std::sync::Arc::clone(&self.blob);
        let receiver = std::sync::Arc::clone(&self.receiver);
        let stream_id = self.stream_id;
        let idle_timeout = receiver.idle_timeout;
        self.pending = Some(Box::pin(async move {
            loop {
                if !receiver.accepted.load(std::sync::atomic::Ordering::SeqCst) {
                    let notified = receiver.notify.notified();
                    if timeout(idle_timeout, notified).await.is_err() {
                        let err = AureliaError::new(ErrorId::BlobStreamIdleTimeout);
                        {
                            let mut guard = receiver.error.lock().await;
                            *guard = Some(err.clone());
                        }
                        receiver
                            .completed
                            .store(true, std::sync::atomic::Ordering::SeqCst);
                        receiver.notify.notify_waiters();
                        let _ = blob.remove_recv_stream(stream_id).await;
                        return ReadOutcome::Error(err);
                    }
                    continue;
                }
                if let Some(err) = receiver.error.lock().await.clone() {
                    return ReadOutcome::Error(err);
                }
                let ring = {
                    let recv = blob.recv_streams.lock().await;
                    recv.get(&stream_id)
                        .map(|state| std::sync::Arc::clone(&state.ring))
                };
                if let Some(ring) = ring {
                    if let Some(chunk) = ring.take_next().await {
                        receiver.notify.notify_waiters();
                        return ReadOutcome::Chunk(chunk);
                    }
                    if receiver.completed.load(std::sync::atomic::Ordering::SeqCst)
                        && ring.is_complete().await
                    {
                        let _ = blob.remove_recv_stream(stream_id).await;
                        blob.note_recv_complete(stream_id, receiver.completion_ttl)
                            .await;
                        return ReadOutcome::Eof;
                    }
                } else {
                    if let Some(err) = receiver.error.lock().await.clone() {
                        return ReadOutcome::Error(err);
                    }
                    if receiver.completed.load(std::sync::atomic::Ordering::SeqCst) {
                        return ReadOutcome::Eof;
                    }
                }
                let notified = receiver.notify.notified();
                if timeout(idle_timeout, notified).await.is_err() {
                    let err = AureliaError::new(ErrorId::BlobStreamIdleTimeout);
                    {
                        let mut guard = receiver.error.lock().await;
                        *guard = Some(err.clone());
                    }
                    receiver
                        .completed
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    receiver.notify.notify_waiters();
                    let _ = blob.remove_recv_stream(stream_id).await;
                    return ReadOutcome::Error(err);
                }
            }
        }));
    }
}

impl AsyncRead for BlobReceiverStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        loop {
            if self.current.is_some() {
                let (to_copy, chunk_len) = {
                    let Some(chunk) = self.current.as_ref() else {
                        return Poll::Ready(Err(std::io::Error::other(
                            "blob receiver missing current chunk",
                        )));
                    };
                    let start = self.offset;
                    let end = (start + buf.remaining()).min(chunk.len());
                    let len = end.saturating_sub(start);
                    if len > 0 {
                        buf.put_slice(&chunk[start..end]);
                    }
                    (len, chunk.len())
                };
                self.offset = self.offset.saturating_add(to_copy);
                if self.offset >= chunk_len {
                    self.current = None;
                    self.offset = 0;
                }
                return Poll::Ready(Ok(()));
            }

            if self.pending.is_none() {
                self.start_read();
            }

            let Some(mut pending) = self.pending.take() else {
                return Poll::Ready(Err(std::io::Error::other(
                    "blob receiver missing read future",
                )));
            };
            match pending.as_mut().poll(cx) {
                Poll::Pending => {
                    self.pending = Some(pending);
                    return Poll::Pending;
                }
                Poll::Ready(outcome) => match outcome {
                    ReadOutcome::Chunk(chunk) => {
                        self.current = Some(chunk);
                    }
                    ReadOutcome::Eof => return Poll::Ready(Ok(())),
                    ReadOutcome::Error(err) => {
                        return Poll::Ready(Err(std::io::Error::other(err.to_string())))
                    }
                },
            }
        }
    }
}

impl Drop for BlobReceiverStream {
    fn drop(&mut self) {
        let blob = std::sync::Arc::clone(&self.blob);
        let receiver = std::sync::Arc::clone(&self.receiver);
        let stream_id = self.stream_id;
        let runtime_handle = self.runtime_handle.clone();
        runtime_handle.spawn(async move {
            if receiver.completed.load(std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            let err = AureliaError::new(ErrorId::PeerUnavailable);
            let _ = blob.send_stream_error(stream_id, err.clone()).await;
            let _ = blob.remove_recv_stream(stream_id).await;
            blob.drop_pending_request(stream_id).await;
            {
                let mut guard = receiver.error.lock().await;
                *guard = Some(err);
            }
            receiver
                .completed
                .store(true, std::sync::atomic::Ordering::SeqCst);
            receiver.notify.notify_waiters();
        });
    }
}

pub(crate) struct BlobSenderStream {
    state: std::sync::Arc<tokio::sync::Mutex<BlobSenderState>>,
    pending: Option<PendingOp>,
    runtime_handle: tokio::runtime::Handle,
}

struct BlobSenderState {
    blob: std::sync::Arc<BlobManager>,
    stream_id: PeerMessageId,
    ring: std::sync::Arc<OutboundRingBuffer>,
    send_timeout: Duration,
    closed: bool,
}

#[derive(Clone, Copy)]
enum PendingKind {
    Write(usize),
    Flush,
    Shutdown,
}

struct PendingOp {
    kind: PendingKind,
    future: Pin<Box<dyn Future<Output = Result<(), AureliaError>> + Send>>,
}

impl BlobSenderStream {
    pub(crate) fn new(
        blob: std::sync::Arc<BlobManager>,
        stream_id: PeerMessageId,
        ring: std::sync::Arc<OutboundRingBuffer>,
        send_timeout: Duration,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        let state = BlobSenderState {
            blob,
            stream_id,
            ring,
            send_timeout,
            closed: false,
        };
        Self {
            state: std::sync::Arc::new(tokio::sync::Mutex::new(state)),
            pending: None,
            runtime_handle,
        }
    }

    fn start_write(&mut self, data: Bytes, len: usize) {
        let state = std::sync::Arc::clone(&self.state);
        let future = async move {
            let state = state.lock().await;
            if state.closed {
                return Err(AureliaError::new(ErrorId::PeerUnavailable));
            }
            state.ring.push_bytes(&data, state.send_timeout).await?;
            state.blob.notify_dispatch();
            Ok(())
        };
        self.pending = Some(PendingOp {
            kind: PendingKind::Write(len),
            future: Box::pin(future),
        });
    }

    fn start_flush(&mut self) {
        let state = std::sync::Arc::clone(&self.state);
        let future = async move {
            let state = state.lock().await;
            if state.closed {
                return Ok(());
            }
            let deadline = tokio::time::Instant::now() + state.send_timeout;
            state.ring.wait_for_inflight_drain(deadline).await
        };
        self.pending = Some(PendingOp {
            kind: PendingKind::Flush,
            future: Box::pin(future),
        });
    }

    fn start_shutdown(&mut self) {
        let state = std::sync::Arc::clone(&self.state);
        let future = async move {
            let mut state = state.lock().await;
            if state.closed {
                return Ok(());
            }
            state.ring.seal(state.send_timeout).await?;
            state.blob.notify_dispatch();

            let deadline = tokio::time::Instant::now() + state.send_timeout;
            if let Err(err) = state.ring.wait_for_complete(deadline).await {
                state.cleanup().await;
                return Err(err);
            }

            state.cleanup().await;
            Ok(())
        };
        self.pending = Some(PendingOp {
            kind: PendingKind::Shutdown,
            future: Box::pin(future),
        });
    }
}

impl BlobSenderState {
    async fn cleanup(&mut self) {
        if !self.closed {
            self.blob.unregister_outbound_stream(self.stream_id).await;
            self.closed = true;
        }
    }
}

impl AsyncWrite for BlobSenderStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        loop {
            if let Some(pending) = self.pending.as_mut() {
                match pending.future.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(result) => {
                        let kind = pending.kind;
                        self.pending = None;
                        if let Err(err) = result {
                            return Poll::Ready(Err(std::io::Error::other(err.to_string())));
                        }
                        if let PendingKind::Write(len) = kind {
                            return Poll::Ready(Ok(len));
                        }
                        continue;
                    }
                }
            }

            if buf.is_empty() {
                return Poll::Ready(Ok(0));
            }
            let data = Bytes::copy_from_slice(buf);
            let len = data.len();
            self.start_write(data, len);
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        loop {
            if let Some(pending) = self.pending.as_mut() {
                match pending.future.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(result) => {
                        let kind = pending.kind;
                        self.pending = None;
                        if let Err(err) = result {
                            return Poll::Ready(Err(std::io::Error::other(err.to_string())));
                        }
                        match kind {
                            PendingKind::Flush | PendingKind::Shutdown => {
                                return Poll::Ready(Ok(()));
                            }
                            PendingKind::Write(_) => continue,
                        }
                    }
                }
            }
            self.start_flush();
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        loop {
            if let Some(pending) = self.pending.as_mut() {
                match pending.future.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(result) => {
                        let kind = pending.kind;
                        self.pending = None;
                        if let Err(err) = result {
                            return Poll::Ready(Err(std::io::Error::other(err.to_string())));
                        }
                        if matches!(kind, PendingKind::Shutdown) {
                            return Poll::Ready(Ok(()));
                        }
                        continue;
                    }
                }
            }
            self.start_shutdown();
        }
    }
}

impl Drop for BlobSenderStream {
    fn drop(&mut self) {
        let state = std::sync::Arc::clone(&self.state);
        let runtime_handle = self.runtime_handle.clone();
        runtime_handle.spawn(async move {
            let mut state = state.lock().await;
            if state.closed {
                return;
            }
            let err = AureliaError::new(ErrorId::PeerUnavailable);
            let _ = state.blob.send_stream_error(state.stream_id, err).await;
            state.cleanup().await;
        });
    }
}
