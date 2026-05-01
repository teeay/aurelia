// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tracing::warn;

use crate::config::DomusConfigAccess;
use crate::send::{SendOptions, SendOutcome};
use crate::taberna::TabernaInbox;
use crate::transport::{blob_buffer_full_error, BlobBufferTracker};
use crate::{BlobReceiver, BlobSender};
use aurelia_ids::{AureliaError, ErrorId, MessageType, TabernaId};

struct LocalBlobSender {
    inner: tokio::io::DuplexStream,
    tracker: Arc<BlobBufferTracker>,
    bytes: u64,
}

impl LocalBlobSender {
    fn new(inner: tokio::io::DuplexStream, tracker: Arc<BlobBufferTracker>, bytes: u64) -> Self {
        Self {
            inner,
            tracker,
            bytes,
        }
    }
}

impl Drop for LocalBlobSender {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.tracker.release_outbound(self.bytes);
        }
    }
}

impl AsyncWrite for LocalBlobSender {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

struct LocalBlobReceiver {
    inner: tokio::io::DuplexStream,
    tracker: Arc<BlobBufferTracker>,
    bytes: u64,
}

impl LocalBlobReceiver {
    fn new(inner: tokio::io::DuplexStream, tracker: Arc<BlobBufferTracker>, bytes: u64) -> Self {
        Self {
            inner,
            tracker,
            bytes,
        }
    }
}

impl Drop for LocalBlobReceiver {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.tracker.release_inbound(self.bytes);
        }
    }
}

impl AsyncRead for LocalBlobReceiver {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

pub(crate) async fn deliver_to_taberna(
    inbox: Arc<dyn TabernaInbox>,
    msg_type: MessageType,
    payload: Bytes,
    blob_receiver: Option<BlobReceiver>,
) -> Result<(), AureliaError> {
    let accept_rx = inbox
        .enqueue(msg_type, payload, blob_receiver, None)
        .await?;
    match accept_rx.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(err),
        Err(_) => Err(AureliaError::new(ErrorId::RemoteTabernaRejected)),
    }
}

pub(crate) async fn deliver_local(
    config: &DomusConfigAccess,
    taberna_id: TabernaId,
    msg_type: MessageType,
    payload: Bytes,
    options: SendOptions,
    blob_buffers: Arc<BlobBufferTracker>,
    inbox: Arc<dyn TabernaInbox>,
) -> Result<SendOutcome, AureliaError> {
    if !options.blob {
        deliver_to_taberna(inbox, msg_type, payload, None).await?;
        return Ok(SendOutcome::MessageOnly);
    }

    let cfg = config.snapshot().await;
    if cfg.blob_chunk_size == 0 || cfg.blob_ack_window == 0 {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    let reservation_bytes = (cfg.blob_chunk_size as u64).saturating_mul(cfg.blob_ack_window as u64);
    if !blob_buffers.try_reserve_outbound(reservation_bytes, cfg.blob_outbound_buffer_bytes) {
        return Err(blob_buffer_full_error(
            "outbound",
            cfg.blob_outbound_buffer_bytes,
        ));
    }
    if !blob_buffers.try_reserve_inbound(reservation_bytes, cfg.blob_inbound_buffer_bytes) {
        blob_buffers.release_outbound(reservation_bytes);
        return Err(blob_buffer_full_error(
            "inbound",
            cfg.blob_inbound_buffer_bytes,
        ));
    }
    let capacity = reservation_bytes.min(usize::MAX as u64) as usize;
    let capacity = capacity.max(1);
    let (sender_stream, receiver_stream) = tokio::io::duplex(capacity);
    let sender = BlobSender::new(Box::new(LocalBlobSender::new(
        sender_stream,
        Arc::clone(&blob_buffers),
        reservation_bytes,
    )));
    let receiver = BlobReceiver::new(Box::new(LocalBlobReceiver::new(
        receiver_stream,
        Arc::clone(&blob_buffers),
        reservation_bytes,
    )));

    match deliver_to_taberna(inbox, msg_type, payload, Some(receiver)).await {
        Ok(()) => Ok(SendOutcome::Blob { sender }),
        Err(err) => {
            warn!(taberna_id, msg_type, error = %err, "local blob send failed");
            Err(err)
        }
    }
}
