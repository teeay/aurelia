// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Outbound blob stream attached to a callis. Implements [`AsyncWrite`].
pub struct BlobSender {
    inner: Box<dyn AsyncWrite + Send + Unpin>,
}

impl BlobSender {
    pub(crate) fn new(inner: Box<dyn AsyncWrite + Send + Unpin>) -> Self {
        Self { inner }
    }
}

impl std::fmt::Debug for BlobSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobSender").finish()
    }
}

impl AsyncWrite for BlobSender {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut *self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut *self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut *self.inner).poll_shutdown(cx)
    }
}

/// Inbound blob stream attached to a callis. Implements [`AsyncRead`].
pub struct BlobReceiver {
    inner: Box<dyn AsyncRead + Send + Unpin>,
}

impl BlobReceiver {
    pub(crate) fn new(inner: Box<dyn AsyncRead + Send + Unpin>) -> Self {
        Self { inner }
    }
}

impl std::fmt::Debug for BlobReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobReceiver").finish()
    }
}

impl AsyncRead for BlobReceiver {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut *self.inner).poll_read(cx, buf)
    }
}
