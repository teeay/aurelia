// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

use aurelia_ids::AureliaError;

pub(crate) struct AuthenticatedStream<S, A> {
    pub(crate) stream: S,
    pub(crate) peer_addr: A,
}

#[async_trait]
pub trait TransportBackend: Send + Sync + 'static {
    type Addr: Clone + Eq + std::hash::Hash + std::fmt::Display + Send + Sync + 'static;
    type Listener: Send + 'static;
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;

    async fn bind(&self, local: &Self::Addr) -> Result<Self::Listener, AureliaError>;
    async fn accept(
        &self,
        listener: &mut Self::Listener,
    ) -> Result<AuthenticatedStream<Self::Stream, Self::Addr>, AureliaError>;
    async fn dial(
        &self,
        peer: &Self::Addr,
    ) -> Result<AuthenticatedStream<Self::Stream, Self::Addr>, AureliaError>;
}
