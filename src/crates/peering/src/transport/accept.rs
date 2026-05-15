// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::config::{DomusConfig, DomusConfigAccess};
use aurelia_data::DomusAddr;
use aurelia_ids::{AureliaError, ErrorId};

use super::backend::AuthenticatedStream;
use super::limits::PreAuthGate;

pub(super) struct InboundAuthContext<S> {
    runtime_handle: tokio::runtime::Handle,
    preauth_gate: Arc<PreAuthGate>,
    config: DomusConfigAccess,
    accepted_tx: mpsc::Sender<Result<AuthenticatedStream<S, DomusAddr>, AureliaError>>,
    timeout_message: &'static str,
}

impl<S> InboundAuthContext<S>
where
    S: Send + 'static,
{
    pub(super) fn new(
        runtime_handle: &tokio::runtime::Handle,
        preauth_gate: Arc<PreAuthGate>,
        config: DomusConfigAccess,
        accepted_tx: mpsc::Sender<Result<AuthenticatedStream<S, DomusAddr>, AureliaError>>,
        timeout_message: &'static str,
    ) -> Self {
        Self {
            runtime_handle: runtime_handle.clone(),
            preauth_gate,
            config,
            accepted_tx,
            timeout_message,
        }
    }

    pub(super) fn spawn<R, F, Fut, T>(self, mut raw_stream: R, timeout_for: T, authenticate: F)
    where
        R: AsyncWrite + Unpin + Send + 'static,
        F: FnOnce(R) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Option<AuthenticatedStream<S, DomusAddr>>, AureliaError>>
            + Send
            + 'static,
        T: FnOnce(&DomusConfig) -> Duration + Send + 'static,
    {
        self.runtime_handle.spawn(async move {
            let permit = match self.preauth_gate.try_acquire(&self.config).await {
                Some(permit) => permit,
                None => {
                    let _ = raw_stream.shutdown().await;
                    return;
                }
            };

            let snapshot = self.config.snapshot().await;
            let handshake_timeout = timeout_for(&snapshot);
            let result = match timeout(handshake_timeout, authenticate(raw_stream)).await {
                Ok(result) => result,
                Err(_) => Err(AureliaError::with_message(
                    ErrorId::PeerUnavailable,
                    self.timeout_message,
                )),
            };
            drop(permit);

            match result {
                Ok(Some(authenticated)) => {
                    let _ = self.accepted_tx.send(Ok(authenticated)).await;
                }
                Ok(None) => {}
                Err(err) => {
                    let _ = self.accepted_tx.send(Err(err)).await;
                }
            }
        });
    }
}
