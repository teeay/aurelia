// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use actix::prelude::{Recipient, SendError};

use crate::codec::MessageCodec;
use crate::taberna::Taberna;
use crate::BlobReceiver;
use aurelia_ids::ErrorId;

/// Typed Actix delivery envelope for a taberna request.
///
/// The envelope preserves both the decoded application message and any blob receiver carried by
/// the inbound request. Actix mailbox admission is the Aurelia acceptance boundary, so handlers
/// return `()` and application-level failures must travel in application messages.
pub struct ActixTabernaDelivery<M>
where
    M: Send + 'static,
{
    /// Decoded application message produced by the configured [`MessageCodec`].
    pub message: M,
    /// Optional blob receiver attached to the inbound request.
    pub blob_receiver: Option<BlobReceiver>,
}

impl<M> actix::Message for ActixTabernaDelivery<M>
where
    M: Send + 'static,
{
    type Result = ();
}

/// Registration handle for an Actix-backed taberna bridge.
///
/// Dropping the handle aborts the Actix-side bridge task, which drops the underlying
/// [`Taberna`] and schedules deregistration from its parent domus.
pub struct ActixTaberna {
    bridge: tokio::task::JoinHandle<()>,
}

impl ActixTaberna {
    pub(crate) fn new<Codec>(
        taberna: Taberna<Codec>,
        recipient: Recipient<ActixTabernaDelivery<Codec::AppMessage>>,
    ) -> Self
    where
        Codec: MessageCodec + 'static,
        Codec::AppMessage: Send + Sync + 'static,
    {
        let bridge = actix::spawn(run_bridge(taberna, recipient));
        Self { bridge }
    }
}

impl Drop for ActixTaberna {
    fn drop(&mut self) {
        self.bridge.abort();
    }
}

async fn run_bridge<Codec>(
    taberna: Taberna<Codec>,
    recipient: Recipient<ActixTabernaDelivery<Codec::AppMessage>>,
) where
    Codec: MessageCodec + 'static,
    Codec::AppMessage: Send + Sync + 'static,
{
    loop {
        let request = match taberna.next(None).await {
            Ok(request) => request,
            Err(err) if err.kind == ErrorId::ReceiveTimeout => continue,
            Err(_) => break,
        };

        let parts = request.into_parts();
        let delivery = ActixTabernaDelivery {
            message: parts.message,
            blob_receiver: parts.blob_receiver,
        };

        match recipient.try_send(delivery) {
            Ok(()) => {
                parts.completion.accept();
            }
            Err(SendError::Full(_delivery)) => {
                parts.completion.busy();
            }
            Err(SendError::Closed(_delivery)) => {
                parts.completion.taberna_shutdown();
                break;
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/actix_adapter.rs"]
mod tests;
