// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use bytes::Bytes;
use tracing::warn;

use crate::address::DomusAddr;
use crate::codec::MessageCodec;
use crate::delivery;
use crate::routing::RouteResolver;
use crate::send::{SendOptions, SendOutcome};
use crate::transport::TransportBackend;
use aurelia_ids::{AureliaError, ErrorId};
use aurelia_ids::{MessageType, TabernaId};

use super::{resolve_dispatch_target, DispatchLogs, DispatchTarget, RouteLocalRemote};

impl<RR, B> RouteLocalRemote<RR, B>
where
    RR: RouteResolver,
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    pub async fn send<Codec: MessageCodec>(
        &self,
        codec: &Codec,
        taberna_id: TabernaId,
        message: &Codec::AppMessage,
        options: SendOptions,
    ) -> Result<SendOutcome, AureliaError> {
        let encoded = codec.encode_app(message)?;
        self.send_encoded(taberna_id, encoded.msg_type, encoded.payload, options)
            .await
    }

    pub(crate) async fn send_encoded(
        &self,
        taberna_id: TabernaId,
        msg_type: MessageType,
        payload: Bytes,
        options: SendOptions,
    ) -> Result<SendOutcome, AureliaError> {
        let target = resolve_dispatch_target(
            self.registry.as_ref(),
            self.resolver.as_ref(),
            &self.config,
            taberna_id,
            msg_type,
            DispatchLogs {
                local: "local send",
                resolve_failed: "remote resolve failed",
                resolve_timeout: "remote resolve timeout",
                remote: "remote send",
            },
        )
        .await?;
        match target {
            DispatchTarget::Local(inbox) => {
                match delivery::deliver_local(
                    &self.config,
                    taberna_id,
                    msg_type,
                    payload,
                    options,
                    Arc::clone(&self.blob_buffers),
                    inbox,
                )
                .await
                {
                    Ok(outcome) => Ok(outcome),
                    Err(err) => {
                        if err.kind == ErrorId::TabernaBusy {
                            warn!(taberna_id, msg_type, "local send taberna busy");
                        } else {
                            warn!(taberna_id, msg_type, error = %err, "local send failed");
                        }
                        Err(err)
                    }
                }
            }
            DispatchTarget::Remote(peer) => {
                self.transport
                    .send_remote(peer, taberna_id, msg_type, payload, options)
                    .await
            }
        }
    }
}
