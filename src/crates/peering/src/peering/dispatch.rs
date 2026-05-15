// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use bytes::Bytes;
use tokio::time::timeout;
use tracing::{debug, warn};

use crate::codec::MessageCodec;
use crate::delivery;
use crate::send::{SendOptions, SendOutcome};
use crate::transport::TransportBackend;
use aurelia_data::DomusAddr;
use aurelia_data::RouteResolver;
use aurelia_ids::{AureliaError, ErrorId, A3_MESSAGE_TYPE_BASE};
use aurelia_ids::{MessageType, TabernaId};

use super::RouteLocalRemote;

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
        let encoded = codec
            .encode_app(message)
            .map_err(|err| AureliaError::with_message(ErrorId::EncodeFailure, err.to_string()))?;
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
        let config = self.config.snapshot().await;
        validate_app_send(msg_type, payload.len(), config.max_payload_len)?;
        if let Some(inbox) = self.registry.resolve_local(taberna_id).await {
            debug!(taberna_id, msg_type, "local send");
            return match delivery::deliver_local(
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
            };
        }

        let resolve = self.resolver.resolve(taberna_id);
        let peer = match timeout(config.send_timeout, resolve).await {
            Ok(Ok(peer)) => peer,
            Ok(Err(err)) => {
                warn!(taberna_id, msg_type, error = %err, "remote resolve failed");
                return Err(err);
            }
            Err(_) => {
                warn!(taberna_id, msg_type, "remote resolve timeout");
                return Err(AureliaError::new(ErrorId::SendTimeout));
            }
        };
        debug!(taberna_id, msg_type, peer = %peer, "remote send");
        self.transport
            .send_remote(peer, taberna_id, msg_type, payload, options)
            .await
    }
}

pub(crate) fn validate_app_send(
    msg_type: MessageType,
    payload_len: usize,
    max_payload_len: usize,
) -> Result<(), AureliaError> {
    if msg_type < A3_MESSAGE_TYPE_BASE {
        return Err(AureliaError::with_message(
            ErrorId::ProtocolViolation,
            "application message type must be in the A3 range",
        ));
    }
    if payload_len > max_payload_len {
        return Err(AureliaError::with_message(
            ErrorId::ProtocolViolation,
            "application payload length exceeds max_payload_len",
        ));
    }
    if payload_len > u32::MAX as usize {
        return Err(AureliaError::with_message(
            ErrorId::ProtocolViolation,
            "application payload length exceeds wire header capacity",
        ));
    }
    Ok(())
}
