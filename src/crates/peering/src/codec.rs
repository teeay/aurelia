// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use aurelia_ids::{AureliaError, MessageType};
use bytes::Bytes;

/// Wire-ready application message: a type discriminator plus its serialised payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedMessage {
    /// Application-defined message type discriminator.
    pub msg_type: MessageType,
    /// Serialised payload bytes.
    pub payload: Bytes,
}

impl EncodedMessage {
    /// Constructs an [`EncodedMessage`] from a type tag and a payload.
    pub fn new(msg_type: MessageType, payload: Bytes) -> Self {
        Self { msg_type, payload }
    }
}

/// Application-supplied codec used by Aurelia to translate between typed
/// application messages and the wire form transferred between peers.
pub trait MessageCodec: Send + Sync + 'static {
    /// The application-defined message enum or struct type.
    type AppMessage;

    /// Serialises an application message into wire form.
    fn encode_app(&self, msg: &Self::AppMessage) -> Result<EncodedMessage, AureliaError>;
    /// Deserialises a received message-type / payload pair into an application message.
    fn decode_app(
        &self,
        msg_type: MessageType,
        payload: &[u8],
    ) -> Result<Self::AppMessage, AureliaError>;
}
