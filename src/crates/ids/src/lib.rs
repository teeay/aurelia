// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::fmt;

/// Stable identifier for a [`crate::Taberna`](../../../aurelia/struct.Taberna.html).
pub type TabernaId = u64;
/// Application-defined message type discriminator on the wire.
pub type MessageType = u32;
/// Per-peer message identifier used for ack tracking; not part of the public API surface.
pub type PeerMessageId = u32;

/// Reserved [`MessageType`] for the protocol-level hello frame.
pub const MSG_HELLO: MessageType = 1;
/// Reserved [`MessageType`] for the hello-response frame.
pub const MSG_HELLO_RESPONSE: MessageType = 2;
/// Reserved [`MessageType`] for keepalive frames.
pub const MSG_KEEPALIVE: MessageType = 3;
/// Reserved [`MessageType`] for acknowledgment frames.
pub const MSG_ACK: MessageType = 4;
/// Reserved [`MessageType`] for connection-close frames.
pub const MSG_CLOSE: MessageType = 5;
/// Reserved [`MessageType`] for error frames.
pub const MSG_ERROR: MessageType = 6;
/// Reserved [`MessageType`] for the blob transfer start frame.
pub const MSG_BLOB_TRANSFER_START: MessageType = 7;
/// Reserved [`MessageType`] for blob chunk frames.
pub const MSG_BLOB_TRANSFER_CHUNK: MessageType = 8;
/// Reserved [`MessageType`] for the blob transfer complete frame.
pub const MSG_BLOB_TRANSFER_COMPLETE: MessageType = 9;

/// Identifier for a rate-limited log line.
pub type LogId = u32;

/// Internal log identifiers used by Aurelia's rate-limited diagnostic logging.
pub mod log_ids {
    use super::LogId;

    /// Triggered when the workspace-wide inbound handshake limit is reached.
    pub const HANDSHAKE_TOTAL_LIMIT: LogId = 1001;
    /// Triggered when the per-peer inbound handshake limit is reached.
    pub const HANDSHAKE_PER_PEER_LIMIT: LogId = 1002;
    /// Triggered when the per-peer concurrent callis limit is reached.
    pub const CALLIS_PER_PEER_LIMIT: LogId = 1003;

    /// All log identifiers subject to rate limiting.
    pub const LIMITED_LOG_IDS: &[LogId] = &[
        HANDSHAKE_TOTAL_LIMIT,
        HANDSHAKE_PER_PEER_LIMIT,
        CALLIS_PER_PEER_LIMIT,
    ];
}

/// Maximum length of an [`AureliaError`] message in bytes.
pub const ERROR_MESSAGE_MAX_LEN: usize = 1024;

/// Stable error identifier carried by every [`AureliaError`]. Each variant
/// corresponds to a distinct failure mode in the Aurelia stack; semantics
/// are stable across releases so applications can match on them safely.
///
/// See the project documentation for the canonical mapping between
/// `ErrorId` variants and the conditions that produce them.
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorId {
    /// The targeted [`TabernaId`] is not registered locally.
    UnknownTaberna = 1,
    /// The local outbound queue is full and back-pressure is in effect.
    LocalQueueFull = 2,
    /// The remote peer cannot be reached.
    PeerUnavailable = 3,
    /// The remote taberna refused to accept the message.
    RemoteTabernaRejected = 4,
    /// The underlying transport connection was lost.
    ConnectionLost = 5,
    /// The remote peer restarted; pending state on the previous instance is gone.
    PeerRestarted = 6,
    /// The peer violated the wire protocol.
    ProtocolViolation = 7,
    /// The peer reported an unsupported protocol version.
    UnsupportedVersion = 8,
    /// Encoding an outbound message failed.
    EncodeFailure = 9,
    /// Decoding an inbound message failed.
    DecodeFailure = 10,
    /// The remote taberna's accept queue is currently full.
    TabernaBusy = 11,
    /// The send did not complete within the configured timeout.
    SendTimeout = 12,
    /// A blob callis was offered without a matching primary callis.
    BlobCallisWithoutPrimary = 13,
    /// The blob acknowledgment window was exceeded.
    BlobAckWindowExceeded = 14,
    /// A blob stream identifier could not be resolved.
    BlobStreamNotFound = 15,
    /// A blob chunk arrived out of order.
    BlobStreamOutOfOrder = 16,
    /// A blob stream timed out while idle.
    BlobStreamIdleTimeout = 17,
    /// A required blob chunk is missing.
    BlobStreamMissingChunk = 18,
    /// The blob buffer is full.
    BlobBufferFull = 19,
    /// A peer's reported address does not match the dialled address.
    AddressMismatch = 20,
    /// A taberna with this id is already registered on the local domus.
    TabernaAlreadyRegistered = 21,
    /// The supplied [`crate::DomusConfig`](../../../aurelia/struct.DomusConfig.html) is invalid.
    InvalidConfig = 22,
    /// The local domus has been shut down.
    DomusClosed = 23,
    /// A receive operation timed out before a message arrived.
    ReceiveTimeout = 24,
}

impl ErrorId {
    /// Returns the numeric discriminant of this variant.
    pub fn as_u32(self) -> u32 {
        self as u32
    }

    /// Converts a numeric discriminant back into an [`ErrorId`], returning
    /// `None` for unknown values.
    #[deprecated(note = "Use ErrorId::try_from or u32::try_into instead.")]
    pub fn from_u32(value: u32) -> Option<Self> {
        ErrorId::from_u32_inner(value)
    }

    fn from_u32_inner(value: u32) -> Option<Self> {
        match value {
            1 => Some(ErrorId::UnknownTaberna),
            2 => Some(ErrorId::LocalQueueFull),
            3 => Some(ErrorId::PeerUnavailable),
            4 => Some(ErrorId::RemoteTabernaRejected),
            5 => Some(ErrorId::ConnectionLost),
            6 => Some(ErrorId::PeerRestarted),
            7 => Some(ErrorId::ProtocolViolation),
            8 => Some(ErrorId::UnsupportedVersion),
            9 => Some(ErrorId::EncodeFailure),
            10 => Some(ErrorId::DecodeFailure),
            11 => Some(ErrorId::TabernaBusy),
            12 => Some(ErrorId::SendTimeout),
            13 => Some(ErrorId::BlobCallisWithoutPrimary),
            14 => Some(ErrorId::BlobAckWindowExceeded),
            15 => Some(ErrorId::BlobStreamNotFound),
            16 => Some(ErrorId::BlobStreamOutOfOrder),
            17 => Some(ErrorId::BlobStreamIdleTimeout),
            18 => Some(ErrorId::BlobStreamMissingChunk),
            19 => Some(ErrorId::BlobBufferFull),
            20 => Some(ErrorId::AddressMismatch),
            21 => Some(ErrorId::TabernaAlreadyRegistered),
            22 => Some(ErrorId::InvalidConfig),
            23 => Some(ErrorId::DomusClosed),
            24 => Some(ErrorId::ReceiveTimeout),
            _ => None,
        }
    }
}

impl TryFrom<u32> for ErrorId {
    type Error = ();

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        ErrorId::from_u32_inner(value).ok_or(())
    }
}

/// The single error type used across Aurelia. Carries a stable
/// [`ErrorId`] discriminant and an optional human-readable message; the
/// `kind` is the field applications match on.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AureliaError {
    /// Stable error discriminant; the field applications should match on.
    pub kind: ErrorId,
    /// Optional descriptive message; truncated at 1024 bytes on a UTF-8
    /// character boundary.
    pub message: Option<String>,
}

impl AureliaError {
    /// Creates an error with the given [`ErrorId`] and no message.
    pub fn new(kind: ErrorId) -> Self {
        Self {
            kind,
            message: None,
        }
    }

    /// Creates an error with the given [`ErrorId`] and a message; the
    /// message is truncated to 1024 bytes on a UTF-8 character boundary.
    pub fn with_message(kind: ErrorId, message: impl Into<String>) -> Self {
        let mut message = message.into();
        if message.len() > ERROR_MESSAGE_MAX_LEN {
            let mut idx = ERROR_MESSAGE_MAX_LEN;
            while idx > 0 && !message.is_char_boundary(idx) {
                idx -= 1;
            }
            message.truncate(idx);
        }
        Self {
            kind,
            message: Some(message),
        }
    }
}

impl fmt::Display for AureliaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.message {
            Some(message) => write!(f, "{:?}: {}", self.kind, message),
            None => write!(f, "{:?}", self.kind),
        }
    }
}

impl std::error::Error for AureliaError {}
