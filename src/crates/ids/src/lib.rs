// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

#![warn(missing_docs)]

//! Shared Aurelia identifiers and the single workspace error type.

use std::fmt;

/// Stable identifier for a [`crate::Taberna`](../../../aurelia/struct.Taberna.html).
pub type TabernaId = u64;
/// Application-defined message type discriminator on the wire.
pub type MessageType = u32;
/// Per-peer message identifier used for ack tracking; not part of the public API surface.
pub type PeerMessageId = u32;

/// First [`MessageType`] in the A3 application range.
pub const A3_MESSAGE_TYPE_BASE: MessageType = 0x0100_0000;
/// Last [`MessageType`] in the A1 transport-control priority range.
pub const A1_MESSAGE_TYPE_MAX: MessageType = 0x0000_FFFF;
/// First [`MessageType`] in the A2 Aurelia-service priority range.
pub const A2_MESSAGE_TYPE_BASE: MessageType = A1_MESSAGE_TYPE_MAX + 1;
/// Last [`MessageType`] in the A2 Aurelia-service priority range.
pub const A2_MESSAGE_TYPE_MAX: MessageType = A3_MESSAGE_TYPE_BASE - 1;
/// Largest offset accepted by [`a3_message_type`] and [`try_a3_message_type`].
pub const A3_MESSAGE_TYPE_MAX_OFFSET: MessageType = MessageType::MAX - A3_MESSAGE_TYPE_BASE;

/// Priority class for a [`MessageType`] on the primary callis.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MessagePriorityClass {
    /// A1 transport-control and transport-critical message type.
    A1,
    /// A2 Aurelia-service message type.
    A2,
    /// A3 application message type.
    A3,
}

/// Classifies a [`MessageType`] into the A1/A2/A3 outbound priority ranges.
pub const fn classify_message_priority(msg_type: MessageType) -> MessagePriorityClass {
    if msg_type <= A1_MESSAGE_TYPE_MAX {
        MessagePriorityClass::A1
    } else if msg_type <= A2_MESSAGE_TYPE_MAX {
        MessagePriorityClass::A2
    } else {
        MessagePriorityClass::A3
    }
}

/// Returns the A3 application [`MessageType`] for an offset from [`A3_MESSAGE_TYPE_BASE`].
///
/// This is the preferred helper for application and test message types because it prevents
/// accidental use of A1 transport-control IDs. The first application ID is
/// `a3_message_type(0) == 0x0100_0000`.
///
/// # Panics
///
/// Panics if `offset` is greater than [`A3_MESSAGE_TYPE_MAX_OFFSET`]. Use
/// [`try_a3_message_type`] when the offset comes from untrusted input.
pub const fn a3_message_type(offset: MessageType) -> MessageType {
    match try_a3_message_type(offset) {
        Some(msg_type) => msg_type,
        None => panic!("A3 message type offset exceeds the A3 range"),
    }
}

/// Returns the A3 application [`MessageType`] for `offset`, or `None` if it would exceed `u32`.
pub const fn try_a3_message_type(offset: MessageType) -> Option<MessageType> {
    A3_MESSAGE_TYPE_BASE.checked_add(offset)
}

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
/// Reserved [`MessageType`] with no valid frame in protocol v1.
pub const MSG_RESERVED_7: MessageType = 7;
/// Reserved [`MessageType`] for blob chunk frames.
pub const MSG_BLOB_TRANSFER_CHUNK: MessageType = 8;
/// Reserved [`MessageType`] for the blob transfer complete frame.
pub const MSG_BLOB_TRANSFER_COMPLETE: MessageType = 9;

#[cfg(test)]
mod tests;

macro_rules! define_log_ids {
    (
        $(#[$enum_meta:meta])*
        $vis:vis enum $name:ident {
            $(
                $(#[$var_meta:meta])*
                $variant:ident = $value:expr
            ),* $(,)?
        }
    ) => {
        $(#[$enum_meta])*
        #[repr(u32)]
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
        $vis enum $name {
            $(
                $(#[$var_meta])*
                $variant = $value,
            )*
        }

        impl $name {
            /// Every variant of this enum, in declaration order. Generated
            /// alongside the variant list so the registry cannot drift from
            /// the enum definition.
            pub const ALL: &'static [$name] = &[ $( $name::$variant, )* ];

            /// Numeric discriminant of this variant.
            pub fn as_u32(self) -> u32 {
                self as u32
            }
        }
    };
}

define_log_ids! {
    /// Identifier for a rate-limited log line. Exhaustive: every variant is
    /// listed in [`LogId::ALL`] so logging registries can be initialized without
    /// drifting from the enum definition.
    pub enum LogId {
        /// Triggered when the workspace-wide inbound handshake limit is reached.
        HandshakeTotalLimit = 1001,
        /// Triggered when the per-peer inbound handshake limit is reached.
        HandshakePerPeerLimit = 1002,
        /// Triggered when the per-peer concurrent callis limit is reached.
        CallisPerPeerLimit = 1003,
        /// Triggered when an outbound ready queue rejects admission because it is full.
        OutboundQueueOverrun = 1004,
        /// Triggered when an outbound ACK response is queued more than once for one peer message.
        DuplicateOutboundAck = 1005,
        /// Triggered when an outbound ERROR response is queued more than once for one peer message.
        DuplicateOutboundError = 1006,
    }
}

macro_rules! define_error_ids {
    (
        $(#[$enum_meta:meta])*
        $vis:vis enum $name:ident {
            $(
                $(#[$var_meta:meta])*
                $variant:ident = $value:expr
            ),* $(,)?
        }
    ) => {
        $(#[$enum_meta])*
        #[repr(u32)]
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        $vis enum $name {
            $(
                $(#[$var_meta])*
                $variant = $value,
            )*
        }

        impl $name {
            /// Every variant of this enum, in declaration order. Generated
            /// alongside the variant list so numeric conversion cannot drift
            /// from the enum definition.
            pub const ALL: &'static [$name] = &[ $( $name::$variant, )* ];

            /// Returns the numeric discriminant of this variant.
            pub fn as_u32(self) -> u32 {
                self as u32
            }

            /// Converts a numeric discriminant back into an error id, returning
            /// `None` for unknown values.
            #[deprecated(note = "Use ErrorId::try_from or u32::try_into instead.")]
            pub fn from_u32(value: u32) -> Option<Self> {
                Self::from_u32_inner(value)
            }

            fn from_u32_inner(value: u32) -> Option<Self> {
                match value {
                    $( $value => Some(Self::$variant), )*
                    _ => None,
                }
            }
        }

        impl TryFrom<u32> for $name {
            type Error = ();

            fn try_from(value: u32) -> Result<Self, Self::Error> {
                Self::from_u32_inner(value).ok_or(())
            }
        }
    };
}

/// Maximum length of an [`AureliaError`] message in bytes.
pub const ERROR_MESSAGE_MAX_LEN: usize = 1024;

define_error_ids! {
    /// Stable error identifier carried by every [`AureliaError`]. Each variant
    /// corresponds to a distinct failure mode in the Aurelia stack; semantics
    /// are stable across releases so applications can match on them safely.
    ///
    /// See the project documentation for the canonical mapping between
    /// `ErrorId` variants and the conditions that produce them.
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
        /// A reporting snapshot or reporting query could not be completed.
        SnapshotNotAvailable = 25,
        /// The destination taberna ingress has shut down.
        TabernaShutdown = 26,
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
