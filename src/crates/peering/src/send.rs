// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use crate::BlobSender;

/// Per-send flags controlling whether a callis carries an attached blob.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SendOptions {
    /// If `true`, the send opens a blob stream alongside the application message.
    pub blob: bool,
}

impl SendOptions {
    /// Send a message without an attached blob.
    pub const MESSAGE_ONLY: Self = Self { blob: false };
    /// Send a message with an outbound blob stream attached.
    pub const BLOB: Self = Self { blob: true };
}

/// Result of a successful send call.
#[derive(Debug)]
pub enum SendOutcome {
    /// The message was sent; no blob stream is attached.
    MessageOnly,
    /// The message was sent and a [`BlobSender`] was opened for streaming bytes.
    Blob {
        /// Outbound blob stream opened for this callis.
        sender: BlobSender,
    },
}
