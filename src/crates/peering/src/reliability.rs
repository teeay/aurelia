// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use aurelia_ids::{MessageType, PeerMessageId, TabernaId};
use bytes::Bytes;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InflightMessage {
    pub peer_msg_id: PeerMessageId,
    pub src_taberna: TabernaId,
    pub dst_taberna: TabernaId,
    pub msg_type: MessageType,
    pub flags: u16,
    pub payload: Bytes,
}
