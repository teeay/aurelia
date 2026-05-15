// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(crate) struct BlobReservationState {
    pub(super) outbound: HashMap<PeerMessageId, u64>,
    pub(super) inbound: HashMap<PeerMessageId, u64>,
}

impl BlobReservationState {
    pub(super) fn new() -> Self {
        Self {
            outbound: HashMap::new(),
            inbound: HashMap::new(),
        }
    }
}
