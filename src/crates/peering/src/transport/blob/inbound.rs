// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(crate) struct PendingBlobRequest {
    pub(crate) taberna_id: TabernaId,
    pub(crate) receiver: Arc<BlobReceiverState>,
}

pub(crate) struct BlobRecvStream {
    pub(crate) taberna_id: TabernaId,
    pub(crate) receiver: Arc<BlobReceiverState>,
    pub(crate) ring: Arc<crate::ring_buffer::InboundRingBuffer>,
    pub(crate) deadline: Instant,
}

pub(crate) enum BlobRecvChunkState {
    Missing,
    RecentlyCompleted,
    IdleTimedOut {
        taberna_id: TabernaId,
        receiver: Arc<BlobReceiverState>,
    },
    Active {
        taberna_id: TabernaId,
        receiver: Arc<BlobReceiverState>,
        ring: Arc<crate::ring_buffer::InboundRingBuffer>,
        notify: Arc<Notify>,
    },
}

pub(crate) struct BlobInboundState {
    pub(super) pending_requests: HashMap<PeerMessageId, PendingBlobRequest>,
    pub(super) recv_streams: HashMap<PeerMessageId, BlobRecvStream>,
    pub(super) completed_recv_streams: HashMap<PeerMessageId, Instant>,
}

impl BlobInboundState {
    pub(super) fn new() -> Self {
        Self {
            pending_requests: HashMap::new(),
            recv_streams: HashMap::new(),
            completed_recv_streams: HashMap::new(),
        }
    }
}
