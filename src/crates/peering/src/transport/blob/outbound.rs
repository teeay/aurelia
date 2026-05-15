// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::collections::VecDeque;

const BLOB_ACK_CAPACITY_MULTIPLIER: usize = 16;
const BLOB_ERROR_CAPACITY_MULTIPLIER: usize = 2;
const BLOB_COMPLETE_CAPACITY_MULTIPLIER: usize = 2;

pub(crate) struct BlobWriteSlot {
    pub(super) stream_id: Option<PeerMessageId>,
    pub(super) write: BlobWriteLease,
    pub(super) state: BlobWriteSlotState,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum BlobResponseLane {
    Ack,
    Error,
    Complete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BlobResponseCapacities {
    pub(super) ack: usize,
    pub(super) error: usize,
    pub(super) complete: usize,
}

impl BlobResponseCapacities {
    pub(super) fn from_send_queue_size(send_queue_size: usize) -> Self {
        let send_queue_size = send_queue_size.max(1);
        Self {
            ack: send_queue_size.saturating_mul(BLOB_ACK_CAPACITY_MULTIPLIER),
            error: send_queue_size.saturating_mul(BLOB_ERROR_CAPACITY_MULTIPLIER),
            complete: send_queue_size.saturating_mul(BLOB_COMPLETE_CAPACITY_MULTIPLIER),
        }
    }

    pub(super) fn capacity(self, lane: BlobResponseLane) -> usize {
        match lane {
            BlobResponseLane::Ack => self.ack,
            BlobResponseLane::Error => self.error,
            BlobResponseLane::Complete => self.complete,
        }
    }
}

#[derive(Clone)]
pub(crate) enum BlobWriteLease {
    Ack {
        peer_msg_id: PeerMessageId,
    },
    Error {
        peer_msg_id: PeerMessageId,
        payload: Bytes,
    },
    Chunk {
        stream_id: PeerMessageId,
        peer_msg_id: PeerMessageId,
        chunk: crate::ring_buffer::ChunkWriteLease,
    },
    Finish {
        stream_id: PeerMessageId,
        peer_msg_id: PeerMessageId,
        payload: Bytes,
    },
}

impl BlobWriteLease {
    pub(crate) fn lane(&self) -> Option<BlobResponseLane> {
        match self {
            Self::Ack { .. } => Some(BlobResponseLane::Ack),
            Self::Error { .. } => Some(BlobResponseLane::Error),
            Self::Finish { .. } => Some(BlobResponseLane::Complete),
            Self::Chunk { .. } => None,
        }
    }

    pub(crate) fn stream_id(&self) -> Option<PeerMessageId> {
        match self {
            Self::Ack { .. } | Self::Error { .. } => None,
            Self::Chunk { stream_id, .. } | Self::Finish { stream_id, .. } => Some(*stream_id),
        }
    }

    pub(crate) fn peer_msg_id(&self) -> PeerMessageId {
        match self {
            Self::Ack { peer_msg_id } | Self::Error { peer_msg_id, .. } => *peer_msg_id,
            Self::Chunk { peer_msg_id, .. } | Self::Finish { peer_msg_id, .. } => *peer_msg_id,
        }
    }

    pub(crate) fn expects_ack(&self) -> bool {
        match self {
            Self::Chunk { .. } => true,
            Self::Ack { .. } | Self::Error { .. } | Self::Finish { .. } => false,
        }
    }

    pub(crate) fn is_chunk(&self) -> bool {
        matches!(self, Self::Chunk { .. })
    }
}

pub(crate) struct InflightBlobFrame {
    pub(super) stream_id: PeerMessageId,
    pub(super) callis_id: CallisId,
    pub(super) is_chunk: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BlobWriteSlotState {
    Ready,
    Writing { callis_id: CallisId },
    InFlight { callis_id: CallisId },
    ReplayReady { previous_callis_id: CallisId },
}

pub(crate) struct BlobOutboundState {
    pub(super) streams: HashMap<PeerMessageId, Arc<crate::ring_buffer::OutboundRingBuffer>>,
    pub(super) inflight: HashMap<PeerMessageId, InflightBlobFrame>,
    pub(super) write_slots: HashMap<PeerMessageId, BlobWriteSlot>,
    pub(super) stream_settings: HashMap<PeerMessageId, BlobCallisSettings>,
    pub(super) ack_ready: VecDeque<PeerMessageId>,
    pub(super) error_ready: VecDeque<PeerMessageId>,
    pub(super) complete_ready: VecDeque<PeerMessageId>,
    pub(super) response_capacities: BlobResponseCapacities,
    pub(super) round_robin_cursor: Option<PeerMessageId>,
}

impl BlobOutboundState {
    pub(super) fn new(send_queue_size: usize) -> Self {
        let response_capacities = BlobResponseCapacities::from_send_queue_size(send_queue_size);
        Self {
            streams: HashMap::new(),
            inflight: HashMap::new(),
            write_slots: HashMap::new(),
            stream_settings: HashMap::new(),
            ack_ready: VecDeque::with_capacity(response_capacities.ack),
            error_ready: VecDeque::with_capacity(response_capacities.error),
            complete_ready: VecDeque::with_capacity(response_capacities.complete),
            response_capacities,
            round_robin_cursor: None,
        }
    }

    pub(super) fn response_ready_count(&self) -> usize {
        self.ack_ready.len() + self.error_ready.len() + self.complete_ready.len()
    }

    pub(super) fn response_lane_len(&self, lane: BlobResponseLane) -> usize {
        match lane {
            BlobResponseLane::Ack => self.ack_ready.len(),
            BlobResponseLane::Error => self.error_ready.len(),
            BlobResponseLane::Complete => self.complete_ready.len(),
        }
    }

    pub(super) fn response_lane_mut(
        &mut self,
        lane: BlobResponseLane,
    ) -> &mut VecDeque<PeerMessageId> {
        match lane {
            BlobResponseLane::Ack => &mut self.ack_ready,
            BlobResponseLane::Error => &mut self.error_ready,
            BlobResponseLane::Complete => &mut self.complete_ready,
        }
    }

    pub(super) fn prune_response_ready(&mut self) {
        let valid_writes: HashSet<PeerMessageId> = self.write_slots.keys().copied().collect();
        self.ack_ready
            .retain(|peer_msg_id| valid_writes.contains(peer_msg_id));
        self.error_ready
            .retain(|peer_msg_id| valid_writes.contains(peer_msg_id));
        self.complete_ready
            .retain(|peer_msg_id| valid_writes.contains(peer_msg_id));
    }
}
