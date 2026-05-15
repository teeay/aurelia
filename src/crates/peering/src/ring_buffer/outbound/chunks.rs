// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::ChunkWriteLease;
use aurelia_ids::PeerMessageId;
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;

#[derive(Debug)]
pub(super) struct OutboundChunks {
    chunk_size: usize,
    window_size: usize,
    next_chunk_id: u64,
    write_cursor: usize,
    write_buf: BytesMut,
    pending_full: Option<Bytes>,
    slots: Vec<OutboundSlot>,
    chunk_index: HashMap<u64, usize>,
    ack_map: HashMap<PeerMessageId, u64>,
    inflight: usize,
    created_any: bool,
}

#[derive(Debug)]
enum OutboundSlot {
    Empty,
    Ready {
        chunk_id: u64,
        data: Bytes,
        is_last: bool,
        seq: u64,
    },
    Writing {
        chunk_id: u64,
        data: Bytes,
        peer_msg_id: PeerMessageId,
        callis_id: u64,
        is_last: bool,
        seq: u64,
    },
    InFlight {
        chunk_id: u64,
        data: Bytes,
        peer_msg_id: PeerMessageId,
        callis_id: u64,
        is_last: bool,
        seq: u64,
    },
    ReplayReady {
        chunk_id: u64,
        data: Bytes,
        #[allow(dead_code)]
        previous_callis_id: u64,
        is_last: bool,
        seq: u64,
    },
}

pub(super) enum FinalizeDecision {
    WaitForCapacity,
    Finalized { notify_waiters: bool },
}

pub(super) enum ChunkCompletion {
    Ack,
    Error,
}

impl OutboundChunks {
    pub(super) fn new(chunk_size: usize, window_size: usize) -> Self {
        Self {
            chunk_size,
            window_size,
            next_chunk_id: 0,
            write_cursor: 0,
            write_buf: BytesMut::new(),
            pending_full: None,
            slots: (0..window_size).map(|_| OutboundSlot::Empty).collect(),
            chunk_index: HashMap::new(),
            ack_map: HashMap::new(),
            inflight: 0,
            created_any: false,
        }
    }

    #[cfg(test)]
    pub(super) fn push_bytes(&mut self, data: &[u8], offset: &mut usize) -> (bool, bool, bool) {
        let mut wait_for_capacity = false;
        let mut notify_waiters = false;
        if *offset < data.len() {
            let remaining = data.len() - *offset;
            let space = self.chunk_size.saturating_sub(self.write_buf.len());
            let take = remaining.min(space);
            if take > 0 {
                self.write_buf
                    .extend_from_slice(&data[*offset..*offset + take]);
                *offset += take;
            }
        }

        while self.write_buf.len() >= self.chunk_size {
            if !self.has_window_capacity_for(1) {
                wait_for_capacity = true;
                break;
            }
            let chunk = self.write_buf.split_to(self.chunk_size).freeze();
            self.created_any = true;
            if let Some(pending) = self.pending_full.take() {
                self.enqueue_chunk(pending, false);
                notify_waiters = true;
            }
            self.pending_full = Some(chunk);
        }
        if self.pending_full.is_some() && !self.write_buf.is_empty() {
            if let Some(pending) = self.pending_full.take() {
                self.enqueue_chunk(pending, false);
                notify_waiters = true;
            }
        }
        let has_full_chunk = self.write_buf.len() >= self.chunk_size;
        (wait_for_capacity, has_full_chunk, notify_waiters)
    }

    pub(super) fn push_available(&mut self, data: &[u8]) -> (usize, bool) {
        let mut accepted = 0;
        let mut notify_waiters = false;

        if accepted < data.len() {
            let remaining = data.len() - accepted;
            let space = self.chunk_size.saturating_sub(self.write_buf.len());
            let take = remaining.min(space);
            if take > 0 {
                self.write_buf
                    .extend_from_slice(&data[accepted..accepted + take]);
                accepted += take;
            }
        }

        while self.write_buf.len() >= self.chunk_size {
            if !self.has_window_capacity_for(1) {
                break;
            }
            let chunk = self.write_buf.split_to(self.chunk_size).freeze();
            self.created_any = true;
            if let Some(pending) = self.pending_full.take() {
                self.enqueue_chunk(pending, false);
                notify_waiters = true;
            }
            self.pending_full = Some(chunk);

            if accepted >= data.len() {
                break;
            }
            let remaining = data.len() - accepted;
            let space = self.chunk_size.saturating_sub(self.write_buf.len());
            let take = remaining.min(space);
            if take > 0 {
                self.write_buf
                    .extend_from_slice(&data[accepted..accepted + take]);
                accepted += take;
            }
        }

        if self.pending_full.is_some() && !self.write_buf.is_empty() {
            if let Some(pending) = self.pending_full.take() {
                self.enqueue_chunk(pending, false);
                notify_waiters = true;
            }
        }

        (accepted, notify_waiters)
    }

    pub(super) fn can_accept_bytes(&self) -> bool {
        self.write_buf.len() < self.chunk_size || self.has_window_capacity_for(1)
    }

    pub(super) fn finalize(&mut self) -> FinalizeDecision {
        let needs_extra = match (self.pending_full.is_some(), self.write_buf.is_empty()) {
            (true, true) => 0,
            (true, false) => 1,
            (false, _) => 1,
        };
        if !self.has_window_capacity_for(needs_extra) {
            return FinalizeDecision::WaitForCapacity;
        }

        let notify_waiters = if let Some(pending) = self.pending_full.take() {
            if self.write_buf.is_empty() {
                self.enqueue_chunk(pending, true);
            } else {
                self.enqueue_chunk(pending, false);
                let len = self.write_buf.len();
                let chunk = self.write_buf.split_to(len).freeze();
                self.enqueue_chunk(chunk, true);
            }
            true
        } else if self.write_buf.is_empty() && self.created_any {
            true
        } else {
            let chunk = if self.write_buf.is_empty() {
                Bytes::new()
            } else {
                let len = self.write_buf.len();
                self.write_buf.split_to(len).freeze()
            };
            self.created_any = self.created_any || !chunk.is_empty();
            self.enqueue_chunk(chunk, true);
            true
        };

        FinalizeDecision::Finalized { notify_waiters }
    }

    pub(super) fn lease_next_chunk_for_write(
        &mut self,
        callis_id: u64,
        peer_msg_id: PeerMessageId,
    ) -> Option<ChunkWriteLease> {
        if let Some(lease) = self.lease_matching(true, callis_id, peer_msg_id) {
            return Some(lease);
        }
        self.lease_matching(false, callis_id, peer_msg_id)
    }

    pub(super) fn mark_inflight(&mut self, lease: &ChunkWriteLease) -> bool {
        let Some(index) = self.chunk_index.get(&lease.chunk_id).copied() else {
            return false;
        };
        let Some(slot) = self.slots.get_mut(index) else {
            return false;
        };
        let OutboundSlot::Writing {
            chunk_id,
            data,
            peer_msg_id,
            callis_id,
            is_last,
            seq,
        } = slot
        else {
            return false;
        };
        if *chunk_id != lease.chunk_id
            || *peer_msg_id != lease.peer_msg_id
            || *callis_id != lease.callis_id
            || *seq != lease.slot_seq
            || *is_last != lease.is_last
        {
            return false;
        }

        let data = data.clone();
        let chunk_id = *chunk_id;
        let is_last = *is_last;
        let seq = *seq;
        *slot = OutboundSlot::InFlight {
            chunk_id,
            data,
            peer_msg_id: lease.peer_msg_id,
            callis_id: lease.callis_id,
            is_last,
            seq,
        };
        self.ack_map.insert(lease.peer_msg_id, lease.chunk_id);
        self.inflight = self.inflight.saturating_add(1);
        true
    }

    pub(super) fn mark_callis_replay_ready(&mut self, callis_id: u64) -> bool {
        let mut changed = false;
        for slot in &mut self.slots {
            match slot {
                OutboundSlot::Writing {
                    chunk_id,
                    data,
                    callis_id: slot_callis_id,
                    is_last,
                    seq,
                    ..
                } if *slot_callis_id == callis_id => {
                    let data = data.clone();
                    let chunk_id = *chunk_id;
                    let is_last = *is_last;
                    let seq = *seq;
                    *slot = OutboundSlot::ReplayReady {
                        chunk_id,
                        data,
                        previous_callis_id: callis_id,
                        is_last,
                        seq,
                    };
                    changed = true;
                }
                OutboundSlot::InFlight {
                    chunk_id,
                    data,
                    peer_msg_id,
                    callis_id: slot_callis_id,
                    is_last,
                    seq,
                } if *slot_callis_id == callis_id => {
                    self.ack_map.remove(peer_msg_id);
                    self.inflight = self.inflight.saturating_sub(1);
                    let data = data.clone();
                    let chunk_id = *chunk_id;
                    let is_last = *is_last;
                    let seq = *seq;
                    *slot = OutboundSlot::ReplayReady {
                        chunk_id,
                        data,
                        previous_callis_id: callis_id,
                        is_last,
                        seq,
                    };
                    changed = true;
                }
                _ => {}
            }
        }
        changed
    }

    pub(super) fn note_completion(
        &mut self,
        peer_msg_id: PeerMessageId,
        completion: ChunkCompletion,
    ) -> bool {
        let Some(chunk_id) = self.ack_map.remove(&peer_msg_id) else {
            return false;
        };
        let Some(index) = self.chunk_index.remove(&chunk_id) else {
            return false;
        };
        let Some(slot) = self.slots.get_mut(index) else {
            return false;
        };
        match completion {
            ChunkCompletion::Ack => {
                if matches!(slot, OutboundSlot::InFlight { .. }) {
                    self.inflight = self.inflight.saturating_sub(1);
                }
                *slot = OutboundSlot::Empty;
            }
            ChunkCompletion::Error => {
                if matches!(slot, OutboundSlot::InFlight { .. }) {
                    self.inflight = self.inflight.saturating_sub(1);
                }
                *slot = OutboundSlot::Empty;
            }
        }
        true
    }

    pub(super) fn has_replay_ready(&self) -> bool {
        self.slots
            .iter()
            .any(|slot| matches!(slot, OutboundSlot::ReplayReady { .. }))
    }

    pub(super) fn has_fresh_ready(&self) -> bool {
        self.slots
            .iter()
            .any(|slot| matches!(slot, OutboundSlot::Ready { .. }))
    }

    #[cfg(test)]
    pub(super) fn has_capacity(&self) -> bool {
        self.has_window_capacity_for(1)
    }

    pub(super) fn live_chunk_count(&self) -> usize {
        self.slots
            .iter()
            .filter(|slot| !matches!(slot, OutboundSlot::Empty))
            .count()
    }

    pub(super) fn inflight(&self) -> usize {
        self.inflight
    }

    pub(super) fn is_drained(&self) -> bool {
        self.live_chunk_count() == 0 && self.pending_full.is_none()
    }

    fn enqueue_chunk(&mut self, chunk: Bytes, is_last: bool) {
        let Some(index) = self.next_empty_slot() else {
            return;
        };
        let chunk_id = self.next_chunk_id;
        self.next_chunk_id = self.next_chunk_id.saturating_add(1);
        self.created_any = true;
        self.slots[index] = OutboundSlot::Ready {
            chunk_id,
            data: chunk,
            is_last,
            seq: 0,
        };
        self.chunk_index.insert(chunk_id, index);
        self.write_cursor = (index + 1) % self.window_size;
    }

    fn has_window_capacity_for(&self, count: usize) -> bool {
        self.live_chunk_count() + self.pending_full.is_some() as usize + count <= self.window_size
    }

    fn next_empty_slot(&self) -> Option<usize> {
        for offset in 0..self.window_size {
            let index = (self.write_cursor + offset) % self.window_size;
            if matches!(self.slots[index], OutboundSlot::Empty) {
                return Some(index);
            }
        }
        None
    }

    fn lease_matching(
        &mut self,
        replay: bool,
        callis_id: u64,
        peer_msg_id: PeerMessageId,
    ) -> Option<ChunkWriteLease> {
        let index = self
            .slots
            .iter()
            .enumerate()
            .filter_map(|(index, slot)| match slot {
                OutboundSlot::Ready { chunk_id, .. } if !replay => Some((index, *chunk_id)),
                OutboundSlot::ReplayReady { chunk_id, .. } if replay => Some((index, *chunk_id)),
                _ => None,
            })
            .min_by_key(|(_, chunk_id)| *chunk_id)
            .map(|(index, _)| index)?;

        let slot = &mut self.slots[index];
        match slot {
            OutboundSlot::Ready {
                chunk_id,
                data,
                is_last,
                seq,
            }
            | OutboundSlot::ReplayReady {
                chunk_id,
                data,
                is_last,
                seq,
                ..
            } => {
                let next_seq = seq.saturating_add(1);
                let lease = ChunkWriteLease {
                    chunk_id: *chunk_id,
                    data: data.clone(),
                    is_last: *is_last,
                    peer_msg_id,
                    callis_id,
                    slot_seq: next_seq,
                };
                *slot = OutboundSlot::Writing {
                    chunk_id: lease.chunk_id,
                    data: lease.data.clone(),
                    peer_msg_id,
                    callis_id,
                    is_last: lease.is_last,
                    seq: next_seq,
                };
                Some(lease)
            }
            _ => None,
        }
    }
}

#[cfg(test)]
impl OutboundChunks {
    pub(super) fn has_ready(&self) -> bool {
        self.slots.iter().any(|slot| {
            matches!(
                slot,
                OutboundSlot::Ready { .. } | OutboundSlot::ReplayReady { .. }
            )
        })
    }
}
