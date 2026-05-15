// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

#![allow(dead_code)]

use super::*;

use std::cmp::{Ordering as CmpOrdering, Reverse};
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;

const A1_ACK_CAPACITY_MULTIPLIER: usize = 16;
const A1_ERROR_CAPACITY_MULTIPLIER: usize = 2;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) enum RetainedLane {
    A1Ack,
    A1Error,
    A2,
    A3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SlotState {
    Empty,
    Ready,
    ReplayReady { last_sent_callis_id: CallisId },
    Writing { callis_id: CallisId },
    Inflight { last_sent_callis_id: CallisId },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ResponseKind {
    Ack,
    Error,
}

pub(super) struct TrackedOutbound {
    message: Arc<PeerMessage>,
    ack_tx: oneshot::Sender<Result<(), AureliaError>>,
}

impl TrackedOutbound {
    pub(super) fn new(
        message: PeerMessage,
        ack_tx: oneshot::Sender<Result<(), AureliaError>>,
    ) -> Self {
        Self {
            message: Arc::new(message),
            ack_tx,
        }
    }

    pub(super) fn peer_msg_id(&self) -> PeerMessageId {
        self.message.peer_msg_id
    }

    pub(super) fn msg_type(&self) -> MessageType {
        self.message.msg_type
    }
}

impl fmt::Debug for TrackedOutbound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TrackedOutbound")
            .field("peer_msg_id", &self.peer_msg_id())
            .field("msg_type", &self.msg_type())
            .finish_non_exhaustive()
    }
}

enum RetainedItem {
    Ack {
        peer_msg_id: PeerMessageId,
    },
    Error {
        peer_msg_id: PeerMessageId,
        frame: Arc<OutboundFrame>,
    },
    Tracked(TrackedOutbound),
}

impl RetainedItem {
    fn peer_msg_id(&self) -> PeerMessageId {
        match self {
            Self::Ack { peer_msg_id } | Self::Error { peer_msg_id, .. } => *peer_msg_id,
            Self::Tracked(tracked) => tracked.peer_msg_id(),
        }
    }

    fn kind(&self) -> ItemKind {
        match self {
            Self::Ack { .. } => ItemKind::Ack,
            Self::Error { .. } => ItemKind::Error,
            Self::Tracked(_) => ItemKind::Tracked,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ItemKind {
    Ack,
    Error,
    Tracked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ItemIdentity {
    peer_msg_id: PeerMessageId,
    seq: u64,
    kind: ItemKind,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct SlotId {
    lane: RetainedLane,
    index: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct QueueKey {
    index: usize,
    seq: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DeadlineKey {
    deadline: Instant,
    slot: SlotId,
    seq: u64,
}

impl Ord for DeadlineKey {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.deadline
            .cmp(&other.deadline)
            .then_with(|| lane_rank(self.slot.lane).cmp(&lane_rank(other.slot.lane)))
            .then_with(|| self.slot.index.cmp(&other.slot.index))
            .then_with(|| self.seq.cmp(&other.seq))
    }
}

impl PartialOrd for DeadlineKey {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

struct RetainedSlot {
    item: Option<RetainedItem>,
    original_deadline: Instant,
    lane: RetainedLane,
    seq: u64,
    state: SlotState,
}

impl RetainedSlot {
    fn empty(lane: RetainedLane) -> Self {
        Self {
            item: None,
            original_deadline: Instant::now(),
            lane,
            seq: 0,
            state: SlotState::Empty,
        }
    }

    fn identity(&self) -> Option<ItemIdentity> {
        let item = self.item.as_ref()?;
        Some(ItemIdentity {
            peer_msg_id: item.peer_msg_id(),
            seq: self.seq,
            kind: item.kind(),
        })
    }
}

struct LaneState {
    lane: RetainedLane,
    slots: Vec<RetainedSlot>,
    free: Vec<usize>,
    ready: VecDeque<QueueKey>,
    replay: VecDeque<QueueKey>,
    target_capacity: usize,
    live_count: usize,
}

impl LaneState {
    fn new(lane: RetainedLane, target_capacity: usize) -> Self {
        let target_capacity = target_capacity.max(1);
        let mut slots = Vec::with_capacity(target_capacity);
        let mut free = Vec::with_capacity(target_capacity);
        for index in 0..target_capacity {
            slots.push(RetainedSlot::empty(lane));
            free.push(index);
        }
        free.reverse();
        Self {
            lane,
            slots,
            free,
            ready: VecDeque::new(),
            replay: VecDeque::new(),
            target_capacity,
            live_count: 0,
        }
    }

    fn set_target_capacity(&mut self, target_capacity: usize) {
        let target_capacity = target_capacity.max(1);
        if target_capacity > self.slots.len() {
            for index in self.slots.len()..target_capacity {
                self.slots.push(RetainedSlot::empty(self.lane));
                self.free.push(index);
            }
        }
        self.target_capacity = target_capacity;
    }

    fn available(&self) -> bool {
        self.live_count < self.target_capacity
    }

    fn allocate(
        &mut self,
        item: RetainedItem,
        deadline: Instant,
        seq: u64,
    ) -> Option<(SlotId, ItemIdentity)> {
        if !self.available() {
            return None;
        }
        let index = match self.free.pop() {
            Some(index) => index,
            None if self.slots.len() < self.target_capacity => {
                let index = self.slots.len();
                self.slots.push(RetainedSlot::empty(self.lane));
                index
            }
            None => return None,
        };
        let slot = &mut self.slots[index];
        slot.item = Some(item);
        slot.original_deadline = deadline;
        slot.seq = seq;
        slot.state = SlotState::Ready;
        self.live_count = self.live_count.saturating_add(1);
        self.ready.push_back(QueueKey { index, seq });
        let slot_id = SlotId {
            lane: self.lane,
            index,
        };
        let identity = slot.identity().expect("allocated slot has item");
        Some((slot_id, identity))
    }

    fn remove_slot(&mut self, index: usize) -> Option<RetainedItem> {
        let slot = self.slots.get_mut(index)?;
        let item = slot.item.take()?;
        slot.state = SlotState::Empty;
        slot.seq = slot.seq.wrapping_add(1).max(1);
        self.live_count = self.live_count.saturating_sub(1);
        self.free.push(index);
        Some(item)
    }

    fn slot(&self, index: usize) -> Option<&RetainedSlot> {
        self.slots.get(index)
    }

    fn slot_mut(&mut self, index: usize) -> Option<&mut RetainedSlot> {
        self.slots.get_mut(index)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RetainedCapacities {
    pub(super) a1_ack: usize,
    pub(super) a1_error: usize,
    pub(super) a2: usize,
    pub(super) a3: usize,
}

impl RetainedCapacities {
    pub(super) fn from_send_queue_size(send_queue_size: usize) -> Self {
        let send_queue_size = send_queue_size.max(1);
        Self {
            a1_ack: send_queue_size.saturating_mul(A1_ACK_CAPACITY_MULTIPLIER),
            a1_error: send_queue_size.saturating_mul(A1_ERROR_CAPACITY_MULTIPLIER),
            a2: A2_CAPACITY,
            a3: send_queue_size,
        }
    }

    pub(super) fn capacity(self, lane: RetainedLane) -> usize {
        match lane {
            RetainedLane::A1Ack => self.a1_ack,
            RetainedLane::A1Error => self.a1_error,
            RetainedLane::A2 => self.a2,
            RetainedLane::A3 => self.a3,
        }
    }
}

#[derive(Debug)]
pub(super) enum RetainedInsertError<T> {
    Full(T),
    Duplicate(T),
    Shutdown(T),
    Expired(T),
    A1Tracked(T),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ResponseInsertOutcome {
    Inserted,
    Duplicate {
        attempted: ResponseKind,
        peer_msg_id: PeerMessageId,
    },
    Full {
        lane: RetainedLane,
        peer_msg_id: PeerMessageId,
    },
    Shutdown {
        peer_msg_id: PeerMessageId,
    },
    Expired {
        peer_msg_id: PeerMessageId,
    },
}

pub(in crate::transport) struct RetainedClaim {
    slot: SlotId,
    identity: ItemIdentity,
    callis_id: CallisId,
    pub(in crate::transport) deadline: Instant,
    pub(in crate::transport) item: RetainedWriteItem,
}

#[derive(Clone)]
pub(in crate::transport) enum RetainedWriteItem {
    Ack {
        peer_msg_id: PeerMessageId,
    },
    Error {
        peer_msg_id: PeerMessageId,
        frame: Arc<OutboundFrame>,
    },
    Message {
        peer_msg_id: PeerMessageId,
        message: Arc<PeerMessage>,
    },
}

impl RetainedWriteItem {
    pub(in crate::transport) fn peer_msg_id(&self) -> PeerMessageId {
        match self {
            Self::Ack { peer_msg_id }
            | Self::Error { peer_msg_id, .. }
            | Self::Message { peer_msg_id, .. } => *peer_msg_id,
        }
    }
}

pub(super) struct CompletionEffect {
    pub(super) peer_msg_id: PeerMessageId,
    pub(super) ack_tx: oneshot::Sender<Result<(), AureliaError>>,
    pub(super) result: Result<(), AureliaError>,
}

pub(super) struct RetainedStore {
    a1_ack: LaneState,
    a1_error: LaneState,
    a2: LaneState,
    a3: LaneState,
    tracked: HashMap<PeerMessageId, SlotId>,
    responses: HashMap<PeerMessageId, SlotId>,
    deadlines: BinaryHeap<Reverse<DeadlineKey>>,
    next_seq: u64,
    shutdown: bool,
}

impl RetainedStore {
    pub(super) fn new(capacities: RetainedCapacities) -> Self {
        Self {
            a1_ack: LaneState::new(RetainedLane::A1Ack, capacities.a1_ack),
            a1_error: LaneState::new(RetainedLane::A1Error, capacities.a1_error),
            a2: LaneState::new(RetainedLane::A2, capacities.a2),
            a3: LaneState::new(RetainedLane::A3, capacities.a3),
            tracked: HashMap::new(),
            responses: HashMap::new(),
            deadlines: BinaryHeap::new(),
            next_seq: 1,
            shutdown: false,
        }
    }

    pub(super) fn set_capacities(&mut self, capacities: RetainedCapacities) {
        self.a1_ack.set_target_capacity(capacities.a1_ack);
        self.a1_error.set_target_capacity(capacities.a1_error);
        self.a2.set_target_capacity(capacities.a2);
        self.a3.set_target_capacity(capacities.a3);
    }

    pub(super) fn set_shutdown(&mut self) {
        self.shutdown = true;
    }

    pub(super) fn insert_ack(
        &mut self,
        peer_msg_id: PeerMessageId,
        deadline: Instant,
    ) -> ResponseInsertOutcome {
        self.insert_response(
            ResponseKind::Ack,
            RetainedLane::A1Ack,
            peer_msg_id,
            deadline,
            RetainedItem::Ack { peer_msg_id },
        )
    }

    pub(super) fn insert_error(
        &mut self,
        peer_msg_id: PeerMessageId,
        frame: OutboundFrame,
        deadline: Instant,
    ) -> ResponseInsertOutcome {
        self.insert_response(
            ResponseKind::Error,
            RetainedLane::A1Error,
            peer_msg_id,
            deadline,
            RetainedItem::Error {
                peer_msg_id,
                frame: Arc::new(frame),
            },
        )
    }

    fn insert_response(
        &mut self,
        attempted: ResponseKind,
        lane: RetainedLane,
        peer_msg_id: PeerMessageId,
        deadline: Instant,
        item: RetainedItem,
    ) -> ResponseInsertOutcome {
        if self.shutdown {
            return ResponseInsertOutcome::Shutdown { peer_msg_id };
        }
        if Instant::now() >= deadline {
            return ResponseInsertOutcome::Expired { peer_msg_id };
        }
        if self.responses.contains_key(&peer_msg_id) {
            return ResponseInsertOutcome::Duplicate {
                attempted,
                peer_msg_id,
            };
        }
        match self.allocate(lane, item, deadline) {
            Some((slot, _identity)) => {
                self.responses.insert(peer_msg_id, slot);
                ResponseInsertOutcome::Inserted
            }
            None => ResponseInsertOutcome::Full { lane, peer_msg_id },
        }
    }

    pub(super) fn insert_tracked(
        &mut self,
        tracked: TrackedOutbound,
        deadline: Instant,
    ) -> Result<(), RetainedInsertError<TrackedOutbound>> {
        if self.shutdown {
            return Err(RetainedInsertError::Shutdown(tracked));
        }
        if Instant::now() >= deadline {
            return Err(RetainedInsertError::Expired(tracked));
        }
        let peer_msg_id = tracked.peer_msg_id();
        if self.tracked.contains_key(&peer_msg_id) {
            return Err(RetainedInsertError::Duplicate(tracked));
        }
        if classify_priority(tracked.msg_type()) == PriorityTier::A1 {
            return Err(RetainedInsertError::A1Tracked(tracked));
        }
        let lane = tracked_lane(tracked.msg_type());
        if !self.lane(lane).available() {
            return Err(RetainedInsertError::Full(tracked));
        }
        match self.allocate(lane, RetainedItem::Tracked(tracked), deadline) {
            Some((slot, _identity)) => {
                self.tracked.insert(peer_msg_id, slot);
                Ok(())
            }
            None => unreachable!("capacity checked before tracked allocation"),
        }
    }

    fn allocate(
        &mut self,
        lane: RetainedLane,
        item: RetainedItem,
        deadline: Instant,
    ) -> Option<(SlotId, ItemIdentity)> {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1).max(1);
        let lane_state = self.lane_mut(lane);
        let (slot, identity) = lane_state.allocate(item, deadline, seq)?;
        self.deadlines.push(Reverse(DeadlineKey {
            deadline,
            slot,
            seq,
        }));
        Some((slot, identity))
    }

    pub(super) fn claim_next(&mut self, callis_id: CallisId) -> Option<RetainedClaim> {
        for lane in [
            RetainedLane::A1Ack,
            RetainedLane::A1Error,
            RetainedLane::A2,
            RetainedLane::A3,
        ] {
            if let Some(claim) = self.claim_lane(lane, callis_id) {
                return Some(claim);
            }
        }
        None
    }

    pub(super) fn has_dispatchable_work(&self) -> bool {
        self.lane_has_dispatchable_work(RetainedLane::A1Ack)
            || self.lane_has_dispatchable_work(RetainedLane::A1Error)
            || (!self.shutdown
                && (self.lane_has_dispatchable_work(RetainedLane::A2)
                    || self.lane_has_dispatchable_work(RetainedLane::A3)))
    }

    fn lane_has_dispatchable_work(&self, lane: RetainedLane) -> bool {
        self.lane(lane).slots.iter().any(|slot| {
            matches!(slot.state, SlotState::Ready | SlotState::ReplayReady { .. })
                && slot.item.is_some()
        })
    }

    fn claim_lane(&mut self, lane: RetainedLane, callis_id: CallisId) -> Option<RetainedClaim> {
        if self.shutdown && !matches!(lane, RetainedLane::A1Ack | RetainedLane::A1Error) {
            return None;
        }
        if !matches!(lane, RetainedLane::A1Ack | RetainedLane::A1Error) {
            if let Some(claim) = self.claim_from_queue(lane, true, callis_id) {
                return Some(claim);
            }
        }
        self.claim_from_queue(lane, false, callis_id)
    }

    fn claim_from_queue(
        &mut self,
        lane: RetainedLane,
        replay: bool,
        callis_id: CallisId,
    ) -> Option<RetainedClaim> {
        loop {
            let key = {
                let lane_state = self.lane_mut(lane);
                if replay {
                    lane_state.replay.pop_front()
                } else {
                    lane_state.ready.pop_front()
                }
            }?;
            let slot = self.lane_mut(lane).slot_mut(key.index)?;
            if slot.seq != key.seq {
                continue;
            }
            let expected = if replay {
                matches!(slot.state, SlotState::ReplayReady { .. })
            } else {
                slot.state == SlotState::Ready
            };
            if !expected {
                continue;
            }
            let identity = slot.identity()?;
            let item = write_item(slot.item.as_ref()?);
            let deadline = slot.original_deadline;
            slot.state = SlotState::Writing { callis_id };
            return Some(RetainedClaim {
                slot: SlotId {
                    lane,
                    index: key.index,
                },
                identity,
                callis_id,
                deadline,
                item,
            });
        }
    }

    pub(super) fn complete_write(
        &mut self,
        claim: RetainedClaim,
        result: Result<(), AureliaError>,
    ) -> Vec<CompletionEffect> {
        enum WriteAction {
            Ignore,
            Clear,
            Inflight,
            RetryReady { seq: u64 },
            RetryReplay { seq: u64 },
        }

        let action = {
            let Some(slot) = self.lane_mut(claim.slot.lane).slot_mut(claim.slot.index) else {
                return Vec::new();
            };
            if slot.state
                != (SlotState::Writing {
                    callis_id: claim.callis_id,
                })
                || slot.identity() != Some(claim.identity)
            {
                WriteAction::Ignore
            } else if result.is_err() {
                match slot.item.as_ref() {
                    Some(RetainedItem::Ack { .. }) | Some(RetainedItem::Error { .. }) => {
                        slot.state = SlotState::Ready;
                        WriteAction::RetryReady { seq: slot.seq }
                    }
                    Some(RetainedItem::Tracked(_)) => {
                        slot.state = SlotState::ReplayReady {
                            last_sent_callis_id: claim.callis_id,
                        };
                        WriteAction::RetryReplay { seq: slot.seq }
                    }
                    None => WriteAction::Ignore,
                }
            } else {
                match slot.item.as_ref() {
                    Some(RetainedItem::Ack { .. }) | Some(RetainedItem::Error { .. }) => {
                        WriteAction::Clear
                    }
                    Some(RetainedItem::Tracked(_)) => {
                        slot.state = SlotState::Inflight {
                            last_sent_callis_id: claim.callis_id,
                        };
                        WriteAction::Inflight
                    }
                    None => WriteAction::Ignore,
                }
            }
        };

        match action {
            WriteAction::Ignore | WriteAction::Inflight => Vec::new(),
            WriteAction::Clear => {
                self.remove_by_slot(claim.slot);
                Vec::new()
            }
            WriteAction::RetryReady { seq } => {
                self.lane_mut(claim.slot.lane).ready.push_back(QueueKey {
                    index: claim.slot.index,
                    seq,
                });
                Vec::new()
            }
            WriteAction::RetryReplay { seq } => {
                self.lane_mut(claim.slot.lane).replay.push_back(QueueKey {
                    index: claim.slot.index,
                    seq,
                });
                Vec::new()
            }
        }
    }

    pub(super) fn ack(&mut self, peer_msg_id: PeerMessageId) -> Option<CompletionEffect> {
        let slot = self.tracked.get(&peer_msg_id).copied()?;
        let item = self.remove_by_slot(slot)?;
        match item {
            RetainedItem::Tracked(tracked) => Some(CompletionEffect {
                peer_msg_id,
                ack_tx: tracked.ack_tx,
                result: Ok(()),
            }),
            _ => None,
        }
    }

    pub(super) fn fail_one(
        &mut self,
        peer_msg_id: PeerMessageId,
        error: AureliaError,
    ) -> Option<CompletionEffect> {
        let slot = self.tracked.get(&peer_msg_id).copied()?;
        let item = self.remove_by_slot(slot)?;
        match item {
            RetainedItem::Tracked(tracked) => Some(CompletionEffect {
                peer_msg_id,
                ack_tx: tracked.ack_tx,
                result: Err(error),
            }),
            _ => None,
        }
    }

    pub(super) fn fail_all(&mut self, error: AureliaError) -> Vec<CompletionEffect> {
        let slots = self.tracked.values().copied().collect::<Vec<_>>();
        let mut effects = Vec::new();
        for slot in slots {
            if let Some(RetainedItem::Tracked(tracked)) = self.remove_by_slot(slot) {
                effects.push(CompletionEffect {
                    peer_msg_id: tracked.peer_msg_id(),
                    ack_tx: tracked.ack_tx,
                    result: Err(error.clone()),
                });
            }
        }
        effects
    }

    pub(super) fn fail_non_a1(&mut self, error: AureliaError) -> Vec<CompletionEffect> {
        let slots = self
            .tracked
            .values()
            .filter_map(|slot| {
                (!matches!(slot.lane, RetainedLane::A1Ack | RetainedLane::A1Error)).then_some(*slot)
            })
            .collect::<Vec<_>>();
        let mut effects = Vec::new();
        for slot in slots {
            if let Some(RetainedItem::Tracked(tracked)) = self.remove_by_slot(slot) {
                effects.push(CompletionEffect {
                    peer_msg_id: tracked.peer_msg_id(),
                    ack_tx: tracked.ack_tx,
                    result: Err(error.clone()),
                });
            }
        }
        effects
    }

    pub(super) fn mark_callis_replay_ready(&mut self, callis_id: CallisId) {
        for lane in [RetainedLane::A1Error, RetainedLane::A2, RetainedLane::A3] {
            let mut replay = Vec::new();
            let lane_state = self.lane_mut(lane);
            for index in 0..lane_state.slots.len() {
                let slot = &mut lane_state.slots[index];
                match slot.state {
                    SlotState::Writing { callis_id: writing } if writing == callis_id => {
                        let seq = slot.seq;
                        match slot.item.as_ref() {
                            Some(RetainedItem::Ack { .. }) | Some(RetainedItem::Error { .. }) => {
                                slot.state = SlotState::Ready;
                                lane_state.ready.push_back(QueueKey { index, seq });
                            }
                            Some(RetainedItem::Tracked(_)) => {
                                slot.state = SlotState::ReplayReady {
                                    last_sent_callis_id: callis_id,
                                };
                                replay.push(QueueKey { index, seq });
                            }
                            None => {}
                        }
                    }
                    SlotState::Inflight {
                        last_sent_callis_id,
                    } if last_sent_callis_id == callis_id => {
                        if matches!(slot.item, Some(RetainedItem::Tracked(_))) {
                            slot.state = SlotState::ReplayReady {
                                last_sent_callis_id: callis_id,
                            };
                            replay.push(QueueKey {
                                index,
                                seq: slot.seq,
                            });
                        }
                    }
                    _ => {}
                }
            }
            lane_state.replay.extend(replay);
        }
    }

    pub(super) fn mark_all_tracked_replay_ready(&mut self) {
        let slots = self.tracked.values().copied().collect::<Vec<_>>();
        self.mark_slots_replay_ready(slots);
    }

    pub(super) fn mark_tracked_replay_ready(&mut self, peer_msg_ids: &[PeerMessageId]) {
        let slots = peer_msg_ids
            .iter()
            .filter_map(|peer_msg_id| self.tracked.get(peer_msg_id).copied())
            .collect::<Vec<_>>();
        self.mark_slots_replay_ready(slots);
    }

    fn mark_slots_replay_ready(&mut self, slots: Vec<SlotId>) {
        for slot_id in slots {
            let lane = slot_id.lane;
            let index = slot_id.index;
            let seq = {
                let Some(slot) = self.lane_mut(lane).slot_mut(index) else {
                    continue;
                };
                if !matches!(slot.item, Some(RetainedItem::Tracked(_))) {
                    continue;
                }
                let last_sent_callis_id = match slot.state {
                    SlotState::Writing { callis_id } => callis_id,
                    SlotState::Inflight {
                        last_sent_callis_id,
                    }
                    | SlotState::ReplayReady {
                        last_sent_callis_id,
                    } => last_sent_callis_id,
                    SlotState::Ready | SlotState::Empty => continue,
                };
                slot.state = SlotState::ReplayReady {
                    last_sent_callis_id,
                };
                slot.seq
            };
            self.lane_mut(lane)
                .replay
                .push_back(QueueKey { index, seq });
        }
    }

    pub(super) fn tracked_messages(&self) -> Vec<PeerMessage> {
        self.tracked
            .values()
            .filter_map(|slot| {
                let slot = self.lane(slot.lane).slot(slot.index)?;
                match slot.item.as_ref()? {
                    RetainedItem::Tracked(tracked) => Some((*tracked.message).clone()),
                    _ => None,
                }
            })
            .collect()
    }

    pub(super) fn message(&self, peer_msg_id: PeerMessageId) -> Option<PeerMessage> {
        let slot = self.tracked.get(&peer_msg_id)?;
        let slot = self.lane(slot.lane).slot(slot.index)?;
        match slot.item.as_ref()? {
            RetainedItem::Tracked(tracked) => Some((*tracked.message).clone()),
            _ => None,
        }
    }

    pub(super) fn deadline(&self, peer_msg_id: PeerMessageId) -> Option<Instant> {
        let slot = self.tracked.get(&peer_msg_id)?;
        self.lane(slot.lane)
            .slot(slot.index)
            .map(|slot| slot.original_deadline)
    }

    pub(super) fn contains_tracked(&self, peer_msg_id: PeerMessageId) -> bool {
        self.tracked.contains_key(&peer_msg_id)
    }

    pub(super) fn tracked_state(&self, peer_msg_id: PeerMessageId) -> Option<SlotState> {
        let slot = self.tracked.get(&peer_msg_id)?;
        self.lane(slot.lane).slot(slot.index).map(|slot| slot.state)
    }

    pub(super) fn expire_due(&mut self, now: Instant) -> Vec<CompletionEffect> {
        let mut effects = Vec::new();
        while let Some(key) = self.deadlines.peek().copied().map(|Reverse(key)| key) {
            if key.deadline > now {
                break;
            }
            self.deadlines.pop();
            let Some(slot) = self.lane(key.slot.lane).slot(key.slot.index) else {
                continue;
            };
            if slot.seq != key.seq
                || slot.original_deadline != key.deadline
                || slot.state == SlotState::Empty
            {
                continue;
            }
            let Some(item) = self.remove_by_slot(key.slot) else {
                continue;
            };
            if let RetainedItem::Tracked(tracked) = item {
                effects.push(CompletionEffect {
                    peer_msg_id: tracked.peer_msg_id(),
                    ack_tx: tracked.ack_tx,
                    result: Err(AureliaError::new(ErrorId::SendTimeout)),
                });
            }
        }
        effects
    }

    pub(super) fn next_deadline(&mut self) -> Option<Instant> {
        loop {
            let key = self.deadlines.peek().copied().map(|Reverse(key)| key)?;
            let Some(slot) = self.lane(key.slot.lane).slot(key.slot.index) else {
                self.deadlines.pop();
                continue;
            };
            if slot.seq == key.seq
                && slot.original_deadline == key.deadline
                && slot.state != SlotState::Empty
            {
                return Some(key.deadline);
            }
            self.deadlines.pop();
        }
    }

    pub(super) fn begin_shutdown(&mut self, error: AureliaError) -> Vec<CompletionEffect> {
        self.shutdown = true;
        let slots = self
            .tracked
            .values()
            .copied()
            .filter(|slot| !matches!(slot.lane, RetainedLane::A1Ack | RetainedLane::A1Error))
            .collect::<Vec<_>>();
        let mut effects = Vec::new();
        for slot in slots {
            if let Some(RetainedItem::Tracked(tracked)) = self.remove_by_slot(slot) {
                effects.push(CompletionEffect {
                    peer_msg_id: tracked.peer_msg_id(),
                    ack_tx: tracked.ack_tx,
                    result: Err(error.clone()),
                });
            }
        }
        effects
    }

    pub(super) fn drop_responses(&mut self) {
        let slots = self.responses.values().copied().collect::<Vec<_>>();
        for slot in slots {
            let _ = self.remove_by_slot(slot);
        }
    }

    pub(super) fn response_lanes_empty(&self) -> bool {
        self.a1_ack.live_count == 0 && self.a1_error_response_count() == 0
    }

    pub(super) fn is_empty(&self) -> bool {
        self.a1_ack.live_count == 0
            && self.a1_error.live_count == 0
            && self.a2.live_count == 0
            && self.a3.live_count == 0
    }

    fn a1_error_response_count(&self) -> usize {
        self.responses
            .values()
            .filter(|slot| slot.lane == RetainedLane::A1Error)
            .count()
    }

    pub(super) fn live_count(&self, lane: RetainedLane) -> usize {
        self.lane(lane).live_count
    }

    pub(super) fn target_capacity(&self, lane: RetainedLane) -> usize {
        self.lane(lane).target_capacity
    }

    pub(super) fn state(&self, slot: SlotId) -> Option<SlotState> {
        self.lane(slot.lane).slot(slot.index).map(|slot| slot.state)
    }

    fn remove_by_slot(&mut self, slot: SlotId) -> Option<RetainedItem> {
        let item = self.lane_mut(slot.lane).remove_slot(slot.index)?;
        match &item {
            RetainedItem::Ack { peer_msg_id } | RetainedItem::Error { peer_msg_id, .. } => {
                self.responses.remove(peer_msg_id);
            }
            RetainedItem::Tracked(tracked) => {
                self.tracked.remove(&tracked.peer_msg_id());
            }
        }
        Some(item)
    }

    fn lane(&self, lane: RetainedLane) -> &LaneState {
        match lane {
            RetainedLane::A1Ack => &self.a1_ack,
            RetainedLane::A1Error => &self.a1_error,
            RetainedLane::A2 => &self.a2,
            RetainedLane::A3 => &self.a3,
        }
    }

    fn lane_mut(&mut self, lane: RetainedLane) -> &mut LaneState {
        match lane {
            RetainedLane::A1Ack => &mut self.a1_ack,
            RetainedLane::A1Error => &mut self.a1_error,
            RetainedLane::A2 => &mut self.a2,
            RetainedLane::A3 => &mut self.a3,
        }
    }
}

fn write_item(item: &RetainedItem) -> RetainedWriteItem {
    match item {
        RetainedItem::Ack { peer_msg_id } => RetainedWriteItem::Ack {
            peer_msg_id: *peer_msg_id,
        },
        RetainedItem::Error { peer_msg_id, frame } => RetainedWriteItem::Error {
            peer_msg_id: *peer_msg_id,
            frame: Arc::clone(frame),
        },
        RetainedItem::Tracked(tracked) => RetainedWriteItem::Message {
            peer_msg_id: tracked.peer_msg_id(),
            message: Arc::clone(&tracked.message),
        },
    }
}

fn tracked_lane(msg_type: MessageType) -> RetainedLane {
    match classify_priority(msg_type) {
        PriorityTier::A1 => unreachable!("completion-bearing A1 messages are not retained"),
        PriorityTier::A2 => RetainedLane::A2,
        PriorityTier::A3 => RetainedLane::A3,
    }
}

fn lane_rank(lane: RetainedLane) -> u8 {
    match lane {
        RetainedLane::A1Ack => 0,
        RetainedLane::A1Error => 1,
        RetainedLane::A2 => 2,
        RetainedLane::A3 => 3,
    }
}

#[cfg(test)]
#[path = "../tests/leaf/primary_dispatch_store.rs"]
mod tests;
