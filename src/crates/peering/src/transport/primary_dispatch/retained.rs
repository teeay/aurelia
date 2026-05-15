// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

#![allow(dead_code)]

use super::store::{
    CompletionEffect, ResponseInsertOutcome, ResponseKind, RetainedCapacities, RetainedClaim,
    RetainedInsertError, RetainedLane, RetainedStore, SlotState, TrackedOutbound,
};
use super::*;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};

pub(super) struct PrimaryOutboundStore {
    state: Mutex<RetainedStore>,
    overrun_reporter: Option<OutboundQueueOverrunReporter>,
    available: Arc<Notify>,
    reaper: Arc<Notify>,
    empty: Arc<Notify>,
    a1_response_empty: Arc<Notify>,
    close_state: Mutex<CloseState>,
    shutdown: AtomicBool,
}

struct CloseState {
    intents: HashSet<CallisId>,
    waiters: HashMap<CallisId, Arc<Notify>>,
}

impl CloseState {
    fn new() -> Self {
        Self {
            intents: HashSet::new(),
            waiters: HashMap::new(),
        }
    }
}

pub(super) struct PrimaryOutboundReaper {
    queue: Weak<PrimaryOutboundStore>,
    reaper: Arc<Notify>,
}

impl PrimaryOutboundStore {
    pub(super) fn new(
        send_queue_size: usize,
        overrun_reporter: Option<OutboundQueueOverrunReporter>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(RetainedStore::new(
                RetainedCapacities::from_send_queue_size(send_queue_size),
            )),
            overrun_reporter,
            available: Arc::new(Notify::new()),
            reaper: Arc::new(Notify::new()),
            empty: Arc::new(Notify::new()),
            a1_response_empty: Arc::new(Notify::new()),
            close_state: Mutex::new(CloseState::new()),
            shutdown: AtomicBool::new(false),
        })
    }

    pub(super) fn reaper(self: &Arc<Self>) -> PrimaryOutboundReaper {
        PrimaryOutboundReaper {
            queue: Arc::downgrade(self),
            reaper: Arc::clone(&self.reaper),
        }
    }

    pub(super) async fn enqueue_message(
        &self,
        message: PeerMessage,
        deadline: Instant,
    ) -> Result<oneshot::Receiver<Result<(), AureliaError>>, AureliaError> {
        let msg_type = message.msg_type;
        let peer_msg_id = message.peer_msg_id;
        let (ack_tx, ack_rx) = oneshot::channel();
        let tracked = TrackedOutbound::new(message, ack_tx);
        let result = {
            let mut guard = self.state.lock().await;
            guard.insert_tracked(tracked, deadline)
        };
        match result {
            Ok(()) => {
                self.notify_work_changed();
                Ok(ack_rx)
            }
            Err(RetainedInsertError::Full(_)) => {
                let lane = tracked_lane_for_msg_type(msg_type);
                self.report_overrun(lane, msg_type, None).await;
                Err(AureliaError::new(ErrorId::LocalQueueFull))
            }
            Err(RetainedInsertError::Shutdown(_)) => {
                Err(AureliaError::new(ErrorId::PeerUnavailable))
            }
            Err(RetainedInsertError::Expired(_)) => Err(AureliaError::new(ErrorId::SendTimeout)),
            Err(RetainedInsertError::Duplicate(_)) => {
                warn!(peer_msg_id, "duplicate tracked outbound message attempted");
                Err(AureliaError::new(ErrorId::ProtocolViolation))
            }
            Err(RetainedInsertError::A1Tracked(_)) => {
                warn!(
                    peer_msg_id,
                    msg_type, "completion-bearing A1 outbound message attempted"
                );
                Err(AureliaError::new(ErrorId::ProtocolViolation))
            }
        }
    }

    pub(super) async fn enqueue_ack(&self, peer_msg_id: PeerMessageId, deadline: Instant) {
        let outcome = {
            let mut guard = self.state.lock().await;
            guard.insert_ack(peer_msg_id, deadline)
        };
        self.handle_response_insert_outcome(outcome, MSG_ACK).await;
    }

    pub(super) async fn enqueue_error(&self, frame: OutboundFrame, deadline: Instant) {
        let peer_msg_id = frame_peer_msg_id(&frame);
        let outcome = {
            let mut guard = self.state.lock().await;
            guard.insert_error(peer_msg_id, frame, deadline)
        };
        self.handle_response_insert_outcome(outcome, MSG_ERROR)
            .await;
    }

    pub(super) async fn claim_next(&self, callis_id: CallisId) -> Option<RetainedClaim> {
        let (claim, has_more) = {
            let mut guard = self.state.lock().await;
            let claim = guard.claim_next(callis_id);
            let has_more = claim.is_some() && guard.has_dispatchable_work();
            (claim, has_more)
        };
        if has_more {
            self.available.notify_one();
        }
        claim
    }

    pub(super) async fn complete_write(
        &self,
        claim: RetainedClaim,
        result: Result<(), AureliaError>,
    ) {
        let effects = {
            let mut guard = self.state.lock().await;
            guard.complete_write(claim, result)
        };
        self.apply_effects(effects);
        self.notify_work_changed();
    }

    pub(super) async fn ack(&self, peer_msg_id: PeerMessageId) -> bool {
        let effect = {
            let mut guard = self.state.lock().await;
            guard.ack(peer_msg_id)
        };
        match effect {
            Some(effect) => {
                self.apply_effects(vec![effect]);
                self.notify_work_changed();
                true
            }
            None => false,
        }
    }

    pub(super) async fn fail_one(&self, peer_msg_id: PeerMessageId, error: AureliaError) -> bool {
        let effect = {
            let mut guard = self.state.lock().await;
            guard.fail_one(peer_msg_id, error)
        };
        match effect {
            Some(effect) => {
                self.apply_effects(vec![effect]);
                self.notify_work_changed();
                true
            }
            None => false,
        }
    }

    pub(super) async fn fail_all(&self, error: AureliaError) {
        let effects = {
            let mut guard = self.state.lock().await;
            guard.fail_all(error)
        };
        self.apply_effects(effects);
        self.notify_work_changed();
    }

    pub(super) async fn fail_non_a1(&self, error: AureliaError) {
        let effects = {
            let mut guard = self.state.lock().await;
            guard.fail_non_a1(error)
        };
        self.apply_effects(effects);
        self.notify_work_changed();
    }

    pub(super) async fn begin_shutdown(&self, error: AureliaError) {
        self.shutdown.store(true, Ordering::SeqCst);
        let effects = {
            let mut guard = self.state.lock().await;
            guard.begin_shutdown(error)
        };
        self.apply_effects(effects);
        self.notify_work_changed();
    }

    pub(super) async fn drop_responses(&self) {
        let mut guard = self.state.lock().await;
        guard.drop_responses();
        drop(guard);
        self.notify_work_changed();
    }

    pub(super) async fn request_close(&self, callis_id: CallisId) {
        let mut guard = self.close_state.lock().await;
        guard.intents.insert(callis_id);
        let notify = guard.waiters.get(&callis_id).cloned();
        drop(guard);
        if let Some(notify) = notify {
            notify.notify_one();
        }
    }

    pub(super) async fn take_close_intent(&self, callis_id: CallisId) -> bool {
        let mut guard = self.close_state.lock().await;
        let found = guard.intents.remove(&callis_id);
        if found {
            guard.waiters.remove(&callis_id);
        }
        found
    }

    pub(super) async fn close_notifier(&self, callis_id: CallisId) -> Arc<Notify> {
        let mut guard = self.close_state.lock().await;
        Arc::clone(
            guard
                .waiters
                .entry(callis_id)
                .or_insert_with(|| Arc::new(Notify::new())),
        )
    }

    pub(super) async fn clear_close_intent(&self, callis_id: CallisId) {
        let mut guard = self.close_state.lock().await;
        guard.intents.remove(&callis_id);
        guard.waiters.remove(&callis_id);
    }

    pub(super) async fn set_capacities(&self, send_queue_size: usize) {
        let capacities = RetainedCapacities::from_send_queue_size(send_queue_size);
        let mut guard = self.state.lock().await;
        guard.set_capacities(capacities);
        drop(guard);
        self.notify_work_changed();
    }

    pub(super) async fn mark_all_tracked_replay_ready(&self) {
        let mut guard = self.state.lock().await;
        guard.mark_all_tracked_replay_ready();
        drop(guard);
        self.notify_work_changed();
    }

    pub(super) async fn mark_tracked_replay_ready(&self, peer_msg_ids: &[PeerMessageId]) {
        let mut guard = self.state.lock().await;
        guard.mark_tracked_replay_ready(peer_msg_ids);
        drop(guard);
        self.notify_work_changed();
    }

    pub(super) async fn mark_callis_replay_ready(&self, callis_id: CallisId) {
        let mut guard = self.state.lock().await;
        guard.mark_callis_replay_ready(callis_id);
        drop(guard);
        self.clear_close_intent(callis_id).await;
        self.notify_work_changed();
    }

    pub(super) async fn tracked_messages(&self) -> Vec<PeerMessage> {
        let guard = self.state.lock().await;
        guard.tracked_messages()
    }

    pub(super) async fn message(&self, peer_msg_id: PeerMessageId) -> Option<PeerMessage> {
        let guard = self.state.lock().await;
        guard.message(peer_msg_id)
    }

    pub(super) async fn deadline(&self, peer_msg_id: PeerMessageId) -> Option<Instant> {
        let guard = self.state.lock().await;
        guard.deadline(peer_msg_id)
    }

    pub(super) async fn contains_tracked(&self, peer_msg_id: PeerMessageId) -> bool {
        let guard = self.state.lock().await;
        guard.contains_tracked(peer_msg_id)
    }

    pub(super) async fn tracked_state(&self, peer_msg_id: PeerMessageId) -> Option<SlotState> {
        let guard = self.state.lock().await;
        guard.tracked_state(peer_msg_id)
    }

    pub(super) async fn wait_for_empty(&self, deadline: Instant) -> bool {
        loop {
            let notified = self.empty.notified();
            tokio::pin!(notified);
            if self.is_empty().await {
                return true;
            }
            if tokio::time::timeout_at(deadline, &mut notified)
                .await
                .is_err()
            {
                return false;
            }
        }
    }

    pub(super) async fn wait_for_a1_response_empty(&self, deadline: Instant) -> bool {
        loop {
            let notified = self.a1_response_empty.notified();
            tokio::pin!(notified);
            if self.response_lanes_empty().await {
                return true;
            }
            if tokio::time::timeout_at(deadline, &mut notified)
                .await
                .is_err()
            {
                return false;
            }
        }
    }

    pub(super) fn notifier(&self) -> &Notify {
        &self.available
    }

    pub(super) async fn is_empty(&self) -> bool {
        let guard = self.state.lock().await;
        guard.is_empty()
    }

    pub(super) async fn response_lanes_empty(&self) -> bool {
        let guard = self.state.lock().await;
        guard.response_lanes_empty()
    }

    async fn handle_response_insert_outcome(
        &self,
        outcome: ResponseInsertOutcome,
        msg_type: MessageType,
    ) {
        match outcome {
            ResponseInsertOutcome::Inserted => self.notify_work_changed(),
            ResponseInsertOutcome::Duplicate {
                attempted,
                peer_msg_id,
            } => self.log_duplicate_response(attempted, peer_msg_id).await,
            ResponseInsertOutcome::Full { lane, peer_msg_id } => {
                self.report_overrun(lane, msg_type, None).await;
                warn!(peer_msg_id, ?lane, "outbound retained response lane full");
            }
            ResponseInsertOutcome::Shutdown { .. } | ResponseInsertOutcome::Expired { .. } => {}
        }
    }

    async fn report_overrun(
        &self,
        lane: RetainedLane,
        msg_type: MessageType,
        error: Option<String>,
    ) {
        let Some(reporter) = self.overrun_reporter.as_ref() else {
            if let Some(error) = error {
                warn!(error = %error, ?lane, "dropping outbound retained item after overrun");
            }
            return;
        };
        let Some(peer) = reporter.peer.lock().await.clone() else {
            return;
        };
        let limit = RetainedCapacities::from_send_queue_size(
            reporter.config.snapshot().await.send_queue_size,
        )
        .capacity(lane) as u64;
        let tier = retained_lane_report(lane);
        aurelia_logging::warn_limited!(
            reporter.config.limited_registry(),
            aurelia_ids::LogId::OutboundQueueOverrun,
            peer = %peer,
            tier = ?tier,
            limit,
            msg_type,
            "outbound retained queue rejected admission"
        );
        reporter
            .observability
            .outbound_queue_overrun(peer, tier, limit, msg_type);
    }

    async fn log_duplicate_response(&self, attempted: ResponseKind, peer_msg_id: PeerMessageId) {
        let Some(reporter) = self.overrun_reporter.as_ref() else {
            match attempted {
                ResponseKind::Ack => {
                    warn!(
                        peer_msg_id,
                        "duplicate outbound ACK response attempted; dropping"
                    );
                }
                ResponseKind::Error => {
                    warn!(
                        peer_msg_id,
                        "duplicate outbound ERROR response attempted; dropping"
                    );
                }
            }
            return;
        };
        match attempted {
            ResponseKind::Ack => {
                aurelia_logging::warn_limited!(
                    reporter.config.limited_registry(),
                    aurelia_ids::LogId::DuplicateOutboundAck,
                    peer_msg_id,
                    "duplicate outbound ACK response attempted; dropping"
                );
            }
            ResponseKind::Error => {
                aurelia_logging::warn_limited!(
                    reporter.config.limited_registry(),
                    aurelia_ids::LogId::DuplicateOutboundError,
                    peer_msg_id,
                    "duplicate outbound ERROR response attempted; dropping"
                );
            }
        }
    }

    fn apply_effects(&self, effects: Vec<CompletionEffect>) {
        for effect in effects {
            let _ = effect.ack_tx.send(effect.result);
        }
    }

    fn notify_work_changed(&self) {
        self.available.notify_one();
        self.reaper.notify_waiters();
        self.empty.notify_waiters();
        self.a1_response_empty.notify_waiters();
    }
}

#[cfg(test)]
impl PrimaryOutboundStore {
    async fn expire_due_for_tests(&self, now: Instant) {
        let effects = {
            let mut guard = self.state.lock().await;
            guard.expire_due(now)
        };
        self.apply_effects(effects);
        self.notify_work_changed();
    }

    async fn live_count_for_tests(&self, lane: RetainedLane) -> usize {
        let guard = self.state.lock().await;
        guard.live_count(lane)
    }

    async fn close_waiter_count_for_tests(&self) -> usize {
        self.close_state.lock().await.waiters.len()
    }
}

impl Drop for PrimaryOutboundStore {
    fn drop(&mut self) {
        self.reaper.notify_waiters();
        self.available.notify_waiters();
        self.empty.notify_waiters();
        self.a1_response_empty.notify_waiters();
    }
}

impl PrimaryOutboundReaper {
    pub(super) async fn run(self) {
        loop {
            let notified = self.reaper.notified();
            tokio::pin!(notified);

            let Some(queue) = self.queue.upgrade() else {
                break;
            };
            if queue.shutdown.load(Ordering::SeqCst) {
                break;
            }
            let deadline = {
                let mut guard = queue.state.lock().await;
                guard.next_deadline()
            };
            let Some(deadline) = deadline else {
                drop(queue);
                notified.await;
                continue;
            };
            let now = Instant::now();
            if deadline > now {
                let sleep = tokio::time::sleep_until(deadline);
                tokio::pin!(sleep);
                drop(queue);
                tokio::select! {
                    _ = &mut sleep => {}
                    _ = &mut notified => continue,
                }
            } else {
                drop(queue);
            }

            let Some(queue) = self.queue.upgrade() else {
                break;
            };
            if queue.shutdown.load(Ordering::SeqCst) {
                break;
            }
            queue.expire_due_for_reaper(Instant::now()).await;
        }
    }
}

impl PrimaryOutboundStore {
    async fn expire_due_for_reaper(&self, now: Instant) {
        let effects = {
            let mut guard = self.state.lock().await;
            guard.expire_due(now)
        };
        self.apply_effects(effects);
        self.notify_work_changed();
    }
}

fn retained_lane_report(lane: RetainedLane) -> OutboundQueueTierReport {
    match lane {
        RetainedLane::A1Ack | RetainedLane::A1Error => OutboundQueueTierReport::A1,
        RetainedLane::A2 => OutboundQueueTierReport::A2,
        RetainedLane::A3 => OutboundQueueTierReport::A3,
    }
}

fn tracked_lane_for_msg_type(msg_type: MessageType) -> RetainedLane {
    match classify_priority(msg_type) {
        PriorityTier::A1 => unreachable!("completion-bearing A1 messages are not retained"),
        PriorityTier::A2 => RetainedLane::A2,
        PriorityTier::A3 => RetainedLane::A3,
    }
}

fn frame_peer_msg_id(frame: &OutboundFrame) -> PeerMessageId {
    match frame {
        OutboundFrame::Ack { peer_msg_id }
        | OutboundFrame::Control { peer_msg_id, .. }
        | OutboundFrame::Message(PeerMessage { peer_msg_id, .. }) => *peer_msg_id,
    }
}

#[cfg(test)]
#[path = "../tests/leaf/primary_dispatch_retained.rs"]
mod tests;
