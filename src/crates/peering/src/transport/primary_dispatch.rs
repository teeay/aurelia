// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;

use std::sync::Arc;

use aurelia_ids::MessagePriorityClass;

mod retained;
mod store;

use retained::{PrimaryOutboundReaper, PrimaryOutboundStore};
use store::{RetainedClaim, RetainedWriteItem};

pub(crate) const A1_RESPONSE_TTL: Duration = Duration::from_secs(600);
const A2_CAPACITY: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PriorityTier {
    A1,
    A2,
    A3,
}

pub(crate) struct PrimaryDispatchManager {
    retained: Arc<PrimaryOutboundStore>,
}

#[derive(Clone)]
pub(crate) struct OutboundQueueOverrunReporter {
    pub(crate) peer: Arc<Mutex<Option<DomusAddr>>>,
    pub(crate) config: DomusConfigAccess,
    pub(crate) observability: ObservabilityHandle,
}

pub(crate) struct PrimaryDispatchManagerTasks {
    retained_reaper: PrimaryOutboundReaper,
}

pub(crate) struct PrimaryDispatchManagerContext {
    pub(crate) initial_send_queue_size: usize,
    pub(crate) overrun_reporter: Option<OutboundQueueOverrunReporter>,
}

impl PrimaryDispatchManagerTasks {
    pub(crate) async fn run(self) {
        self.retained_reaper.run().await;
    }
}

impl PrimaryDispatchManager {
    pub(crate) fn new(
        context: PrimaryDispatchManagerContext,
    ) -> (Arc<Self>, PrimaryDispatchManagerTasks) {
        let PrimaryDispatchManagerContext {
            initial_send_queue_size,
            overrun_reporter,
        } = context;
        let retained = PrimaryOutboundStore::new(initial_send_queue_size.max(1), overrun_reporter);
        let retained_reaper = retained.reaper();
        (
            Arc::new(Self { retained }),
            PrimaryDispatchManagerTasks { retained_reaper },
        )
    }

    pub(super) async fn enqueue_a1_frame(&self, frame: OutboundFrame) {
        match frame {
            OutboundFrame::Ack { peer_msg_id } => {
                self.retained
                    .enqueue_ack(peer_msg_id, Instant::now() + A1_RESPONSE_TTL)
                    .await;
            }
            frame @ OutboundFrame::Control { msg_type, .. } if msg_type == MSG_ERROR => {
                self.retained
                    .enqueue_error(frame, Instant::now() + A1_RESPONSE_TTL)
                    .await;
            }
            OutboundFrame::Control {
                msg_type: MSG_KEEPALIVE,
                ..
            } => {
                warn!(
                    msg_type = frame_msg_type(&frame),
                    "immediate primary control frame attempted through retained dispatch"
                );
            }
            frame => {
                let msg_type = frame_msg_type(&frame);
                warn!(
                    msg_type,
                    "unsupported bare A1 frame attempted through retained dispatch"
                );
            }
        }
    }

    pub(crate) async fn enqueue_new(
        &self,
        message: PeerMessage,
        deadline: Instant,
    ) -> Result<oneshot::Receiver<Result<(), AureliaError>>, AureliaError> {
        self.retained.enqueue_message(message, deadline).await
    }

    pub(in crate::transport) async fn claim_next(
        &self,
        callis_id: CallisId,
    ) -> Option<RetainedClaim> {
        self.retained.claim_next(callis_id).await
    }

    pub(in crate::transport) async fn complete_claim(
        &self,
        claim: RetainedClaim,
        result: Result<(), AureliaError>,
    ) {
        self.retained.complete_write(claim, result).await;
    }

    pub(super) async fn is_empty(&self) -> bool {
        self.retained.is_empty().await
    }

    pub(crate) async fn has_entries(&self) -> bool {
        !self.retained.is_empty().await
    }

    pub(crate) async fn wait_for_a1_response_empty(&self, deadline: Instant) -> bool {
        self.retained.wait_for_a1_response_empty(deadline).await
    }

    pub(crate) async fn clear(&self) {
        self.retained
            .fail_all(AureliaError::new(ErrorId::PeerRestarted))
            .await;
    }

    pub(crate) async fn mark_callis_replay_ready(&self, callis_id: CallisId) {
        self.retained.mark_callis_replay_ready(callis_id).await;
    }

    pub(crate) async fn mark_tracked_replay_ready(&self, pending: Vec<PeerMessageId>) {
        self.retained.mark_tracked_replay_ready(&pending).await;
    }

    pub(crate) async fn begin_shutdown(&self, error: AureliaError) {
        self.retained.begin_shutdown(error).await;
    }

    pub(crate) async fn drop_a1_responses(&self) {
        self.retained.drop_responses().await;
    }

    pub(crate) async fn request_close(&self, callis_id: CallisId) {
        self.retained.request_close(callis_id).await;
    }

    pub(in crate::transport) async fn take_close_intent(&self, callis_id: CallisId) -> bool {
        self.retained.take_close_intent(callis_id).await
    }

    pub(in crate::transport) async fn close_notifier(&self, callis_id: CallisId) -> Arc<Notify> {
        self.retained.close_notifier(callis_id).await
    }

    pub(in crate::transport) async fn clear_close_intent(&self, callis_id: CallisId) {
        self.retained.clear_close_intent(callis_id).await;
    }

    pub(crate) fn notifier(&self) -> &Notify {
        self.retained.notifier()
    }

    pub(crate) async fn deadline(&self, peer_msg_id: PeerMessageId) -> Option<Instant> {
        self.retained.deadline(peer_msg_id).await
    }

    pub(crate) async fn set_capacity(&self, send_queue_size: usize) {
        self.retained.set_capacities(send_queue_size).await;
    }

    pub(crate) async fn ack(&self, peer_msg_id: PeerMessageId) -> bool {
        if self.retained.ack(peer_msg_id).await {
            trace!(peer_msg_id, "outbound message acked");
            true
        } else {
            false
        }
    }

    pub(crate) async fn fail_one(&self, peer_msg_id: PeerMessageId, error: AureliaError) -> bool {
        if self.retained.fail_one(peer_msg_id, error).await {
            debug!(peer_msg_id, "outbound message failed");
            true
        } else {
            false
        }
    }

    pub(crate) async fn fail_all(&self, error: AureliaError) -> Vec<PeerMessage> {
        let messages = self.retained.tracked_messages().await;
        self.retained.fail_all(error).await;
        messages
    }

    pub(crate) async fn fail_non_a1(&self, error: AureliaError) {
        self.retained.fail_non_a1(error).await;
    }

    pub(crate) async fn inflight_messages(&self) -> Vec<PeerMessage> {
        self.retained.tracked_messages().await
    }
}

#[cfg(test)]
impl PrimaryDispatchManager {
    pub(crate) fn new_for_tests(runtime_handle: tokio::runtime::Handle) -> Arc<Self> {
        let (queue, tasks) = Self::new(PrimaryDispatchManagerContext {
            initial_send_queue_size: DomusConfig::default().send_queue_size,
            overrun_reporter: None,
        });
        runtime_handle.spawn(async move { tasks.run().await });
        queue
    }

    pub(super) async fn pop_a1_frame(&self) -> Option<OutboundFrame> {
        let claim = self.retained.claim_next(0).await?;
        let frame = match &claim.item {
            RetainedWriteItem::Ack { peer_msg_id } => OutboundFrame::Ack {
                peer_msg_id: *peer_msg_id,
            },
            RetainedWriteItem::Error { frame, .. } => (**frame).clone(),
            RetainedWriteItem::Message { .. } => {
                self.retained
                    .complete_write(claim, Err(AureliaError::new(ErrorId::ConnectionLost)))
                    .await;
                return None;
            }
        };
        self.retained.complete_write(claim, Ok(())).await;
        Some(frame)
    }

    pub(crate) async fn mark_dispatched_for_tests(
        &self,
        peer_msg_id: PeerMessageId,
    ) -> Result<(), AureliaError> {
        let Some(claim) = self.retained.claim_next(0).await else {
            return Err(AureliaError::new(ErrorId::SendTimeout));
        };
        if claim.item.peer_msg_id() == peer_msg_id {
            self.retained.complete_write(claim, Ok(())).await;
            Ok(())
        } else {
            self.retained
                .complete_write(claim, Err(AureliaError::new(ErrorId::ConnectionLost)))
                .await;
            Err(AureliaError::new(ErrorId::SendTimeout))
        }
    }

    pub(crate) async fn message(&self, peer_msg_id: PeerMessageId) -> Option<PeerMessage> {
        self.retained.message(peer_msg_id).await
    }

    pub(crate) async fn is_inflight(&self, peer_msg_id: PeerMessageId) -> Option<bool> {
        self.retained.tracked_state(peer_msg_id).await.map(|state| {
            matches!(
                state,
                store::SlotState::Inflight { .. } | store::SlotState::Writing { .. }
            )
        })
    }
}

pub(in crate::transport) fn claim_to_frame(claim: &RetainedClaim) -> OutboundFrame {
    match &claim.item {
        RetainedWriteItem::Ack { peer_msg_id } => OutboundFrame::Ack {
            peer_msg_id: *peer_msg_id,
        },
        RetainedWriteItem::Error { frame, .. } => (**frame).clone(),
        RetainedWriteItem::Message { message, .. } => OutboundFrame::Message((**message).clone()),
    }
}

#[cfg(test)]
fn claim_peer_msg_id(claim: &RetainedClaim) -> PeerMessageId {
    claim.item.peer_msg_id()
}

pub(in crate::transport) fn claim_message_id(claim: &RetainedClaim) -> Option<PeerMessageId> {
    match &claim.item {
        RetainedWriteItem::Message { peer_msg_id, .. } => Some(*peer_msg_id),
        RetainedWriteItem::Ack { .. } | RetainedWriteItem::Error { .. } => None,
    }
}

pub(super) fn classify_priority(msg_type: MessageType) -> PriorityTier {
    match aurelia_ids::classify_message_priority(msg_type) {
        MessagePriorityClass::A1 => PriorityTier::A1,
        MessagePriorityClass::A2 => PriorityTier::A2,
        MessagePriorityClass::A3 => PriorityTier::A3,
    }
}

fn frame_msg_type(frame: &OutboundFrame) -> MessageType {
    match frame {
        OutboundFrame::Ack { .. } => MSG_ACK,
        OutboundFrame::Message(message) => message.msg_type,
        OutboundFrame::Control { msg_type, .. } => *msg_type,
    }
}

#[cfg(test)]
#[path = "tests/leaf/primary_dispatch_overrun.rs"]
mod overrun_tests;
