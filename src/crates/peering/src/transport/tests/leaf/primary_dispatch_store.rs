// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn store(send_queue_size: usize) -> RetainedStore {
    RetainedStore::new(RetainedCapacities::from_send_queue_size(send_queue_size))
}

fn tracked(
    peer_msg_id: PeerMessageId,
    msg_type: MessageType,
) -> (TrackedOutbound, oneshot::Receiver<Result<(), AureliaError>>) {
    let (ack_tx, ack_rx) = oneshot::channel();
    (
        TrackedOutbound::new(
            PeerMessage {
                peer_msg_id,
                src_taberna: 1,
                dst_taberna: 2,
                msg_type,
                flags: 0,
                payload: Bytes::from_static(b"payload"),
            },
            ack_tx,
        ),
        ack_rx,
    )
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}

fn error_frame(peer_msg_id: PeerMessageId) -> OutboundFrame {
    OutboundFrame::Control {
        msg_type: MSG_ERROR,
        peer_msg_id,
        payload: Bytes::from_static(b"error"),
    }
}

#[test]
fn retained_capacities_follow_send_queue_size() {
    let capacities = RetainedCapacities::from_send_queue_size(8);

    assert_eq!(capacities.a1_ack, 128);
    assert_eq!(capacities.a1_error, 16);
    assert_eq!(capacities.a2, A2_CAPACITY);
    assert_eq!(capacities.a3, 8);
}

#[test]
fn lane_capacity_shrink_rejects_until_live_count_drops_then_growth_admits() {
    let mut store = store(2);
    let (first, _rx1) = tracked(1, crate::a3_message_type(0));
    let (second, _rx2) = tracked(2, crate::a3_message_type(0));
    store.insert_tracked(first, deadline()).expect("first");
    store.insert_tracked(second, deadline()).expect("second");

    store.set_capacities(RetainedCapacities::from_send_queue_size(1));
    assert_eq!(store.target_capacity(RetainedLane::A3), 1);
    assert_eq!(store.live_count(RetainedLane::A3), 2);

    let (third, _rx3) = tracked(3, crate::a3_message_type(0));
    assert!(matches!(
        store.insert_tracked(third, deadline()),
        Err(RetainedInsertError::Full(_))
    ));

    let effect = store
        .ack(1)
        .expect("acking one item releases one retained slot");
    let _ = effect.ack_tx.send(effect.result);
    let (fourth, _rx4) = tracked(4, crate::a3_message_type(0));
    assert!(matches!(
        store.insert_tracked(fourth, deadline()),
        Err(RetainedInsertError::Full(_))
    ));

    store.set_capacities(RetainedCapacities::from_send_queue_size(3));
    let (fifth, _rx5) = tracked(5, crate::a3_message_type(0));
    store
        .insert_tracked(fifth, deadline())
        .expect("growth admits");
}

#[test]
fn response_insertion_deduplicates_before_capacity_checks() {
    let mut store = store(1);

    assert_eq!(
        store.insert_ack(10, deadline()),
        ResponseInsertOutcome::Inserted
    );
    assert_eq!(
        store.insert_ack(10, deadline()),
        ResponseInsertOutcome::Duplicate {
            attempted: ResponseKind::Ack,
            peer_msg_id: 10
        }
    );
    assert_eq!(
        store.insert_error(10, error_frame(10), deadline()),
        ResponseInsertOutcome::Duplicate {
            attempted: ResponseKind::Error,
            peer_msg_id: 10
        }
    );
}

#[test]
fn tracked_a1_messages_are_rejected() {
    let mut store = store(1);
    let (tracked, _rx) = tracked(1, 0x0000_0001);

    assert!(matches!(
        store.insert_tracked(tracked, deadline()),
        Err(RetainedInsertError::A1Tracked(_))
    ));
    assert_eq!(store.live_count(RetainedLane::A1Error), 0);
}

#[test]
fn strict_priority_prefers_a1_ack_then_a1_error_then_a2_then_a3() {
    let mut store = store(4);
    let (a2, _rx2) = tracked(2, 0x0001_0000);
    let (a3, _rx3) = tracked(3, crate::a3_message_type(0));
    store.insert_tracked(a3, deadline()).expect("a3");
    store.insert_tracked(a2, deadline()).expect("a2");
    assert_eq!(
        store.insert_error(4, error_frame(4), deadline()),
        ResponseInsertOutcome::Inserted
    );
    assert_eq!(
        store.insert_ack(1, deadline()),
        ResponseInsertOutcome::Inserted
    );

    assert_eq!(store.claim_next(7).expect("ack").item.peer_msg_id(), 1);
    assert_eq!(store.claim_next(7).expect("error").item.peer_msg_id(), 4);
    assert_eq!(store.claim_next(7).expect("a2").item.peer_msg_id(), 2);
    assert_eq!(store.claim_next(7).expect("a3").item.peer_msg_id(), 3);
}

#[test]
fn replay_ready_is_selected_before_fresh_work_in_same_tracked_lane() {
    let mut store = store(4);
    let (first, _rx1) = tracked(1, crate::a3_message_type(0));
    let (second, _rx2) = tracked(2, crate::a3_message_type(0));
    store.insert_tracked(first, deadline()).expect("first");
    store.insert_tracked(second, deadline()).expect("second");

    let claim = store.claim_next(10).expect("first claim");
    assert_eq!(claim.item.peer_msg_id(), 1);
    store.complete_write(claim, Ok(()));
    store.mark_callis_replay_ready(10);

    let replay = store.claim_next(11).expect("replay");
    assert_eq!(replay.item.peer_msg_id(), 1);
    let fresh = store.claim_next(11).expect("fresh");
    assert_eq!(fresh.item.peer_msg_id(), 2);
}

#[test]
fn write_success_retains_tracked_item_until_ack_but_clears_bare_responses() {
    let mut store = store(2);
    let (tracked_item, mut ack_rx) = tracked(1, crate::a3_message_type(0));
    store
        .insert_tracked(tracked_item, deadline())
        .expect("tracked");
    assert_eq!(
        store.insert_ack(2, deadline()),
        ResponseInsertOutcome::Inserted
    );

    let ack_claim = store.claim_next(5).expect("ack claim");
    assert_eq!(ack_claim.item.peer_msg_id(), 2);
    let tracked_claim = store.claim_next(5).expect("tracked claim");
    assert_eq!(tracked_claim.item.peer_msg_id(), 1);

    store.complete_write(ack_claim, Ok(()));
    assert!(store.response_lanes_empty());

    store.complete_write(tracked_claim, Ok(()));
    let effect = store.ack(1).expect("acked tracked item");
    assert_eq!(effect.peer_msg_id, 1);
    effect.ack_tx.send(effect.result).expect("send ack result");
    assert_eq!(ack_rx.try_recv().expect("ack received").expect("ok"), ());
}

#[test]
fn callis_teardown_recovers_writing_and_inflight_slots() {
    let mut store = store(4);
    let (writing, _rx1) = tracked(1, crate::a3_message_type(0));
    let (inflight, _rx2) = tracked(2, crate::a3_message_type(0));
    store.insert_tracked(writing, deadline()).expect("writing");
    store
        .insert_tracked(inflight, deadline())
        .expect("inflight");

    let writing_claim = store.claim_next(20).expect("writing claim");
    let inflight_claim = store.claim_next(20).expect("inflight claim");
    store.complete_write(inflight_claim, Ok(()));

    store.mark_callis_replay_ready(20);
    let replay_a = store.claim_next(21).expect("writing recovered");
    let replay_b = store.claim_next(21).expect("inflight recovered");
    assert_eq!(
        replay_a.item.peer_msg_id(),
        writing_claim.item.peer_msg_id()
    );
    assert_eq!(replay_b.item.peer_msg_id(), 2);
}

#[test]
fn deadline_expiry_ignores_stale_deadline_entries_and_fails_tracked_items() {
    let mut store = store(2);
    let (first, mut first_rx) = tracked(1, crate::a3_message_type(0));
    let original_deadline = Instant::now() + Duration::from_millis(1);
    store
        .insert_tracked(first, original_deadline)
        .expect("first");
    let effect = store.ack(1).expect("ack removes first");
    let _ = effect.ack_tx.send(effect.result);

    let (second, mut second_rx) = tracked(2, crate::a3_message_type(0));
    let second_deadline = Instant::now() + Duration::from_millis(5);
    store
        .insert_tracked(second, second_deadline)
        .expect("second reuses capacity");

    assert!(store
        .expire_due(original_deadline + Duration::from_millis(1))
        .is_empty());
    assert!(first_rx.try_recv().expect("first completed").is_ok());

    let effects = store.expire_due(second_deadline + Duration::from_millis(1));
    assert_eq!(effects.len(), 1);
    let effect = effects.into_iter().next().expect("effect");
    effect.ack_tx.send(effect.result).expect("send timeout");
    let err = second_rx
        .try_recv()
        .expect("second completed")
        .expect_err("timeout");
    assert_eq!(err.kind, ErrorId::SendTimeout);
}

#[test]
fn shutdown_rejects_new_work_and_fails_non_response_tracked_items() {
    let mut store = store(2);
    let (tracked_item, mut rx) = tracked(1, crate::a3_message_type(0));
    store
        .insert_tracked(tracked_item, deadline())
        .expect("tracked");
    assert_eq!(
        store.insert_ack(2, deadline()),
        ResponseInsertOutcome::Inserted
    );

    let effects = store.begin_shutdown(AureliaError::new(ErrorId::PeerUnavailable));
    assert_eq!(effects.len(), 1);
    let effect = effects.into_iter().next().expect("effect");
    effect.ack_tx.send(effect.result).expect("send failure");
    let err = rx
        .try_recv()
        .expect("failed")
        .expect_err("peer unavailable");
    assert_eq!(err.kind, ErrorId::PeerUnavailable);

    let (new_item, _rx) = tracked(3, crate::a3_message_type(0));
    assert!(matches!(
        store.insert_tracked(new_item, deadline()),
        Err(RetainedInsertError::Shutdown(_))
    ));
    assert_eq!(
        store.insert_ack(3, deadline()),
        ResponseInsertOutcome::Shutdown { peer_msg_id: 3 }
    );
}
