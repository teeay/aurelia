// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;

use std::collections::{HashMap, VecDeque};

use tokio::sync::oneshot;

use crate::limiter::LimiterPermit;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PriorityTier {
    A1,
    A2,
    A3,
}

#[derive(Clone)]
pub(super) struct A1Frame {
    frame: OutboundFrame,
    target: Option<CallisId>,
}

enum A1Item {
    Frame(A1Frame),
    Message(PeerMessageId),
}

pub(super) enum DispatchItem {
    A1Frame(A1Frame),
    Message {
        tier: PriorityTier,
        peer_msg_id: PeerMessageId,
    },
}

struct DispatchEntry {
    message: PeerMessage,
    ack_tx: oneshot::Sender<Result<(), AureliaError>>,
    deadline: Instant,
    queue_permit: Option<LimiterPermit>,
    inflight_permit: Option<LimiterPermit>,
}

struct DispatchState {
    a1: VecDeque<A1Item>,
    a2: VecDeque<PeerMessageId>,
    a3: VecDeque<PeerMessageId>,
    entries: HashMap<PeerMessageId, DispatchEntry>,
}

pub(crate) struct PrimaryDispatchQueue {
    state: Mutex<DispatchState>,
    notify: Notify,
    inflight_notify: Notify,
}

impl PrimaryDispatchQueue {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(DispatchState {
                a1: VecDeque::new(),
                a2: VecDeque::new(),
                a3: VecDeque::new(),
                entries: HashMap::new(),
            }),
            notify: Notify::new(),
            inflight_notify: Notify::new(),
        })
    }

    pub(super) async fn enqueue_a1_frame(&self, frame: OutboundFrame, target: Option<CallisId>) {
        let mut guard = self.state.lock().await;
        guard.a1.push_back(A1Item::Frame(A1Frame { frame, target }));
        drop(guard);
        self.notify.notify_one();
    }

    pub(crate) async fn enqueue_new(
        &self,
        message: PeerMessage,
        deadline: Instant,
        queue_permit: LimiterPermit,
    ) -> oneshot::Receiver<Result<(), AureliaError>> {
        let (tx, rx) = oneshot::channel();
        let peer_msg_id = message.peer_msg_id;
        let mut guard = self.state.lock().await;
        if guard.entries.contains_key(&peer_msg_id) {
            warn!(peer_msg_id, "replacing existing outbound entry");
        }
        let msg_type = message.msg_type;
        guard.entries.insert(
            peer_msg_id,
            DispatchEntry {
                message,
                ack_tx: tx,
                deadline,
                queue_permit: Some(queue_permit),
                inflight_permit: None,
            },
        );
        match classify_priority(msg_type) {
            PriorityTier::A1 => guard.a1.push_back(A1Item::Message(peer_msg_id)),
            PriorityTier::A2 => guard.a2.push_back(peer_msg_id),
            PriorityTier::A3 => guard.a3.push_back(peer_msg_id),
        }
        drop(guard);
        self.notify.notify_one();
        rx
    }

    pub(super) async fn requeue_back(&self, item: DispatchItem) {
        let mut guard = self.state.lock().await;
        match item {
            DispatchItem::A1Frame(frame) => guard.a1.push_back(A1Item::Frame(frame)),
            DispatchItem::Message { tier, peer_msg_id } => match tier {
                PriorityTier::A1 => guard.a1.push_back(A1Item::Message(peer_msg_id)),
                PriorityTier::A2 => guard.a2.push_back(peer_msg_id),
                PriorityTier::A3 => guard.a3.push_back(peer_msg_id),
            },
        }
    }

    pub(super) async fn push_front_many(&self, pending: Vec<PeerMessageId>) {
        let mut guard = self.state.lock().await;
        for peer_msg_id in pending {
            let Some(entry) = guard.entries.get(&peer_msg_id) else {
                continue;
            };
            match classify_priority(entry.message.msg_type) {
                PriorityTier::A1 => guard.a1.push_front(A1Item::Message(peer_msg_id)),
                PriorityTier::A2 => guard.a2.push_front(peer_msg_id),
                PriorityTier::A3 => guard.a3.push_front(peer_msg_id),
            }
        }
        drop(guard);
        self.notify.notify_one();
    }

    pub(super) async fn pop_next(&self) -> Option<DispatchItem> {
        let mut guard = self.state.lock().await;
        while let Some(item) = guard.a1.pop_front() {
            match item {
                A1Item::Frame(frame) => return Some(DispatchItem::A1Frame(frame)),
                A1Item::Message(peer_msg_id) => {
                    if guard.entries.contains_key(&peer_msg_id) {
                        return Some(DispatchItem::Message {
                            tier: PriorityTier::A1,
                            peer_msg_id,
                        });
                    }
                }
            }
        }
        while let Some(peer_msg_id) = guard.a2.pop_front() {
            if guard.entries.contains_key(&peer_msg_id) {
                return Some(DispatchItem::Message {
                    tier: PriorityTier::A2,
                    peer_msg_id,
                });
            }
        }
        while let Some(peer_msg_id) = guard.a3.pop_front() {
            if guard.entries.contains_key(&peer_msg_id) {
                return Some(DispatchItem::Message {
                    tier: PriorityTier::A3,
                    peer_msg_id,
                });
            }
        }
        None
    }

    pub(super) async fn is_empty(&self) -> bool {
        let guard = self.state.lock().await;
        guard.a1.is_empty() && guard.a2.is_empty() && guard.a3.is_empty()
    }

    pub(crate) async fn has_entries(&self) -> bool {
        let guard = self.state.lock().await;
        !guard.entries.is_empty()
    }

    pub(crate) async fn wait_for_inflight_empty(&self, deadline: Instant) -> bool {
        loop {
            if !self.has_entries().await {
                return true;
            }
            let notified = self.inflight_notify.notified();
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return false;
            }
        }
    }

    pub(crate) async fn clear(&self) {
        let mut guard = self.state.lock().await;
        guard.a1.clear();
        guard.a2.clear();
        guard.a3.clear();
    }

    pub(crate) async fn drain_new_on_close(&self, error: AureliaError) {
        let mut failed = Vec::new();
        let empty = {
            let mut guard = self.state.lock().await;
            guard.a1.retain(|item| matches!(item, A1Item::Frame(_)));
            guard.a2.clear();
            guard.a3.clear();
            let mut entries = std::mem::take(&mut guard.entries);
            let mut remaining = HashMap::new();
            for (peer_msg_id, entry) in entries.drain() {
                if entry.inflight_permit.is_some() {
                    remaining.insert(peer_msg_id, entry);
                } else {
                    failed.push(entry.ack_tx);
                }
            }
            guard.entries = remaining;
            guard.entries.is_empty()
        };
        if empty {
            self.inflight_notify.notify_waiters();
        }
        if !failed.is_empty() {
            warn!(
                count = failed.len(),
                error = %error,
                "failing queued outbound messages on close"
            );
        }
        for ack_tx in failed {
            let _ = ack_tx.send(Err(error.clone()));
        }
    }

    pub(crate) fn notifier(&self) -> &Notify {
        &self.notify
    }

    #[cfg(test)]
    pub(super) async fn pop_a1_frame(&self) -> Option<OutboundFrame> {
        let mut guard = self.state.lock().await;
        while let Some(item) = guard.a1.pop_front() {
            match item {
                A1Item::Frame(frame) => return Some(frame.frame),
                A1Item::Message(peer_msg_id) => {
                    if let Some(entry) = guard.entries.get(&peer_msg_id) {
                        return Some(OutboundFrame::Message(entry.message.clone()));
                    }
                }
            }
        }
        None
    }

    pub(crate) async fn message(&self, peer_msg_id: PeerMessageId) -> Option<PeerMessage> {
        let guard = self.state.lock().await;
        guard
            .entries
            .get(&peer_msg_id)
            .map(|entry| entry.message.clone())
    }

    pub(crate) async fn is_inflight(&self, peer_msg_id: PeerMessageId) -> Option<bool> {
        let guard = self.state.lock().await;
        guard
            .entries
            .get(&peer_msg_id)
            .map(|entry| entry.inflight_permit.is_some())
    }

    pub(crate) async fn deadline(&self, peer_msg_id: PeerMessageId) -> Option<Instant> {
        let guard = self.state.lock().await;
        guard.entries.get(&peer_msg_id).map(|entry| entry.deadline)
    }

    pub(crate) async fn attach_inflight_permit(
        &self,
        peer_msg_id: PeerMessageId,
        permit: LimiterPermit,
    ) -> bool {
        let mut guard = self.state.lock().await;
        if let Some(entry) = guard.entries.get_mut(&peer_msg_id) {
            entry.inflight_permit = Some(permit);
            true
        } else {
            false
        }
    }

    pub(crate) async fn detach_inflight_permit(&self, peer_msg_id: PeerMessageId) -> bool {
        let mut guard = self.state.lock().await;
        if let Some(entry) = guard.entries.get_mut(&peer_msg_id) {
            entry.inflight_permit.take();
            true
        } else {
            false
        }
    }

    pub(crate) async fn release_queue_permit(&self, peer_msg_id: PeerMessageId) -> bool {
        let mut guard = self.state.lock().await;
        if let Some(entry) = guard.entries.get_mut(&peer_msg_id) {
            entry.queue_permit.take();
            true
        } else {
            false
        }
    }

    pub(crate) async fn ack(&self, peer_msg_id: PeerMessageId) -> bool {
        let (entry, empty) = {
            let mut guard = self.state.lock().await;
            let entry = guard.entries.remove(&peer_msg_id);
            let empty = guard.entries.is_empty();
            (entry, empty)
        };
        if empty {
            self.inflight_notify.notify_waiters();
        }
        if let Some(entry) = entry {
            let _ = entry.ack_tx.send(Ok(()));
            trace!(peer_msg_id, "outbound message acked");
            true
        } else {
            false
        }
    }

    pub(crate) async fn fail_one(&self, peer_msg_id: PeerMessageId, error: AureliaError) -> bool {
        let (entry, empty) = {
            let mut guard = self.state.lock().await;
            let entry = guard.entries.remove(&peer_msg_id);
            let empty = guard.entries.is_empty();
            (entry, empty)
        };
        if empty {
            self.inflight_notify.notify_waiters();
        }
        if let Some(entry) = entry {
            let _ = entry.ack_tx.send(Err(error));
            debug!(peer_msg_id, "outbound message failed");
            true
        } else {
            false
        }
    }

    pub(crate) async fn fail_all(&self, error: AureliaError) -> Vec<InflightMessage> {
        let entries = {
            let mut guard = self.state.lock().await;
            guard.a1.clear();
            guard.a2.clear();
            guard.a3.clear();
            guard.entries.drain().collect::<Vec<_>>()
        };
        self.inflight_notify.notify_waiters();
        if !entries.is_empty() {
            warn!(
                count = entries.len(),
                error = %error,
                "failing all outbound messages"
            );
        }
        let mut messages = Vec::with_capacity(entries.len());
        for (_, entry) in entries {
            let _ = entry.ack_tx.send(Err(error.clone()));
            messages.push(InflightMessage {
                peer_msg_id: entry.message.peer_msg_id,
                src_taberna: entry.message.src_taberna,
                dst_taberna: entry.message.dst_taberna,
                msg_type: entry.message.msg_type,
                flags: entry.message.flags,
                payload: entry.message.payload,
            });
        }
        messages
    }

    pub(crate) async fn fail_non_a1(&self, error: AureliaError) {
        let entries = {
            let mut guard = self.state.lock().await;
            guard.a1.clear();
            guard.a2.clear();
            guard.a3.clear();
            guard.entries.drain().collect::<Vec<_>>()
        };
        self.inflight_notify.notify_waiters();
        let mut failed = 0usize;
        for (_, entry) in entries {
            if classify_priority(entry.message.msg_type) == PriorityTier::A1 {
                continue;
            }
            failed = failed.saturating_add(1);
            let _ = entry.ack_tx.send(Err(error.clone()));
            debug!(
                peer_msg_id = entry.message.peer_msg_id,
                "outbound message failed"
            );
        }
        if failed > 0 {
            warn!(
                count = failed,
                error = %error,
                "failing non-A1 outbound messages"
            );
        }
    }

    pub(crate) async fn inflight_messages(&self) -> Vec<InflightMessage> {
        let guard = self.state.lock().await;
        guard
            .entries
            .values()
            .map(|entry| InflightMessage {
                peer_msg_id: entry.message.peer_msg_id,
                src_taberna: entry.message.src_taberna,
                dst_taberna: entry.message.dst_taberna,
                msg_type: entry.message.msg_type,
                flags: entry.message.flags,
                payload: entry.message.payload.clone(),
            })
            .collect()
    }
}

pub(super) async fn run_primary_dispatcher(
    session: Arc<PeerSession>,
    queue: Arc<PrimaryDispatchQueue>,
    mut snapshot_rx: watch::Receiver<PeerStateSnapshot>,
    primary_available: Arc<Notify>,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    config: DomusConfigAccess,
) {
    let mut next_index = 0usize;
    let mut dial_requested = false;
    let mut last_send = Instant::now();
    let mut keepalive_tick = tokio::time::interval(Duration::from_millis(200));
    keepalive_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        if session.is_closing() {
            queue
                .drain_new_on_close(AureliaError::new(ErrorId::PeerUnavailable))
                .await;
        }

        let item = match queue.pop_next().await {
            Some(item) => item,
            None => {
                tokio::select! {
                    _ = queue.notifier().notified() => {}
                    _ = primary_available.notified() => {}
                    _ = keepalive_tick.tick() => {
                        maybe_enqueue_keepalive(&queue, &snapshot_rx, &session, &config, &last_send).await;
                    }
                    result = snapshot_rx.changed() => {
                        if result.is_err() {
                            return;
                        }
                        dial_requested = false;
                        next_index = 0;
                    }
                }
                continue;
            }
        };

        if session.is_closing() {
            if let DispatchItem::Message { peer_msg_id, .. } = item {
                if let Some(false) = queue.is_inflight(peer_msg_id).await {
                    let _ = session
                        .handle_error(peer_msg_id, AureliaError::new(ErrorId::PeerUnavailable))
                        .await;
                }
                continue;
            }
        }

        let snapshot = snapshot_rx.borrow().clone();
        if snapshot.primary_handles.is_empty() {
            queue.requeue_back(item).await;
            if !dial_requested {
                let _ = peer_state_tx.send(PeerStateUpdate::EnsurePrimaryDial).await;
                dial_requested = true;
            }
            tokio::select! {
                _ = primary_available.notified() => {}
                _ = queue.notifier().notified() => {}
                _ = keepalive_tick.tick() => {
                    maybe_enqueue_keepalive(&queue, &snapshot_rx, &session, &config, &last_send).await;
                }
                result = snapshot_rx.changed() => {
                    if result.is_err() {
                        return;
                    }
                    dial_requested = false;
                    next_index = 0;
                }
            }
            continue;
        }

        let (selected_index, handle, advance_index) = match select_handle_for_item(
            &snapshot.primary_handles,
            next_index,
            &peer_state_tx,
            &item,
        )
        .await
        {
            Some(selection) => selection,
            None => {
                queue.requeue_back(item).await;
                tokio::select! {
                    _ = primary_available.notified() => {}
                    _ = queue.notifier().notified() => {}
                    _ = keepalive_tick.tick() => {
                        maybe_enqueue_keepalive(&queue, &snapshot_rx, &session, &config, &last_send).await;
                    }
                    result = snapshot_rx.changed() => {
                        if result.is_err() {
                            return;
                        }
                        dial_requested = false;
                        next_index = 0;
                    }
                }
                continue;
            }
        };

        if advance_index {
            next_index = selected_index.saturating_add(1) % snapshot.primary_handles.len();
        }

        let (send_frame, inflight, peer_msg_id, send_deadline) = match &item {
            DispatchItem::A1Frame(frame) => {
                let send_timeout = config.snapshot().await.send_timeout;
                (
                    frame.frame.clone(),
                    true,
                    None,
                    Some(Instant::now() + send_timeout),
                )
            }
            DispatchItem::Message { peer_msg_id, .. } => {
                let message = match queue.message(*peer_msg_id).await {
                    Some(message) => message,
                    None => {
                        handle.available.store(true, Ordering::SeqCst);
                        primary_available.notify_one();
                        continue;
                    }
                };

                let inflight = queue.is_inflight(*peer_msg_id).await.unwrap_or(false);
                if !inflight && session.prepare_dispatch(*peer_msg_id).await.is_err() {
                    handle.available.store(true, Ordering::SeqCst);
                    primary_available.notify_one();
                    continue;
                }
                (
                    OutboundFrame::Message(message),
                    inflight,
                    Some(*peer_msg_id),
                    queue.deadline(*peer_msg_id).await,
                )
            }
        };

        let send_deadline = match send_deadline {
            Some(deadline) => deadline,
            None => {
                handle.available.store(true, Ordering::SeqCst);
                primary_available.notify_one();
                if let Some(peer_msg_id) = peer_msg_id {
                    if !inflight {
                        session.rollback_dispatch(peer_msg_id).await;
                    }
                }
                continue;
            }
        };

        if let Some(peer_msg_id) = peer_msg_id {
            if Instant::now() >= send_deadline {
                handle.available.store(true, Ordering::SeqCst);
                primary_available.notify_one();
                let _ = session
                    .handle_error(peer_msg_id, AureliaError::new(ErrorId::SendTimeout))
                    .await;
                continue;
            }
        }

        let send_result = tokio::time::timeout_at(send_deadline, handle.tx.send(send_frame)).await;

        match send_result {
            Ok(Ok(())) => {
                last_send = Instant::now();
                if let Some(peer_msg_id) = peer_msg_id {
                    if !inflight {
                        session.commit_dispatch(peer_msg_id).await;
                    }
                }
            }
            Ok(Err(_)) => {
                if let Some(peer_msg_id) = peer_msg_id {
                    if !inflight {
                        session.rollback_dispatch(peer_msg_id).await;
                    }
                }
                queue.requeue_back(item).await;
                let _ = peer_state_tx
                    .send(PeerStateUpdate::Disconnect {
                        callis: CallisKind::Primary,
                        id: Some(handle.id),
                    })
                    .await;
                dial_requested = false;
            }
            Err(_) => {
                handle.available.store(true, Ordering::SeqCst);
                primary_available.notify_one();
                if let Some(peer_msg_id) = peer_msg_id {
                    let _ = session
                        .handle_error(peer_msg_id, AureliaError::new(ErrorId::SendTimeout))
                        .await;
                } else {
                    queue.requeue_back(item).await;
                }
            }
        }
    }
}

async fn select_available_primary(
    handles: &[CallisHandle],
    start_index: usize,
    peer_state_tx: &mpsc::Sender<PeerStateUpdate>,
) -> Option<(usize, CallisHandle)> {
    if handles.is_empty() {
        return None;
    }
    for offset in 0..handles.len() {
        let idx = (start_index + offset) % handles.len();
        let handle = &handles[idx];
        if handle.tx.is_closed() {
            let _ = peer_state_tx
                .send(PeerStateUpdate::Disconnect {
                    callis: CallisKind::Primary,
                    id: Some(handle.id),
                })
                .await;
            continue;
        }
        if handle
            .available
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return Some((idx, handle.clone()));
        }
    }
    None
}

async fn select_target_primary(
    handles: &[CallisHandle],
    target: CallisId,
    peer_state_tx: &mpsc::Sender<PeerStateUpdate>,
) -> Option<CallisHandle> {
    let handle = handles.iter().find(|handle| handle.id == target)?;
    if handle.tx.is_closed() {
        let _ = peer_state_tx
            .send(PeerStateUpdate::Disconnect {
                callis: CallisKind::Primary,
                id: Some(handle.id),
            })
            .await;
        return None;
    }
    if handle
        .available
        .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        return Some(handle.clone());
    }
    None
}

async fn select_handle_for_item(
    handles: &[CallisHandle],
    start_index: usize,
    peer_state_tx: &mpsc::Sender<PeerStateUpdate>,
    item: &DispatchItem,
) -> Option<(usize, CallisHandle, bool)> {
    match item {
        DispatchItem::A1Frame(frame) => {
            if let Some(target) = frame.target {
                let handle = select_target_primary(handles, target, peer_state_tx).await?;
                let idx = handles
                    .iter()
                    .position(|handle| handle.id == target)
                    .unwrap_or(0);
                return Some((idx, handle, false));
            }
            let (idx, handle) =
                select_available_primary(handles, start_index, peer_state_tx).await?;
            Some((idx, handle, true))
        }
        DispatchItem::Message { .. } => {
            let (idx, handle) =
                select_available_primary(handles, start_index, peer_state_tx).await?;
            Some((idx, handle, true))
        }
    }
}

fn classify_priority(msg_type: MessageType) -> PriorityTier {
    if msg_type <= 0xFFFF {
        PriorityTier::A1
    } else if msg_type <= 0x00FF_FFFF {
        PriorityTier::A2
    } else {
        PriorityTier::A3
    }
}

async fn maybe_enqueue_keepalive(
    queue: &PrimaryDispatchQueue,
    snapshot_rx: &watch::Receiver<PeerStateSnapshot>,
    session: &PeerSession,
    config: &DomusConfigAccess,
    last_send: &Instant,
) {
    if session.is_closing() {
        return;
    }
    if !queue.is_empty().await {
        return;
    }
    let has_handles = !snapshot_rx.borrow().primary_handles.is_empty();
    if !has_handles {
        return;
    }
    let cfg = config.snapshot().await;
    if Instant::now().duration_since(*last_send) < cfg.keepalive_interval {
        return;
    }
    queue
        .enqueue_a1_frame(
            OutboundFrame::Control {
                msg_type: MSG_KEEPALIVE,
                peer_msg_id: 0,
                payload: Bytes::new(),
            },
            None,
        )
        .await;
}
