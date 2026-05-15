// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;

mod callis_pool;
mod inbound;
mod lifecycle;
mod outbound;
mod reservations;

use callis_pool::BlobCallisPool;
use inbound::{BlobInboundState, BlobRecvChunkState, BlobRecvStream, PendingBlobRequest};
use lifecycle::BlobLifecycleSnapshot;
pub(super) use outbound::BlobWriteLease;
use outbound::{
    BlobOutboundState, BlobResponseLane, BlobWriteSlot, BlobWriteSlotState, InflightBlobFrame,
};
use reservations::BlobReservationState;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BlobCallisSettings {
    pub(super) chunk_size: u32,
    pub(super) ack_window_chunks: u32,
}

pub(crate) struct BlobReceiverState {
    pub(super) notify: Arc<Notify>,
    pub(super) accepted: AtomicBool,
    pub(super) completed: AtomicBool,
    pub(super) error: Mutex<Option<AureliaError>>,
    pub(super) completion_ttl: Duration,
    pub(super) idle_timeout: Duration,
}

impl BlobReceiverState {
    pub(super) async fn fail(&self, err: AureliaError) {
        {
            let mut guard = self.error.lock().await;
            *guard = Some(err);
        }
        self.completed.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
}

#[derive(Debug)]
pub(super) enum BlobChunkOutcome {
    Continue,
    Complete(PeerMessageId),
}

/// Peer-level blob coordinator.
///
/// This is deliberately one manager with passive ownership domains underneath it. Blob state is
/// cross-cutting at the peer level: primary callis messages establish blob transfers, blob callis
/// carry the chunks, and shutdown/backpressure cleanup has to see both sides. The submodules keep
/// the domains auditable; active work stays with the existing peer and callis tasks instead of a
/// separate blob task or queue. Methods that need to wake another task return or collect the
/// relevant data, drop the domain lock, and notify afterwards.
///
/// Locking invariant: never hold more than one BlobManager domain mutex at a time. Cross-domain
/// methods snapshot under one lock, drop that guard, then proceed to the next domain or notify.
/// If a future change needs atomic mutation across domains, that is a design change: merge the
/// affected domains or introduce a reviewed transaction/actor shape instead of nesting locks.
pub(super) struct BlobManager {
    callis: Mutex<BlobCallisPool>,
    // Callis-pool generation counter. Incremented on every add/remove/drain so consumers
    // observe pool transitions via watch::Receiver::changed(), which is naturally latched
    // and therefore race-free. See docs/concurrency.md Pattern 4.
    callis_gen_tx: watch::Sender<u64>,
    work_notify: Arc<Notify>,
    inbound_idle_notify: Arc<Notify>,
    had_callis: AtomicBool,
    outbound: Mutex<BlobOutboundState>,
    inbound: Mutex<BlobInboundState>,
    reservations: Mutex<BlobReservationState>,
    buffer_tracker: Arc<BlobBufferTracker>,
    allocator: Arc<PeerMessageIdAllocator>,
}

impl BlobManager {
    pub(super) fn new(
        buffer_tracker: Arc<BlobBufferTracker>,
        work_notify: Arc<Notify>,
        allocator: Arc<PeerMessageIdAllocator>,
        initial_send_queue_size: usize,
    ) -> Self {
        let (callis_gen_tx, _) = watch::channel(0u64);
        Self {
            callis: Mutex::new(BlobCallisPool::new()),
            callis_gen_tx,
            work_notify,
            inbound_idle_notify: Arc::new(Notify::new()),
            had_callis: AtomicBool::new(false),
            outbound: Mutex::new(BlobOutboundState::new(initial_send_queue_size)),
            inbound: Mutex::new(BlobInboundState::new()),
            reservations: Mutex::new(BlobReservationState::new()),
            buffer_tracker,
            allocator,
        }
    }

    pub(super) fn notify_work(&self) {
        self.work_notify.notify_one();
    }

    pub(super) fn work_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.work_notify)
    }

    /// Returns a watch receiver tracking the callis-pool generation counter. Consumers
    /// observe pool transitions by calling `changed()` on this receiver. Because watch
    /// receivers are naturally latched against the latest value, this is race-free
    /// regardless of registration timing.
    pub(super) fn subscribe_callis_gen(&self) -> watch::Receiver<u64> {
        self.callis_gen_tx.subscribe()
    }

    fn bump_callis_gen(&self) {
        self.callis_gen_tx.send_modify(|v| *v = v.wrapping_add(1));
    }

    pub(super) fn next_peer_msg_id(&self) -> PeerMessageId {
        self.allocator.next()
    }

    pub(super) async fn reserve_outbound(
        &self,
        stream_id: PeerMessageId,
        bytes: u64,
        cap: u64,
    ) -> bool {
        if !self.buffer_tracker.try_reserve_outbound(bytes, cap) {
            return false;
        }
        let mut reservations = self.reservations.lock().await;
        reservations.outbound.insert(stream_id, bytes);
        true
    }

    pub(super) async fn release_outbound(&self, stream_id: PeerMessageId) {
        let bytes = {
            let mut reservations = self.reservations.lock().await;
            reservations.outbound.remove(&stream_id)
        };
        if let Some(bytes) = bytes {
            self.buffer_tracker.release_outbound(bytes);
        }
    }

    pub(super) async fn reserve_inbound(
        &self,
        stream_id: PeerMessageId,
        bytes: u64,
        cap: u64,
    ) -> bool {
        if !self.buffer_tracker.try_reserve_inbound(bytes, cap) {
            return false;
        }
        let mut reservations = self.reservations.lock().await;
        reservations.inbound.insert(stream_id, bytes);
        true
    }

    pub(super) async fn release_inbound(&self, stream_id: PeerMessageId) {
        let bytes = {
            let mut reservations = self.reservations.lock().await;
            reservations.inbound.remove(&stream_id)
        };
        if let Some(bytes) = bytes {
            self.buffer_tracker.release_inbound(bytes);
        }
    }

    pub(super) async fn enqueue_blob_write(&self, write: BlobWriteLease) -> bool {
        let peer_msg_id = write.peer_msg_id();
        let stream_id = write.stream_id();
        let mut outbound = self.outbound.lock().await;
        let Some(lane) = write.lane() else {
            return false;
        };
        if outbound.write_slots.contains_key(&peer_msg_id) {
            return false;
        }
        if outbound.response_lane_len(lane) >= outbound.response_capacities.capacity(lane) {
            warn!(peer_msg_id, "blob response lane full");
            return false;
        }
        outbound.write_slots.insert(
            peer_msg_id,
            BlobWriteSlot {
                stream_id,
                write,
                state: BlobWriteSlotState::Ready,
            },
        );
        outbound.response_lane_mut(lane).push_back(peer_msg_id);
        drop(outbound);
        self.notify_work();
        true
    }

    pub(super) async fn next_blob_write_slot(&self, callis_id: CallisId) -> Option<BlobWriteLease> {
        let mut outbound = self.outbound.lock().await;
        for lane in [
            BlobResponseLane::Ack,
            BlobResponseLane::Error,
            BlobResponseLane::Complete,
        ] {
            while let Some(peer_msg_id) = outbound.response_lane_mut(lane).pop_front() {
                let Some(slot) = outbound.write_slots.get_mut(&peer_msg_id) else {
                    continue;
                };
                if matches!(
                    slot.state,
                    BlobWriteSlotState::Ready | BlobWriteSlotState::ReplayReady { .. }
                ) {
                    slot.state = BlobWriteSlotState::Writing { callis_id };
                    return Some(slot.write.clone());
                }
            }
        }
        None
    }

    pub(super) async fn lease_next_blob_write(
        &self,
        callis_id: CallisId,
    ) -> Option<BlobWriteLease> {
        let write = if let Some(write) = self.next_blob_write_slot(callis_id).await {
            write
        } else {
            self.next_chunk_frame(callis_id).await?
        };
        self.mark_dispatch_inflight(&write, callis_id).await;
        if self.has_dispatchable_blob_write().await {
            self.notify_work();
        }
        Some(write)
    }

    pub(super) async fn finish_blob_write_attempt(
        &self,
        write: &BlobWriteLease,
        callis_id: CallisId,
        result: Result<(), AureliaError>,
    ) {
        if result.is_err() {
            self.rollback_dispatch_inflight(write, callis_id).await;
        } else if !write.is_chunk() {
            let mut outbound = self.outbound.lock().await;
            outbound.write_slots.remove(&write.peer_msg_id());
        }
        self.notify_work();
    }

    pub(super) async fn next_chunk_frame(&self, callis_id: CallisId) -> Option<BlobWriteLease> {
        let streams = {
            let outbound = self.outbound.lock().await;
            let mut streams = outbound
                .streams
                .iter()
                .map(|(stream_id, ring)| (*stream_id, Arc::clone(ring)))
                .collect::<Vec<_>>();
            order_streams_from_cursor(&mut streams, outbound.round_robin_cursor);
            streams
        };
        if streams.is_empty() {
            return None;
        }
        if let Some(write) = self
            .next_chunk_frame_from_streams(callis_id, &streams, true)
            .await
        {
            if let Some(stream_id) = write.stream_id() {
                self.advance_round_robin_cursor(stream_id).await;
            }
            return Some(write);
        }
        let write = self
            .next_chunk_frame_from_streams(callis_id, &streams, false)
            .await;
        if let Some(write) = write.as_ref() {
            if let Some(stream_id) = write.stream_id() {
                self.advance_round_robin_cursor(stream_id).await;
            }
        }
        write
    }

    async fn next_chunk_frame_from_streams(
        &self,
        callis_id: CallisId,
        streams: &[(PeerMessageId, Arc<crate::ring_buffer::OutboundRingBuffer>)],
        replay: bool,
    ) -> Option<BlobWriteLease> {
        for (stream_id, ring) in streams {
            let dispatchable = if replay {
                ring.has_dispatchable_replay().await
            } else {
                ring.has_dispatchable_fresh().await
            };
            if !dispatchable {
                continue;
            }
            let peer_msg_id = self.allocator.next();
            let Some(lease) = ring
                .lease_next_chunk_for_write(callis_id, peer_msg_id)
                .await
            else {
                continue;
            };
            return Some(BlobWriteLease::Chunk {
                stream_id: *stream_id,
                peer_msg_id,
                chunk: lease,
            });
        }
        None
    }

    async fn advance_round_robin_cursor(&self, stream_id: PeerMessageId) {
        let mut outbound = self.outbound.lock().await;
        outbound.round_robin_cursor = next_stream_after(&outbound.streams, stream_id);
    }

    async fn has_dispatchable_blob_write(&self) -> bool {
        let outbound = self.outbound.lock().await;
        if outbound.response_ready_count() > 0 {
            return true;
        }
        let streams = outbound.streams.values().cloned().collect::<Vec<_>>();
        for ring in streams {
            if ring.has_dispatchable_replay().await || ring.has_dispatchable_fresh().await {
                return true;
            }
        }
        false
    }

    pub(super) async fn add_callis(
        &self,
        handle: CallisHandle,
        settings: BlobCallisSettings,
        resume: bool,
    ) {
        let should_fail = !resume && self.has_stream_state().await;
        if should_fail {
            self.fail_all_streams(AureliaError::new(ErrorId::PeerRestarted))
                .await;
        }
        if resume {
            let streams_to_fail = {
                let outbound = self.outbound.lock().await;
                outbound
                    .stream_settings
                    .iter()
                    .filter_map(|(stream_id, stream_settings)| {
                        (*stream_settings != settings).then_some(*stream_id)
                    })
                    .collect::<Vec<_>>()
            };
            for stream_id in streams_to_fail {
                self.fail_stream(stream_id, AureliaError::new(ErrorId::PeerRestarted))
                    .await;
            }
        }
        self.had_callis.store(true, Ordering::SeqCst);
        {
            let mut pool = self.callis.lock().await;
            pool.handles.insert(handle.id, handle.clone());
            pool.settings.insert(handle.id, settings);
            if !pool.order.contains(&handle.id) {
                pool.order.push_back(handle.id);
            }
        }
        self.bump_callis_gen();
        self.notify_work();
    }

    pub(super) async fn has_stream_state(&self) -> bool {
        let outbound = self.outbound.lock().await;
        if !outbound.streams.is_empty() || !outbound.write_slots.is_empty() {
            return true;
        }
        drop(outbound);
        let inbound = self.inbound.lock().await;
        !inbound.recv_streams.is_empty()
    }

    pub(super) async fn remove_callis(&self, callis_id: CallisId) -> Option<CallisHandle> {
        let handle = {
            let mut pool = self.callis.lock().await;
            let handle = pool.handles.remove(&callis_id);
            pool.settings.remove(&callis_id);
            if let Some(pos) = pool.order.iter().position(|id| *id == callis_id) {
                pool.order.remove(pos);
            }
            handle
        };
        self.bump_callis_gen();
        handle
    }

    pub(super) async fn drain_callis(&self) -> Vec<CallisHandle> {
        let mut pool = self.callis.lock().await;
        let handles = pool.handles.values().cloned().collect::<Vec<_>>();
        pool.handles.clear();
        pool.settings.clear();
        pool.order.clear();
        drop(pool);
        self.bump_callis_gen();
        handles
    }

    pub(super) async fn has_callis(&self) -> bool {
        let pool = self.callis.lock().await;
        !pool.order.is_empty()
    }

    pub(super) fn had_callis(&self) -> bool {
        self.had_callis.load(Ordering::SeqCst)
    }

    pub(super) fn reset_callis_history(&self) {
        self.had_callis.store(false, Ordering::SeqCst);
    }

    pub(super) async fn select_callis(
        &self,
    ) -> Option<(CallisId, CallisHandle, BlobCallisSettings)> {
        let mut pool = self.callis.lock().await;
        let len = pool.order.len();
        for _ in 0..len {
            let Some(id) = pool.order.pop_front() else {
                break;
            };
            let handle = match pool.handles.get(&id).cloned() {
                Some(handle) => handle,
                None => {
                    pool.settings.remove(&id);
                    continue;
                }
            };
            let settings = match pool.settings.get(&id).copied() {
                Some(settings) => settings,
                None => {
                    pool.handles.remove(&id);
                    continue;
                }
            };
            if handle.tx.is_closed() {
                pool.handles.remove(&id);
                pool.settings.remove(&id);
                continue;
            }
            pool.order.push_back(id);
            return Some((id, handle, settings));
        }
        None
    }

    pub(super) async fn wait_for_callis(&self, timeout: Duration) -> Result<(), AureliaError> {
        let deadline = Instant::now() + timeout;
        // Subscribe BEFORE the first has_callis() check so any bump that fires
        // between the subscribe and the check is observed by the next changed().
        // See docs/concurrency.md Pattern 4 (watch primitive choice).
        let mut rx = self.callis_gen_tx.subscribe();
        loop {
            if self.has_callis().await {
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(AureliaError::new(ErrorId::SendTimeout));
            }
            if tokio::time::timeout(remaining, rx.changed()).await.is_err() {
                return Err(AureliaError::new(ErrorId::SendTimeout));
            }
        }
    }

    pub(super) async fn register_outbound_stream(
        &self,
        stream_id: PeerMessageId,
        settings: BlobCallisSettings,
    ) -> Result<Arc<crate::ring_buffer::OutboundRingBuffer>, AureliaError> {
        let ring = Arc::new(crate::ring_buffer::OutboundRingBuffer::new(
            settings.chunk_size as usize,
            settings.ack_window_chunks as usize,
        )?);
        {
            let mut outbound = self.outbound.lock().await;
            outbound.streams.insert(stream_id, Arc::clone(&ring));
            outbound.stream_settings.insert(stream_id, settings);
        }
        Ok(ring)
    }

    pub(super) async fn unregister_outbound_stream(&self, stream_id: PeerMessageId) {
        self.release_outbound(stream_id).await;
        let ring = {
            let mut outbound = self.outbound.lock().await;
            let ring = outbound.streams.remove(&stream_id);
            outbound
                .inflight
                .retain(|_, entry| entry.stream_id != stream_id);
            outbound
                .write_slots
                .retain(|_, slot| slot.stream_id != Some(stream_id));
            outbound.prune_response_ready();
            outbound.stream_settings.remove(&stream_id);
            ring
        };
        if let Some(ring) = ring {
            ring.close().await;
        }
    }

    pub(super) async fn mark_dispatch_inflight(&self, write: &BlobWriteLease, callis_id: CallisId) {
        let mut outbound = self.outbound.lock().await;
        let peer_msg_id = write.peer_msg_id();
        let stream_id = write.stream_id();
        if let Some(slot) = outbound.write_slots.get_mut(&peer_msg_id) {
            slot.state = BlobWriteSlotState::InFlight { callis_id };
        }
        if write.expects_ack() {
            if let Some(stream_id) = stream_id {
                outbound.inflight.insert(
                    peer_msg_id,
                    InflightBlobFrame {
                        stream_id,
                        callis_id,
                        is_chunk: write.is_chunk(),
                    },
                );
            }
        }
        // Advance chunk slots before the socket write so a fast peer ACK can always
        // resolve the ring slot through the already-installed inflight lookup.
        let chunk_write_lease = match write {
            BlobWriteLease::Chunk { chunk, .. } => Some(chunk.clone()),
            BlobWriteLease::Ack { .. }
            | BlobWriteLease::Error { .. }
            | BlobWriteLease::Finish { .. } => None,
        };
        let ring = stream_id.and_then(|stream_id| outbound.streams.get(&stream_id).cloned());
        drop(outbound);
        if let (Some(lease), Some(ring)) = (chunk_write_lease, ring) {
            ring.mark_chunk_inflight(&lease).await;
        }
    }

    pub(super) async fn rollback_dispatch_inflight(
        &self,
        write: &BlobWriteLease,
        callis_id: CallisId,
    ) {
        let mut outbound = self.outbound.lock().await;
        let peer_msg_id = write.peer_msg_id();
        let stream_id = write.stream_id();
        if write.expects_ack() {
            outbound.inflight.remove(&peer_msg_id);
        }
        if !write.is_chunk() {
            if let Some(slot) = outbound.write_slots.get_mut(&peer_msg_id) {
                slot.state = BlobWriteSlotState::ReplayReady {
                    previous_callis_id: callis_id,
                };
                if let Some(lane) = slot.write.lane() {
                    outbound.response_lane_mut(lane).push_back(peer_msg_id);
                }
            }
        }
        let should_replay_chunk = write.is_chunk();
        let ring = stream_id.and_then(|stream_id| outbound.streams.get(&stream_id).cloned());
        drop(outbound);
        if should_replay_chunk {
            let Some(ring) = ring else {
                return;
            };
            ring.mark_callis_replay_ready(callis_id).await;
        }
    }

    pub(super) async fn stream_ring(
        &self,
        stream_id: PeerMessageId,
    ) -> Option<Arc<crate::ring_buffer::OutboundRingBuffer>> {
        let outbound = self.outbound.lock().await;
        outbound.streams.get(&stream_id).cloned()
    }

    pub(super) async fn settings_for_callis(
        &self,
        callis_id: CallisId,
    ) -> Option<BlobCallisSettings> {
        let pool = self.callis.lock().await;
        pool.settings.get(&callis_id).copied()
    }

    pub(super) async fn current_settings(&self) -> Option<BlobCallisSettings> {
        let pool = self.callis.lock().await;
        pool.order
            .front()
            .and_then(|callis_id| pool.settings.get(callis_id).copied())
    }

    pub(super) async fn fail_stream(&self, stream_id: PeerMessageId, error: AureliaError) {
        self.release_outbound(stream_id).await;
        let ring = {
            let mut outbound = self.outbound.lock().await;
            let ring = outbound.streams.remove(&stream_id);
            outbound
                .inflight
                .retain(|_, entry| entry.stream_id != stream_id);
            outbound
                .write_slots
                .retain(|_, slot| slot.stream_id != Some(stream_id));
            outbound.prune_response_ready();
            outbound.stream_settings.remove(&stream_id);
            ring
        };
        if let Some(ring) = ring {
            ring.fail(error.clone()).await;
            ring.close().await;
        }
    }

    pub(super) async fn handle_ack(&self, peer_msg_id: PeerMessageId) {
        let (entry, ring) = {
            let mut outbound = self.outbound.lock().await;
            let entry = outbound.inflight.remove(&peer_msg_id);
            outbound.write_slots.remove(&peer_msg_id);
            let ring = if let Some(entry) = entry.as_ref() {
                outbound.streams.get(&entry.stream_id).cloned()
            } else {
                None
            };
            (entry, ring)
        };
        let Some(entry) = entry else {
            return;
        };
        if entry.is_chunk {
            self.notify_work();
        }
        if let Some(ring) = ring {
            ring.note_ack(peer_msg_id).await;
        }
    }

    pub(super) async fn handle_error(&self, peer_msg_id: PeerMessageId, error: AureliaError) {
        let (entry, ring) = {
            let mut outbound = self.outbound.lock().await;
            let entry = outbound.inflight.remove(&peer_msg_id);
            outbound.write_slots.remove(&peer_msg_id);
            let ring = if let Some(entry) = entry.as_ref() {
                outbound.streams.get(&entry.stream_id).cloned()
            } else {
                None
            };
            (entry, ring)
        };
        if let Some(entry) = entry {
            if entry.is_chunk {
                self.notify_work();
            }
            if let Some(ring) = ring {
                ring.note_error(peer_msg_id, error).await;
            }
            return;
        }

        if self.fail_outbound_stream(peer_msg_id, error.clone()).await {
            return;
        }

        let _ = self.fail_inbound_stream(peer_msg_id, error).await;
    }

    async fn fail_outbound_stream(&self, stream_id: PeerMessageId, error: AureliaError) -> bool {
        let ring = {
            let outbound = self.outbound.lock().await;
            outbound.streams.get(&stream_id).cloned()
        };
        let Some(ring) = ring else {
            return false;
        };
        ring.fail(error).await;
        self.unregister_outbound_stream(stream_id).await;
        true
    }

    pub(super) async fn fail_inbound_stream(
        &self,
        stream_id: PeerMessageId,
        error: AureliaError,
    ) -> bool {
        let pending = {
            let mut inbound = self.inbound.lock().await;
            inbound.pending_requests.remove(&stream_id)
        };
        if let Some(pending) = pending {
            pending.receiver.fail(error).await;
            self.release_inbound(stream_id).await;
            return true;
        }

        if let Some(stream) = self.remove_recv_stream(stream_id).await {
            stream.receiver.fail(error).await;
            return true;
        }
        false
    }

    pub(super) async fn handle_complete(
        &self,
        stream_id: PeerMessageId,
    ) -> Result<(), AureliaError> {
        let ring = {
            let outbound = self.outbound.lock().await;
            outbound.streams.get(&stream_id).cloned()
        };
        if let Some(ring) = ring {
            ring.mark_complete().await;
            Ok(())
        } else {
            Err(AureliaError::new(ErrorId::ProtocolViolation))
        }
    }

    pub(super) async fn note_recv_complete(&self, stream_id: PeerMessageId, ttl: Duration) {
        let now = Instant::now();
        let mut inbound = self.inbound.lock().await;
        inbound
            .completed_recv_streams
            .retain(|_, ts| now.duration_since(*ts) <= ttl);
        inbound.completed_recv_streams.insert(stream_id, now);
    }

    pub(super) async fn requeue_inflight_for_callis(&self, callis_id: CallisId) {
        let inflight_entries = {
            let mut outbound = self.outbound.lock().await;
            let mut entries = Vec::new();
            outbound.inflight.retain(|peer_msg_id, entry| {
                if entry.callis_id == callis_id {
                    entries.push((*peer_msg_id, entry.stream_id, entry.is_chunk));
                    false
                } else {
                    true
                }
            });
            let response_ids = outbound
                .write_slots
                .iter_mut()
                .filter_map(|(peer_msg_id, slot)| match slot.state {
                    BlobWriteSlotState::Writing {
                        callis_id: slot_callis_id,
                    }
                    | BlobWriteSlotState::InFlight {
                        callis_id: slot_callis_id,
                    } if slot_callis_id == callis_id => {
                        slot.state = BlobWriteSlotState::ReplayReady {
                            previous_callis_id: callis_id,
                        };
                        Some((*peer_msg_id, slot.write.lane()))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            for (peer_msg_id, lane) in response_ids {
                if let Some(lane) = lane {
                    outbound.response_lane_mut(lane).push_back(peer_msg_id);
                }
            }
            entries
        };
        if inflight_entries.is_empty() {
            return;
        }
        let mut streams = HashSet::new();
        for (_peer_msg_id, stream_id, _is_chunk) in inflight_entries {
            streams.insert(stream_id);
        }
        if !streams.is_empty() {
            let rings = {
                let outbound = self.outbound.lock().await;
                streams
                    .iter()
                    .filter_map(|stream_id| outbound.streams.get(stream_id).cloned())
                    .collect::<Vec<_>>()
            };
            for ring in rings {
                ring.mark_callis_replay_ready(callis_id).await;
            }
            self.notify_work();
        }
    }

    pub(super) async fn requeue_all_inflight(&self) {
        let inflight_entries = {
            let mut outbound = self.outbound.lock().await;
            let entries: Vec<(PeerMessageId, PeerMessageId, CallisId, bool)> = outbound
                .inflight
                .iter()
                .map(|(peer_msg_id, entry)| {
                    (
                        *peer_msg_id,
                        entry.stream_id,
                        entry.callis_id,
                        entry.is_chunk,
                    )
                })
                .collect();
            outbound.inflight.clear();
            let response_ids = outbound
                .write_slots
                .iter_mut()
                .filter_map(|(peer_msg_id, slot)| match slot.state {
                    BlobWriteSlotState::Writing { callis_id }
                    | BlobWriteSlotState::InFlight { callis_id } => {
                        slot.state = BlobWriteSlotState::ReplayReady {
                            previous_callis_id: callis_id,
                        };
                        Some((*peer_msg_id, slot.write.lane()))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            for (peer_msg_id, lane) in response_ids {
                if let Some(lane) = lane {
                    outbound.response_lane_mut(lane).push_back(peer_msg_id);
                }
            }
            entries
        };
        if inflight_entries.is_empty() {
            return;
        }
        let mut streams = HashSet::new();
        let mut ring_replay: HashMap<CallisId, Vec<Arc<crate::ring_buffer::OutboundRingBuffer>>> =
            HashMap::new();
        for (_peer_msg_id, stream_id, callis_id, is_chunk) in inflight_entries {
            if is_chunk {
                if let Some(ring) = self.stream_ring(stream_id).await {
                    ring_replay.entry(callis_id).or_default().push(ring);
                }
            }
            streams.insert(stream_id);
        }
        for (callis_id, rings) in ring_replay {
            for ring in rings {
                ring.mark_callis_replay_ready(callis_id).await;
            }
        }
        if !streams.is_empty() {
            self.notify_work();
        }
    }

    pub(super) async fn add_pending_request(
        &self,
        stream_id: PeerMessageId,
        taberna_id: TabernaId,
        receiver: Arc<BlobReceiverState>,
    ) {
        let mut inbound = self.inbound.lock().await;
        inbound.pending_requests.insert(
            stream_id,
            PendingBlobRequest {
                taberna_id,
                receiver,
            },
        );
    }

    pub(super) async fn activate_pending_request(
        &self,
        stream_id: PeerMessageId,
        settings: BlobCallisSettings,
    ) -> Result<(), AureliaError> {
        if self.recv_stream_exists(stream_id).await {
            return Ok(());
        }
        let Some(pending) = self.take_pending_request(stream_id).await else {
            warn!(stream_id, "blob stream activation without pending request");
            return Err(AureliaError::new(ErrorId::BlobStreamNotFound));
        };
        let ring = Arc::new(crate::ring_buffer::InboundRingBuffer::new(
            settings.chunk_size as usize,
            settings.ack_window_chunks as usize,
        )?);
        self.insert_recv_stream(
            stream_id,
            pending.taberna_id,
            Arc::clone(&pending.receiver),
            ring,
        )
        .await;
        pending.receiver.notify.notify_waiters();
        info!(
            taberna_id = pending.taberna_id,
            stream_id, "blob stream activated"
        );
        Ok(())
    }

    pub(super) async fn take_pending_request(
        &self,
        stream_id: PeerMessageId,
    ) -> Option<PendingBlobRequest> {
        let mut inbound = self.inbound.lock().await;
        inbound.pending_requests.remove(&stream_id)
    }

    pub(super) async fn drop_pending_request(&self, stream_id: PeerMessageId) {
        let removed = {
            let mut inbound = self.inbound.lock().await;
            inbound.pending_requests.remove(&stream_id)
        };
        if removed.is_some() {
            self.release_inbound(stream_id).await;
        }
    }

    pub(super) async fn insert_recv_stream(
        &self,
        stream_id: PeerMessageId,
        taberna_id: TabernaId,
        receiver: Arc<BlobReceiverState>,
        ring: Arc<crate::ring_buffer::InboundRingBuffer>,
    ) {
        let deadline = Instant::now() + receiver.idle_timeout;
        let mut inbound = self.inbound.lock().await;
        inbound.recv_streams.insert(
            stream_id,
            BlobRecvStream {
                taberna_id,
                receiver,
                ring,
                deadline,
            },
        );
        drop(inbound);
        self.inbound_idle_notify.notify_one();
    }

    pub(super) async fn recv_stream_exists(&self, stream_id: PeerMessageId) -> bool {
        let inbound = self.inbound.lock().await;
        inbound.recv_streams.contains_key(&stream_id)
    }

    pub(super) async fn recv_ring(
        &self,
        stream_id: PeerMessageId,
    ) -> Option<Arc<crate::ring_buffer::InboundRingBuffer>> {
        let inbound = self.inbound.lock().await;
        inbound
            .recv_streams
            .get(&stream_id)
            .map(|state| Arc::clone(&state.ring))
    }

    pub(super) async fn recv_chunk_state(
        &self,
        stream_id: PeerMessageId,
        idle_timeout: Duration,
    ) -> BlobRecvChunkState {
        let now = Instant::now();
        let removed = {
            let mut inbound = self.inbound.lock().await;
            let Some(state) = inbound.recv_streams.get_mut(&stream_id) else {
                if let Some(ts) = inbound.completed_recv_streams.get(&stream_id).copied() {
                    if now.duration_since(ts) <= idle_timeout {
                        return BlobRecvChunkState::RecentlyCompleted;
                    }
                    inbound.completed_recv_streams.remove(&stream_id);
                }
                return BlobRecvChunkState::Missing;
            };

            if now >= state.deadline {
                let taberna_id = state.taberna_id;
                let receiver = Arc::clone(&state.receiver);
                inbound.recv_streams.remove(&stream_id);
                Some((taberna_id, receiver))
            } else {
                return BlobRecvChunkState::Active {
                    taberna_id: state.taberna_id,
                    receiver: Arc::clone(&state.receiver),
                    ring: Arc::clone(&state.ring),
                    notify: Arc::clone(&state.receiver.notify),
                };
            }
        };
        let Some((taberna_id, receiver)) = removed else {
            return BlobRecvChunkState::Missing;
        };
        self.release_inbound(stream_id).await;
        BlobRecvChunkState::IdleTimedOut {
            taberna_id,
            receiver,
        }
    }

    pub(super) async fn refresh_recv_deadline(
        &self,
        stream_id: PeerMessageId,
        idle_timeout: Duration,
    ) {
        let mut inbound = self.inbound.lock().await;
        if let Some(state) = inbound.recv_streams.get_mut(&stream_id) {
            state.deadline = Instant::now() + idle_timeout;
        }
        drop(inbound);
        self.inbound_idle_notify.notify_one();
    }

    pub(super) async fn remove_recv_stream(
        &self,
        stream_id: PeerMessageId,
    ) -> Option<BlobRecvStream> {
        let removed = {
            let mut inbound = self.inbound.lock().await;
            inbound.recv_streams.remove(&stream_id)
        };
        self.release_inbound(stream_id).await;
        removed
    }

    async fn earliest_recv_deadline(&self) -> Option<Instant> {
        let inbound = self.inbound.lock().await;
        inbound
            .recv_streams
            .values()
            .map(|stream| stream.deadline)
            .min()
    }

    async fn expire_recv_streams(
        &self,
        now: Instant,
    ) -> Vec<(PeerMessageId, Arc<BlobReceiverState>)> {
        let expired = {
            let mut inbound = self.inbound.lock().await;
            let expired_ids = inbound
                .recv_streams
                .iter()
                .filter_map(|(stream_id, stream)| (stream.deadline <= now).then_some(*stream_id))
                .collect::<Vec<_>>();
            expired_ids
                .into_iter()
                .filter_map(|stream_id| {
                    inbound
                        .recv_streams
                        .remove(&stream_id)
                        .map(|stream| (stream_id, stream.receiver))
                })
                .collect::<Vec<_>>()
        };
        for (stream_id, _) in &expired {
            self.release_inbound(*stream_id).await;
        }
        expired
    }

    pub(super) async fn run_inbound_idle_reaper(
        self: Arc<Self>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        loop {
            let waiter = self.inbound_idle_notify.notified();
            tokio::pin!(waiter);
            if *shutdown_rx.borrow() {
                break;
            }
            let Some(deadline) = self.earliest_recv_deadline().await else {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = &mut waiter => {}
                }
                continue;
            };
            let now = Instant::now();
            if deadline <= now {
                let expired = self.expire_recv_streams(now).await;
                for (stream_id, receiver) in expired {
                    let err = AureliaError::new(ErrorId::BlobStreamIdleTimeout);
                    receiver.fail(err.clone()).await;
                    let payload = ErrorPayload::new(err.kind.as_u32(), err.to_string()).to_bytes();
                    let _ = self
                        .enqueue_blob_write(BlobWriteLease::Error {
                            peer_msg_id: stream_id,
                            payload: Bytes::from(payload),
                        })
                        .await;
                }
                continue;
            }
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {}
                _ = &mut waiter => {}
            }
        }
    }

    pub(super) async fn has_active_streams(&self) -> bool {
        let inbound = self.inbound.lock().await;
        if !inbound.pending_requests.is_empty() || !inbound.recv_streams.is_empty() {
            return true;
        }
        drop(inbound);
        let outbound = self.outbound.lock().await;
        !outbound.streams.is_empty()
    }

    pub(super) async fn lifecycle_snapshot(&self) -> BlobLifecycleSnapshot {
        let has_callis = {
            let pool = self.callis.lock().await;
            !pool.order.is_empty()
        };
        let inbound_active = {
            let inbound = self.inbound.lock().await;
            !inbound.pending_requests.is_empty() || !inbound.recv_streams.is_empty()
        };
        let (outbound_active, outbound_work) = {
            let outbound = self.outbound.lock().await;
            (
                !outbound.streams.is_empty(),
                outbound.response_ready_count() > 0 || !outbound.streams.is_empty(),
            )
        };
        let has_active_streams = inbound_active || outbound_active;
        BlobLifecycleSnapshot {
            has_callis,
            has_active_streams,
            has_blob_work: has_active_streams || outbound_work,
        }
    }

    pub(super) async fn fail_all_streams(&self, error: AureliaError) {
        let rings = {
            let mut outbound = self.outbound.lock().await;
            let rings: Vec<Arc<crate::ring_buffer::OutboundRingBuffer>> =
                outbound.streams.values().cloned().collect();
            outbound.streams.clear();
            outbound.inflight.clear();
            outbound.write_slots.clear();
            outbound.ack_ready.clear();
            outbound.error_ready.clear();
            outbound.complete_ready.clear();
            outbound.stream_settings.clear();
            rings
        };
        for ring in rings {
            ring.fail(error.clone()).await;
            ring.close().await;
        }

        let reservations = {
            let mut reservations = self.reservations.lock().await;
            let outbound = reservations
                .outbound
                .drain()
                .map(|(_, bytes)| bytes)
                .collect::<Vec<_>>();
            let inbound = reservations
                .inbound
                .drain()
                .map(|(_, bytes)| bytes)
                .collect::<Vec<_>>();
            (outbound, inbound)
        };
        for bytes in reservations.0 {
            self.buffer_tracker.release_outbound(bytes);
        }
        for bytes in reservations.1 {
            self.buffer_tracker.release_inbound(bytes);
        }

        let streams = {
            let mut inbound = self.inbound.lock().await;
            inbound.pending_requests.clear();
            inbound.completed_recv_streams.clear();
            inbound.recv_streams.drain().collect::<Vec<_>>()
        };
        for (_stream_id, stream) in streams {
            stream.receiver.fail(error.clone()).await;
        }
    }
}

fn order_streams_from_cursor(
    streams: &mut [(PeerMessageId, Arc<crate::ring_buffer::OutboundRingBuffer>)],
    cursor: Option<PeerMessageId>,
) {
    streams.sort_unstable_by_key(|(stream_id, _)| *stream_id);
    let Some(cursor) = cursor else {
        return;
    };
    let split = streams
        .iter()
        .position(|(stream_id, _)| *stream_id >= cursor)
        .unwrap_or(0);
    streams.rotate_left(split);
}

fn next_stream_after(
    streams: &HashMap<PeerMessageId, Arc<crate::ring_buffer::OutboundRingBuffer>>,
    stream_id: PeerMessageId,
) -> Option<PeerMessageId> {
    let mut ids = streams.keys().copied().collect::<Vec<_>>();
    ids.sort_unstable();
    if ids.is_empty() {
        return None;
    }
    ids.iter()
        .copied()
        .find(|candidate| *candidate > stream_id)
        .or_else(|| ids.first().copied())
}

pub(super) mod io;
pub(super) mod receive;

pub(crate) use receive::blob_buffer_full_error;
pub(super) use receive::{handle_blob_request, handle_blob_transfer_chunk};
