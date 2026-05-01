// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BlobCallisSettings {
    pub(super) chunk_size: u32,
    pub(super) ack_window_chunks: u32,
}

pub(super) struct PendingBlobRequest {
    taberna_id: TabernaId,
    receiver: Arc<BlobReceiverState>,
}

pub(super) struct BlobRecvStream {
    taberna_id: TabernaId,
    receiver: Arc<BlobReceiverState>,
    pub(super) ring: Arc<crate::ring_buffer::InboundRingBuffer>,
    pub(super) last_activity: Instant,
}

pub(crate) struct BlobReceiverState {
    pub(super) notify: Arc<Notify>,
    pub(super) accepted: AtomicBool,
    pub(super) completed: AtomicBool,
    pub(super) error: Mutex<Option<AureliaError>>,
    pub(super) completion_ttl: Duration,
    pub(super) idle_timeout: Duration,
}

#[derive(Debug)]
pub(super) enum BlobChunkOutcome {
    Continue,
    Complete(PeerMessageId),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RetainedBlobKind {
    Start,
    Chunk { chunk_id: u64 },
}

#[derive(Clone)]
pub(super) struct RetainedBlobFrame {
    stream_id: PeerMessageId,
    kind: RetainedBlobKind,
    frame: OutboundFrame,
}

pub(super) struct DispatchFrame {
    pub(super) stream_id: PeerMessageId,
    pub(super) peer_msg_id: PeerMessageId,
    pub(super) frame: OutboundFrame,
    pub(super) is_chunk: bool,
    pub(super) retained_kind: Option<RetainedBlobKind>,
}

pub(super) struct BlobCallisPool {
    order: VecDeque<CallisId>,
    handles: HashMap<CallisId, CallisHandle>,
    settings: HashMap<CallisId, BlobCallisSettings>,
}

pub(super) struct InflightBlobFrame {
    stream_id: PeerMessageId,
    callis_id: CallisId,
    is_chunk: bool,
}

type RetainedReplayStart = Option<(PeerMessageId, OutboundFrame)>;
type RetainedReplayChunks = BTreeMap<u64, (PeerMessageId, OutboundFrame)>;
type RetainedReplayByStream = BTreeMap<PeerMessageId, (RetainedReplayStart, RetainedReplayChunks)>;

pub(super) struct BlobManager {
    callis: Mutex<BlobCallisPool>,
    callis_notify: Notify,
    dispatch_notify: Arc<Notify>,
    had_callis: AtomicBool,
    outbound_streams: Mutex<HashMap<PeerMessageId, Arc<crate::ring_buffer::OutboundRingBuffer>>>,
    inflight_map: Mutex<HashMap<PeerMessageId, InflightBlobFrame>>,
    inflight_chunks: Mutex<HashMap<PeerMessageId, usize>>,
    pub(super) retained: Mutex<HashMap<PeerMessageId, RetainedBlobFrame>>,
    pending_requests: Mutex<HashMap<PeerMessageId, PendingBlobRequest>>,
    pub(super) recv_streams: Mutex<HashMap<PeerMessageId, BlobRecvStream>>,
    completed_streams: Mutex<HashSet<PeerMessageId>>,
    completed_recv_streams: Mutex<HashMap<PeerMessageId, Instant>>,
    pending_replay: Mutex<HashSet<PeerMessageId>>,
    outbound_reservations: Mutex<HashMap<PeerMessageId, u64>>,
    inbound_reservations: Mutex<HashMap<PeerMessageId, u64>>,
    buffer_tracker: Arc<BlobBufferTracker>,
    allocator: Arc<PeerMessageIdAllocator>,
    stream_callis: Mutex<HashMap<PeerMessageId, CallisId>>,
    callis_streams: Mutex<HashMap<CallisId, HashSet<PeerMessageId>>>,
    stream_settings: Mutex<HashMap<PeerMessageId, BlobCallisSettings>>,
    unassigned_streams: Mutex<HashSet<PeerMessageId>>,
    outbound_rr: AtomicUsize,
}

impl BlobManager {
    pub(super) fn new(
        buffer_tracker: Arc<BlobBufferTracker>,
        dispatch_notify: Arc<Notify>,
        allocator: Arc<PeerMessageIdAllocator>,
    ) -> Self {
        Self {
            callis: Mutex::new(BlobCallisPool {
                order: VecDeque::new(),
                handles: HashMap::new(),
                settings: HashMap::new(),
            }),
            callis_notify: Notify::new(),
            dispatch_notify,
            had_callis: AtomicBool::new(false),
            outbound_streams: Mutex::new(HashMap::new()),
            inflight_map: Mutex::new(HashMap::new()),
            inflight_chunks: Mutex::new(HashMap::new()),
            retained: Mutex::new(HashMap::new()),
            pending_requests: Mutex::new(HashMap::new()),
            recv_streams: Mutex::new(HashMap::new()),
            completed_streams: Mutex::new(HashSet::new()),
            completed_recv_streams: Mutex::new(HashMap::new()),
            pending_replay: Mutex::new(HashSet::new()),
            outbound_reservations: Mutex::new(HashMap::new()),
            inbound_reservations: Mutex::new(HashMap::new()),
            buffer_tracker,
            allocator,
            stream_callis: Mutex::new(HashMap::new()),
            callis_streams: Mutex::new(HashMap::new()),
            stream_settings: Mutex::new(HashMap::new()),
            unassigned_streams: Mutex::new(HashSet::new()),
            outbound_rr: AtomicUsize::new(0),
        }
    }

    #[cfg(test)]
    pub(super) fn new_for_tests() -> Self {
        Self::new(
            Arc::new(BlobBufferTracker::default()),
            Arc::new(Notify::new()),
            Arc::new(PeerMessageIdAllocator::default()),
        )
    }

    pub(super) fn notify_dispatch(&self) {
        self.dispatch_notify.notify_one();
    }

    pub(super) fn dispatch_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.dispatch_notify)
    }

    pub(super) fn callis_notify(&self) -> &Notify {
        &self.callis_notify
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
        let mut guard = self.outbound_reservations.lock().await;
        guard.insert(stream_id, bytes);
        true
    }

    pub(super) async fn release_outbound(&self, stream_id: PeerMessageId) {
        let bytes = {
            let mut guard = self.outbound_reservations.lock().await;
            guard.remove(&stream_id)
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
        let mut guard = self.inbound_reservations.lock().await;
        guard.insert(stream_id, bytes);
        true
    }

    pub(super) async fn release_inbound(&self, stream_id: PeerMessageId) {
        let bytes = {
            let mut guard = self.inbound_reservations.lock().await;
            guard.remove(&stream_id)
        };
        if let Some(bytes) = bytes {
            self.buffer_tracker.release_inbound(bytes);
        }
    }

    pub(super) async fn schedule_replay_for_streams<I>(&self, streams: I)
    where
        I: IntoIterator<Item = PeerMessageId>,
    {
        let mut pending = self.pending_replay.lock().await;
        let mut changed = false;
        for stream_id in streams {
            if pending.insert(stream_id) {
                changed = true;
            }
        }
        drop(pending);
        if changed {
            self.notify_dispatch();
        }
    }

    pub(super) async fn take_pending_replay_snapshot(&self) -> Vec<PeerMessageId> {
        let pending = self.pending_replay.lock().await;
        pending.iter().copied().collect()
    }

    pub(super) async fn next_replay_frame(&self) -> Option<DispatchFrame> {
        let pending_streams = self.take_pending_replay_snapshot().await;
        if pending_streams.is_empty() {
            return None;
        }
        let inflight = {
            let guard = self.inflight_map.lock().await;
            guard.keys().copied().collect::<HashSet<_>>()
        };
        let pending_set: HashSet<PeerMessageId> = pending_streams.iter().copied().collect();
        let mut per_stream: RetainedReplayByStream = BTreeMap::new();
        let retained = self.retained.lock().await;
        for (peer_msg_id, retained) in retained.iter() {
            if inflight.contains(peer_msg_id) {
                continue;
            }
            if !pending_set.contains(&retained.stream_id) {
                continue;
            }
            let entry = per_stream
                .entry(retained.stream_id)
                .or_insert_with(|| (None, BTreeMap::new()));
            match retained.kind {
                RetainedBlobKind::Start => {
                    entry.0 = Some((*peer_msg_id, retained.frame.clone()));
                }
                RetainedBlobKind::Chunk { chunk_id } => {
                    entry
                        .1
                        .insert(chunk_id, (*peer_msg_id, retained.frame.clone()));
                }
            }
        }
        drop(retained);
        let mut empty_streams = Vec::new();
        let mut selected: Option<(PeerMessageId, PeerMessageId, OutboundFrame)> = None;
        let mut ordered_streams = pending_streams.clone();
        ordered_streams.sort_unstable();
        for stream_id in ordered_streams {
            match per_stream.get(&stream_id) {
                Some((start, chunks)) => {
                    if let Some((peer_msg_id, frame)) = start {
                        selected = Some((stream_id, *peer_msg_id, frame.clone()));
                        break;
                    }
                    if let Some((_chunk_id, (peer_msg_id, frame))) = chunks.iter().next() {
                        if !self.is_chunk_window_full(stream_id).await {
                            selected = Some((stream_id, *peer_msg_id, frame.clone()));
                            break;
                        }
                        continue;
                    }
                    empty_streams.push(stream_id);
                }
                None => empty_streams.push(stream_id),
            }
        }
        if !empty_streams.is_empty() {
            let mut pending = self.pending_replay.lock().await;
            for stream_id in empty_streams {
                pending.remove(&stream_id);
            }
        }
        let (stream_id, peer_msg_id, frame) = selected?;
        let is_chunk = matches!(
            frame,
            OutboundFrame::Control { msg_type, .. } if msg_type == MSG_BLOB_TRANSFER_CHUNK
        );
        if is_chunk && self.is_chunk_window_full(stream_id).await {
            return None;
        }
        Some(DispatchFrame {
            stream_id,
            peer_msg_id,
            frame,
            is_chunk,
            retained_kind: None,
        })
    }

    pub(super) async fn next_chunk_frame(&self) -> Option<DispatchFrame> {
        let streams = {
            let guard = self.outbound_streams.lock().await;
            guard
                .iter()
                .map(|(stream_id, ring)| (*stream_id, Arc::clone(ring)))
                .collect::<Vec<_>>()
        };
        if streams.is_empty() {
            return None;
        }
        let start = self.outbound_rr.load(Ordering::SeqCst);
        let total = streams.len();
        for offset in 0..total {
            let idx = (start + offset) % total;
            let (stream_id, ring) = &streams[idx];
            if self.is_chunk_window_full(*stream_id).await {
                continue;
            }
            if !ring.is_sendable().await {
                continue;
            }
            let peer_msg_id = self.allocator.next();
            let Some(chunk) = ring.take_next_chunk(peer_msg_id).await else {
                continue;
            };
            let mut flags = BlobChunkFlags::empty();
            if chunk.is_last {
                flags |= BlobChunkFlags::LAST_CHUNK;
            }
            let payload = BlobTransferChunkPayload {
                request_msg_id: *stream_id,
                chunk_id: chunk.chunk_id,
                flags,
                chunk: chunk.data,
            };
            let frame = OutboundFrame::Control {
                msg_type: MSG_BLOB_TRANSFER_CHUNK,
                peer_msg_id,
                payload: Bytes::from(payload.to_bytes()),
            };
            self.outbound_rr.store((idx + 1) % total, Ordering::SeqCst);
            return Some(DispatchFrame {
                stream_id: *stream_id,
                peer_msg_id,
                frame,
                is_chunk: true,
                retained_kind: Some(RetainedBlobKind::Chunk {
                    chunk_id: chunk.chunk_id,
                }),
            });
        }
        None
    }

    pub(super) async fn is_chunk_window_full(&self, stream_id: PeerMessageId) -> bool {
        let Some(settings) = self.settings_for_stream(stream_id).await else {
            return false;
        };
        if settings.ack_window_chunks == 0 {
            return true;
        }
        let inflight = self.inflight_chunk_count(stream_id).await;
        inflight as u32 >= settings.ack_window_chunks
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
        self.had_callis.store(true, Ordering::SeqCst);
        {
            let mut pool = self.callis.lock().await;
            pool.handles.insert(handle.id, handle.clone());
            pool.settings.insert(handle.id, settings);
            if !pool.order.contains(&handle.id) {
                pool.order.push_back(handle.id);
            }
        }
        self.callis_notify.notify_waiters();
        self.notify_dispatch();
        if resume {
            self.reassign_unassigned_streams(handle.id).await;
        }
    }

    pub(super) async fn has_stream_state(&self) -> bool {
        let outbound = self.outbound_streams.lock().await;
        if !outbound.is_empty() {
            return true;
        }
        drop(outbound);
        let recv = self.recv_streams.lock().await;
        if !recv.is_empty() {
            return true;
        }
        drop(recv);
        let retained = self.retained.lock().await;
        !retained.is_empty()
    }

    pub(super) async fn remove_callis(
        &self,
        callis_id: CallisId,
    ) -> (Option<CallisHandle>, Vec<PeerMessageId>) {
        let handle = {
            let mut pool = self.callis.lock().await;
            let handle = pool.handles.remove(&callis_id);
            pool.settings.remove(&callis_id);
            if let Some(pos) = pool.order.iter().position(|id| *id == callis_id) {
                pool.order.remove(pos);
            }
            handle
        };
        let mut streams = Vec::new();
        {
            let mut callis_streams = self.callis_streams.lock().await;
            if let Some(set) = callis_streams.remove(&callis_id) {
                streams.extend(set);
            }
        }
        if !streams.is_empty() {
            let mut stream_callis = self.stream_callis.lock().await;
            let mut unassigned = self.unassigned_streams.lock().await;
            for stream_id in &streams {
                stream_callis.remove(stream_id);
                unassigned.insert(*stream_id);
            }
        }
        (handle, streams)
    }

    pub(super) async fn drain_callis(&self) -> (Vec<CallisHandle>, Vec<PeerMessageId>) {
        let mut pool = self.callis.lock().await;
        let handles = pool.handles.values().cloned().collect::<Vec<_>>();
        pool.handles.clear();
        pool.settings.clear();
        pool.order.clear();
        drop(pool);
        let streams = {
            let mut callis_streams = self.callis_streams.lock().await;
            let streams: Vec<PeerMessageId> = callis_streams
                .values()
                .flat_map(|set| set.iter().copied())
                .collect();
            callis_streams.clear();
            streams
        };
        if !streams.is_empty() {
            let mut stream_callis = self.stream_callis.lock().await;
            let mut unassigned = self.unassigned_streams.lock().await;
            for stream_id in &streams {
                stream_callis.remove(stream_id);
                unassigned.insert(*stream_id);
            }
        }
        (handles, streams)
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

    pub(super) async fn send_stream_error(
        &self,
        stream_id: PeerMessageId,
        error: AureliaError,
    ) -> Result<(), AureliaError> {
        let payload = ErrorPayload::new(error.kind.as_u32(), error.to_string()).to_bytes();
        let frame = OutboundFrame::Control {
            msg_type: MSG_ERROR,
            peer_msg_id: stream_id,
            payload: Bytes::from(payload),
        };
        let Some((_callis_id, handle, _settings)) = self.select_callis().await else {
            return Err(AureliaError::new(ErrorId::PeerUnavailable));
        };
        handle
            .tx
            .send(frame)
            .await
            .map_err(|_| AureliaError::new(ErrorId::PeerUnavailable))
    }

    pub(super) async fn take_available_callis(&self) -> Option<(CallisId, CallisHandle)> {
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
            if handle.tx.is_closed() {
                pool.handles.remove(&id);
                pool.settings.remove(&id);
                continue;
            }
            pool.order.push_back(id);
            if handle
                .available
                .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Some((id, handle));
            }
        }
        None
    }

    pub(super) async fn settings_for_stream(
        &self,
        stream_id: PeerMessageId,
    ) -> Option<BlobCallisSettings> {
        let guard = self.stream_settings.lock().await;
        guard.get(&stream_id).copied()
    }

    pub(super) async fn wait_for_callis(&self, timeout: Duration) -> Result<(), AureliaError> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.has_callis().await {
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(AureliaError::new(ErrorId::SendTimeout));
            }
            if tokio::time::timeout(remaining, self.callis_notify.notified())
                .await
                .is_err()
            {
                return Err(AureliaError::new(ErrorId::SendTimeout));
            }
        }
    }

    pub(super) async fn register_outbound_stream(
        &self,
        stream_id: PeerMessageId,
        callis_id: CallisId,
        settings: BlobCallisSettings,
    ) -> Result<Arc<crate::ring_buffer::OutboundRingBuffer>, AureliaError> {
        let ring = Arc::new(crate::ring_buffer::OutboundRingBuffer::new(
            settings.chunk_size as usize,
            settings.ack_window_chunks as usize,
        )?);
        let mut guard = self.outbound_streams.lock().await;
        guard.insert(stream_id, Arc::clone(&ring));
        drop(guard);
        let mut completed = self.completed_streams.lock().await;
        let pending_complete = completed.remove(&stream_id);
        drop(completed);
        if pending_complete {
            ring.mark_complete().await;
        }
        let mut stream_callis = self.stream_callis.lock().await;
        stream_callis.insert(stream_id, callis_id);
        drop(stream_callis);
        let mut stream_settings = self.stream_settings.lock().await;
        stream_settings.insert(stream_id, settings);
        drop(stream_settings);
        let mut callis_streams = self.callis_streams.lock().await;
        callis_streams
            .entry(callis_id)
            .or_insert_with(HashSet::new)
            .insert(stream_id);
        drop(callis_streams);
        let mut unassigned = self.unassigned_streams.lock().await;
        unassigned.remove(&stream_id);
        Ok(ring)
    }

    pub(super) async fn unregister_outbound_stream(&self, stream_id: PeerMessageId) {
        self.release_outbound(stream_id).await;
        let mut streams = self.outbound_streams.lock().await;
        let ring = streams.remove(&stream_id);
        drop(streams);
        if let Some(ring) = ring {
            ring.close().await;
        }
        let removed_chunks = {
            let mut inflight = self.inflight_map.lock().await;
            let mut removed = 0usize;
            inflight.retain(|_, entry| {
                if entry.stream_id == stream_id {
                    if entry.is_chunk {
                        removed += 1;
                    }
                    false
                } else {
                    true
                }
            });
            removed
        };
        if removed_chunks > 0 {
            let mut counts = self.inflight_chunks.lock().await;
            if let Some(count) = counts.get_mut(&stream_id) {
                if *count <= removed_chunks {
                    counts.remove(&stream_id);
                } else {
                    *count -= removed_chunks;
                }
            }
        }
        let mut retained = self.retained.lock().await;
        retained.retain(|_, frame| frame.stream_id != stream_id);
        drop(retained);
        let callis_id = {
            let mut stream_callis = self.stream_callis.lock().await;
            stream_callis.remove(&stream_id)
        };
        let mut stream_settings = self.stream_settings.lock().await;
        stream_settings.remove(&stream_id);
        drop(stream_settings);
        if let Some(callis_id) = callis_id {
            let mut callis_streams = self.callis_streams.lock().await;
            if let Some(set) = callis_streams.get_mut(&callis_id) {
                set.remove(&stream_id);
            }
        }
        let mut unassigned = self.unassigned_streams.lock().await;
        unassigned.remove(&stream_id);
        drop(unassigned);
        let mut completed = self.completed_streams.lock().await;
        completed.remove(&stream_id);
        drop(completed);
        let mut pending_replay = self.pending_replay.lock().await;
        pending_replay.remove(&stream_id);
    }

    pub(super) async fn track_inflight(
        &self,
        stream_id: PeerMessageId,
        peer_msg_id: PeerMessageId,
        callis_id: CallisId,
        is_chunk: bool,
    ) {
        let mut guard = self.inflight_map.lock().await;
        guard.insert(
            peer_msg_id,
            InflightBlobFrame {
                stream_id,
                callis_id,
                is_chunk,
            },
        );
        drop(guard);
        if is_chunk {
            let mut counts = self.inflight_chunks.lock().await;
            let entry = counts.entry(stream_id).or_insert(0);
            *entry = entry.saturating_add(1);
        }
    }

    pub(super) async fn inflight_chunk_count(&self, stream_id: PeerMessageId) -> usize {
        let counts = self.inflight_chunks.lock().await;
        counts.get(&stream_id).copied().unwrap_or(0)
    }

    async fn release_inflight_chunk(&self, stream_id: PeerMessageId) {
        let mut counts = self.inflight_chunks.lock().await;
        if let Some(count) = counts.get_mut(&stream_id) {
            if *count <= 1 {
                counts.remove(&stream_id);
            } else {
                *count -= 1;
            }
        }
    }

    pub(super) async fn retain_frame(
        &self,
        stream_id: PeerMessageId,
        peer_msg_id: PeerMessageId,
        kind: RetainedBlobKind,
        frame: OutboundFrame,
    ) {
        let mut guard = self.retained.lock().await;
        guard.insert(
            peer_msg_id,
            RetainedBlobFrame {
                stream_id,
                kind,
                frame,
            },
        );
    }

    pub(super) async fn release_frame(&self, peer_msg_id: PeerMessageId) {
        let mut guard = self.retained.lock().await;
        guard.remove(&peer_msg_id);
    }

    pub(super) async fn callis_entry(
        &self,
        callis_id: CallisId,
    ) -> Option<(CallisHandle, BlobCallisSettings)> {
        let pool = self.callis.lock().await;
        let handle = pool.handles.get(&callis_id).cloned()?;
        let settings = *pool.settings.get(&callis_id)?;
        Some((handle, settings))
    }

    pub(super) async fn settings_for_callis(
        &self,
        callis_id: CallisId,
    ) -> Option<BlobCallisSettings> {
        let pool = self.callis.lock().await;
        pool.settings.get(&callis_id).copied()
    }

    pub(super) async fn assign_stream_to_callis(
        &self,
        stream_id: PeerMessageId,
        callis_id: CallisId,
        settings: BlobCallisSettings,
    ) {
        let mut stream_callis = self.stream_callis.lock().await;
        stream_callis.insert(stream_id, callis_id);
        drop(stream_callis);
        let mut callis_streams = self.callis_streams.lock().await;
        callis_streams
            .entry(callis_id)
            .or_insert_with(HashSet::new)
            .insert(stream_id);
        drop(callis_streams);
        let mut stream_settings = self.stream_settings.lock().await;
        stream_settings.entry(stream_id).or_insert(settings);
        drop(stream_settings);
        let mut unassigned = self.unassigned_streams.lock().await;
        unassigned.remove(&stream_id);
    }

    pub(super) async fn reassign_unassigned_streams(&self, callis_id: CallisId) {
        let Some((_handle, settings)) = self.callis_entry(callis_id).await else {
            return;
        };
        let streams: Vec<PeerMessageId> = {
            let guard = self.unassigned_streams.lock().await;
            guard.iter().copied().collect()
        };
        if streams.is_empty() {
            return;
        }
        let mut replay = HashSet::new();
        for stream_id in streams {
            if let Some(prev_settings) = self.settings_for_stream(stream_id).await {
                if prev_settings != settings {
                    self.fail_stream(stream_id, AureliaError::new(ErrorId::PeerRestarted))
                        .await;
                    continue;
                }
            }
            self.assign_stream_to_callis(stream_id, callis_id, settings)
                .await;
            replay.insert(stream_id);
        }
        if !replay.is_empty() {
            self.schedule_replay_for_streams(replay).await;
        }
    }

    pub(super) async fn reassign_streams(&self, streams: Vec<PeerMessageId>) {
        if streams.is_empty() {
            return;
        }
        let mut replay_map: HashMap<CallisId, HashSet<PeerMessageId>> = HashMap::new();
        for stream_id in streams {
            let Some((callis_id, _handle, settings)) = self.select_callis().await else {
                let mut unassigned = self.unassigned_streams.lock().await;
                unassigned.insert(stream_id);
                continue;
            };
            if let Some(prev_settings) = self.settings_for_stream(stream_id).await {
                if prev_settings != settings {
                    self.fail_stream(stream_id, AureliaError::new(ErrorId::PeerRestarted))
                        .await;
                    continue;
                }
            }
            self.assign_stream_to_callis(stream_id, callis_id, settings)
                .await;
            replay_map.entry(callis_id).or_default().insert(stream_id);
        }
        for (callis_id, streams) in replay_map {
            if self.callis_entry(callis_id).await.is_some() {
                self.schedule_replay_for_streams(streams).await;
            }
        }
    }

    pub(super) async fn fail_stream(&self, stream_id: PeerMessageId, error: AureliaError) {
        self.release_outbound(stream_id).await;
        let ring = {
            let mut outbound = self.outbound_streams.lock().await;
            outbound.remove(&stream_id)
        };
        if let Some(ring) = ring {
            ring.fail(error.clone()).await;
            ring.close().await;
        }
        let removed_chunks = {
            let mut inflight = self.inflight_map.lock().await;
            let mut removed = 0usize;
            inflight.retain(|_, entry| {
                if entry.stream_id == stream_id {
                    if entry.is_chunk {
                        removed += 1;
                    }
                    false
                } else {
                    true
                }
            });
            removed
        };
        if removed_chunks > 0 {
            let mut counts = self.inflight_chunks.lock().await;
            if let Some(count) = counts.get_mut(&stream_id) {
                if *count <= removed_chunks {
                    counts.remove(&stream_id);
                } else {
                    *count -= removed_chunks;
                }
            }
        }
        let mut retained = self.retained.lock().await;
        retained.retain(|_, frame| frame.stream_id != stream_id);
        drop(retained);
        let mut stream_callis = self.stream_callis.lock().await;
        let callis_id = stream_callis.remove(&stream_id);
        drop(stream_callis);
        let mut stream_settings = self.stream_settings.lock().await;
        stream_settings.remove(&stream_id);
        drop(stream_settings);
        let mut unassigned = self.unassigned_streams.lock().await;
        unassigned.remove(&stream_id);
        drop(unassigned);
        let mut completed = self.completed_streams.lock().await;
        completed.remove(&stream_id);
        drop(completed);
        let mut pending_replay = self.pending_replay.lock().await;
        pending_replay.remove(&stream_id);
        drop(pending_replay);
        if let Some(callis_id) = callis_id {
            let mut callis_streams = self.callis_streams.lock().await;
            if let Some(set) = callis_streams.get_mut(&callis_id) {
                set.remove(&stream_id);
            }
        }
    }

    pub(super) async fn handle_ack(&self, peer_msg_id: PeerMessageId) {
        let entry = {
            let mut guard = self.inflight_map.lock().await;
            guard.remove(&peer_msg_id)
        };
        self.release_frame(peer_msg_id).await;
        let Some(entry) = entry else {
            return;
        };
        if entry.is_chunk {
            self.release_inflight_chunk(entry.stream_id).await;
            self.notify_dispatch();
        }
        let ring = {
            let guard = self.outbound_streams.lock().await;
            guard.get(&entry.stream_id).cloned()
        };
        if let Some(ring) = ring {
            ring.note_ack(peer_msg_id).await;
        }
    }

    pub(super) async fn handle_error(&self, peer_msg_id: PeerMessageId, error: AureliaError) {
        let entry = {
            let mut guard = self.inflight_map.lock().await;
            guard.remove(&peer_msg_id)
        };
        self.release_frame(peer_msg_id).await;
        if let Some(entry) = entry {
            if entry.is_chunk {
                self.release_inflight_chunk(entry.stream_id).await;
                self.notify_dispatch();
            }
            let ring = {
                let guard = self.outbound_streams.lock().await;
                guard.get(&entry.stream_id).cloned()
            };
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
            let guard = self.outbound_streams.lock().await;
            guard.get(&stream_id).cloned()
        };
        let Some(ring) = ring else {
            return false;
        };
        ring.fail(error).await;
        self.unregister_outbound_stream(stream_id).await;
        true
    }

    async fn fail_inbound_stream(&self, stream_id: PeerMessageId, error: AureliaError) -> bool {
        let pending = {
            let mut guard = self.pending_requests.lock().await;
            guard.remove(&stream_id)
        };
        if let Some(pending) = pending {
            {
                let mut guard = pending.receiver.error.lock().await;
                *guard = Some(error);
            }
            pending.receiver.completed.store(true, Ordering::SeqCst);
            pending.receiver.notify.notify_waiters();
            self.release_inbound(stream_id).await;
            return true;
        }

        if let Some(stream) = self.remove_recv_stream(stream_id).await {
            {
                let mut guard = stream.receiver.error.lock().await;
                *guard = Some(error);
            }
            stream.receiver.completed.store(true, Ordering::SeqCst);
            stream.receiver.notify.notify_waiters();
            return true;
        }
        false
    }

    pub(super) async fn handle_complete(&self, stream_id: PeerMessageId) {
        let ring = {
            let guard = self.outbound_streams.lock().await;
            guard.get(&stream_id).cloned()
        };
        if let Some(ring) = ring {
            ring.mark_complete().await;
        } else {
            self.stash_complete(stream_id).await;
        }
    }

    pub(super) async fn stash_complete(&self, stream_id: PeerMessageId) {
        let mut completed = self.completed_streams.lock().await;
        completed.insert(stream_id);
    }

    pub(super) async fn note_recv_complete(&self, stream_id: PeerMessageId, ttl: Duration) {
        let now = Instant::now();
        let mut completed = self.completed_recv_streams.lock().await;
        completed.retain(|_, ts| now.duration_since(*ts) <= ttl);
        completed.insert(stream_id, now);
    }

    pub(super) async fn recently_completed(&self, stream_id: PeerMessageId, ttl: Duration) -> bool {
        let now = Instant::now();
        let mut completed = self.completed_recv_streams.lock().await;
        match completed.get(&stream_id).copied() {
            Some(ts) if now.duration_since(ts) <= ttl => true,
            Some(_) => {
                completed.remove(&stream_id);
                false
            }
            None => false,
        }
    }

    pub(super) async fn requeue_inflight_for_callis(&self, callis_id: CallisId) {
        let inflight_entries = {
            let mut guard = self.inflight_map.lock().await;
            let mut entries = Vec::new();
            guard.retain(|peer_msg_id, entry| {
                if entry.callis_id == callis_id {
                    entries.push((*peer_msg_id, entry.stream_id, entry.is_chunk));
                    false
                } else {
                    true
                }
            });
            entries
        };
        if inflight_entries.is_empty() {
            return;
        }
        {
            let mut removed: HashMap<PeerMessageId, usize> = HashMap::new();
            for (_, stream_id, is_chunk) in &inflight_entries {
                if *is_chunk {
                    *removed.entry(*stream_id).or_insert(0) += 1;
                }
            }
            let mut counts = self.inflight_chunks.lock().await;
            for (stream_id, removed_count) in removed {
                if let Some(count) = counts.get_mut(&stream_id) {
                    if *count <= removed_count {
                        counts.remove(&stream_id);
                    } else {
                        *count -= removed_count;
                    }
                }
            }
        }
        let mut streams = HashSet::new();
        for (_peer_msg_id, stream_id, _is_chunk) in inflight_entries {
            streams.insert(stream_id);
        }
        if !streams.is_empty() {
            self.schedule_replay_for_streams(streams).await;
        }
    }

    pub(super) async fn requeue_all_inflight(&self) {
        let inflight_entries = {
            let mut guard = self.inflight_map.lock().await;
            let entries: Vec<(PeerMessageId, PeerMessageId, bool)> = guard
                .iter()
                .map(|(peer_msg_id, entry)| (*peer_msg_id, entry.stream_id, entry.is_chunk))
                .collect();
            guard.clear();
            entries
        };
        if inflight_entries.is_empty() {
            return;
        }
        {
            let mut removed: HashMap<PeerMessageId, usize> = HashMap::new();
            for (_, stream_id, is_chunk) in &inflight_entries {
                if *is_chunk {
                    *removed.entry(*stream_id).or_insert(0) += 1;
                }
            }
            let mut counts = self.inflight_chunks.lock().await;
            for (stream_id, removed_count) in removed {
                if let Some(count) = counts.get_mut(&stream_id) {
                    if *count <= removed_count {
                        counts.remove(&stream_id);
                    } else {
                        *count -= removed_count;
                    }
                }
            }
        }
        let mut streams = HashSet::new();
        for (_peer_msg_id, stream_id, _is_chunk) in inflight_entries {
            streams.insert(stream_id);
        }
        if !streams.is_empty() {
            self.schedule_replay_for_streams(streams).await;
        }
    }

    pub(super) async fn add_pending_request(
        &self,
        stream_id: PeerMessageId,
        taberna_id: TabernaId,
        receiver: Arc<BlobReceiverState>,
    ) {
        let mut guard = self.pending_requests.lock().await;
        guard.insert(
            stream_id,
            PendingBlobRequest {
                taberna_id,
                receiver,
            },
        );
    }

    pub(super) async fn take_pending_request(
        &self,
        stream_id: PeerMessageId,
    ) -> Option<PendingBlobRequest> {
        let mut guard = self.pending_requests.lock().await;
        guard.remove(&stream_id)
    }

    pub(super) async fn drop_pending_request(&self, stream_id: PeerMessageId) {
        let removed = {
            let mut guard = self.pending_requests.lock().await;
            guard.remove(&stream_id)
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
        let mut guard = self.recv_streams.lock().await;
        guard.insert(
            stream_id,
            BlobRecvStream {
                taberna_id,
                receiver,
                ring,
                last_activity: Instant::now(),
            },
        );
    }

    pub(super) async fn remove_recv_stream(
        &self,
        stream_id: PeerMessageId,
    ) -> Option<BlobRecvStream> {
        let mut guard = self.recv_streams.lock().await;
        let removed = guard.remove(&stream_id);
        drop(guard);
        self.release_inbound(stream_id).await;
        removed
    }

    pub(super) async fn has_active_streams(&self) -> bool {
        let pending = self.pending_requests.lock().await;
        if !pending.is_empty() {
            return true;
        }
        drop(pending);
        let recv = self.recv_streams.lock().await;
        if !recv.is_empty() {
            return true;
        }
        drop(recv);
        let outbound = self.outbound_streams.lock().await;
        !outbound.is_empty()
    }

    pub(super) async fn fail_all_streams(&self, error: AureliaError) {
        let mut outbound = self.outbound_streams.lock().await;
        let rings: Vec<Arc<crate::ring_buffer::OutboundRingBuffer>> =
            outbound.values().cloned().collect();
        outbound.clear();
        drop(outbound);
        for ring in rings {
            ring.fail(error.clone()).await;
            ring.close().await;
        }

        let mut outbound_reservations = self.outbound_reservations.lock().await;
        for bytes in outbound_reservations.values() {
            self.buffer_tracker.release_outbound(*bytes);
        }
        outbound_reservations.clear();
        drop(outbound_reservations);

        let mut inbound_reservations = self.inbound_reservations.lock().await;
        for bytes in inbound_reservations.values() {
            self.buffer_tracker.release_inbound(*bytes);
        }
        inbound_reservations.clear();
        drop(inbound_reservations);

        let mut inflight = self.inflight_map.lock().await;
        inflight.clear();
        drop(inflight);
        let mut inflight_chunks = self.inflight_chunks.lock().await;
        inflight_chunks.clear();
        drop(inflight_chunks);

        let mut retained = self.retained.lock().await;
        retained.clear();
        drop(retained);

        let mut stream_callis = self.stream_callis.lock().await;
        stream_callis.clear();
        drop(stream_callis);

        let mut callis_streams = self.callis_streams.lock().await;
        callis_streams.clear();
        drop(callis_streams);

        let mut stream_settings = self.stream_settings.lock().await;
        stream_settings.clear();
        drop(stream_settings);

        let mut unassigned = self.unassigned_streams.lock().await;
        unassigned.clear();
        drop(unassigned);

        let mut completed = self.completed_streams.lock().await;
        completed.clear();
        drop(completed);
        let mut completed_recv = self.completed_recv_streams.lock().await;
        completed_recv.clear();
        drop(completed_recv);

        let mut pending_replay = self.pending_replay.lock().await;
        pending_replay.clear();
        drop(pending_replay);

        let mut pending = self.pending_requests.lock().await;
        pending.clear();
        drop(pending);

        let mut recv = self.recv_streams.lock().await;
        let streams: Vec<(PeerMessageId, BlobRecvStream)> = recv.drain().collect();
        drop(recv);
        for (_stream_id, stream) in streams {
            {
                let mut guard = stream.receiver.error.lock().await;
                *guard = Some(error.clone());
            }
            stream.receiver.completed.store(true, Ordering::SeqCst);
            stream.receiver.notify.notify_waiters();
        }
    }
}

pub(super) mod dispatch;
pub(super) mod io;
pub(super) mod receive;

pub(super) use dispatch::{dispatch_blob, send_blob_control_and_wait_ack};
pub(crate) use receive::blob_buffer_full_error;
pub(super) use receive::{
    handle_blob_request, handle_blob_transfer_chunk, handle_blob_transfer_start,
};
