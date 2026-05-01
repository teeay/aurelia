// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use aurelia_ids::PeerMessageId;
use aurelia_ids::{AureliaError, ErrorId};
use bytes::{Bytes, BytesMut};
use std::collections::{HashMap, VecDeque};
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tokio::time::{timeout, Instant};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboundChunk {
    pub chunk_id: u64,
    pub data: Bytes,
    pub is_last: bool,
}

#[derive(Debug)]
struct OutboundState {
    chunk_size: usize,
    window_size: usize,
    next_chunk_id: u64,
    write_buf: BytesMut,
    pending_full: Option<Bytes>,
    slots: HashMap<u64, OutboundSlot>,
    ready_queue: VecDeque<u64>,
    ack_map: HashMap<PeerMessageId, u64>,
    inflight: usize,
    sealed: bool,
    sealed_sent: bool,
    created_any: bool,
    closed: bool,
    failure: Option<AureliaError>,
    complete: bool,
    control: HashMap<PeerMessageId, ControlStatus>,
}

#[derive(Debug)]
enum OutboundSlot {
    Ready {
        data: Bytes,
        is_last: bool,
    },
    InFlight {
        #[allow(dead_code)]
        peer_msg_id: PeerMessageId,
        #[allow(dead_code)]
        is_last: bool,
    },
}

#[derive(Debug, Clone)]
enum ControlStatus {
    Pending,
    Acked,
    Error(AureliaError),
}

pub struct OutboundRingBuffer {
    inner: Mutex<OutboundState>,
    notify: Notify,
}

impl OutboundRingBuffer {
    pub fn new(chunk_size: usize, window_size: usize) -> Result<Self, AureliaError> {
        if chunk_size == 0 || window_size == 0 {
            return Err(AureliaError::new(ErrorId::ProtocolViolation));
        }
        Ok(Self {
            inner: Mutex::new(OutboundState {
                chunk_size,
                window_size,
                next_chunk_id: 0,
                write_buf: BytesMut::new(),
                pending_full: None,
                slots: HashMap::new(),
                ready_queue: VecDeque::new(),
                ack_map: HashMap::new(),
                inflight: 0,
                sealed: false,
                sealed_sent: false,
                created_any: false,
                closed: false,
                failure: None,
                complete: false,
                control: HashMap::new(),
            }),
            notify: Notify::new(),
        })
    }

    pub async fn push_bytes(
        &self,
        data: &[u8],
        send_timeout: Duration,
    ) -> Result<usize, AureliaError> {
        let mut offset = 0;
        loop {
            let (wait_for_capacity, has_full_chunk, notify_waiters) = {
                let mut wait_for_capacity = false;
                let mut notify_waiters = false;
                let mut state = self.inner.lock().await;
                if let Some(err) = state.failure.clone() {
                    return Err(err);
                }
                if state.closed || state.sealed || state.complete {
                    return Err(AureliaError::new(ErrorId::PeerUnavailable));
                }
                if offset < data.len() {
                    let remaining = data.len() - offset;
                    let space = state.chunk_size.saturating_sub(state.write_buf.len());
                    let take = remaining.min(space);
                    if take > 0 {
                        state
                            .write_buf
                            .extend_from_slice(&data[offset..offset + take]);
                        offset += take;
                    }
                }

                let chunk_size = state.chunk_size;
                while state.write_buf.len() >= chunk_size {
                    let buffered_chunks = state.slots.len() + state.pending_full.is_some() as usize;
                    if buffered_chunks + 1 > state.window_size {
                        wait_for_capacity = true;
                        break;
                    }
                    let chunk = state.write_buf.split_to(chunk_size).freeze();
                    state.created_any = true;
                    if let Some(pending) = state.pending_full.take() {
                        Self::enqueue_chunk(&mut state, pending, false);
                        notify_waiters = true;
                    }
                    state.pending_full = Some(chunk);
                }
                if state.pending_full.is_some() && !state.write_buf.is_empty() {
                    if let Some(pending) = state.pending_full.take() {
                        Self::enqueue_chunk(&mut state, pending, false);
                        notify_waiters = true;
                    }
                }
                let has_full_chunk = state.write_buf.len() >= state.chunk_size;
                (wait_for_capacity, has_full_chunk, notify_waiters)
            };

            if notify_waiters {
                self.notify.notify_waiters();
            }
            if wait_for_capacity {
                let deadline = Instant::now() + send_timeout;
                self.wait_for_capacity(deadline).await?;
                continue;
            }
            if offset >= data.len() && !has_full_chunk {
                break;
            }
        }
        Ok(offset)
    }

    pub async fn seal(&self, send_timeout: Duration) -> Result<(), AureliaError> {
        loop {
            let (wait_for_capacity, done, notify_waiters) = {
                let mut state = self.inner.lock().await;
                if let Some(err) = state.failure.clone() {
                    return Err(err);
                }
                if state.closed {
                    return Err(AureliaError::new(ErrorId::PeerUnavailable));
                }
                if state.sealed_sent {
                    state.sealed = true;
                    return Ok(());
                }
                state.sealed = true;

                let mut wait_for_capacity = false;
                let mut done = false;
                let mut notify_waiters = false;
                let buffered_chunks = state.slots.len() + state.pending_full.is_some() as usize;
                let needs_extra = match (state.pending_full.is_some(), state.write_buf.is_empty()) {
                    (true, true) => 0,
                    (true, false) => 1,
                    (false, _) => 1,
                };
                if buffered_chunks + needs_extra > state.window_size {
                    wait_for_capacity = true;
                } else if let Some(pending) = state.pending_full.take() {
                    if state.write_buf.is_empty() {
                        Self::enqueue_chunk(&mut state, pending, true);
                    } else {
                        Self::enqueue_chunk(&mut state, pending, false);
                        let len = state.write_buf.len();
                        let chunk = state.write_buf.split_to(len).freeze();
                        Self::enqueue_chunk(&mut state, chunk, true);
                    }
                    state.sealed_sent = true;
                    done = true;
                    notify_waiters = true;
                } else if state.write_buf.is_empty() && state.created_any {
                    state.sealed_sent = true;
                    done = true;
                    notify_waiters = true;
                } else {
                    let chunk = if state.write_buf.is_empty() {
                        Bytes::new()
                    } else {
                        let len = state.write_buf.len();
                        state.write_buf.split_to(len).freeze()
                    };
                    state.created_any = state.created_any || !chunk.is_empty();
                    Self::enqueue_chunk(&mut state, chunk, true);
                    state.sealed_sent = true;
                    done = true;
                    notify_waiters = true;
                }
                (wait_for_capacity, done, notify_waiters)
            };

            if notify_waiters {
                self.notify.notify_waiters();
            }
            if done {
                return Ok(());
            }
            if wait_for_capacity {
                let deadline = Instant::now() + send_timeout;
                self.wait_for_capacity(deadline).await?;
            }
        }
    }

    pub async fn take_next_chunk(&self, peer_msg_id: PeerMessageId) -> Option<OutboundChunk> {
        let mut state = self.inner.lock().await;
        if state.closed {
            return None;
        }
        if let Some(err) = state.failure.as_ref() {
            let _ = err;
            return None;
        }

        while let Some(chunk_id) = state.ready_queue.pop_front() {
            let Some(slot) = state.slots.get_mut(&chunk_id) else {
                continue;
            };
            if let OutboundSlot::Ready { data, is_last } = slot {
                let chunk = OutboundChunk {
                    chunk_id,
                    data: std::mem::replace(data, Bytes::new()),
                    is_last: *is_last,
                };
                *slot = OutboundSlot::InFlight {
                    peer_msg_id,
                    is_last: chunk.is_last,
                };
                state.ack_map.insert(peer_msg_id, chunk_id);
                state.inflight = state.inflight.saturating_add(1);
                return Some(chunk);
            }
        }
        None
    }

    pub async fn wait_for_sendable(&self) -> Result<bool, AureliaError> {
        loop {
            {
                let state = self.inner.lock().await;
                if let Some(err) = state.failure.clone() {
                    return Err(err);
                }
                if state.closed {
                    return Ok(false);
                }
                if !state.ready_queue.is_empty() {
                    return Ok(true);
                }
                if state.sealed
                    && state.sealed_sent
                    && state.slots.is_empty()
                    && state.pending_full.is_none()
                {
                    return Ok(false);
                }
            }
            self.notify.notified().await;
        }
    }

    pub async fn is_sendable(&self) -> bool {
        let state = self.inner.lock().await;
        if state.closed {
            return false;
        }
        if state.failure.is_some() {
            return false;
        }
        !state.ready_queue.is_empty()
    }

    pub async fn wait_for_inflight_drain(&self, deadline: Instant) -> Result<(), AureliaError> {
        loop {
            {
                let state = self.inner.lock().await;
                if let Some(err) = state.failure.clone() {
                    return Err(err);
                }
                if state.inflight == 0 {
                    return Ok(());
                }
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(AureliaError::new(ErrorId::SendTimeout));
            }
            if timeout(remaining, self.notify.notified()).await.is_err() {
                return Err(AureliaError::new(ErrorId::SendTimeout));
            }
        }
    }

    pub async fn register_control(&self, peer_msg_id: PeerMessageId) {
        let mut state = self.inner.lock().await;
        state.control.insert(peer_msg_id, ControlStatus::Pending);
    }

    pub async fn wait_for_control(
        &self,
        peer_msg_id: PeerMessageId,
        deadline: Instant,
    ) -> Result<(), AureliaError> {
        loop {
            {
                let mut state = self.inner.lock().await;
                if let Some(err) = state.failure.clone() {
                    return Err(err);
                }
                match state.control.get(&peer_msg_id).cloned() {
                    Some(ControlStatus::Acked) => {
                        state.control.remove(&peer_msg_id);
                        return Ok(());
                    }
                    Some(ControlStatus::Error(err)) => {
                        state.control.remove(&peer_msg_id);
                        return Err(err);
                    }
                    Some(ControlStatus::Pending) => {}
                    None => {
                        return Err(AureliaError::new(ErrorId::PeerUnavailable));
                    }
                }
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(AureliaError::new(ErrorId::SendTimeout));
            }
            if timeout(remaining, self.notify.notified()).await.is_err() {
                return Err(AureliaError::new(ErrorId::SendTimeout));
            }
        }
    }

    pub async fn note_ack(&self, peer_msg_id: PeerMessageId) {
        let mut state = self.inner.lock().await;
        if let Some(entry) = state.control.get_mut(&peer_msg_id) {
            *entry = ControlStatus::Acked;
            drop(state);
            self.notify.notify_waiters();
            return;
        }
        let Some(chunk_id) = state.ack_map.remove(&peer_msg_id) else {
            return;
        };
        if let Some(slot) = state.slots.remove(&chunk_id) {
            if matches!(slot, OutboundSlot::InFlight { .. }) {
                state.inflight = state.inflight.saturating_sub(1);
            }
        }
        drop(state);
        self.notify.notify_waiters();
    }

    pub async fn note_error(&self, peer_msg_id: PeerMessageId, err: AureliaError) {
        let mut state = self.inner.lock().await;
        if let Some(entry) = state.control.get_mut(&peer_msg_id) {
            *entry = ControlStatus::Error(err.clone());
            drop(state);
            self.notify.notify_waiters();
            return;
        }
        if let Some(chunk_id) = state.ack_map.remove(&peer_msg_id) {
            state.slots.remove(&chunk_id);
            state.inflight = state.inflight.saturating_sub(1);
        }
        state.failure = Some(err);
        drop(state);
        self.notify.notify_waiters();
    }

    pub async fn fail(&self, err: AureliaError) {
        let mut state = self.inner.lock().await;
        if state.failure.is_none() {
            state.failure = Some(err);
        }
        drop(state);
        self.notify.notify_waiters();
    }

    pub async fn mark_complete(&self) {
        let mut state = self.inner.lock().await;
        state.complete = true;
        drop(state);
        self.notify.notify_waiters();
    }

    pub async fn wait_for_complete(&self, deadline: Instant) -> Result<(), AureliaError> {
        loop {
            {
                let state = self.inner.lock().await;
                if let Some(err) = state.failure.clone() {
                    return Err(err);
                }
                if state.complete {
                    return Ok(());
                }
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(AureliaError::new(ErrorId::SendTimeout));
            }
            if timeout(remaining, self.notify.notified()).await.is_err() {
                return Err(AureliaError::new(ErrorId::SendTimeout));
            }
        }
    }

    pub async fn close(&self) {
        let mut state = self.inner.lock().await;
        state.closed = true;
        drop(state);
        self.notify.notify_waiters();
    }

    pub async fn is_closed(&self) -> bool {
        let state = self.inner.lock().await;
        state.closed
    }

    fn enqueue_chunk(state: &mut OutboundState, chunk: Bytes, is_last: bool) {
        let chunk_id = state.next_chunk_id;
        state.next_chunk_id = state.next_chunk_id.saturating_add(1);
        state.created_any = true;
        state.slots.insert(
            chunk_id,
            OutboundSlot::Ready {
                data: chunk,
                is_last,
            },
        );
        state.ready_queue.push_back(chunk_id);
    }

    async fn wait_for_capacity(&self, deadline: Instant) -> Result<(), AureliaError> {
        loop {
            {
                let state = self.inner.lock().await;
                if let Some(err) = state.failure.clone() {
                    return Err(err);
                }
                if state.closed {
                    return Err(AureliaError::new(ErrorId::PeerUnavailable));
                }
                let buffered_chunks = state.slots.len() + state.pending_full.is_some() as usize;
                if buffered_chunks < state.window_size {
                    return Ok(());
                }
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(AureliaError::new(ErrorId::SendTimeout));
            }
            if timeout(remaining, self.notify.notified()).await.is_err() {
                return Err(AureliaError::new(ErrorId::SendTimeout));
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InboundInsertOutcome {
    Duplicate,
    Stored {
        complete: bool,
        wait_for_space: bool,
    },
}

#[derive(Debug)]
struct InboundState {
    chunk_size: usize,
    window_size: usize,
    next_expected: u64,
    next_deliver: u64,
    last_chunk_id: Option<u64>,
    received: HashMap<u64, Bytes>,
}

pub struct InboundRingBuffer {
    inner: Mutex<InboundState>,
    notify: Notify,
}

impl InboundRingBuffer {
    pub fn new(chunk_size: usize, window_size: usize) -> Result<Self, AureliaError> {
        if chunk_size == 0 || window_size == 0 {
            return Err(AureliaError::new(ErrorId::ProtocolViolation));
        }
        Ok(Self {
            inner: Mutex::new(InboundState {
                chunk_size,
                window_size,
                next_expected: 0,
                next_deliver: 0,
                last_chunk_id: None,
                received: HashMap::new(),
            }),
            notify: Notify::new(),
        })
    }

    pub async fn insert_chunk(
        &self,
        chunk_id: u64,
        data: Bytes,
        is_last: bool,
    ) -> Result<InboundInsertOutcome, AureliaError> {
        let mut state = self.inner.lock().await;
        if data.len() > state.chunk_size {
            return Err(AureliaError::with_message(
                ErrorId::ProtocolViolation,
                format!(
                    "chunk_len={} max_chunk_size={}",
                    data.len(),
                    state.chunk_size
                ),
            ));
        }
        if chunk_id < state.next_deliver {
            return Ok(InboundInsertOutcome::Duplicate);
        }
        if chunk_id >= state.next_expected.saturating_add(state.window_size as u64) {
            return Err(AureliaError::with_message(
                ErrorId::BlobAckWindowExceeded,
                format!(
                    "expected_chunk_id={} window_size={}",
                    state.next_expected, state.window_size
                ),
            ));
        }
        if state.received.contains_key(&chunk_id) {
            return Ok(InboundInsertOutcome::Duplicate);
        }
        if is_last && chunk_id > state.next_expected {
            return Err(missing_chunk_error(state.next_expected, state.chunk_size));
        }

        state.received.insert(chunk_id, data);

        if chunk_id == state.next_expected {
            while state.received.contains_key(&state.next_expected) {
                state.next_expected = state.next_expected.saturating_add(1);
            }
        }

        if state.received.len() >= state.window_size
            && !state.received.contains_key(&state.next_expected)
        {
            let has_gap = state
                .received
                .keys()
                .any(|&chunk_id| chunk_id > state.next_expected);
            if has_gap {
                return Err(missing_chunk_error(state.next_expected, state.chunk_size));
            }
        }

        if is_last {
            state.last_chunk_id = Some(chunk_id);
        }

        let complete = match state.last_chunk_id {
            Some(last) => state.next_expected == last.saturating_add(1),
            None => false,
        };
        let wait_for_space = state.received.len() >= state.window_size;
        drop(state);
        self.notify.notify_waiters();
        Ok(InboundInsertOutcome::Stored {
            complete,
            wait_for_space,
        })
    }

    pub async fn take_next(&self) -> Option<Bytes> {
        let mut state = self.inner.lock().await;
        let chunk_id = state.next_deliver;
        let data = state.received.remove(&chunk_id)?;
        state.next_deliver = state.next_deliver.saturating_add(1);
        if state.next_expected < state.next_deliver {
            state.next_expected = state.next_deliver;
        }
        drop(state);
        self.notify.notify_waiters();
        Some(data)
    }

    pub async fn len(&self) -> usize {
        let state = self.inner.lock().await;
        state.received.len()
    }

    pub async fn is_empty(&self) -> bool {
        let state = self.inner.lock().await;
        state.received.is_empty()
    }

    pub async fn is_complete(&self) -> bool {
        let state = self.inner.lock().await;
        match state.last_chunk_id {
            Some(last) => state.next_deliver > last,
            None => false,
        }
    }

    pub async fn wait_for_space(&self, deadline: Instant) -> bool {
        loop {
            {
                let state = self.inner.lock().await;
                if state.received.len() < state.window_size {
                    return true;
                }
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            if timeout(remaining, self.notify.notified()).await.is_err() {
                return false;
            }
        }
    }

    pub async fn next_expected(&self) -> u64 {
        let state = self.inner.lock().await;
        state.next_expected
    }
}

fn missing_chunk_error(expected: u64, chunk_size: usize) -> AureliaError {
    let delivered_bytes = expected.saturating_mul(chunk_size as u64);
    let last_delivered = if expected == 0 {
        "none".to_string()
    } else {
        expected.saturating_sub(1).to_string()
    };
    AureliaError::with_message(
        ErrorId::BlobStreamMissingChunk,
        format!(
            "last_delivered_chunk_id={} delivered_bytes={}",
            last_delivered, delivered_bytes
        ),
    )
}
