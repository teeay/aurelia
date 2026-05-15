// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

mod chunks;
mod state;

use self::chunks::{ChunkCompletion, FinalizeDecision};
use self::state::{OutboundLifecycle, OutboundState};
use aurelia_ids::PeerMessageId;
use aurelia_ids::{AureliaError, ErrorId};
use bytes::Bytes;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tokio::time::{timeout, Instant};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkWriteLease {
    pub chunk_id: u64,
    pub data: Bytes,
    pub is_last: bool,
    pub peer_msg_id: PeerMessageId,
    pub callis_id: u64,
    pub slot_seq: u64,
}

pub struct OutboundRingBuffer {
    inner: Mutex<OutboundState>,
    notify: Notify,
}

pub enum TryPushAvailable {
    Accepted { bytes: usize },
    Full,
    Busy,
}

impl OutboundRingBuffer {
    pub fn new(chunk_size: usize, window_size: usize) -> Result<Self, AureliaError> {
        if chunk_size == 0 || window_size == 0 {
            return Err(AureliaError::new(ErrorId::ProtocolViolation));
        }
        Ok(Self {
            inner: Mutex::new(OutboundState::new(chunk_size, window_size)),
            notify: Notify::new(),
        })
    }

    #[cfg(test)]
    pub(crate) async fn push_bytes_with_progress(
        &self,
        data: &[u8],
        send_timeout: Duration,
        mut on_progress: impl FnMut(),
    ) -> Result<usize, AureliaError> {
        let mut offset = 0;
        loop {
            let (wait_for_capacity, has_full_chunk, notify_waiters) = {
                let mut state = self.inner.lock().await;
                state.lifecycle.sender_error()?;
                state.chunks.push_bytes(data, &mut offset)
            };

            if notify_waiters {
                self.notify.notify_waiters();
                on_progress();
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

    pub(crate) fn try_push_available(
        &self,
        data: &[u8],
        mut on_progress: impl FnMut(),
    ) -> Result<TryPushAvailable, AureliaError> {
        let mut state = match self.inner.try_lock() {
            Ok(state) => state,
            Err(_) => return Ok(TryPushAvailable::Busy),
        };
        state.lifecycle.sender_error()?;
        if !state.chunks.can_accept_bytes() {
            return Ok(TryPushAvailable::Full);
        }
        let (bytes, notify_waiters) = state.chunks.push_available(data);
        drop(state);
        if notify_waiters {
            self.notify.notify_waiters();
            on_progress();
        }
        if bytes == 0 {
            Ok(TryPushAvailable::Full)
        } else {
            Ok(TryPushAvailable::Accepted { bytes })
        }
    }

    pub async fn seal(&self, send_timeout: Duration) -> Result<(), AureliaError> {
        loop {
            let (wait_for_capacity, done, notify_waiters) = {
                let mut state = self.inner.lock().await;
                state.lifecycle.seal_error()?;
                if state.lifecycle.final_chunk_materialized() {
                    return Ok(());
                }
                state.lifecycle.mark_sealed();

                match state.chunks.finalize() {
                    FinalizeDecision::WaitForCapacity => (true, false, false),
                    FinalizeDecision::Finalized { notify_waiters } => {
                        state.lifecycle.mark_final_chunk_sent();
                        (false, true, notify_waiters)
                    }
                }
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

    pub async fn lease_next_chunk_for_write(
        &self,
        callis_id: u64,
        peer_msg_id: PeerMessageId,
    ) -> Option<ChunkWriteLease> {
        let mut state = self.inner.lock().await;
        if !state.lifecycle.can_drain_chunks() {
            return None;
        }
        state
            .chunks
            .lease_next_chunk_for_write(callis_id, peer_msg_id)
    }

    pub async fn mark_chunk_inflight(&self, lease: &ChunkWriteLease) {
        let mut state = self.inner.lock().await;
        state.chunks.mark_inflight(lease);
    }

    pub async fn mark_callis_replay_ready(&self, callis_id: u64) {
        let mut state = self.inner.lock().await;
        let notify = state.chunks.mark_callis_replay_ready(callis_id);
        drop(state);
        if notify {
            self.notify.notify_waiters();
        }
    }

    pub async fn has_dispatchable_replay(&self) -> bool {
        let state = self.inner.lock().await;
        state.lifecycle.can_drain_chunks() && state.chunks.has_replay_ready()
    }

    pub async fn has_dispatchable_fresh(&self) -> bool {
        let state = self.inner.lock().await;
        state.lifecycle.can_drain_chunks() && state.chunks.has_fresh_ready()
    }

    pub async fn wait_for_inflight_drain(&self, deadline: Instant) -> Result<(), AureliaError> {
        self.wait_for_deadline(deadline, |state| {
            if let Some(err) = state.lifecycle.failure() {
                return Some(Err(err));
            }
            if state.chunks.inflight() == 0 {
                return Some(Ok(()));
            }
            None
        })
        .await
    }

    pub async fn note_ack(&self, peer_msg_id: PeerMessageId) {
        self.note_completion(peer_msg_id, CompletionNote::Ack).await;
    }

    pub async fn note_error(&self, peer_msg_id: PeerMessageId, err: AureliaError) {
        self.note_completion(peer_msg_id, CompletionNote::Error(err))
            .await;
    }

    pub async fn fail(&self, err: AureliaError) {
        let mut state = self.inner.lock().await;
        state.lifecycle.mark_failed(err);
        drop(state);
        self.notify.notify_waiters();
    }

    pub async fn mark_complete(&self) {
        let mut state = self.inner.lock().await;
        state.lifecycle.mark_complete();
        drop(state);
        self.notify.notify_waiters();
    }

    pub async fn wait_for_complete(&self, deadline: Instant) -> Result<(), AureliaError> {
        self.wait_for_deadline(deadline, |state| {
            if let Some(err) = state.lifecycle.failure() {
                return Some(Err(err));
            }
            if matches!(state.lifecycle, OutboundLifecycle::Completed) && state.chunks.is_drained()
            {
                return Some(Ok(()));
            }
            None
        })
        .await
    }

    pub async fn close(&self) {
        let mut state = self.inner.lock().await;
        state.lifecycle.mark_closed();
        drop(state);
        self.notify.notify_waiters();
    }

    pub(crate) async fn wait_for_capacity(&self, deadline: Instant) -> Result<(), AureliaError> {
        self.wait_for_deadline(deadline, |state| {
            if let Some(err) = state.lifecycle.failure() {
                return Some(Err(err));
            }
            if state.lifecycle.is_closed() {
                return Some(Err(AureliaError::new(ErrorId::PeerUnavailable)));
            }
            if state.chunks.can_accept_bytes() {
                return Some(Ok(()));
            }
            None
        })
        .await
    }

    async fn wait_for_deadline<T>(
        &self,
        deadline: Instant,
        mut check: impl FnMut(&mut OutboundState) -> Option<Result<T, AureliaError>>,
    ) -> Result<T, AureliaError> {
        // docs/concurrency.md Pattern 1: arm the notified() future BEFORE the state check.
        loop {
            let waiter = self.notify.notified();
            tokio::pin!(waiter);
            {
                let mut state = self.inner.lock().await;
                if let Some(result) = check(&mut state) {
                    return result;
                }
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(AureliaError::new(ErrorId::SendTimeout));
            }
            if timeout(remaining, &mut waiter).await.is_err() {
                return Err(AureliaError::new(ErrorId::SendTimeout));
            }
        }
    }

    async fn note_completion(&self, peer_msg_id: PeerMessageId, note: CompletionNote) {
        let mut state = self.inner.lock().await;
        let mut notify = false;
        let chunk_handled = state
            .chunks
            .note_completion(peer_msg_id, note.chunk_completion());
        if chunk_handled {
            notify = true;
        }
        if let CompletionNote::Error(err) = note {
            state.lifecycle.replace_failure(err);
            notify = true;
        }
        drop(state);
        if notify {
            self.notify.notify_waiters();
        }
    }
}

#[cfg(test)]
impl OutboundRingBuffer {
    pub async fn push_bytes(
        &self,
        data: &[u8],
        send_timeout: Duration,
    ) -> Result<usize, AureliaError> {
        self.push_bytes_with_progress(data, send_timeout, || {})
            .await
    }

    pub async fn wait_for_sendable(&self) -> Result<bool, AureliaError> {
        // docs/concurrency.md Pattern 1: arm the notified() future BEFORE the state check
        // so any notify_waiters fired between the check and the await is observed.
        loop {
            let waiter = self.notify.notified();
            tokio::pin!(waiter);
            {
                let state = self.inner.lock().await;
                if let Some(err) = state.lifecycle.failure() {
                    return Err(err);
                }
                if state.lifecycle.is_closed() {
                    return Ok(false);
                }
                if state.chunks.has_ready() {
                    return Ok(true);
                }
                if state.lifecycle.final_chunk_materialized() && state.chunks.is_drained() {
                    return Ok(false);
                }
            }
            waiter.await;
        }
    }

    pub async fn has_window_capacity(&self) -> bool {
        let state = self.inner.lock().await;
        state.chunks.has_capacity()
    }

    pub async fn live_chunk_count(&self) -> usize {
        let state = self.inner.lock().await;
        state.chunks.live_chunk_count()
    }

    pub async fn inflight_chunk_count(&self) -> usize {
        let state = self.inner.lock().await;
        state.chunks.inflight()
    }
}

enum CompletionNote {
    Ack,
    Error(AureliaError),
}

impl CompletionNote {
    fn chunk_completion(&self) -> ChunkCompletion {
        match self {
            Self::Ack => ChunkCompletion::Ack,
            Self::Error(_) => ChunkCompletion::Error,
        }
    }
}
