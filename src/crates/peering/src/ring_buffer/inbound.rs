// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use aurelia_ids::{AureliaError, ErrorId};
use bytes::Bytes;
use std::collections::HashMap;
use tokio::sync::{Mutex, Notify};

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
        if state.received.len() >= state.window_size {
            return Err(AureliaError::with_message(
                ErrorId::BlobBufferFull,
                format!(
                    "received_chunks={} window_size={}",
                    state.received.len(),
                    state.window_size
                ),
            ));
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
        drop(state);
        self.notify.notify_waiters();
        Ok(InboundInsertOutcome::Stored {
            complete,
            wait_for_space: false,
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

    pub async fn is_complete(&self) -> bool {
        let state = self.inner.lock().await;
        match state.last_chunk_id {
            Some(last) => state.next_deliver > last,
            None => false,
        }
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
