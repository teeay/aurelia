// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::hash::Hash;

use tokio::sync::{oneshot, Mutex};

use aurelia_ids::{AureliaError, ErrorId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub(super) enum CallbackTransition {
    PendingRegistered,
    CallbackArrived,
    Cleanup,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub(super) struct CallbackSnapshot {
    pub(super) transition: CallbackTransition,
    pub(super) pending_len: usize,
}

#[derive(Debug)]
struct PendingCallback<E, R> {
    expected: E,
    reply: oneshot::Sender<R>,
}

#[derive(Debug)]
pub(super) struct CallbackRendezvous<K, E, R> {
    pending: Mutex<HashMap<K, PendingCallback<E, R>>>,
}

impl<K, E, R> CallbackRendezvous<K, E, R>
where
    K: Eq + Hash,
{
    pub(super) fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
        }
    }

    pub(super) async fn register(
        &self,
        key: K,
        expected: E,
    ) -> (oneshot::Receiver<R>, CallbackSnapshot) {
        let (tx, rx) = oneshot::channel();
        let mut guard = self.pending.lock().await;
        guard.insert(
            key,
            PendingCallback {
                expected,
                reply: tx,
            },
        );
        let snapshot = CallbackSnapshot {
            transition: CallbackTransition::PendingRegistered,
            pending_len: guard.len(),
        };
        (rx, snapshot)
    }

    pub(super) async fn cleanup(&self, key: K) -> CallbackSnapshot {
        let mut guard = self.pending.lock().await;
        guard.remove(&key);
        CallbackSnapshot {
            transition: CallbackTransition::Cleanup,
            pending_len: guard.len(),
        }
    }

    pub(super) async fn fulfill<F>(
        &self,
        key: K,
        validate: F,
        reply: R,
    ) -> Result<CallbackSnapshot, AureliaError>
    where
        F: FnOnce(&E) -> bool,
    {
        let mut guard = self.pending.lock().await;
        let entry = guard
            .remove(&key)
            .ok_or_else(|| AureliaError::new(ErrorId::ProtocolViolation))?;
        if !validate(&entry.expected) {
            return Err(AureliaError::new(ErrorId::ProtocolViolation));
        }
        let pending_len = guard.len();
        drop(guard);
        let _ = entry.reply.send(reply);
        Ok(CallbackSnapshot {
            transition: CallbackTransition::CallbackArrived,
            pending_len,
        })
    }
}

#[cfg(test)]
impl<K, E, R> CallbackRendezvous<K, E, R>
where
    K: Eq + Hash,
{
    pub(super) async fn pending_len(&self) -> usize {
        self.pending.lock().await.len()
    }
}
