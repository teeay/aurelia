// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use aurelia_ids::{AureliaError, ErrorId};
use tokio::sync::Notify;
use tokio::time::{timeout_at, Instant};

#[derive(Debug)]
pub struct DynamicLimiter {
    used: AtomicUsize,
    limit: AtomicUsize,
    notify: Notify,
}

impl DynamicLimiter {
    pub fn new(initial_limit: usize) -> Arc<Self> {
        Arc::new(Self {
            used: AtomicUsize::new(0),
            limit: AtomicUsize::new(initial_limit),
            notify: Notify::new(),
        })
    }

    pub fn set_limit(&self, limit: usize) {
        let current = self.limit.swap(limit, Ordering::SeqCst);
        if limit > current {
            self.notify.notify_waiters();
        }
    }

    pub async fn acquire(
        self: &Arc<Self>,
        deadline: Instant,
    ) -> Result<LimiterPermit, AureliaError> {
        loop {
            let limit = self.limit.load(Ordering::SeqCst);
            let used = self.used.load(Ordering::SeqCst);
            if used < limit {
                if self
                    .used
                    .compare_exchange(used, used + 1, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    return Ok(LimiterPermit {
                        limiter: Arc::clone(self),
                    });
                }
                continue;
            }

            if Instant::now() >= deadline {
                return Err(AureliaError::new(ErrorId::SendTimeout));
            }

            let notified = self.notify.notified();
            if timeout_at(deadline, notified).await.is_err() {
                return Err(AureliaError::new(ErrorId::SendTimeout));
            }
        }
    }

    fn release(&self) {
        let _ = self
            .used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                Some(value.saturating_sub(1))
            });
        self.notify.notify_one();
    }
}

#[derive(Debug)]
pub struct LimiterPermit {
    limiter: Arc<DynamicLimiter>,
}

impl Drop for LimiterPermit {
    fn drop(&mut self) {
        self.limiter.release();
    }
}
