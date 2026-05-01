// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;

pub type LogId = aurelia_ids::LogId;
pub use aurelia_ids::log_ids;

#[derive(Clone, Debug)]
pub struct LimitedLogControl {
    interval_tx: watch::Sender<u64>,
}

impl LimitedLogControl {
    pub fn set_interval(&self, interval: Duration) {
        let _ = self.interval_tx.send_replace(interval.as_secs());
    }
}

#[derive(Debug)]
pub struct LimitedLogRegistry {
    last_log: HashMap<LogId, AtomicU64>,
    interval_rx: watch::Receiver<u64>,
}

impl LimitedLogRegistry {
    fn new(ids: &[LogId], interval_rx: watch::Receiver<u64>) -> Self {
        let mut last_log = HashMap::with_capacity(ids.len());
        for id in ids {
            last_log.insert(*id, AtomicU64::new(0));
        }
        Self {
            last_log,
            interval_rx,
        }
    }

    pub fn should_log(&self, id: LogId) -> bool {
        let Some(last) = self.last_log.get(&id) else {
            return false;
        };
        let interval = *self.interval_rx.borrow();
        if interval == 0 {
            return true;
        }
        let now = now_secs();
        let mut current = last.load(Ordering::SeqCst);
        loop {
            if now.saturating_sub(current) < interval {
                return false;
            }
            match last.compare_exchange(current, now, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => return true,
                Err(updated) => current = updated,
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct LimitedLogContext {
    registry: Arc<LimitedLogRegistry>,
    control: LimitedLogControl,
}

impl LimitedLogContext {
    pub fn new(ids: &[LogId], interval: Duration) -> Self {
        let (tx, rx) = watch::channel(interval.as_secs());
        let registry = Arc::new(LimitedLogRegistry::new(ids, rx));
        let control = LimitedLogControl { interval_tx: tx };
        Self { registry, control }
    }

    pub fn registry(&self) -> Arc<LimitedLogRegistry> {
        Arc::clone(&self.registry)
    }

    pub fn control(&self) -> LimitedLogControl {
        self.control.clone()
    }
}

pub fn init_limited_logging(ids: &[LogId], interval: Duration) -> LimitedLogContext {
    LimitedLogContext::new(ids, interval)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[doc(hidden)]
#[macro_export]
macro_rules! info_limited {
    ($registry:expr, $id:expr, $($arg:tt)+) => {{
        if $registry.should_log($id) {
            tracing::info!(log_id = $id, $($arg)+);
        }
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! warn_limited {
    ($registry:expr, $id:expr, $($arg:tt)+) => {{
        if $registry.should_log($id) {
            tracing::warn!(log_id = $id, $($arg)+);
        }
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! debug_limited {
    ($registry:expr, $id:expr, $($arg:tt)+) => {{
        if $registry.should_log($id) {
            tracing::debug!(log_id = $id, $($arg)+);
        }
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! error_limited {
    ($registry:expr, $id:expr, $($arg:tt)+) => {{
        if $registry.should_log($id) {
            tracing::error!(log_id = $id, $($arg)+);
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limited_logging_blocks_within_interval() {
        let (tx, rx) = watch::channel(120u64);
        let registry = LimitedLogRegistry::new(&[log_ids::HANDSHAKE_TOTAL_LIMIT], rx);
        let last = registry
            .last_log
            .get(&log_ids::HANDSHAKE_TOTAL_LIMIT)
            .expect("log id");
        last.store(now_secs().saturating_sub(240), Ordering::SeqCst);

        assert!(registry.should_log(log_ids::HANDSHAKE_TOTAL_LIMIT));
        assert!(!registry.should_log(log_ids::HANDSHAKE_TOTAL_LIMIT));

        let _ = tx.send_replace(0);
        assert!(registry.should_log(log_ids::HANDSHAKE_TOTAL_LIMIT));
    }

    #[test]
    fn limited_logging_ignores_unknown_ids() {
        let (_tx, rx) = watch::channel(120u64);
        let registry = LimitedLogRegistry::new(&[log_ids::HANDSHAKE_TOTAL_LIMIT], rx);
        assert!(!registry.should_log(9999));
    }
}
