// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;

pub type LogId = aurelia_ids::LogId;

#[derive(Clone, Debug)]
pub struct LimitedLogControl {
    interval_tx: watch::Sender<u64>,
}

impl LimitedLogControl {
    /// Updates the minimum interval between rate-limited log emissions.
    ///
    /// The interval is stored at seconds resolution: sub-second precision is
    /// truncated. A `Duration` shorter than one second resolves to zero, which
    /// disables throttling (every call to [`LimitedLogRegistry::should_log`]
    /// returns `true`).
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
    fn new(interval_rx: watch::Receiver<u64>) -> Self {
        let mut last_log = HashMap::with_capacity(LogId::ALL.len());
        for id in LogId::ALL {
            last_log.insert(*id, AtomicU64::new(0));
        }
        Self {
            last_log,
            interval_rx,
        }
    }

    pub fn should_log(&self, id: LogId) -> bool {
        // The registry is populated from `LogId::ALL` at construction, so
        // every variant has an entry. The fallback below is dead in
        // practice; if a future change drifts the invariant we allow the
        // log through rather than panic or silently drop it.
        let Some(last) = self.last_log.get(&id) else {
            return true;
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
    /// Creates a new context with the given rate-limit interval.
    ///
    /// The interval is stored at seconds resolution: sub-second precision is
    /// truncated. A `Duration` shorter than one second resolves to zero, which
    /// disables throttling for the lifetime of the returned context (until
    /// [`LimitedLogControl::set_interval`] is called with a non-zero value).
    pub fn new(interval: Duration) -> Self {
        let (tx, rx) = watch::channel(interval.as_secs());
        let registry = Arc::new(LimitedLogRegistry::new(rx));
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

/// Convenience constructor for [`LimitedLogContext`].
///
/// See [`LimitedLogContext::new`] for the seconds-resolution behaviour of
/// the `interval` parameter.
pub fn init_limited_logging(interval: Duration) -> LimitedLogContext {
    LimitedLogContext::new(interval)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[doc(hidden)]
#[macro_export]
macro_rules! __limited_event {
    ($level:expr, $registry:expr, $id:expr, $($arg:tt)+) => {{
        if $registry.should_log($id) {
            tracing::event!($level, log_id = $id.as_u32(), $($arg)+);
        }
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! info_limited {
    ($registry:expr, $id:expr, $($arg:tt)+) => {
        $crate::__limited_event!(tracing::Level::INFO, $registry, $id, $($arg)+)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! warn_limited {
    ($registry:expr, $id:expr, $($arg:tt)+) => {
        $crate::__limited_event!(tracing::Level::WARN, $registry, $id, $($arg)+)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! debug_limited {
    ($registry:expr, $id:expr, $($arg:tt)+) => {
        $crate::__limited_event!(tracing::Level::DEBUG, $registry, $id, $($arg)+)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! error_limited {
    ($registry:expr, $id:expr, $($arg:tt)+) => {
        $crate::__limited_event!(tracing::Level::ERROR, $registry, $id, $($arg)+)
    };
}

#[cfg(test)]
#[path = "tests/limited.rs"]
mod tests;
