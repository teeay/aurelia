// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

//! Resolver implementations for Aurelia.

#![warn(missing_docs)]

use std::collections::HashMap;

use aurelia_data::{DomusAddr, RouteResolver};
use aurelia_ids::{AureliaError, ErrorId, TabernaId};
use tokio::sync::RwLock;

/// In-memory [`RouteResolver`] backed by a mutable map from [`TabernaId`] to
/// [`DomusAddr`]. Suitable for tests, fixtures, and applications with a small
/// static topology.
pub struct SimpleResolver {
    inner: RwLock<HashMap<TabernaId, DomusAddr>>,
}

impl SimpleResolver {
    /// Constructs an empty resolver with no routes installed.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Installs (or replaces) the route for `taberna_id`.
    pub async fn insert(&self, taberna_id: TabernaId, domus: DomusAddr) {
        let mut guard = self.inner.write().await;
        guard.insert(taberna_id, domus);
    }

    /// Removes the route for `taberna_id`, if any.
    pub async fn remove(&self, taberna_id: TabernaId) {
        let mut guard = self.inner.write().await;
        guard.remove(&taberna_id);
    }

    /// Removes every installed route.
    pub async fn clear_all(&self) {
        let mut guard = self.inner.write().await;
        guard.clear();
    }
}

impl Default for SimpleResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl RouteResolver for SimpleResolver {
    async fn resolve(&self, taberna_id: TabernaId) -> Result<DomusAddr, AureliaError> {
        let guard = self.inner.read().await;
        guard
            .get(&taberna_id)
            .cloned()
            .ok_or_else(|| AureliaError::new(ErrorId::UnknownTaberna))
    }
}

#[cfg(test)]
mod tests;
