// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use tokio::sync::RwLock;

use crate::address::DomusAddr;
use crate::routing::RouteResolver;
use aurelia_ids::{AureliaError, ErrorId, TabernaId};

/// In-memory [`RouteResolver`] backed by a mutable map from [`TabernaId`] to
/// [`DomusAddr`]. Suitable for tests, fixtures, and applications with a small
/// static topology.
pub struct SimpleResolver {
    inner: RwLock<SimpleResolverState>,
}

struct SimpleResolverState {
    routes: HashMap<TabernaId, DomusAddr>,
}

impl SimpleResolver {
    /// Constructs an empty resolver with no routes installed.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(SimpleResolverState {
                routes: HashMap::new(),
            }),
        }
    }

    /// Installs (or replaces) the route for `taberna_id`.
    pub async fn insert(&self, taberna_id: TabernaId, domus: DomusAddr) {
        let mut guard = self.inner.write().await;
        guard.routes.insert(taberna_id, domus);
    }

    /// Removes the route for `taberna_id`, if any.
    pub async fn remove(&self, taberna_id: TabernaId) {
        let mut guard = self.inner.write().await;
        guard.routes.remove(&taberna_id);
    }

    /// Removes every installed route.
    pub async fn clear_all(&self) {
        let mut guard = self.inner.write().await;
        guard.routes.clear();
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
            .routes
            .get(&taberna_id)
            .cloned()
            .ok_or_else(|| AureliaError::new(ErrorId::UnknownTaberna))
    }
}
