// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Debug)]
pub struct PeerMessageIdAllocator {
    next: AtomicU32,
}

impl PeerMessageIdAllocator {
    pub fn new(initial: u32) -> Self {
        Self {
            next: AtomicU32::new(initial),
        }
    }

    pub fn next(&self) -> u32 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }
}

impl Default for PeerMessageIdAllocator {
    fn default() -> Self {
        Self::new(0)
    }
}
