// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(crate) struct BlobCallisPool {
    pub(super) order: VecDeque<CallisId>,
    pub(super) handles: HashMap<CallisId, CallisHandle>,
    pub(super) settings: HashMap<CallisId, BlobCallisSettings>,
}

impl BlobCallisPool {
    pub(super) fn new() -> Self {
        Self {
            order: VecDeque::new(),
            handles: HashMap::new(),
            settings: HashMap::new(),
        }
    }
}
