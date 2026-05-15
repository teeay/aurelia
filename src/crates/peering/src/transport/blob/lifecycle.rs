// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

pub(crate) struct BlobLifecycleSnapshot {
    pub(crate) has_callis: bool,
    pub(crate) has_active_streams: bool,
    #[allow(dead_code)]
    pub(crate) has_blob_work: bool,
}
