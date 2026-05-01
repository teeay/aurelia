# Containers Layout

- `containers/_shared/`: base images and shared container assets for tests.
- `containers/<suite>/`: suite-specific Dockerfiles and Compose files (for example `containers/peering/`).

Use `containers/_shared/Dockerfile.test-base` as the base for test app images.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
