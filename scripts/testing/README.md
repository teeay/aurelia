# Testing Scripts

- `generate-certs.sh`: creates a shared local CA and per-domus certs in `tmp/certs/` (expects `name=ip:port`).
- `netem-apply.sh`: applies a named `tc/netem` profile to a container interface.
- `netem-clear.sh`: removes the root netem qdisc from an interface.
- `run-compose.sh`: runs a docker compose file and always tears it down afterward.

All scripts are intended to be called from the workspace root.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
