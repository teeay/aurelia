#!/usr/bin/env bash
# This file is part of the Aurelia workspace.
# SPDX-FileCopyrightText: 2026 Zivatar Limited
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

timeout_bin=$(command -v timeout || true)
if [[ -z "$timeout_bin" ]]; then
  echo "timeout command not found; install coreutils to enforce suite timeouts" >&2
  exit 1
fi

suite_timeout_secs="${AURELIA_TEST_SUITE_TIMEOUT_SECS:-600}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

exec "$timeout_bin" "${suite_timeout_secs}s" bash -c '
set -euo pipefail
repo_root="$1"
"$repo_root/scripts/testing/check-async-test-timeouts.py"
cargo test --manifest-path "$repo_root/testing/peering/apps/peering-domus/Cargo.toml"
cargo test --manifest-path "$repo_root/testing/peering/apps/peering-driver/Cargo.toml"
' bash "$repo_root"
