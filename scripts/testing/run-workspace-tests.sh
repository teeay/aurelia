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

exec "$timeout_bin" "${suite_timeout_secs}s" cargo test
