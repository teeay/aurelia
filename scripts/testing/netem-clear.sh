#!/usr/bin/env bash
# This file is part of the Aurelia workspace.
# SPDX-FileCopyrightText: 2026 Zivatar Limited
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

iface="${1:-eth0}"

if [[ "$iface" == "--help" || "$iface" == "-h" ]]; then
  echo "Usage: netem-clear.sh [iface]" >&2
  exit 1
fi

tc qdisc del dev "$iface" root 2>/dev/null || true
