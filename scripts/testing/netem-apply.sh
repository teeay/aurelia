#!/usr/bin/env bash
# This file is part of the Aurelia workspace.
# SPDX-FileCopyrightText: 2026 Zivatar Limited
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: netem-apply.sh <profile> [iface]

Profiles:
  loss-1        1% packet loss
  loss-5        5% packet loss
  delay-100ms   100ms delay with 20ms jitter
  jitter-50ms   50ms delay with 10ms jitter
  partition     100% packet loss (full partition)

Example:
  scripts/testing/netem-apply.sh loss-5 eth0
USAGE
}

profile="${1:-}"
iface="${2:-eth0}"

if [[ -z "$profile" || "$profile" == "--help" || "$profile" == "-h" ]]; then
  usage
  exit 1
fi

case "$profile" in
  loss-1)
    args="loss 1%"
    ;;
  loss-5)
    args="loss 5%"
    ;;
  delay-100ms)
    args="delay 100ms 20ms distribution normal"
    ;;
  jitter-50ms)
    args="delay 50ms 10ms distribution normal"
    ;;
  partition)
    args="loss 100%"
    ;;
  *)
    echo "Unknown profile: $profile" >&2
    usage
    exit 1
    ;;
esac

tc qdisc replace dev "$iface" root netem $args
