#!/usr/bin/env bash
# This file is part of the Aurelia workspace.
# SPDX-FileCopyrightText: 2026 Zivatar Limited
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

certs_dir="$root_dir/tmp/certs"

project_suffix=$(basename "$root_dir")
project_name="aurelia-${project_suffix}"

sanitize_image_tag() {
  local raw="$1"
  local lower
  lower=$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')
  local cleaned
  cleaned=$(printf '%s' "$lower" | sed -E 's/[^a-z0-9._-]+/-/g; s/[._-]{2,}/-/g; s/^[._-]+//; s/[._-]+$//')
  if [[ -z "$cleaned" ]]; then
    cleaned="e2e"
  fi
  echo "$cleaned"
}

image_tag=$(sanitize_image_tag "$project_suffix")

hash_hex() {
  local input="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    printf '%s' "$input" | sha256sum | awk '{print $1}'
    return 0
  fi
  if command -v shasum >/dev/null 2>&1; then
    printf '%s' "$input" | shasum -a 256 | awk '{print $1}'
    return 0
  fi
  if command -v openssl >/dev/null 2>&1; then
    printf '%s' "$input" | openssl dgst -sha256 | awk '{print $2}'
    return 0
  fi
  echo "No sha256 hashing tool found (need sha256sum, shasum, or openssl)" >&2
  exit 1
}

hash_octet() {
  local hex
  hex=$(hash_hex "$1")
  local byte
  byte=$(printf '%s' "$hex" | awk '{print substr($1, length($1)-1, 2)}')
  printf '%d' "0x$byte"
}

collect_docker_subnets() {
  if ! command -v docker >/dev/null 2>&1; then
    return 0
  fi
  local ids
  ids=$(docker network ls -q 2>/dev/null || true)
  if [[ -z "$ids" ]]; then
    return 0
  fi
  while read -r id; do
    if [[ -n "$id" ]]; then
      docker network inspect -f '{{range .IPAM.Config}}{{.Subnet}}{{"\n"}}{{end}}' "$id" 2>/dev/null || true
    fi
  done <<< "$ids"
}

collect_host_routes() {
  if ! command -v python3 >/dev/null 2>&1; then
    echo "python3 is required to inspect host routes" >&2
    exit 1
  fi
  python3 - <<'PY'
import ipaddress
import shutil
import subprocess

nets = set()

if shutil.which("ip"):
    out = subprocess.check_output(["ip", "-4", "route", "show"], text=True)
    for line in out.splitlines():
        dest = line.split()[0]
        if dest == "default":
            continue
        try:
            nets.add(str(ipaddress.ip_network(dest, strict=False)))
        except ValueError:
            pass
elif shutil.which("netstat"):
    out = subprocess.check_output(["netstat", "-rn", "-f", "inet"], text=True)
    for line in out.splitlines():
        if not line or line.startswith("Destination") or line.startswith("Routing") or line.startswith("Kernel"):
            continue
        parts = line.split()
        if len(parts) < 3:
            continue
        dest, mask = parts[0], parts[2]
        if dest == "default":
            continue
        try:
            if mask.startswith("0x"):
                mask_ip = str(ipaddress.IPv4Address(int(mask, 16)))
            else:
                mask_ip = mask
            net = ipaddress.IPv4Network(f"{dest}/{mask_ip}", strict=False)
            nets.add(str(net))
        except ValueError:
            pass

for net in sorted(nets):
    print(net)
PY
}

subnet_overlaps() {
  local candidate="$1"
  python3 - "$candidate" <<'PY'
import ipaddress
import sys

candidate = ipaddress.ip_network(sys.argv[1], strict=False)
for line in sys.stdin.read().splitlines():
    line = line.strip()
    if not line:
        continue
    try:
        net = ipaddress.ip_network(line, strict=False)
    except ValueError:
        continue
    if candidate.overlaps(net):
        sys.exit(0)
sys.exit(1)
PY
}

max_attempts=20
attempt=0
while [[ $attempt -lt $max_attempts ]]; do
  seed="$project_name"
  if [[ $attempt -gt 0 ]]; then
    seed="${project_name}#${attempt}"
  fi

  existing_subnets="$(
    collect_docker_subnets
    collect_host_routes
  )"

  hash_value=$(hash_octet "$seed")
  subnet="172.20.${hash_value}.0/24"

  if echo "$existing_subnets" | subnet_overlaps "$subnet"; then
    attempt=$((attempt + 1))
    continue
  fi

  subnet_base="${subnet%0/24}"
  domus_1_ip="${subnet_base}11"
  domus_2_ip="${subnet_base}12"
  domus_3_ip="${subnet_base}13"
  driver_ip="${subnet_base}20"

  export COMPOSE_PROJECT_NAME="$project_name"
  export AURELIA_E2E_IMAGE_TAG="$image_tag"
  export AURELIA_E2E_SUBNET="$subnet"
  export AURELIA_E2E_DOMUS_1_IP="$domus_1_ip"
  export AURELIA_E2E_DOMUS_2_IP="$domus_2_ip"
  export AURELIA_E2E_DOMUS_3_IP="$domus_3_ip"
  export AURELIA_E2E_DRIVER_IP="$driver_ip"

  echo "Using compose project: $project_name"
  echo "Using subnet: $subnet"

  "$root_dir/scripts/testing/generate-certs.sh" --out "$certs_dir" --force \
    "domus-1=${domus_1_ip}:5000" \
    "domus-2=${domus_2_ip}:5000" \
    "domus-3=${domus_3_ip}:5000" \
    "driver=${driver_ip}:5000"

  run_log=$(mktemp)
  set +e
  "$root_dir/scripts/testing/run-compose.sh" "$root_dir/containers/peering/docker-compose.yml" \
    --build --exit-code-from driver 2>&1 | tee "$run_log"
  status=${PIPESTATUS[0]}
  set -e

  if [[ $status -eq 0 ]]; then
    rm -f "$run_log"
    exit 0
  fi

  if grep -qi "overlaps with other one on this address space" "$run_log"; then
    rm -f "$run_log"
    attempt=$((attempt + 1))
    continue
  fi

  rm -f "$run_log"
  exit "$status"
done

echo "Unable to select a free subnet after $max_attempts attempts" >&2
exit 1
