#!/usr/bin/env bash
# This file is part of the Aurelia workspace.
# SPDX-FileCopyrightText: 2026 Zivatar Limited
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: generate-certs.sh --out <dir> [--force] name=ip:port [name=ip:port ...]

Generates a local CA and per-domus certs in <dir>.
Example:
  scripts/testing/generate-certs.sh --out tmp/certs domus-1=127.0.0.1:5000 domus-2=127.0.0.1:5000
USAGE
}

out_dir=""
force=0
domus_specs=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)
      out_dir="$2"
      shift 2
      ;;
    --force)
      force=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      domus_specs+=("$1")
      shift
      ;;
  esac
done

if [[ -z "$out_dir" || ${#domus_specs[@]} -eq 0 ]]; then
  usage
  exit 1
fi

mkdir -p "$out_dir"
ca_dir="$out_dir/ca"
domus_dir="$out_dir/domus"
mkdir -p "$ca_dir" "$domus_dir"

ca_key="$ca_dir/ca.key"
ca_crt="$ca_dir/ca.crt"

if [[ -f "$ca_key" || -f "$ca_crt" ]]; then
  if [[ "$force" -ne 1 ]]; then
    echo "CA already exists in $ca_dir (use --force to overwrite)" >&2
    exit 1
  fi
  rm -f "$ca_key" "$ca_crt" "$ca_dir/ca.srl"
fi

openssl req -x509 -newkey rsa:4096 -keyout "$ca_key" -out "$ca_crt" -sha256 -days 3650 -nodes -subj "/CN=Aurelia Test CA" >/dev/null 2>&1

tmp_ext=$(mktemp)
trap 'rm -f "$tmp_ext"' EXIT

for domus in "${domus_specs[@]}"; do
  name="${domus%%=*}"
  addr="${domus#*=}"
  if [[ -z "$name" || -z "$addr" || "$name" == "$addr" ]]; then
    echo "Invalid domus spec: $domus (expected name=ip:port)" >&2
    exit 1
  fi
  if [[ "$addr" =~ ^\\[(.*)\\]:(.+)$ ]]; then
    ip="${BASH_REMATCH[1]}"
    port="${BASH_REMATCH[2]}"
  else
    if [[ "$addr" != *:* ]]; then
      echo "Invalid domus address: $addr (expected ip:port)" >&2
      exit 1
    fi
    ip="${addr%:*}"
    port="${addr##*:}"
  fi
  if [[ -z "$ip" || -z "$port" ]]; then
    echo "Invalid domus address: $addr (expected ip:port)" >&2
    exit 1
  fi
  if ! [[ "$port" =~ ^[0-9]+$ ]]; then
    echo "Invalid domus port: $port (expected numeric)" >&2
    exit 1
  fi
  domus_path="$domus_dir/$name"
  mkdir -p "$domus_path"
  key="$domus_path/key.pem"
  csr="$domus_path/domus.csr"
  crt="$domus_path/cert.pem"

  openssl genrsa -out "$key" 2048 >/dev/null 2>&1
  openssl req -new -key "$key" -out "$csr" -subj "/CN=$name" >/dev/null 2>&1

  cat > "$tmp_ext" <<EOF_EXT
subjectAltName = URI:aurelia+tcp://$addr,IP:$ip
extendedKeyUsage = serverAuth, clientAuth
EOF_EXT

  openssl x509 -req -in "$csr" -CA "$ca_crt" -CAkey "$ca_key" -CAcreateserial \
    -out "$crt" -days 365 -sha256 -extfile "$tmp_ext" >/dev/null 2>&1
  rm -f "$csr"

done

echo "Certificates written to $out_dir"
