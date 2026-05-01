#!/usr/bin/env bash
# This file is part of the Aurelia workspace.
# SPDX-FileCopyrightText: 2026 Zivatar Limited
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: run-compose.sh <compose-file> [--build] [--exit-code-from <service>]

Runs docker compose and always tears down the stack when finished.
USAGE
}

compose_file="${1:-}"
shift || true

if [[ -z "$compose_file" || "$compose_file" == "--help" || "$compose_file" == "-h" ]]; then
  usage
  exit 1
fi

build_flag=""
exit_code_from=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --build)
      build_flag="--build"
      shift
      ;;
    --exit-code-from)
      exit_code_from="${2:-}"
      if [[ -z "$exit_code_from" ]]; then
        echo "--exit-code-from requires a service name" >&2
        usage
        exit 1
      fi
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -n "$exit_code_from" ]]; then
  set +e
  docker compose -f "$compose_file" up $build_flag -d
  status=$?
  set -e

  if [[ $status -ne 0 ]]; then
    docker compose -f "$compose_file" down -v
    exit $status
  fi

  service_id=""
  for _ in {1..50}; do
    service_id=$(docker compose -f "$compose_file" ps -q "$exit_code_from")
    if [[ -n "$service_id" ]]; then
      break
    fi
    sleep 0.2
  done

  if [[ -z "$service_id" ]]; then
    echo "Unable to locate container for service: $exit_code_from" >&2
    docker compose -f "$compose_file" down -v
    exit 1
  fi

  timeout_bin=$(command -v timeout || true)
  if [[ -z "$timeout_bin" ]]; then
    echo "timeout command not found; install coreutils to enforce suite timeouts" >&2
    docker compose -f "$compose_file" down -v
    exit 1
  fi
  suite_timeout_secs="${AURELIA_E2E_TIMEOUT_SECS:-600}"
  set +e
  wait_output=$("$timeout_bin" "${suite_timeout_secs}s" docker wait "$service_id")
  wait_status=$?
  set -e
  status=0
  if [[ "$wait_status" -ne 0 ]]; then
    status=$wait_status
    docker compose -f "$compose_file" logs --no-color
  else
    if ! [[ "$wait_output" =~ ^[0-9]+$ ]]; then
      echo "Unexpected docker wait output: $wait_output" >&2
      status=1
      docker compose -f "$compose_file" logs --no-color
    else
      if [[ "$wait_output" -ne 0 ]]; then
        echo "Service ${exit_code_from} exited with code ${wait_output}" >&2
        status=1
        docker compose -f "$compose_file" logs --no-color
      fi
    fi
  fi

  service_ids=$(docker compose -f "$compose_file" ps -q 2>/dev/null || true)
  other_failure=0
  if [[ -n "$service_ids" ]]; then
    while read -r id; do
      if [[ -z "$id" || "$id" == "$service_id" ]]; then
        continue
      fi
      inspect_line=$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}} {{.Name}}' "$id" 2>/dev/null || true)
      if [[ -z "$inspect_line" ]]; then
        echo "Unable to inspect container $id" >&2
        other_failure=1
        continue
      fi
      state=$(printf '%s' "$inspect_line" | awk '{print $1}')
      exit_code=$(printf '%s' "$inspect_line" | awk '{print $2}')
      name=$(printf '%s' "$inspect_line" | awk '{print $3}')
      if [[ "$state" != "running" ]]; then
        echo "Container ${name} not running (state=${state} exit_code=${exit_code})" >&2
        other_failure=1
      fi
    done <<< "$service_ids"
  fi
  if [[ "$other_failure" -ne 0 ]]; then
    docker compose -f "$compose_file" logs --no-color
    status=1
  fi

  docker compose -f "$compose_file" down -v
  exit "$status"
fi

set +e
timeout_bin=$(command -v timeout || true)
if [[ -z "$timeout_bin" ]]; then
  echo "timeout command not found; install coreutils to enforce suite timeouts" >&2
  exit 1
fi
suite_timeout_secs="${AURELIA_E2E_TIMEOUT_SECS:-600}"
"$timeout_bin" "${suite_timeout_secs}s" docker compose -f "$compose_file" up $build_flag --abort-on-container-exit
status=$?
set -e

docker compose -f "$compose_file" down -v
exit $status
