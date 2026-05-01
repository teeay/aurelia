#!/usr/bin/env bash
# This file is part of the Aurelia workspace.
# SPDX-FileCopyrightText: 2026 Zivatar Limited
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo "prep-publish: cargo fmt --check"
cargo fmt --all -- --check

echo "prep-publish: cargo build"
cargo build --workspace --all-targets

echo "prep-publish: cargo test"
cargo test --workspace --all-targets

echo "prep-publish: cargo clippy"
cargo clippy --workspace --all-targets --all-features -- -D warnings

echo "prep-publish: workspace is clean."
