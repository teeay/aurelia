#!/usr/bin/env bash
# This file is part of the Aurelia workspace.
# SPDX-FileCopyrightText: 2026 Zivatar Limited
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE/.."

"$HERE/prep-publish.sh"

echo "publish: cargo xtask publish-tree"
cargo xtask publish-tree

cat <<'EOF'

publish.sh: all checks passed.
To publish, run:

  (cd publish/aurelia && cargo publish)

EOF
