# Documentation

This directory contains cross-cutting and crate-specific documentation for the Aurelia workspace.

## Structure

- `docs/README.md`: overview and navigation
- `docs/aurelia.md`: Aurelia structure (A1/A2/A3)
- `docs/concurrency.md`: mandatory concurrency patterns for live library/runtime code
- `docs/coupling.md`: loose-coupling and composition rules
- `docs/data/`: shared domain data and boundary contract documentation
- `docs/errors.md`: error semantics for `AureliaError` and `ErrorId`
- `docs/glossary.md`: project terminology
- `docs/ids.md`: gold source of IDs used across all Aurelia crates
- `docs/logging.md`: logging levels and rate-limited logging utilities
- `docs/peering/`: A1 design notes (current focus)
- `docs/publish.md`: publish-tree generation and release validation
- `docs/resolver/`: resolver implementation documentation
- `docs/runtime.md`: Aurelia runtime wrapper and ownership model
- `docs/testing.md`: test harness and execution strategy
- `docs/<crate-name>/`: crate-specific docs for supporting crates

## Conventions

- Keep cross-cutting docs directly under `docs/`.
- When a supporting crate is added under `src/crates/<crate-name>`, add a matching `docs/<crate-name>/`.
- Update `docs/README.md` when adding new top-level sections.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
