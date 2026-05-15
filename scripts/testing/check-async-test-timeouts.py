#!/usr/bin/env python3
# This file is part of the Aurelia workspace.
# SPDX-FileCopyrightText: 2026 Zivatar Limited
# SPDX-License-Identifier: Apache-2.0

"""Validate async test deadlines, E2E sleep allowlists, and stale test-helper-crate policy."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[2]
SEARCH_ROOTS = (ROOT / "src", ROOT / "testing")
STALE_REFERENCE_ROOTS = (
    ROOT / "Cargo.toml",
    ROOT / "docs",
    ROOT / "scripts",
    ROOT / "src",
    ROOT / "testing",
)
STALE_REFERENCE_EXCLUDES = {
    ROOT / "docs" / "testing.md",
    Path(__file__).resolve(),
}
TIMEOUT_MARKERS = (
    "timeout(",
    "timeout_at(",
    "tokio::time::timeout",
    "tokio::time::timeout_at",
    "run_resolver_test(",
)
TEST_ATTR_RE = re.compile(r"#\[(?:tokio|actix)::test(?:\([^]]*\))?\]\s*(?P<prefix>(?:#\[[^]]+\]\s*)*)async\s+fn\s+(?P<name>[A-Za-z0-9_]+)")
RAW_BLANKET_TIMEOUT_RE = re.compile(
    r"(?:tokio::time::)?timeout\(\s*(?:std::time::)?Duration::from_(?P<unit>millis|secs)\((?P<value>[0-9_]+)\)"
)
CONST_DURATION_RE = re.compile(
    r"const\s+(?P<name>[A-Z0-9_]+)\s*:\s*(?P<ty>[^=]+?)\s*=\s*(?:std::time::)?Duration::from_(?P<unit>millis|secs)\((?P<value>[0-9_]+)\)\s*;"
)
SLEEP_RE = re.compile(
    r"(?:tokio::time::sleep(?:_until)?|actix::clock::sleep|std::thread::sleep)\s*\("
)
SLEEP_ALLOW_RE = re.compile(
    r"aurelia-test-allow-sleep:\s*(behavior-duration|negative-assertion|poll-interval|explicit-backoff)\b"
)
IN_MEMORY_TIMEOUT_LIMIT_MS = 1_000


@dataclass(frozen=True)
class AsyncTest:
    path: Path
    line: int
    name: str
    body: str


@dataclass(frozen=True)
class TimeoutCalibrationFinding:
    path: Path
    line: int
    test_name: str
    detail: str


@dataclass(frozen=True)
class SleepFinding:
    path: Path
    line: int
    detail: str


def line_number(source: str, offset: int) -> int:
    return source.count("\n", 0, offset) + 1


def find_matching_brace(source: str, open_pos: int) -> int | None:
    depth = 0
    in_line_comment = False
    in_block_comment = 0
    in_string = False
    in_char = False
    raw_hashes: int | None = None
    escape = False
    i = open_pos
    while i < len(source):
        ch = source[i]
        nxt = source[i + 1] if i + 1 < len(source) else ""

        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue
        if in_block_comment:
            if ch == "/" and nxt == "*":
                in_block_comment += 1
                i += 2
                continue
            if ch == "*" and nxt == "/":
                in_block_comment -= 1
                i += 2
                continue
            i += 1
            continue
        if raw_hashes is not None:
            if ch == '"' and source[i + 1 : i + 1 + raw_hashes] == "#" * raw_hashes:
                i += 1 + raw_hashes
                raw_hashes = None
                continue
            i += 1
            continue
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            i += 1
            continue
        if in_char:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == "'":
                in_char = False
            i += 1
            continue

        if ch == "/" and nxt == "/":
            in_line_comment = True
            i += 2
            continue
        if ch == "/" and nxt == "*":
            in_block_comment = 1
            i += 2
            continue
        if ch == "r":
            raw_match = re.match(r'r(#+)"', source[i:])
            if raw_match:
                raw_hashes = len(raw_match.group(1))
                i += 2 + raw_hashes
                continue
            if nxt == '"':
                raw_hashes = 0
                i += 2
                continue
        if ch == '"':
            in_string = True
            i += 1
            continue
        if ch == "'":
            in_char = True
            i += 1
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return None


def iter_rust_files(root: Path) -> list[Path]:
    return sorted(path for path in root.rglob("*.rs") if path.is_file())


def async_tests(path: Path) -> list[AsyncTest]:
    source = path.read_text()
    found: list[AsyncTest] = []
    for match in TEST_ATTR_RE.finditer(source):
        open_pos = source.find("{", match.end())
        if open_pos == -1:
            continue
        close_pos = find_matching_brace(source, open_pos)
        if close_pos is None:
            continue
        found.append(
            AsyncTest(
                path=path,
                line=line_number(source, match.start()),
                name=match.group("name"),
                body=source[open_pos + 1 : close_pos],
            )
        )
    return found


def deadline_gaps() -> list[AsyncTest]:
    gaps: list[AsyncTest] = []
    for root in SEARCH_ROOTS:
        if not root.exists():
            continue
        for path in iter_rust_files(root):
            for test in async_tests(path):
                if not any(marker in test.body for marker in TIMEOUT_MARKERS):
                    gaps.append(test)
    return gaps


def duration_to_ms(unit: str, value: str) -> int:
    parsed = int(value.replace("_", ""))
    if unit == "secs":
        return parsed * 1_000
    return parsed


def is_default_large_timeout(unit: str, value: str) -> bool:
    return duration_to_ms(unit, value) >= 10_000


def is_in_memory_timeout_path(path: Path) -> bool:
    rel = path.relative_to(ROOT)
    text = rel.as_posix()
    in_memory_paths = (
        "src/crates/peering/src/tests/actix_adapter.rs",
        "src/crates/peering/src/tests/config.rs",
        "src/crates/peering/src/tests/faults.rs",
        "src/crates/peering/src/tests/mod.rs",
        "src/crates/peering/src/tests/observability.rs",
        "src/crates/peering/src/tests/ring_buffer.rs",
        "src/crates/peering/src/tests/runtime.rs",
        "src/crates/peering/src/tests/session.rs",
        "src/crates/peering/src/transport/tests/backend.rs",
        "src/crates/peering/src/transport/tests/limits.rs",
        "src/crates/peering/src/transport/tests/primary.rs",
        "src/crates/peering/src/transport/tests/leaf/primary_dispatch_overrun.rs",
        "src/crates/peering/src/transport/tests/leaf/primary_dispatch_retained.rs",
        "testing/peering/apps/peering-domus/src/oob_control/tests/mod.rs",
    )
    return text in in_memory_paths


def duration_constants(path: Path) -> dict[str, int]:
    source = path.read_text()
    constants: dict[str, int] = {}
    for match in CONST_DURATION_RE.finditer(source):
        constants[match.group("name")] = duration_to_ms(
            match.group("unit"), match.group("value")
        )
    return constants


def timeout_calibration_findings() -> list[TimeoutCalibrationFinding]:
    findings: list[TimeoutCalibrationFinding] = []
    for root in SEARCH_ROOTS:
        if not root.exists():
            continue
        for path in iter_rust_files(root):
            constants = duration_constants(path)
            for test in async_tests(path):
                for match in RAW_BLANKET_TIMEOUT_RE.finditer(test.body):
                    if is_default_large_timeout(match.group("unit"), match.group("value")):
                        findings.append(
                            TimeoutCalibrationFinding(
                                path,
                                test.line,
                                test.name,
                                f"raw default-large timeout {match.group(0)}",
                            )
                        )
                if is_in_memory_timeout_path(path):
                    for name, millis in constants.items():
                        if millis > IN_MEMORY_TIMEOUT_LIMIT_MS and name in test.body:
                            findings.append(
                                TimeoutCalibrationFinding(
                                    path,
                                    test.line,
                                    test.name,
                                    f"{name} is {millis}ms; in-memory async tests must stay <= {IN_MEMORY_TIMEOUT_LIMIT_MS}ms",
                                )
                            )
    return findings


def iter_text_files(root: Path) -> list[Path]:
    if root.is_file():
        return [root]
    ignored_dirs = {".git", "target", "tmp", "publish"}
    files: list[Path] = []
    for path in root.rglob("*"):
        if any(part in ignored_dirs for part in path.parts):
            continue
        if path.is_file():
            files.append(path)
    return files


def e2e_app_rust_files() -> list[Path]:
    testing_root = ROOT / "testing"
    if not testing_root.exists():
        return []
    return sorted(path for path in testing_root.glob("**/apps/**/*.rs") if path.is_file())


def sleep_allowlisted(source: str, line_start: int) -> bool:
    line_no = line_number(source, line_start)
    lines = source.splitlines()
    start = max(0, line_no - 4)
    end = min(len(lines), line_no + 1)
    context = "\n".join(lines[start:end])
    return SLEEP_ALLOW_RE.search(context) is not None


def sleep_findings() -> list[SleepFinding]:
    findings: list[SleepFinding] = []
    for path in e2e_app_rust_files():
        source = path.read_text()
        for match in SLEEP_RE.finditer(source):
            if not sleep_allowlisted(source, match.start()):
                findings.append(
                    SleepFinding(
                        path,
                        line_number(source, match.start()),
                        "fixed sleep lacks aurelia-test-allow-sleep reason",
                    )
                )
    return findings


def stale_test_crate_references() -> list[tuple[Path, int, str]]:
    stale_crate_path = "src/crates/" + "testing"
    stale_crate_name = "aurelia-" + "testing"
    findings: list[tuple[Path, int, str]] = []

    if (ROOT / stale_crate_path).exists():
        findings.append((ROOT / stale_crate_path, 1, "standalone test helper crate directory exists"))

    for root in STALE_REFERENCE_ROOTS:
        if not root.exists():
            continue
        for path in iter_text_files(root):
            if path in STALE_REFERENCE_EXCLUDES:
                continue
            try:
                text = path.read_text()
            except UnicodeDecodeError:
                continue
            for needle in (stale_crate_path, stale_crate_name):
                start = 0
                while True:
                    index = text.find(needle, start)
                    if index == -1:
                        break
                    findings.append((path, line_number(text, index), needle))
                    start = index + len(needle)
    return findings


def main() -> int:
    gaps = deadline_gaps()
    timeout_calibration = timeout_calibration_findings()
    sleeps = sleep_findings()
    stale = stale_test_crate_references()

    if gaps:
        print("async tests without explicit timeout wrappers:", file=sys.stderr)
        for test in gaps:
            print(
                f"  {test.path.relative_to(ROOT)}:{test.line}: {test.name}",
                file=sys.stderr,
            )
    if timeout_calibration:
        print("async tests with uncalibrated timeout wrappers:", file=sys.stderr)
        for finding in timeout_calibration:
            print(
                f"  {finding.path.relative_to(ROOT)}:{finding.line}: {finding.test_name}: {finding.detail}",
                file=sys.stderr,
            )
    if sleeps:
        print("E2E app fixed sleeps without allowlist comments:", file=sys.stderr)
        for finding in sleeps:
            print(
                f"  {finding.path.relative_to(ROOT)}:{finding.line}: {finding.detail}",
                file=sys.stderr,
            )
    if stale:
        print("stale standalone test-crate references:", file=sys.stderr)
        for path, line, detail in stale:
            label = path.relative_to(ROOT) if path.is_relative_to(ROOT) else path
            print(f"  {label}:{line}: {detail}", file=sys.stderr)
    if gaps or timeout_calibration or sleeps or stale:
        return 1

    print("async test timeout scan passed")
    print("async test timeout calibration scan passed")
    print("E2E fixed-sleep allowlist scan passed")
    print("standalone test-crate reference scan passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
