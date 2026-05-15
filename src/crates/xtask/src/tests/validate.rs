// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn full_validation_runs_tests_with_all_targets_and_all_features() {
    let steps = validation_steps(false);
    let test = steps
        .iter()
        .find(|step| step.label == "test")
        .expect("test step");

    assert_eq!(test.args, ["test", "--all-targets", "--all-features"]);
}

#[test]
fn check_validation_keeps_fast_build_and_dry_run_only() {
    let steps = validation_steps(true);
    let labels = steps.iter().map(|step| step.label).collect::<Vec<_>>();

    assert_eq!(labels, ["build", "dry-run"]);
}
