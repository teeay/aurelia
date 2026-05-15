// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn default_module_strips_prefix_and_dashes() {
    assert_eq!(default_module("aurelia-peering"), "peering");
    assert_eq!(default_module("aurelia-resolver"), "resolver");
    assert_eq!(default_module("aurelia-foo-bar"), "foo_bar");
    assert_eq!(default_module("standalone"), "standalone");
    assert_eq!(default_module("aurelia-ids"), "ids");
}

#[test]
fn excluded_crates_must_be_workspace_members() {
    let workspace_members = vec!["aurelia".to_string(), "xtask".to_string()];
    let excluded = vec!["xtask".to_string()];

    validate_excluded_crates(&excluded, &workspace_members).expect("valid excluded crate");
}

#[test]
fn misspelled_excluded_crate_is_rejected() {
    let workspace_members = vec!["aurelia".to_string(), "xtask".to_string()];
    let excluded = vec!["xtaskk".to_string()];

    let err = validate_excluded_crates(&excluded, &workspace_members)
        .expect_err("invalid excluded crate");

    assert!(err.to_string().contains("xtaskk"));
    assert!(err.to_string().contains("workspace member"));
}
