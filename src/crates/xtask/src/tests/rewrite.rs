// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn internal_crates() -> Rewriter {
    Rewriter::new(&[
        ("aurelia_ids".to_string(), "crate::ids".to_string()),
        ("aurelia_data".to_string(), "crate::data".to_string()),
        (
            "aurelia_platform".to_string(),
            "crate::platform".to_string(),
        ),
        ("aurelia_logging".to_string(), "crate::logging".to_string()),
        ("aurelia_peering".to_string(), "crate::peering".to_string()),
        (
            "aurelia_resolver".to_string(),
            "crate::resolver".to_string(),
        ),
    ])
    .unwrap()
}

#[test]
fn rewrites_bare_path() {
    assert_eq!(
        internal_crates().rewrite("aurelia_peering::Domus"),
        "crate::peering::Domus"
    );
}

#[test]
fn rewrites_use_statement() {
    assert_eq!(
        internal_crates().rewrite("use aurelia_peering;"),
        "use crate::peering;"
    );
    assert_eq!(
        internal_crates().rewrite("use aurelia_peering::{A, B};"),
        "use crate::peering::{A, B};"
    );
}

#[test]
fn rewrites_doc_comment() {
    assert_eq!(
        internal_crates().rewrite("/// See aurelia_peering for details"),
        "/// See crate::peering for details"
    );
}

#[test]
fn does_not_rewrite_substring() {
    let r = internal_crates();
    assert_eq!(r.rewrite("aurelia_peeringx"), "aurelia_peeringx");
    assert_eq!(r.rewrite("xaurelia_peering"), "xaurelia_peering");
    assert_eq!(r.rewrite("foo_aurelia_peering"), "foo_aurelia_peering");
    assert_eq!(r.rewrite("aurelia_peering_extra"), "aurelia_peering_extra");
}

#[test]
fn rewrites_all_internal_crates() {
    let r = internal_crates();
    assert_eq!(r.rewrite("aurelia_ids::ErrorId"), "crate::ids::ErrorId");
    assert_eq!(
        r.rewrite("aurelia_data::DomusAddr"),
        "crate::data::DomusAddr"
    );
    assert_eq!(
        r.rewrite("aurelia_platform::runtime::handle"),
        "crate::platform::runtime::handle"
    );
    assert_eq!(r.rewrite("aurelia_logging::limit"), "crate::logging::limit");
    assert_eq!(r.rewrite("aurelia_peering::Domus"), "crate::peering::Domus");
    assert_eq!(
        r.rewrite("aurelia_resolver::SimpleResolver"),
        "crate::resolver::SimpleResolver"
    );
}

#[test]
fn empty_table_is_noop() {
    let r = Rewriter::new(&[]).unwrap();
    assert_eq!(r.rewrite("aurelia_peering::Foo"), "aurelia_peering::Foo");
}

#[test]
fn single_crate_table() {
    let r = Rewriter::new(&[("aurelia_ids".to_string(), "crate::ids".to_string())]).unwrap();
    assert_eq!(
        r.rewrite("aurelia_ids::X aurelia_peering::Y"),
        "crate::ids::X aurelia_peering::Y"
    );
}

#[test]
fn rewrites_string_literal() {
    assert_eq!(
        internal_crates().rewrite(r#"let s = "aurelia_peering";"#),
        r#"let s = "crate::peering";"#
    );
}

#[test]
fn handles_multiple_occurrences_in_one_line() {
    assert_eq!(
        internal_crates().rewrite("aurelia_ids::A + aurelia_peering::B"),
        "crate::ids::A + crate::peering::B"
    );
}
