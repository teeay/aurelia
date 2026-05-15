// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn retarget_crate_paths_skips_macro_crate_variable_and_root_macro_invocation() {
    let input =
        "$crate::__limited_event!(x); crate::log_info!(x); crate::local::Thing; foo(crate::bar());";
    let output = retarget_crate_paths(input, "logging");
    assert_eq!(
        output,
        "$crate::__limited_event!(x); crate::log_info!(x); crate::logging::local::Thing; foo(crate::logging::bar());"
    );
}

#[test]
fn inject_mod_declarations_preserves_valid_rust_file_header() {
    let dir = test_dir("inject-mod-header");
    fs_err::create_dir_all(&dir).expect("create dir");
    let lib = dir.join("lib.rs");
    fs_err::write(
        &lib,
        r#"// leading regular comment
//! line doc
/*! block doc
still block doc */
#![warn(missing_docs)]

pub struct Root;
"#,
    )
    .expect("write lib");
    let crates = vec![InternalCrate {
        name: "aurelia-peering".to_string(),
        module: "peering".to_string(),
    }];

    inject_mod_declarations(&lib, &crates).expect("inject");

    let content = fs_err::read_to_string(&lib).expect("read lib");
    let mod_pos = content.find("mod peering;").expect("module declaration");
    let item_pos = content.find("pub struct Root;").expect("item");
    let attr_pos = content.find("#![warn(missing_docs)]").expect("attribute");
    let block_doc_pos = content.find("/*! block doc").expect("block doc");
    assert!(block_doc_pos < mod_pos);
    assert!(attr_pos < mod_pos);
    assert!(mod_pos < item_pos);
    fs_err::remove_dir_all(&dir).expect("cleanup");
}

#[test]
fn supported_crate_root_rejects_build_script() {
    let dir = test_dir("build-script-guard");
    fs_err::create_dir_all(&dir).expect("create dir");
    fs_err::write(dir.join("build.rs"), "fn main() {}\n").expect("write build script");

    let err =
        ensure_supported_crate_root("aurelia-peering", &dir).expect_err("build script rejected");

    assert!(err.to_string().contains("aurelia-peering"));
    assert!(err.to_string().contains("build.rs"));
    fs_err::remove_dir_all(&dir).expect("cleanup");
}

fn test_dir(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "aurelia-xtask-{}-{}-{}",
        label,
        std::process::id(),
        nanos
    ))
}
