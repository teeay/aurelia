// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::path::Path;

fn workspace_deps(root: &DocumentMut) -> &Table {
    root["workspace"]["dependencies"]
        .as_table()
        .expect("workspace dependencies")
}

fn dep_table(doc: &DocumentMut, table_name: &str, dep_name: &str) -> toml_edit::InlineTable {
    to_inline_table(
        doc[table_name]
            .as_table()
            .expect("dependency table")
            .get(dep_name)
            .expect("dependency"),
    )
}

fn dep_features(dep: &toml_edit::InlineTable) -> Vec<&str> {
    dep.get("features")
        .and_then(|value| value.as_array())
        .expect("features")
        .iter()
        .filter_map(|value| value.as_str())
        .collect()
}

#[test]
fn flattens_target_workspace_dependency_inheritance() {
    let root: DocumentMut = r#"
[workspace.dependencies]
tokio = "1"
"#
    .parse()
    .expect("root");
    let mut doc: DocumentMut = r#"
[dependencies]
tokio = { workspace = true, features = ["sync"] }
"#
    .parse()
    .expect("doc");

    flatten_workspace_dependency_tables(&mut doc, Some(workspace_deps(&root)), "aurelia")
        .expect("flatten");

    let tokio = dep_table(&doc, "dependencies", "tokio");
    assert_eq!(
        tokio.get("version").and_then(|value| value.as_str()),
        Some("1")
    );
    assert_eq!(dep_features(&tokio), ["sync"]);
    assert!(!tokio.contains_key("workspace"));
}

#[test]
fn flattens_internal_workspace_dependency_before_merge() {
    let root: DocumentMut = r#"
[workspace.dependencies]
tokio = "1"
"#
    .parse()
    .expect("root");
    let mut doc: DocumentMut = r#"
[dependencies]
"#
    .parse()
    .expect("doc");
    let mut internal_doc: DocumentMut = r#"
[dependencies]
tokio = { workspace = true, features = ["sync"] }
"#
    .parse()
    .expect("internal");
    let internal_deps = InternalDependencySet::default();

    flatten_workspace_dependency_tables(
        &mut internal_doc,
        Some(workspace_deps(&root)),
        "aurelia-platform",
    )
    .expect("flatten");
    merge_deps(
        &mut doc,
        "dependencies",
        internal_doc["dependencies"]
            .as_table()
            .expect("dependencies"),
        &internal_deps,
        Path::new("/workspace/src/crates/platform"),
    )
    .expect("merge");

    let tokio = dep_table(&doc, "dependencies", "tokio");
    assert_eq!(
        tokio.get("version").and_then(|value| value.as_str()),
        Some("1")
    );
    assert_eq!(dep_features(&tokio), ["sync"]);
}

#[test]
fn unions_workspace_and_local_features() {
    let root: DocumentMut = r#"
[workspace.dependencies]
serde = { version = "1", features = ["std"] }
"#
    .parse()
    .expect("root");
    let mut doc: DocumentMut = r#"
[dependencies]
serde = { workspace = true, features = ["derive"] }
"#
    .parse()
    .expect("doc");

    flatten_workspace_dependency_tables(&mut doc, Some(workspace_deps(&root)), "aurelia-data")
        .expect("flatten");

    let serde = dep_table(&doc, "dependencies", "serde");
    assert_eq!(
        serde.get("version").and_then(|value| value.as_str()),
        Some("1")
    );
    assert_eq!(dep_features(&serde), ["std", "derive"]);
}

#[test]
fn preserves_local_optional_on_workspace_dependency() {
    let root: DocumentMut = r#"
[workspace.dependencies]
actix = "0.13"
"#
    .parse()
    .expect("root");
    let mut doc: DocumentMut = r#"
[dependencies]
actix = { workspace = true, optional = true }
"#
    .parse()
    .expect("doc");

    flatten_workspace_dependency_tables(&mut doc, Some(workspace_deps(&root)), "aurelia-peering")
        .expect("flatten");

    let actix = dep_table(&doc, "dependencies", "actix");
    assert_eq!(
        actix.get("optional").and_then(|value| value.as_bool()),
        Some(true)
    );
}

#[test]
fn default_features_false_wins_for_workspace_dependency() {
    let root: DocumentMut = r#"
[workspace.dependencies]
tokio = "1"
"#
    .parse()
    .expect("root");
    let mut doc: DocumentMut = r#"
[dependencies]
tokio = { workspace = true, default-features = false }
"#
    .parse()
    .expect("doc");

    flatten_workspace_dependency_tables(&mut doc, Some(workspace_deps(&root)), "aurelia-peering")
        .expect("flatten");

    let tokio = dep_table(&doc, "dependencies", "tokio");
    assert_eq!(
        tokio
            .get("default-features")
            .and_then(|value| value.as_bool()),
        Some(false)
    );
}

#[test]
fn flattens_workspace_dev_dependency_inheritance() {
    let root: DocumentMut = r#"
[workspace.dependencies]
rcgen = "0.12"
"#
    .parse()
    .expect("root");
    let mut doc: DocumentMut = r#"
[dev-dependencies]
rcgen.workspace = true
"#
    .parse()
    .expect("doc");

    flatten_workspace_dependency_tables(&mut doc, Some(workspace_deps(&root)), "aurelia-peering")
        .expect("flatten");

    let rcgen = dep_table(&doc, "dev-dependencies", "rcgen");
    assert_eq!(
        rcgen.get("version").and_then(|value| value.as_str()),
        Some("0.12")
    );
    assert!(!rcgen.contains_key("workspace"));
}

#[test]
fn missing_workspace_dependency_is_clear_error() {
    let root: DocumentMut = r#"
[workspace.dependencies]
tokio = "1"
"#
    .parse()
    .expect("root");
    let mut doc: DocumentMut = r#"
[dependencies]
bytes.workspace = true
"#
    .parse()
    .expect("doc");

    let err = flatten_workspace_dependency_tables(
        &mut doc,
        Some(workspace_deps(&root)),
        "aurelia-peering",
    )
    .expect_err("missing workspace dependency");

    assert!(err.to_string().contains("aurelia-peering"));
    assert!(err.to_string().contains("dependencies"));
    assert!(err.to_string().contains("bytes"));
}

#[test]
fn local_source_override_on_workspace_dependency_is_rejected() {
    let root: DocumentMut = r#"
[workspace.dependencies]
tokio = "1"
"#
    .parse()
    .expect("root");
    let mut doc: DocumentMut = r#"
[dependencies]
tokio = { workspace = true, version = "2" }
"#
    .parse()
    .expect("doc");

    let err = flatten_workspace_dependency_tables(
        &mut doc,
        Some(workspace_deps(&root)),
        "aurelia-peering",
    )
    .expect_err("local version override");

    assert!(err.to_string().contains("tokio"));
    assert!(err.to_string().contains("version"));
}

#[test]
fn normalizes_internal_feature_edges_to_merged_dependencies() {
    let mut doc: DocumentMut = r#"
[features]
actix = ["aurelia-peering/actix"]
"#
    .parse()
    .expect("doc");
    let peering: DocumentMut = r#"
[features]
actix = ["dep:actix"]
"#
    .parse()
    .expect("peering");
    let mut internal = BTreeMap::new();
    internal.insert(
        "aurelia-peering".to_string(),
        peering["features"].as_table().expect("features").clone(),
    );

    normalize_internal_feature_edges(&mut doc, &internal).expect("normalize");

    let features = doc["features"].as_table().expect("features");
    let entries: Vec<&str> = features["actix"]
        .as_value()
        .and_then(|value| value.as_array())
        .expect("actix")
        .iter()
        .filter_map(|value| value.as_str())
        .collect();
    assert_eq!(entries, ["dep:actix"]);
}

#[test]
fn normalizes_nested_internal_feature_edges() {
    let mut doc: DocumentMut = r#"
[features]
bridge = ["aurelia-peering/bridge"]
"#
    .parse()
    .expect("doc");
    let peering: DocumentMut = r#"
[features]
bridge = ["aurelia-data/serde", "dep:bridge"]
"#
    .parse()
    .expect("peering");
    let data: DocumentMut = r#"
[features]
serde = ["dep:serde"]
"#
    .parse()
    .expect("data");
    let mut internal = BTreeMap::new();
    internal.insert(
        "aurelia-peering".to_string(),
        peering["features"].as_table().expect("features").clone(),
    );
    internal.insert(
        "aurelia-data".to_string(),
        data["features"].as_table().expect("features").clone(),
    );

    normalize_internal_feature_edges(&mut doc, &internal).expect("normalize");

    let features = doc["features"].as_table().expect("features");
    let entries: Vec<&str> = features["bridge"]
        .as_value()
        .and_then(|value| value.as_array())
        .expect("bridge")
        .iter()
        .filter_map(|value| value.as_str())
        .collect();
    assert_eq!(entries, ["dep:serde", "dep:bridge"]);
}

#[test]
fn drops_aliased_internal_dependency_by_package_name() {
    let mut doc: DocumentMut = r#"
[dependencies]
ids = { package = "aurelia-ids", path = "../crates/ids" }
tokio = "1"
"#
    .parse()
    .expect("doc");
    let internal =
        InternalDependencySet::from_names_and_paths(&["aurelia-ids"], &["/repo/src/crates/ids"]);

    drop_internal_deps(
        &mut doc,
        "dependencies",
        &internal,
        Path::new("/repo/src/lib"),
    )
    .expect("drop internal");

    let deps = doc["dependencies"].as_table().expect("dependencies");
    assert!(!deps.contains_key("ids"));
    assert!(deps.contains_key("tokio"));
}

#[test]
fn drops_aliased_internal_dependency_by_path() {
    let mut doc: DocumentMut = r#"
[dependencies]
ids = { path = "../crates/ids" }
bytes = "1"
"#
    .parse()
    .expect("doc");
    let internal =
        InternalDependencySet::from_names_and_paths(&["aurelia-ids"], &["/repo/src/crates/ids"]);

    drop_internal_deps(
        &mut doc,
        "dependencies",
        &internal,
        Path::new("/repo/src/lib"),
    )
    .expect("drop internal");

    let deps = doc["dependencies"].as_table().expect("dependencies");
    assert!(!deps.contains_key("ids"));
    assert!(deps.contains_key("bytes"));
}

#[test]
fn merge_skips_internal_dependency_alias_and_keeps_external_dependency() {
    let mut doc: DocumentMut = r#"
[dependencies]
"#
    .parse()
    .expect("doc");
    let incoming: DocumentMut = r#"
[dependencies]
ids = { package = "aurelia-ids", path = "../ids" }
bytes = "1"
"#
    .parse()
    .expect("incoming");
    let internal =
        InternalDependencySet::from_names_and_paths(&["aurelia-ids"], &["/repo/src/crates/ids"]);

    merge_deps(
        &mut doc,
        "dependencies",
        incoming["dependencies"].as_table().expect("dependencies"),
        &internal,
        Path::new("/repo/src/crates/peering"),
    )
    .expect("merge");

    let deps = doc["dependencies"].as_table().expect("dependencies");
    assert!(!deps.contains_key("ids"));
    assert!(deps.contains_key("bytes"));
}

#[test]
fn generated_manifest_guard_accepts_expected_external_dependencies() {
    let doc: DocumentMut = r#"
[dependencies]
tokio = { version = "1", features = ["sync"] }
bytes = "1"

[dev-dependencies]
rcgen = "0.12"

[features]
actix = ["dep:actix"]
"#
    .parse()
    .expect("doc");
    let internal = InternalDependencySet::from_names_and_paths(
        &["aurelia-ids", "aurelia-peering"],
        &["/repo/src/crates/ids", "/repo/src/crates/peering"],
    );

    validate_generated_manifest(&doc, &internal).expect("external dependencies are valid");
}

#[test]
fn generated_manifest_guard_rejects_leftover_internal_package() {
    let doc: DocumentMut = r#"
[dependencies]
ids = { package = "aurelia-ids", version = "0.1" }
"#
    .parse()
    .expect("doc");
    let internal =
        InternalDependencySet::from_names_and_paths(&["aurelia-ids"], &["/repo/src/crates/ids"]);

    let err = validate_generated_manifest(&doc, &internal).expect_err("internal package rejected");

    assert!(err.to_string().contains("aurelia-ids"));
}

#[test]
fn generated_manifest_guard_rejects_local_path_dependency() {
    let doc: DocumentMut = r#"
[dependencies]
local-helper = { path = "../local-helper" }
"#
    .parse()
    .expect("doc");
    let internal = InternalDependencySet::default();

    let err = validate_generated_manifest(&doc, &internal).expect_err("local path rejected");

    assert!(err.to_string().contains("local path"));
}

#[test]
fn generated_manifest_guard_rejects_internal_feature_dependency() {
    let doc: DocumentMut = r#"
[dependencies]
actix = { version = "0.13", optional = true }

[features]
actix = ["aurelia-peering/actix", "dep:actix"]
"#
    .parse()
    .expect("doc");
    let internal = InternalDependencySet::from_names_and_paths(
        &["aurelia-peering"],
        &["/repo/src/crates/peering"],
    );

    let err = validate_generated_manifest(&doc, &internal).expect_err("internal feature rejected");

    assert!(err.to_string().contains("aurelia-peering/actix"));
}

#[test]
fn table_form_dependency_is_preserved_when_first_merged() {
    let mut doc: DocumentMut = r#"
[dependencies]
"#
    .parse()
    .expect("doc");
    let incoming: DocumentMut = r#"
[dependencies.tabledep]
version = "1"
features = ["derive"]
"#
    .parse()
    .expect("incoming");
    let internal = InternalDependencySet::default();

    merge_deps(
        &mut doc,
        "dependencies",
        incoming["dependencies"].as_table().expect("dependencies"),
        &internal,
        Path::new("/repo/src/crates/data"),
    )
    .expect("merge");

    let tabledep = dep_table(&doc, "dependencies", "tabledep");
    assert_eq!(
        tabledep.get("version").and_then(|value| value.as_str()),
        Some("1")
    );
    assert_eq!(dep_features(&tabledep), ["derive"]);
}

#[test]
fn unsupported_build_dependencies_are_rejected() {
    let doc: DocumentMut = r#"
[build-dependencies]
cc = "1"
"#
    .parse()
    .expect("doc");

    let err = validate_supported_dependency_tables(&doc, "aurelia-peering")
        .expect_err("build dependencies rejected");

    assert!(err.to_string().contains("aurelia-peering"));
    assert!(err.to_string().contains("build-dependencies"));
}

#[test]
fn unsupported_target_dependencies_are_rejected() {
    let doc: DocumentMut = r#"
[target.'cfg(unix)'.dependencies]
libc = "0.2"
"#
    .parse()
    .expect("doc");

    let err = validate_supported_dependency_tables(&doc, "aurelia-peering")
        .expect_err("target dependencies rejected");

    assert!(err.to_string().contains("aurelia-peering"));
    assert!(err.to_string().contains("target"));
    assert!(err.to_string().contains("dependencies"));
}

#[test]
fn unsupported_target_dev_dependencies_are_rejected() {
    let doc: DocumentMut = r#"
[target.'cfg(unix)'.dev-dependencies]
tempfile = "3"
"#
    .parse()
    .expect("doc");

    let err = validate_supported_dependency_tables(&doc, "aurelia-peering")
        .expect_err("target dev-dependencies rejected");

    assert!(err.to_string().contains("aurelia-peering"));
    assert!(err.to_string().contains("dev-dependencies"));
}

#[test]
fn unsupported_target_build_dependencies_are_rejected() {
    let doc: DocumentMut = r#"
[target.'cfg(unix)'.build-dependencies]
cc = "1"
"#
    .parse()
    .expect("doc");

    let err = validate_supported_dependency_tables(&doc, "aurelia-peering")
        .expect_err("target build-dependencies rejected");

    assert!(err.to_string().contains("aurelia-peering"));
    assert!(err.to_string().contains("build-dependencies"));
}

#[test]
fn canonical_lib_path_is_inserted_when_missing() {
    let mut doc: DocumentMut = r#"
[package]
name = "aurelia"
"#
    .parse()
    .expect("doc");

    ensure_canonical_lib_path(&mut doc);

    assert_eq!(
        doc["lib"]["path"]
            .as_value()
            .and_then(|value| value.as_str()),
        Some("src/lib.rs")
    );
}
