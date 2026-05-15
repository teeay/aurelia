// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use crate::config::PublishConfig;
use anyhow::{anyhow, bail, Context, Result};
use cargo_metadata::Metadata;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};
use toml_edit::{DocumentMut, Formatted, Item, Table, Value};

/// Synthesise the publish-tree Cargo.toml as a string.
pub fn synthesize(metadata: &Metadata, cfg: &PublishConfig) -> Result<String> {
    let target_pkg = metadata
        .workspace_packages()
        .into_iter()
        .find(|p| p.name.as_str() == cfg.target_crate.as_str())
        .ok_or_else(|| anyhow!("target crate not found in workspace"))?;

    let target_toml_path = target_pkg.manifest_path.as_std_path();
    let target_toml_str = fs_err::read_to_string(target_toml_path)?;
    let mut doc: DocumentMut = target_toml_str
        .parse()
        .context("failed to parse target Cargo.toml")?;
    validate_supported_dependency_tables(&doc, target_pkg.name.as_str())?;

    let workspace_root_toml_str =
        fs_err::read_to_string(metadata.workspace_root.join("Cargo.toml").as_std_path())?;
    let workspace_root_toml: DocumentMut = workspace_root_toml_str
        .parse()
        .context("failed to parse workspace root Cargo.toml")?;
    let workspace_pkg = workspace_root_toml
        .get("workspace")
        .and_then(|w| w.get("package"))
        .ok_or_else(|| anyhow!("workspace.package missing from root Cargo.toml"))?
        .clone();
    let workspace_deps = workspace_root_toml
        .get("workspace")
        .and_then(|w| w.get("dependencies"))
        .and_then(|i| i.as_table())
        .cloned();

    {
        let pkg = doc
            .get_mut("package")
            .and_then(|i| i.as_table_mut())
            .ok_or_else(|| anyhow!("[package] missing in target Cargo.toml"))?;
        flatten_workspace_fields(pkg, &workspace_pkg)?;
        if pkg.contains_key("readme") {
            pkg["readme"] = toml_edit::value("README.md");
        }
    }
    flatten_workspace_dependency_tables(
        &mut doc,
        workspace_deps.as_ref(),
        target_pkg.name.as_str(),
    )?;

    let internal_deps = InternalDependencySet::from_metadata(metadata, cfg)?;
    let target_manifest_dir = target_pkg
        .manifest_path
        .parent()
        .ok_or_else(|| anyhow!("target manifest has no parent directory"))?
        .as_std_path()
        .to_path_buf();

    // Drop path deps to internal crates
    drop_internal_deps(
        &mut doc,
        "dependencies",
        &internal_deps,
        &target_manifest_dir,
    )?;
    drop_internal_deps(
        &mut doc,
        "dev-dependencies",
        &internal_deps,
        &target_manifest_dir,
    )?;

    let mut internal_features: BTreeMap<String, Table> = BTreeMap::new();

    // Merge dependencies / dev-dependencies / features from each internal crate
    for ic in &cfg.internal_crates {
        let pkg_meta = metadata
            .workspace_packages()
            .into_iter()
            .find(|p| p.name.as_str() == ic.name.as_str())
            .ok_or_else(|| anyhow!("internal crate `{}` not found", ic.name))?;
        let ic_toml_str = fs_err::read_to_string(pkg_meta.manifest_path.as_std_path())?;
        let mut ic_doc: DocumentMut = ic_toml_str
            .parse()
            .with_context(|| format!("failed to parse Cargo.toml for `{}`", ic.name))?;
        validate_supported_dependency_tables(&ic_doc, &ic.name)?;
        flatten_workspace_dependency_tables(&mut ic_doc, workspace_deps.as_ref(), &ic.name)?;
        let ic_manifest_dir = pkg_meta
            .manifest_path
            .parent()
            .ok_or_else(|| anyhow!("internal crate `{}` manifest has no parent", ic.name))?
            .as_std_path()
            .to_path_buf();
        if let Some(t) = ic_doc.get("dependencies").and_then(|i| i.as_table()) {
            merge_deps(
                &mut doc,
                "dependencies",
                t,
                &internal_deps,
                &ic_manifest_dir,
            )?;
        }
        if let Some(t) = ic_doc.get("dev-dependencies").and_then(|i| i.as_table()) {
            merge_deps(
                &mut doc,
                "dev-dependencies",
                t,
                &internal_deps,
                &ic_manifest_dir,
            )?;
        }
        if let Some(t) = ic_doc.get("features").and_then(|i| i.as_table()) {
            internal_features.insert(ic.name.clone(), t.clone());
            merge_features(&mut doc, t)?;
        }
    }
    normalize_internal_feature_edges(&mut doc, &internal_features)?;

    ensure_canonical_lib_path(&mut doc);

    validate_generated_manifest(&doc, &internal_deps)?;

    Ok(doc.to_string())
}

fn validate_supported_dependency_tables(doc: &DocumentMut, manifest_label: &str) -> Result<()> {
    if doc.contains_key("build-dependencies") {
        bail!(
            "`{}` uses [build-dependencies], which is outside the current publish-tree merge contract",
            manifest_label
        );
    }

    let Some(targets) = doc.get("target").and_then(|item| item.as_table()) else {
        return Ok(());
    };
    for (target_name, target_item) in targets.iter() {
        let Some(target_table) = target_item.as_table() else {
            continue;
        };
        for table_name in ["dependencies", "dev-dependencies", "build-dependencies"] {
            if target_table.contains_key(table_name) {
                bail!(
                    "`{}` uses [target.{}.{}], which is outside the current publish-tree merge contract",
                    manifest_label,
                    target_name,
                    table_name
                );
            }
        }
    }
    Ok(())
}

fn ensure_canonical_lib_path(doc: &mut DocumentMut) {
    if !doc.contains_key("lib") {
        doc.insert("lib", Item::Table(Table::new()));
    }
    if let Some(lib) = doc.get_mut("lib").and_then(|i| i.as_table_mut()) {
        lib["path"] = toml_edit::value("src/lib.rs");
    }
}

#[derive(Debug, Clone, Default)]
struct InternalDependencySet {
    names: BTreeSet<String>,
    manifest_dirs: BTreeSet<PathBuf>,
}

impl InternalDependencySet {
    fn from_metadata(metadata: &Metadata, cfg: &PublishConfig) -> Result<Self> {
        let mut set = Self::default();
        for ic in &cfg.internal_crates {
            let pkg = metadata
                .workspace_packages()
                .into_iter()
                .find(|p| p.name.as_str() == ic.name.as_str())
                .ok_or_else(|| anyhow!("internal crate `{}` not found", ic.name))?;
            let manifest_dir = pkg
                .manifest_path
                .parent()
                .ok_or_else(|| anyhow!("internal crate `{}` manifest has no parent", ic.name))?
                .as_std_path();
            set.names.insert(ic.name.clone());
            set.manifest_dirs.insert(lexical_normalize(manifest_dir));
        }
        Ok(set)
    }

    #[cfg(test)]
    fn from_names_and_paths(names: &[&str], paths: &[&str]) -> Self {
        Self {
            names: names.iter().map(|name| (*name).to_string()).collect(),
            manifest_dirs: paths
                .iter()
                .map(|path| lexical_normalize(Path::new(path)))
                .collect(),
        }
    }

    fn is_internal_dependency(&self, dep_key: &str, dep: &Item, manifest_dir: &Path) -> bool {
        if self.names.contains(dep_key) {
            return true;
        }
        let package = dep_package_name(dep_key, dep);
        if self.names.contains(&package) {
            return true;
        }
        if let Some(path) = dep_path(dep) {
            let resolved = resolve_dependency_path(manifest_dir, &path);
            return self.manifest_dirs.contains(&resolved);
        }
        false
    }
}

fn lexical_normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::RootDir | Component::Prefix(_) | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
        }
    }
    normalized
}

fn resolve_dependency_path(manifest_dir: &Path, path: &str) -> PathBuf {
    let path = Path::new(path);
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else {
        manifest_dir.join(path)
    };
    lexical_normalize(&joined)
}

fn normalize_internal_feature_edges(
    doc: &mut DocumentMut,
    internal_features: &BTreeMap<String, Table>,
) -> Result<()> {
    let Some(features) = doc.get_mut("features").and_then(|i| i.as_table_mut()) else {
        return Ok(());
    };
    let names: Vec<String> = features.iter().map(|(name, _)| name.to_string()).collect();
    for name in names {
        let existing = features
            .get(&name)
            .and_then(|i| i.as_value())
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("feature `{}` value must be an array", name))?
            .clone();
        let mut normalized = Vec::new();
        for entry in existing.iter().filter_map(|v| v.as_str()) {
            let mut stack = Vec::new();
            for expanded in expand_internal_feature_entry(entry, internal_features, &mut stack)? {
                if !normalized.iter().any(|value| value == &expanded) {
                    normalized.push(expanded);
                }
            }
        }
        let mut arr = toml_edit::Array::new();
        for entry in normalized {
            arr.push(entry);
        }
        features.insert(&name, Item::Value(Value::Array(arr)));
    }
    Ok(())
}

fn expand_internal_feature_entry(
    entry: &str,
    internal_features: &BTreeMap<String, Table>,
    stack: &mut Vec<(String, String)>,
) -> Result<Vec<String>> {
    if let Some(internal_dep) = internal_dependency_entry(entry) {
        if internal_features.contains_key(internal_dep) {
            return Ok(Vec::new());
        }
    }

    let Some((crate_name, feature_name)) = internal_feature_edge(entry, internal_features) else {
        return Ok(vec![entry.to_string()]);
    };

    let stack_entry = (crate_name.to_string(), feature_name.to_string());
    if stack.iter().any(|entry| entry == &stack_entry) {
        bail!(
            "internal feature cycle while expanding `{}/{}'",
            crate_name,
            feature_name
        );
    }
    stack.push(stack_entry);

    let feature_table = internal_features
        .get(crate_name)
        .ok_or_else(|| anyhow!("internal crate `{}` has no feature table", crate_name))?;
    let feature = feature_table.get(feature_name).ok_or_else(|| {
        anyhow!(
            "internal feature `{}/{}' not found",
            crate_name,
            feature_name
        )
    })?;
    let entries = feature
        .as_value()
        .and_then(|value| value.as_array())
        .ok_or_else(|| {
            anyhow!(
                "feature `{}/{}' value must be an array",
                crate_name,
                feature_name
            )
        })?
        .iter()
        .filter_map(|value| value.as_str())
        .map(String::from)
        .collect::<Vec<_>>();

    let mut expanded = Vec::new();
    for nested in entries {
        for entry in expand_internal_feature_entry(&nested, internal_features, stack)? {
            if !expanded.iter().any(|value| value == &entry) {
                expanded.push(entry);
            }
        }
    }
    stack.pop();
    Ok(expanded)
}

fn internal_feature_edge<'a>(
    entry: &'a str,
    internal_features: &BTreeMap<String, Table>,
) -> Option<(&'a str, &'a str)> {
    let (crate_part, feature_name) = entry.split_once('/')?;
    let crate_name = crate_part.strip_suffix('?').unwrap_or(crate_part);
    internal_features
        .contains_key(crate_name)
        .then_some((crate_name, feature_name))
}

fn internal_dependency_entry(entry: &str) -> Option<&str> {
    let dependency = entry.strip_prefix("dep:").unwrap_or(entry);
    Some(dependency.strip_suffix('?').unwrap_or(dependency))
}

fn dep_package_name(dep_key: &str, dep: &Item) -> String {
    let table = to_inline_table(dep);
    table
        .get("package")
        .and_then(|value| value.as_str())
        .unwrap_or(dep_key)
        .to_string()
}

fn dep_path(dep: &Item) -> Option<String> {
    let table = to_inline_table(dep);
    table
        .get("path")
        .and_then(|value| value.as_str())
        .map(String::from)
}

fn flatten_workspace_fields(pkg: &mut Table, workspace_pkg: &Item) -> Result<()> {
    let keys: Vec<String> = pkg.iter().map(|(k, _)| k.to_string()).collect();
    for key in keys {
        let item = pkg.get(&key).cloned().unwrap();
        let inherits = match &item {
            Item::Value(Value::InlineTable(t)) => {
                matches!(t.get("workspace"), Some(Value::Boolean(b)) if *b.value())
            }
            Item::Table(t) => matches!(
                t.get("workspace"),
                Some(Item::Value(Value::Boolean(b))) if *b.value()
            ),
            _ => false,
        };
        if inherits {
            let ws_val = workspace_pkg
                .get(&key)
                .ok_or_else(|| anyhow!("workspace.package.{} missing", key))?
                .clone();
            pkg.insert(&key, ws_val);
        }
    }
    Ok(())
}

fn flatten_workspace_dependency_tables(
    doc: &mut DocumentMut,
    workspace_deps: Option<&Table>,
    manifest_label: &str,
) -> Result<()> {
    for table_name in ["dependencies", "dev-dependencies"] {
        let Some(deps) = doc.get_mut(table_name).and_then(|i| i.as_table_mut()) else {
            continue;
        };
        let names = deps
            .iter()
            .map(|(name, _)| name.to_string())
            .collect::<Vec<_>>();
        for name in names {
            let Some(dep) = deps.get(&name).cloned() else {
                continue;
            };
            if !dependency_inherits_workspace(&dep) {
                continue;
            }
            let resolved = resolve_workspace_dependency(
                manifest_label,
                table_name,
                &name,
                &dep,
                workspace_deps,
            )?;
            deps.insert(&name, resolved);
        }
    }
    Ok(())
}

fn dependency_inherits_workspace(dep: &Item) -> bool {
    match dep {
        Item::Value(Value::InlineTable(table)) => {
            matches!(table.get("workspace"), Some(Value::Boolean(value)) if *value.value())
        }
        Item::Table(table) => matches!(
            table.get("workspace"),
            Some(Item::Value(Value::Boolean(value))) if *value.value()
        ),
        _ => false,
    }
}

fn resolve_workspace_dependency(
    manifest_label: &str,
    table_name: &str,
    dep_name: &str,
    local: &Item,
    workspace_deps: Option<&Table>,
) -> Result<Item> {
    let workspace_deps = workspace_deps
        .ok_or_else(|| anyhow!("workspace.dependencies missing from root Cargo.toml"))?;
    let root = workspace_deps.get(dep_name).ok_or_else(|| {
        anyhow!(
            "`{}` {} dependency `{}` inherits from workspace, but root [workspace.dependencies] has no `{}` entry",
            manifest_label,
            table_name,
            dep_name,
            dep_name
        )
    })?;

    let mut out = to_inline_table(root);
    let local = to_inline_table(local);

    for forbidden in ["version", "path", "git", "branch", "tag", "rev", "registry"] {
        if local.contains_key(forbidden) {
            bail!(
                "`{}` {} dependency `{}` uses workspace = true and must not set `{}` locally",
                manifest_label,
                table_name,
                dep_name,
                forbidden
            );
        }
    }

    let root_package = out
        .get("package")
        .and_then(|value| value.as_str())
        .map(String::from);
    let local_package = local
        .get("package")
        .and_then(|value| value.as_str())
        .map(String::from);
    if let Some(local_package) = local_package {
        if root_package.as_deref() != Some(local_package.as_str()) {
            bail!(
                "`{}` {} dependency `{}` sets local package `{}` that does not match root workspace dependency package",
                manifest_label,
                table_name,
                dep_name,
                local_package
            );
        }
    }

    let mut features = collect_features(&out);
    for feature in collect_features(&local) {
        if !features.iter().any(|existing| existing == &feature) {
            features.push(feature);
        }
    }
    if !features.is_empty() {
        let mut arr = toml_edit::Array::new();
        for feature in features {
            arr.push(feature);
        }
        out.insert("features", Value::Array(arr));
    }

    let root_optional = out
        .get("optional")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let local_optional = local
        .get("optional")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    if root_optional || local_optional {
        out.insert("optional", Value::Boolean(Formatted::new(true)));
    }

    let root_default_features = out
        .get("default-features")
        .and_then(|value| value.as_bool())
        .unwrap_or(true);
    let local_default_features = local
        .get("default-features")
        .and_then(|value| value.as_bool())
        .unwrap_or(true);
    if !(root_default_features && local_default_features) {
        out.insert("default-features", Value::Boolean(Formatted::new(false)));
    }

    Ok(Item::Value(Value::InlineTable(out)))
}

fn drop_internal_deps(
    doc: &mut DocumentMut,
    table: &str,
    internal_deps: &InternalDependencySet,
    manifest_dir: &Path,
) -> Result<()> {
    let Some(deps) = doc.get_mut(table).and_then(|i| i.as_table_mut()) else {
        return Ok(());
    };
    let to_remove: Vec<String> = deps
        .iter()
        .filter_map(|(k, value)| {
            if internal_deps.is_internal_dependency(k, value, manifest_dir) {
                Some(k.to_string())
            } else {
                None
            }
        })
        .collect();
    for k in to_remove {
        deps.remove(&k);
    }
    Ok(())
}

fn merge_deps(
    doc: &mut DocumentMut,
    table_name: &str,
    incoming: &Table,
    internal_deps: &InternalDependencySet,
    manifest_dir: &Path,
) -> Result<()> {
    if !doc.contains_key(table_name) {
        doc.insert(table_name, Item::Table(Table::new()));
    }
    for (name, value) in incoming.iter() {
        if internal_deps.is_internal_dependency(name, value, manifest_dir) {
            continue;
        }
        let target = doc
            .get_mut(table_name)
            .and_then(|i| i.as_table_mut())
            .unwrap();
        if let Some(existing) = target.get(name).cloned() {
            let merged = merge_dep_entries(name, &existing, value)?;
            target.insert(name, merged);
        } else {
            target.insert(name, normalize_dependency_item(value));
        }
    }
    Ok(())
}

fn normalize_dependency_item(value: &Item) -> Item {
    match value {
        Item::Table(_) => Item::Value(Value::InlineTable(to_inline_table(value))),
        Item::Value(value) => Item::Value(value.clone()),
        _ => Item::Value(Value::String(Formatted::new(String::new()))),
    }
}

fn validate_generated_manifest(
    doc: &DocumentMut,
    internal_deps: &InternalDependencySet,
) -> Result<()> {
    for table_name in ["dependencies", "dev-dependencies"] {
        let Some(deps) = doc.get(table_name).and_then(|item| item.as_table()) else {
            continue;
        };
        for (name, value) in deps.iter() {
            if dependency_inherits_workspace(value) {
                bail!(
                    "generated manifest {} dependency `{}` still inherits from workspace",
                    table_name,
                    name
                );
            }

            let package = dep_package_name(name, value);
            if internal_deps.names.contains(name) || internal_deps.names.contains(&package) {
                bail!(
                    "generated manifest {} dependency `{}` still references internal crate `{}`",
                    table_name,
                    name,
                    package
                );
            }

            if dep_path(value).is_some() {
                bail!(
                    "generated manifest {} dependency `{}` still has a local path",
                    table_name,
                    name
                );
            }
        }
    }

    if let Some(features) = doc.get("features").and_then(|item| item.as_table()) {
        for (feature_name, value) in features.iter() {
            let entries = value
                .as_value()
                .and_then(|value| value.as_array())
                .ok_or_else(|| anyhow!("feature `{}` value must be an array", feature_name))?;
            for entry in entries.iter().filter_map(|value| value.as_str()) {
                if let Some(dep_name) = internal_dependency_entry(entry) {
                    if internal_deps.names.contains(dep_name) {
                        bail!(
                            "generated manifest feature `{}` still references internal dependency `{}`",
                            feature_name,
                            dep_name
                        );
                    }
                }
                if let Some((crate_name, _feature)) = entry.split_once('/') {
                    let crate_name = crate_name.strip_suffix('?').unwrap_or(crate_name);
                    if internal_deps.names.contains(crate_name) {
                        bail!(
                            "generated manifest feature `{}` still references internal feature edge `{}`",
                            feature_name,
                            entry
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

fn merge_features(doc: &mut DocumentMut, incoming: &Table) -> Result<()> {
    if !doc.contains_key("features") {
        doc.insert("features", Item::Table(Table::new()));
    }
    let target = doc
        .get_mut("features")
        .and_then(|i| i.as_table_mut())
        .unwrap();
    for (name, value) in incoming.iter() {
        if let Some(existing) = target.get(name).cloned() {
            let existing_arr = existing
                .as_value()
                .and_then(|v| v.as_array())
                .ok_or_else(|| anyhow!("feature `{}` value must be an array", name))?
                .clone();
            let new_arr = value
                .as_value()
                .and_then(|v| v.as_array())
                .ok_or_else(|| anyhow!("feature `{}` value must be an array", name))?;
            let mut merged: Vec<String> = existing_arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
            for v in new_arr.iter() {
                if let Some(s) = v.as_str() {
                    if !merged.iter().any(|m| m == s) {
                        merged.push(s.to_string());
                    }
                }
            }
            let mut arr = toml_edit::Array::new();
            for s in merged {
                arr.push(s);
            }
            target.insert(name, Item::Value(Value::Array(arr)));
        } else {
            target.insert(
                name,
                Item::Value(
                    value
                        .as_value()
                        .cloned()
                        .unwrap_or_else(|| Value::Array(toml_edit::Array::new())),
                ),
            );
        }
    }
    Ok(())
}

fn merge_dep_entries(name: &str, existing: &Item, incoming: &Item) -> Result<Item> {
    let e_val = existing.as_value();
    let i_val = incoming.as_value();

    // Identical: keep existing.
    if let (Some(ev), Some(iv)) = (e_val, i_val) {
        if ev.to_string().trim() == iv.to_string().trim() {
            return Ok(existing.clone());
        }
    }

    let existing_table = to_inline_table(existing);
    let incoming_table = to_inline_table(incoming);
    let (mut out, src) = (existing_table, incoming_table);

    // Version: must agree if both specify.
    let e_ver = out
        .get("version")
        .and_then(|v| v.as_str())
        .map(String::from);
    let i_ver = src
        .get("version")
        .and_then(|v| v.as_str())
        .map(String::from);
    match (e_ver.as_deref(), i_ver.as_deref()) {
        (Some(a), Some(b)) if a != b => {
            bail!("dependency `{}` version conflict: `{}` vs `{}`", name, a, b);
        }
        (None, Some(_)) => {
            out.insert("version", src.get("version").unwrap().clone());
        }
        _ => {}
    }

    // Features: union.
    let mut features: Vec<String> = collect_features(&out);
    for f in collect_features(&src) {
        if !features.iter().any(|m| m == &f) {
            features.push(f);
        }
    }
    if !features.is_empty() {
        let mut arr = toml_edit::Array::new();
        for f in &features {
            arr.push(f.clone());
        }
        out.insert("features", Value::Array(arr));
    }

    // optional: OR.
    let e_opt = out
        .get("optional")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let i_opt = src
        .get("optional")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if e_opt || i_opt {
        out.insert("optional", Value::Boolean(Formatted::new(true)));
    }

    // default-features: AND (any disabling wins).
    let e_def = out
        .get("default-features")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let i_def = src
        .get("default-features")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    if !(e_def && i_def) {
        out.insert("default-features", Value::Boolean(Formatted::new(false)));
    }

    Ok(Item::Value(Value::InlineTable(out)))
}

fn to_inline_table(item: &Item) -> toml_edit::InlineTable {
    if let Some(Value::InlineTable(t)) = item.as_value() {
        return t.clone();
    }
    if let Item::Table(t) = item {
        let mut inline = toml_edit::InlineTable::new();
        for (k, v) in t.iter() {
            if let Item::Value(val) = v {
                inline.insert(k, val.clone());
            }
        }
        return inline;
    }
    if let Some(Value::String(s)) = item.as_value() {
        let mut inline = toml_edit::InlineTable::new();
        inline.insert("version", Value::String(s.clone()));
        return inline;
    }
    toml_edit::InlineTable::new()
}

fn collect_features(t: &toml_edit::InlineTable) -> Vec<String> {
    t.get("features")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
#[path = "tests/manifest.rs"]
mod tests;
