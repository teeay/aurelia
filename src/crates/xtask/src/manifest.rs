// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use crate::config::PublishConfig;
use anyhow::{anyhow, bail, Context, Result};
use cargo_metadata::Metadata;
use std::collections::BTreeSet;
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

    let internal_set: BTreeSet<String> =
        cfg.internal_crates.iter().map(|c| c.name.clone()).collect();

    // Drop path deps to internal crates
    drop_internal_deps(&mut doc, "dependencies", &internal_set);
    drop_internal_deps(&mut doc, "dev-dependencies", &internal_set);

    // Merge dependencies / dev-dependencies / features from each internal crate
    for ic in &cfg.internal_crates {
        let pkg_meta = metadata
            .workspace_packages()
            .into_iter()
            .find(|p| p.name.as_str() == ic.name.as_str())
            .ok_or_else(|| anyhow!("internal crate `{}` not found", ic.name))?;
        let ic_toml_str = fs_err::read_to_string(pkg_meta.manifest_path.as_std_path())?;
        let ic_doc: DocumentMut = ic_toml_str
            .parse()
            .with_context(|| format!("failed to parse Cargo.toml for `{}`", ic.name))?;
        if let Some(t) = ic_doc.get("dependencies").and_then(|i| i.as_table()) {
            merge_deps(&mut doc, "dependencies", t, &internal_set)?;
        }
        if let Some(t) = ic_doc.get("dev-dependencies").and_then(|i| i.as_table()) {
            merge_deps(&mut doc, "dev-dependencies", t, &internal_set)?;
        }
        if let Some(t) = ic_doc.get("features").and_then(|i| i.as_table()) {
            merge_features(&mut doc, t)?;
        }
    }

    // Canonical [lib] path
    if let Some(lib) = doc.get_mut("lib").and_then(|i| i.as_table_mut()) {
        lib["path"] = toml_edit::value("src/lib.rs");
    }

    Ok(doc.to_string())
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

fn drop_internal_deps(doc: &mut DocumentMut, table: &str, internal_set: &BTreeSet<String>) {
    let Some(deps) = doc.get_mut(table).and_then(|i| i.as_table_mut()) else {
        return;
    };
    let to_remove: Vec<String> = deps
        .iter()
        .filter_map(|(k, _)| {
            if internal_set.contains(k) {
                Some(k.to_string())
            } else {
                None
            }
        })
        .collect();
    for k in to_remove {
        deps.remove(&k);
    }
}

fn merge_deps(
    doc: &mut DocumentMut,
    table_name: &str,
    incoming: &Table,
    internal_set: &BTreeSet<String>,
) -> Result<()> {
    if !doc.contains_key(table_name) {
        doc.insert(table_name, Item::Table(Table::new()));
    }
    for (name, value) in incoming.iter() {
        if internal_set.contains(name) {
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
            target.insert(
                name,
                Item::Value(
                    value
                        .as_value()
                        .cloned()
                        .unwrap_or_else(|| Value::String(Formatted::new(String::new()))),
                ),
            );
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
