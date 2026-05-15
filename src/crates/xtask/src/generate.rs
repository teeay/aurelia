// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use crate::config::{InternalCrate, PublishConfig};
use crate::manifest;
use crate::rewrite::Rewriter;
use anyhow::{anyhow, Result};
use cargo_metadata::Metadata;
use regex::Regex;
use std::path::Path;
use walkdir::WalkDir;

pub fn regenerate(
    metadata: &Metadata,
    cfg: &PublishConfig,
    publish_root: &Path,
    keep: bool,
) -> Result<()> {
    if !keep && publish_root.exists() {
        fs_err::remove_dir_all(publish_root)?;
    }
    fs_err::create_dir_all(publish_root.join("src"))?;

    let rewriter = build_rewriter(cfg)?;
    let macro_regex = build_macro_regex(cfg)?;

    // Target crate sources → publish/<target>/src/
    let target_pkg = metadata
        .workspace_packages()
        .into_iter()
        .find(|p| p.name.as_str() == cfg.target_crate.as_str())
        .ok_or_else(|| anyhow!("target crate not found"))?;
    let target_manifest_dir = target_pkg
        .manifest_path
        .parent()
        .unwrap()
        .as_std_path()
        .to_path_buf();
    ensure_supported_crate_root(&target_pkg.name, &target_manifest_dir)?;
    let target_src = target_pkg
        .manifest_path
        .parent()
        .unwrap()
        .join("src")
        .into_std_path_buf();
    copy_rewriting(
        &target_src,
        &publish_root.join("src"),
        &rewriter,
        &macro_regex,
        None,
    )?;

    // Each internal crate → publish/<target>/src/<module>/
    for ic in &cfg.internal_crates {
        let pkg = metadata
            .workspace_packages()
            .into_iter()
            .find(|p| p.name.as_str() == ic.name.as_str())
            .ok_or_else(|| anyhow!("internal crate `{}` not found", ic.name))?;
        let manifest_dir = pkg
            .manifest_path
            .parent()
            .unwrap()
            .as_std_path()
            .to_path_buf();
        ensure_supported_crate_root(&pkg.name, &manifest_dir)?;
        let src = pkg
            .manifest_path
            .parent()
            .unwrap()
            .join("src")
            .into_std_path_buf();
        let dest = publish_root.join("src").join(&ic.module);
        copy_rewriting(&src, &dest, &rewriter, &macro_regex, Some(&ic.module))?;
        // Internal crate's `lib.rs` becomes the module's `mod.rs`.
        let lib = dest.join("lib.rs");
        if lib.exists() {
            fs_err::rename(&lib, dest.join("mod.rs"))?;
        }
        // Suppress lints that fire only because the internal crate is now
        // a private module: re-exports and pub items that fed the source
        // crate's external API have no external consumer in the merged
        // form, and a `mod foo` inside `mod foo` becomes module_inception.
        prepend_inner_allow(&dest.join("mod.rs"))?;
    }

    inject_mod_declarations(&publish_root.join("src/lib.rs"), &cfg.internal_crates)?;

    // LICENSE / NOTICE / README
    let workspace_root = metadata.workspace_root.as_std_path();
    for f in ["LICENSE", "NOTICE", "README.md"] {
        let src = workspace_root.join(f);
        if src.exists() {
            fs_err::copy(&src, publish_root.join(f))?;
        }
    }

    // Cargo.toml
    let manifest_str = manifest::synthesize(metadata, cfg)?;
    fs_err::write(publish_root.join("Cargo.toml"), manifest_str)?;

    // Auto-format the merged tree so subsequent fmt-check passes are
    // meaningful: any drift after this point is a real regression.
    let status = std::process::Command::new("cargo")
        .args(["fmt"])
        .current_dir(publish_root)
        .status()?;
    if !status.success() {
        anyhow::bail!("cargo fmt failed inside publish tree");
    }

    Ok(())
}

fn build_rewriter(cfg: &PublishConfig) -> Result<Rewriter> {
    let pairs: Vec<(String, String)> = cfg
        .internal_crates
        .iter()
        .map(|ic| {
            let ident = ic.name.replace('-', "_");
            let replacement = format!("crate::{}", ic.module);
            (ident, replacement)
        })
        .collect();
    Rewriter::new(&pairs)
}

fn build_macro_regex(cfg: &PublishConfig) -> Result<Regex> {
    let alternation = cfg
        .internal_crates
        .iter()
        .map(|ic| regex::escape(&ic.name.replace('-', "_")))
        .collect::<Vec<_>>()
        .join("|");
    let pattern = format!(r"\b(?:{})::(?P<ident>[A-Za-z_][A-Za-z0-9_]*)!", alternation);
    Ok(Regex::new(&pattern)?)
}

fn copy_rewriting(
    src_dir: &Path,
    dest_dir: &Path,
    rewriter: &Rewriter,
    macro_regex: &Regex,
    self_module: Option<&str>,
) -> Result<()> {
    fs_err::create_dir_all(dest_dir)?;
    for entry in WalkDir::new(src_dir) {
        let entry = entry?;
        let rel = entry.path().strip_prefix(src_dir).unwrap();
        let dest = dest_dir.join(rel);
        if entry.file_type().is_dir() {
            fs_err::create_dir_all(&dest)?;
        } else if entry.file_type().is_file() {
            if entry.path().extension().and_then(|s| s.to_str()) == Some("rs") {
                let content = fs_err::read_to_string(entry.path())?;
                // Stage 1: internal-crate files retarget bare `crate::` to
                // their new sub-module path. Run first so subsequent rewrites
                // do not double-prefix `crate::` paths produced by later
                // stages.
                let stage1 = match self_module {
                    Some(module) => retarget_crate_paths(&content, module),
                    None => content,
                };
                // Stage 2: macro invocations across configured internal crates
                // collapse to `crate::<macro>!(...)` because `#[macro_export]`
                // hoists them to the merged crate root.
                let stage2 = macro_regex
                    .replace_all(&stage1, "crate::$ident!")
                    .into_owned();
                // Stage 3: remaining cross-crate identifier rewrites.
                let rewritten = rewriter.rewrite(&stage2);
                fs_err::write(&dest, rewritten)?;
            } else {
                fs_err::copy(entry.path(), &dest)?;
            }
        }
    }
    Ok(())
}

fn ensure_supported_crate_root(crate_name: &str, manifest_dir: &Path) -> Result<()> {
    let build_rs = manifest_dir.join("build.rs");
    if build_rs.exists() {
        anyhow::bail!(
            "crate `{}` contains build.rs, which is outside the current publish-tree source inclusion contract",
            crate_name
        );
    }
    Ok(())
}

fn retarget_crate_paths(content: &str, module: &str) -> String {
    let mut out = String::with_capacity(content.len());
    let mut cursor = 0usize;
    while let Some(offset) = content[cursor..].find("crate::") {
        let start = cursor + offset;
        let after = start + "crate::".len();
        out.push_str(&content[cursor..start]);

        if is_retargetable_crate_path(content, start) && !is_root_macro_invocation(content, after) {
            out.push_str("crate::");
            out.push_str(module);
            out.push_str("::");
        } else {
            out.push_str("crate::");
        }
        cursor = after;
    }
    out.push_str(&content[cursor..]);
    out
}

fn is_retargetable_crate_path(content: &str, start: usize) -> bool {
    if start == 0 {
        return true;
    }
    let Some(previous) = content[..start].chars().next_back() else {
        return true;
    };
    previous != '$' && !previous.is_ascii_alphanumeric() && previous != '_'
}

fn is_root_macro_invocation(content: &str, mut cursor: usize) -> bool {
    let Some(first) = content[cursor..].chars().next() else {
        return false;
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return false;
    }
    cursor += first.len_utf8();
    while let Some(ch) = content[cursor..].chars().next() {
        if ch == '_' || ch.is_ascii_alphanumeric() {
            cursor += ch.len_utf8();
        } else {
            break;
        }
    }
    content[cursor..].starts_with('!')
}

fn inject_mod_declarations(lib_rs: &Path, crates: &[InternalCrate]) -> Result<()> {
    let content = fs_err::read_to_string(lib_rs)?;
    let mod_block: String = crates
        .iter()
        .map(|c| format!("mod {};\n", c.module))
        .collect();

    let split = rust_file_header_end(&content);
    let prefix = &content[..split];
    let suffix = &content[split..];

    let mut new = String::new();
    new.push_str(prefix);
    if !prefix.is_empty() && !prefix.ends_with('\n') {
        new.push('\n');
    }
    new.push_str(&mod_block);
    new.push('\n');
    new.push_str(suffix);

    fs_err::write(lib_rs, new)?;
    Ok(())
}

fn rust_file_header_end(content: &str) -> usize {
    let mut cursor = 0usize;
    loop {
        let line_start = cursor;
        while cursor < content.len() {
            let ch = content[cursor..].chars().next().unwrap();
            if ch.is_whitespace() && ch != '\n' {
                cursor += ch.len_utf8();
            } else {
                break;
            }
        }
        let trimmed_start = cursor;
        let rest = &content[trimmed_start..];

        if rest.is_empty() {
            return content.len();
        }
        if rest.starts_with('\n') {
            cursor = trimmed_start + 1;
            continue;
        }
        if rest.starts_with("//") {
            cursor = line_end_inclusive(content, trimmed_start);
            continue;
        }
        if rest.starts_with("/*!") {
            if let Some(end) = rest.find("*/") {
                cursor = trimmed_start + end + 2;
                cursor = consume_one_trailing_newline(content, cursor);
                continue;
            }
            return content.len();
        }
        if rest.starts_with("#![") {
            if let Some(end) = rest.find(']') {
                cursor = trimmed_start + end + 1;
                cursor = consume_one_trailing_newline(content, cursor);
                continue;
            }
            return line_start;
        }
        return line_start;
    }
}

fn line_end_inclusive(content: &str, start: usize) -> usize {
    match content[start..].find('\n') {
        Some(offset) => start + offset + 1,
        None => content.len(),
    }
}

fn consume_one_trailing_newline(content: &str, cursor: usize) -> usize {
    if content[cursor..].starts_with('\n') {
        cursor + 1
    } else {
        cursor
    }
}

/// Prepends `#![allow(...)]` inner attributes to a merged internal-crate
/// `mod.rs` so that lints which only fire because the crate is now a
/// private module of the published crate do not block validation.
fn prepend_inner_allow(mod_rs: &Path) -> Result<()> {
    const ALLOW_BLOCK: &str = "#![allow(unused_imports, dead_code, clippy::module_inception)]\n";
    let content = fs_err::read_to_string(mod_rs)?;
    let lines: Vec<&str> = content.split_inclusive('\n').collect();
    let mut idx = 0usize;
    while idx < lines.len() {
        let trimmed = lines[idx].trim_start();
        let is_line_comment = trimmed.starts_with("//");
        let is_blank = trimmed.is_empty() || trimmed == "\n";
        if is_line_comment || is_blank {
            idx += 1;
        } else {
            break;
        }
    }
    let prefix: String = lines[..idx].iter().copied().collect();
    let suffix: String = lines[idx..].iter().copied().collect();
    let mut new = String::new();
    new.push_str(&prefix);
    if !prefix.ends_with('\n') {
        new.push('\n');
    }
    new.push_str(ALLOW_BLOCK);
    new.push('\n');
    new.push_str(&suffix);
    fs_err::write(mod_rs, new)?;
    Ok(())
}

#[cfg(test)]
#[path = "tests/generate.rs"]
mod tests;
