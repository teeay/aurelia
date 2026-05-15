// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use anyhow::{anyhow, bail, Context, Result};
use cargo_metadata::Metadata;
use serde::Deserialize;
use std::collections::BTreeSet;

#[derive(Debug, Clone)]
pub struct PublishConfig {
    pub target_crate: String,
    pub internal_crates: Vec<InternalCrate>,
    pub excluded_crates: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct InternalCrate {
    pub name: String,
    pub module: String,
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    target_crate: String,
    internal_crates: Vec<RawInternalCrate>,
    #[serde(default)]
    excluded_crates: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RawInternalCrate {
    name: String,
    #[serde(default)]
    module: Option<String>,
}

impl PublishConfig {
    pub fn load(metadata: &Metadata) -> Result<Self> {
        let raw_value = metadata
            .workspace_metadata
            .get("aurelia-publish")
            .ok_or_else(|| {
                anyhow!("workspace.metadata.aurelia-publish missing from root Cargo.toml")
            })?;
        let raw: RawConfig = serde_json::from_value(raw_value.clone())
            .context("failed to parse [workspace.metadata.aurelia-publish]")?;

        let internal_crates: Vec<InternalCrate> = raw
            .internal_crates
            .into_iter()
            .map(|r| {
                let module = r.module.unwrap_or_else(|| default_module(&r.name));
                InternalCrate {
                    name: r.name,
                    module,
                }
            })
            .collect();

        let cfg = PublishConfig {
            target_crate: raw.target_crate,
            internal_crates,
            excluded_crates: raw.excluded_crates,
        };
        cfg.validate(metadata)?;
        Ok(cfg)
    }

    fn validate(&self, metadata: &Metadata) -> Result<()> {
        // Module name uniqueness
        let mut seen = BTreeSet::new();
        for c in &self.internal_crates {
            if !seen.insert(c.module.clone()) {
                bail!(
                    "module name `{}` is used by more than one internal crate",
                    c.module
                );
            }
        }

        // Workspace member set
        let workspace_members: Vec<String> = metadata
            .workspace_packages()
            .into_iter()
            .map(|p| p.name.to_string())
            .collect();

        if !workspace_members.iter().any(|m| m == &self.target_crate) {
            bail!(
                "target_crate `{}` is not a workspace member",
                self.target_crate
            );
        }
        validate_excluded_crates(&self.excluded_crates, &workspace_members)?;

        for ic in &self.internal_crates {
            let pkg = metadata
                .workspace_packages()
                .into_iter()
                .find(|p| p.name.as_str() == ic.name.as_str())
                .ok_or_else(|| anyhow!("internal_crate `{}` is not a workspace member", ic.name))?;
            if pkg.publish != Some(vec![]) {
                bail!("internal crate `{}` must be `publish = false`", ic.name);
            }
        }

        let known: BTreeSet<&str> = std::iter::once(self.target_crate.as_str())
            .chain(self.internal_crates.iter().map(|c| c.name.as_str()))
            .chain(self.excluded_crates.iter().map(|s| s.as_str()))
            .collect();
        for member in &workspace_members {
            if !known.contains(member.as_str()) {
                bail!(
                    "workspace member `{}` is not listed in `internal_crates` or `excluded_crates`",
                    member
                );
            }
        }
        Ok(())
    }
}

fn validate_excluded_crates(
    excluded_crates: &[String],
    workspace_members: &[String],
) -> Result<()> {
    for excluded in excluded_crates {
        if !workspace_members.iter().any(|member| member == excluded) {
            bail!(
                "excluded_crates entry `{}` is not a workspace member",
                excluded
            );
        }
    }
    Ok(())
}

pub fn default_module(crate_name: &str) -> String {
    crate_name
        .strip_prefix("aurelia-")
        .unwrap_or(crate_name)
        .replace('-', "_")
}

#[cfg(test)]
#[path = "tests/config.rs"]
mod tests;
