// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use anyhow::{bail, Result};
use std::path::Path;
use std::process::Command;

pub fn run(publish_root: &Path, check_only: bool) -> Result<()> {
    let steps = validation_steps(check_only);

    for step in steps {
        let args = step.args;
        eprintln!("publish-tree: running cargo {}", args.join(" "));
        let status = Command::new("cargo")
            .args(&args)
            .current_dir(publish_root)
            .status()?;
        if !status.success() {
            bail!("publish-tree validation failed at step `{}`", step.label);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidationStep {
    pub(crate) label: &'static str,
    pub(crate) args: Vec<&'static str>,
}

pub(crate) fn validation_steps(check_only: bool) -> Vec<ValidationStep> {
    if check_only {
        return vec![
            ValidationStep {
                label: "build",
                args: vec!["build"],
            },
            ValidationStep {
                label: "dry-run",
                args: vec!["publish", "--dry-run", "--allow-dirty"],
            },
        ];
    }

    vec![
        ValidationStep {
            label: "fmt-check",
            args: vec!["fmt", "--", "--check"],
        },
        ValidationStep {
            label: "build",
            args: vec!["build"],
        },
        ValidationStep {
            label: "test",
            args: vec!["test", "--all-targets", "--all-features"],
        },
        ValidationStep {
            label: "clippy",
            args: vec![
                "clippy",
                "--all-targets",
                "--all-features",
                "--",
                "-D",
                "warnings",
            ],
        },
        ValidationStep {
            label: "dry-run",
            args: vec!["publish", "--dry-run", "--allow-dirty"],
        },
    ]
}

#[cfg(test)]
#[path = "tests/validate.rs"]
mod tests;
