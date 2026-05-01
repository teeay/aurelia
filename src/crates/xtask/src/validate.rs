// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use anyhow::{bail, Result};
use std::path::Path;
use std::process::Command;

pub fn run(publish_root: &Path, check_only: bool) -> Result<()> {
    let steps: Vec<(&str, Vec<&str>)> = if check_only {
        vec![
            ("build", vec!["build"]),
            ("dry-run", vec!["publish", "--dry-run", "--allow-dirty"]),
        ]
    } else {
        vec![
            ("fmt-check", vec!["fmt", "--", "--check"]),
            ("build", vec!["build"]),
            ("test", vec!["test"]),
            (
                "clippy",
                vec![
                    "clippy",
                    "--all-targets",
                    "--all-features",
                    "--",
                    "-D",
                    "warnings",
                ],
            ),
            ("dry-run", vec!["publish", "--dry-run", "--allow-dirty"]),
        ]
    };

    for (label, args) in steps {
        eprintln!("publish-tree: running cargo {}", args.join(" "));
        let status = Command::new("cargo")
            .args(&args)
            .current_dir(publish_root)
            .status()?;
        if !status.success() {
            bail!("publish-tree validation failed at step `{}`", label);
        }
    }
    Ok(())
}
