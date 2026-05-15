// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use clap::{Parser, Subcommand};

mod config;
mod generate;
mod manifest;
mod rewrite;
mod validate;

#[derive(Parser)]
#[command(about = "Aurelia workspace tooling")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Regenerate publish/<target_crate>/ and validate it.
    PublishTree(PublishTreeArgs),
}

#[derive(clap::Args)]
struct PublishTreeArgs {
    /// Skip the wipe step for debugging; stale files may remain.
    #[arg(long)]
    keep: bool,
    /// Run only build + cargo publish --dry-run, skipping fmt/test/clippy.
    #[arg(long)]
    check: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::PublishTree(args) => publish_tree(args),
    }
}

fn publish_tree(args: PublishTreeArgs) -> Result<()> {
    let metadata = cargo_metadata::MetadataCommand::new().exec()?;
    let cfg = config::PublishConfig::load(&metadata)?;
    let workspace_root = metadata.workspace_root.as_std_path().to_path_buf();
    let publish_root = workspace_root.join("publish").join(&cfg.target_crate);
    if args.keep && publish_root.exists() {
        eprintln!(
            "publish-tree: warning: --keep is set and {} already exists; stale files may remain",
            publish_root.display()
        );
    }

    generate::regenerate(&metadata, &cfg, &publish_root, args.keep)?;
    println!("publish-tree: regenerated {}", publish_root.display());

    validate::run(&publish_root, args.check)
}
