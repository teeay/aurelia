// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use regex::{Captures, Regex};
use std::collections::HashMap;

pub struct Rewriter {
    regex: Option<Regex>,
    table: HashMap<String, String>,
}

impl Rewriter {
    pub fn new(pairs: &[(String, String)]) -> Result<Self> {
        if pairs.is_empty() {
            return Ok(Self {
                regex: None,
                table: HashMap::new(),
            });
        }
        let mut idents: Vec<&str> = pairs.iter().map(|(k, _)| k.as_str()).collect();
        idents.sort_by_key(|s| std::cmp::Reverse(s.len()));
        let alternation = idents
            .iter()
            .map(|s| regex::escape(s))
            .collect::<Vec<_>>()
            .join("|");
        let pattern = format!(r"\b({})\b", alternation);
        let regex = Regex::new(&pattern)?;
        let table = pairs.iter().cloned().collect();
        Ok(Self {
            regex: Some(regex),
            table,
        })
    }

    pub fn rewrite(&self, src: &str) -> String {
        match &self.regex {
            None => src.to_string(),
            Some(rx) => rx
                .replace_all(src, |caps: &Captures| {
                    self.table
                        .get(&caps[1])
                        .cloned()
                        .unwrap_or_else(|| caps[0].to_string())
                })
                .into_owned(),
        }
    }
}

#[cfg(test)]
#[path = "tests/rewrite.rs"]
mod tests;
