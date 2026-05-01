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
mod tests {
    use super::*;

    fn three() -> Rewriter {
        Rewriter::new(&[
            ("aurelia_ids".to_string(), "crate::ids".to_string()),
            ("aurelia_logging".to_string(), "crate::logging".to_string()),
            ("aurelia_peering".to_string(), "crate::peering".to_string()),
        ])
        .unwrap()
    }

    #[test]
    fn rewrites_bare_path() {
        assert_eq!(
            three().rewrite("aurelia_peering::Domus"),
            "crate::peering::Domus"
        );
    }

    #[test]
    fn rewrites_use_statement() {
        assert_eq!(
            three().rewrite("use aurelia_peering;"),
            "use crate::peering;"
        );
        assert_eq!(
            three().rewrite("use aurelia_peering::{A, B};"),
            "use crate::peering::{A, B};"
        );
    }

    #[test]
    fn rewrites_doc_comment() {
        assert_eq!(
            three().rewrite("/// See aurelia_peering for details"),
            "/// See crate::peering for details"
        );
    }

    #[test]
    fn does_not_rewrite_substring() {
        let r = three();
        assert_eq!(r.rewrite("aurelia_peeringx"), "aurelia_peeringx");
        assert_eq!(r.rewrite("xaurelia_peering"), "xaurelia_peering");
        assert_eq!(r.rewrite("foo_aurelia_peering"), "foo_aurelia_peering");
        assert_eq!(r.rewrite("aurelia_peering_extra"), "aurelia_peering_extra");
    }

    #[test]
    fn rewrites_all_three_crates() {
        let r = three();
        assert_eq!(r.rewrite("aurelia_ids::ErrorId"), "crate::ids::ErrorId");
        assert_eq!(r.rewrite("aurelia_logging::limit"), "crate::logging::limit");
        assert_eq!(r.rewrite("aurelia_peering::Domus"), "crate::peering::Domus");
    }

    #[test]
    fn empty_table_is_noop() {
        let r = Rewriter::new(&[]).unwrap();
        assert_eq!(r.rewrite("aurelia_peering::Foo"), "aurelia_peering::Foo");
    }

    #[test]
    fn single_crate_table() {
        let r = Rewriter::new(&[("aurelia_ids".to_string(), "crate::ids".to_string())]).unwrap();
        assert_eq!(
            r.rewrite("aurelia_ids::X aurelia_peering::Y"),
            "crate::ids::X aurelia_peering::Y"
        );
    }

    #[test]
    fn rewrites_string_literal() {
        assert_eq!(
            three().rewrite(r#"let s = "aurelia_peering";"#),
            r#"let s = "crate::peering";"#
        );
    }

    #[test]
    fn handles_multiple_occurrences_in_one_line() {
        assert_eq!(
            three().rewrite("aurelia_ids::A + aurelia_peering::B"),
            "crate::ids::A + crate::peering::B"
        );
    }
}
