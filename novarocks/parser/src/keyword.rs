// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! MySQL/StarRocks keyword classification for the parser foundation.

use crate::Keyword;

/// Whether a recognized keyword is reserved in the initial parser dialect.
///
/// The lexer records only [`Keyword`] in a token. This classification is kept
/// separately so grammar families can decide which non-reserved words they
/// accept as identifiers without changing lexical recognition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeywordClass {
    Reserved,
    NonReserved,
}

/// Looks up a keyword using ASCII-only case folding.
///
/// Identifiers retain their original source spelling; only the comparison here
/// is case-insensitive.
pub fn lookup(word: &str) -> Option<Keyword> {
    KEYWORDS
        .iter()
        .find(|(spelling, _)| word.eq_ignore_ascii_case(spelling))
        .map(|(_, keyword)| *keyword)
}

/// Returns the reservedness recorded for a recognized keyword.
pub const fn class(keyword: Keyword) -> KeywordClass {
    match keyword {
        Keyword::Add
        | Keyword::Alter
        | Keyword::Analyze
        | Keyword::And
        | Keyword::As
        | Keyword::Call
        | Keyword::Cancel
        | Keyword::Create
        | Keyword::Drop
        | Keyword::Explain
        | Keyword::False
        | Keyword::From
        | Keyword::Kill
        | Keyword::Not
        | Keyword::Null
        | Keyword::Or
        | Keyword::Show
        | Keyword::True
        | Keyword::Truncate => KeywordClass::Reserved,
        _ => KeywordClass::NonReserved,
    }
}

const KEYWORDS: &[(&str, Keyword)] = &[
    ("ADD", Keyword::Add),
    ("ALTER", Keyword::Alter),
    ("ANALYZE", Keyword::Analyze),
    ("AND", Keyword::And),
    ("AS", Keyword::As),
    ("ASYNC", Keyword::Async),
    ("BACKEND", Keyword::Backend),
    ("BACKENDS", Keyword::Backends),
    ("BASIC", Keyword::Basic),
    ("BRANCH", Keyword::Branch),
    ("BUCKETS", Keyword::Buckets),
    ("BY", Keyword::By),
    ("CALL", Keyword::Call),
    ("CANCEL", Keyword::Cancel),
    ("CATALOG", Keyword::Catalog),
    ("COLUMN", Keyword::Column),
    ("COLUMNS", Keyword::Columns),
    ("COMMENT", Keyword::Comment),
    ("COSTS", Keyword::Costs),
    ("CREATE", Keyword::Create),
    ("DATABASE", Keyword::Database),
    ("DEFAULT", Keyword::Default),
    ("DROP", Keyword::Drop),
    ("EXISTS", Keyword::Exists),
    ("EXPIRE", Keyword::Expire),
    ("EXPLAIN", Keyword::Explain),
    ("EXTERNAL", Keyword::External),
    ("FALSE", Keyword::False),
    ("FILES", Keyword::Files),
    ("FORCE", Keyword::Force),
    ("FROM", Keyword::From),
    ("FULL", Keyword::Full),
    ("HISTOGRAM", Keyword::Histogram),
    ("IF", Keyword::If),
    ("JOBS", Keyword::Jobs),
    ("KILL", Keyword::Kill),
    ("MANIFESTS", Keyword::Manifests),
    ("MATERIALIZED", Keyword::Materialized),
    ("META", Keyword::Meta),
    ("NOT", Keyword::Not),
    ("NULL", Keyword::Null),
    ("OR", Keyword::Or),
    ("ORPHAN", Keyword::Orphan),
    ("PARTITION", Keyword::Partition),
    ("PROPERTIES", Keyword::Properties),
    ("REFRESH", Keyword::Refresh),
    ("REMOVE", Keyword::Remove),
    ("REWRITE", Keyword::Rewrite),
    ("SAMPLE", Keyword::Sample),
    ("SET", Keyword::Set),
    ("SHOW", Keyword::Show),
    ("SNAPSHOTS", Keyword::Snapshots),
    ("STATS", Keyword::Stats),
    ("SYNC", Keyword::Sync),
    ("TABLE", Keyword::Table),
    ("TABLES", Keyword::Tables),
    ("TAG", Keyword::Tag),
    ("TO", Keyword::To),
    ("TRUE", Keyword::True),
    ("TRUNCATE", Keyword::Truncate),
    ("UNSET", Keyword::Unset),
    ("VERBOSE", Keyword::Verbose),
    ("VIEW", Keyword::View),
    ("VIEWS", Keyword::Views),
    ("WITH", Keyword::With),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_is_ascii_case_insensitive_without_normalizing_identifiers() {
        assert_eq!(lookup("sHoW"), Some(Keyword::Show));
        assert_eq!(lookup("backends"), Some(Keyword::Backends));
        assert_eq!(lookup("backends_x"), None);
        assert_eq!(class(Keyword::Backends), KeywordClass::NonReserved);
        assert_eq!(class(Keyword::Show), KeywordClass::Reserved);
    }
}
