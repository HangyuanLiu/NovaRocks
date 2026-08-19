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
    if word.eq_ignore_ascii_case("AS") {
        Some(Keyword::As)
    } else if word.eq_ignore_ascii_case("BACKENDS") {
        Some(Keyword::Backends)
    } else if word.eq_ignore_ascii_case("FALSE") {
        Some(Keyword::False)
    } else if word.eq_ignore_ascii_case("FROM") {
        Some(Keyword::From)
    } else if word.eq_ignore_ascii_case("SHOW") {
        Some(Keyword::Show)
    } else if word.eq_ignore_ascii_case("TRUE") {
        Some(Keyword::True)
    } else {
        None
    }
}

/// Returns the reservedness recorded for a recognized keyword.
pub const fn class(keyword: Keyword) -> KeywordClass {
    match keyword {
        Keyword::Backends => KeywordClass::NonReserved,
        Keyword::As | Keyword::False | Keyword::From | Keyword::Show | Keyword::True => {
            KeywordClass::Reserved
        }
    }
}

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
