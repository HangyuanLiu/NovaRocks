// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Strict, side-effect-free classifier for `ALTER TABLE … ADD FILES FROM …`.
//!
//! This deliberately does not participate in the legacy generic SQL parser.
//! The frontend calls it before choosing the ADD FILES application route: an
//! unrelated statement is `Ok(None)`, while input that has reached the ADD
//! FILES grammar is either a complete typed command or an error.  Errors are
//! intentionally static so a source location (which may carry credentials) is
//! never reflected into a client-visible parser error.

use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

use super::{StarRocksDialect, convert_object_name};
/// A fully parsed ADD FILES command.  Resolving one/two/three-part names and
/// invoking a provider remain application-layer responsibilities.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AddFilesCommand {
    /// Unresolved one/two/three-part SQL table name.  The public command does
    /// not expose parser-private AST types to the frontend application crate.
    pub table_parts: Vec<String>,
    pub location: String,
}

/// Classify one SQL string as ADD FILES without performing side effects.
///
/// Only `ALTER TABLE <one/two/three-part-name> ADD FILES FROM <quoted-string>`
/// is accepted.  A single final semicolon is permitted; any other remaining
/// token, including a second statement delimiter, is rejected.
pub fn classify_add_files(sql: &str) -> Result<Option<AddFilesCommand>, String> {
    let dialect = StarRocksDialect;
    let mut tokens = Vec::new();
    let tokenized = Tokenizer::new(&dialect, sql).tokenize_with_location_into_buf(&mut tokens);
    if tokenized.is_err() {
        return if add_files_preamble_tokens(&tokens) {
            Err(add_files_error("invalid SQL"))
        } else {
            Ok(None)
        };
    }

    let mut parser = Parser::new(&dialect)
        .try_with_sql(sql)
        .map_err(|_| add_files_error("invalid SQL"))?;

    if !consume_unquoted_word(&mut parser, "ALTER") || !consume_unquoted_word(&mut parser, "TABLE")
    {
        return Ok(None);
    }

    let raw_table = match parser.parse_object_name(false) {
        Ok(name) => name,
        Err(_) => {
            return if add_files_preamble_tokens(&tokens) {
                Err(add_files_error("invalid table name"))
            } else {
                Ok(None)
            };
        }
    };

    if !consume_unquoted_word(&mut parser, "ADD") {
        return Ok(None);
    }
    if !consume_unquoted_word(&mut parser, "FILES") {
        return Ok(None);
    }

    let table =
        convert_object_name(raw_table).map_err(|_| add_files_error("invalid table name"))?;
    if !(1..=3).contains(&table.parts.len()) {
        return Err(add_files_error("table name must have one to three parts"));
    }

    if !consume_unquoted_word(&mut parser, "FROM") {
        return Err(add_files_error("requires FROM <location>"));
    }

    let location = match parser.next_token().token {
        Token::SingleQuotedString(location) | Token::DoubleQuotedString(location)
            if !location.is_empty() =>
        {
            location
        }
        _ => return Err(add_files_error("requires a quoted non-empty location")),
    };

    if parser.consume_token(&Token::SemiColon) {
        if parser.peek_token().token != Token::EOF {
            return Err(add_files_error("must contain exactly one statement"));
        }
    } else if parser.peek_token().token != Token::EOF {
        return Err(add_files_error("unexpected trailing token"));
    }

    Ok(Some(AddFilesCommand {
        table_parts: table.parts,
        location,
    }))
}

fn consume_unquoted_word(parser: &mut Parser<'_>, expected: &str) -> bool {
    let token = parser.peek_token();
    let Token::Word(word) = &token.token else {
        return false;
    };
    if word.quote_style.is_some() || !word.value.eq_ignore_ascii_case(expected) {
        return false;
    }
    parser.next_token();
    true
}

/// Tokenization can fail after a valid ADD FILES prefix (for example, an
/// unterminated source literal).  The tokenizer leaves successfully scanned
/// tokens in the supplied buffer, which is enough to classify that input as a
/// malformed ADD FILES statement without inspecting raw string contents.
fn add_files_preamble_tokens(tokens: &[sqlparser::tokenizer::TokenWithSpan]) -> bool {
    let significant = tokens
        .iter()
        .filter_map(|token| match &token.token {
            Token::Whitespace(_) => None,
            token => Some(token),
        })
        .collect::<Vec<_>>();

    if significant.len() < 2
        || !unquoted_word_eq(significant[0], "ALTER")
        || !unquoted_word_eq(significant[1], "TABLE")
    {
        return false;
    }

    significant
        .windows(2)
        .any(|pair| unquoted_word_eq(pair[0], "ADD") && unquoted_word_eq(pair[1], "FILES"))
}

fn unquoted_word_eq(token: &Token, expected: &str) -> bool {
    matches!(token, Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected))
}

fn add_files_error(detail: &str) -> String {
    format!("ADD FILES: {detail}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command(sql: &str) -> AddFilesCommand {
        classify_add_files(sql)
            .expect("classifier must not fail")
            .expect("expected ADD FILES command")
    }

    #[test]
    fn classifies_one_two_and_three_part_names() {
        for (sql, parts) in [
            ("ALTER TABLE t ADD FILES FROM 's3://bucket/path'", vec!["t"]),
            (
                "ALTER TABLE db.t ADD FILES FROM 's3://bucket/path'",
                vec!["db", "t"],
            ),
            (
                "ALTER TABLE cat.db.t ADD FILES FROM 's3://bucket/path'",
                vec!["cat", "db", "t"],
            ),
        ] {
            let parsed = command(sql);
            assert_eq!(parsed.table_parts, parts);
            assert_eq!(parsed.location, "s3://bucket/path");
        }
    }

    #[test]
    fn accepts_quoted_identifiers_comments_whitespace_and_double_quoted_location() {
        let parsed = command(
            " /* leading */ ALTER /* a */ TABLE `cat`.`db`.`t`\n             ADD /* b */ FILES FROM \"s3://bucket/a path\" ; -- final\n",
        );
        assert_eq!(parsed.table_parts, vec!["cat", "db", "t"]);
        assert_eq!(parsed.location, "s3://bucket/a path");
    }

    #[test]
    fn non_target_statements_return_none() {
        for sql in [
            "SELECT 'ALTER TABLE t ADD FILES FROM \\'s3://bucket/path\\''",
            "ALTER TABLE t ADD COLUMN c INT",
            "ALTER TABLE t SET (x = 'ADD FILES FROM')",
            "ALTER TABLE t ADD FILE 's3://bucket/path'",
            "ALTER TABLE t ADD `FILES` FROM 's3://bucket/path'",
        ] {
            assert_eq!(classify_add_files(sql).unwrap(), None, "sql: {sql}");
        }
    }

    #[test]
    fn rejects_malformed_add_files_without_reflecting_location() {
        for sql in [
            "ALTER TABLE t ADD FILES",
            "ALTER TABLE t ADD FILES FROM",
            "ALTER TABLE t ADD FILES FROM s3://bucket/path",
            "ALTER TABLE a.b.c.d ADD FILES FROM 's3://bucket/path'",
            "ALTER TABLE t ADD FILES FROM 's3://secret/path' extra",
            "ALTER TABLE t ADD FILES FROM 's3://secret/path'; SELECT 1",
            "ALTER TABLE t ADD FILES FROM 'unterminated",
        ] {
            let error = classify_add_files(sql).expect_err("must reject malformed ADD FILES");
            assert!(error.starts_with("ADD FILES:"), "error: {error}");
            assert!(!error.contains("secret"), "error leaked location: {error}");
        }
    }
}
