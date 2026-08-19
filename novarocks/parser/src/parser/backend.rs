// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Backend-membership statement grammar.

use crate::{ParseError, ast::Statement};

use super::{StatementParser, show_backends};

pub(super) fn parse(parser: &mut StatementParser<'_, '_>) -> Result<Option<Statement>, ParseError> {
    if parser.current_is_word("SHOW")
        && !["ANALYZE", "ALTER", "CREATE", "MATERIALIZED", "TABLE"]
            .iter()
            .any(|word| parser.peek_word(1, word))
    {
        return show_backends::parse(parser).map(Some);
    }
    Ok(None)
}
