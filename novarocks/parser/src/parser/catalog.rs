// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Catalog and truncate statement grammar.

use crate::{ParseError, ast::Statement};

use super::StatementParser;

pub(super) fn parse(_: &mut StatementParser<'_, '_>) -> Result<Option<Statement>, ParseError> {
    Ok(None)
}
