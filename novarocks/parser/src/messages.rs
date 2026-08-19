// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

pub(crate) fn unexpected_character(character: char) -> String {
    format!("unexpected character `{character}`")
}

pub(crate) fn unterminated(kind: &str) -> String {
    format!("unterminated {kind}")
}

pub(crate) fn unexpected_token(expected: &str, found: &str) -> String {
    format!("expected {expected}, found {found}")
}

pub(crate) fn unsupported_statement(statement: &str) -> String {
    format!("recognized but unsupported statement {statement}")
}

pub(crate) fn invalid_structure(detail: &str) -> String {
    format!("invalid SQL structure: {detail}")
}
