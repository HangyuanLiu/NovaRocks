// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Shared syntax-only command values.

use crate::{
    Span,
    ast::{Ident, Literal},
};

/// A command property key and value retaining their source spans.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PropertyKeyValue {
    pub key: Ident,
    pub value: Literal,
    pub span: Span,
}

/// A property key that can appear where no value is syntactically required.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Property {
    pub key: Ident,
    pub span: Span,
}
