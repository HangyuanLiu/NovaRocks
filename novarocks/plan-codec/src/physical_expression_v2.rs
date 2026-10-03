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

//! Borrowed definition projection from the original expression owner. This
//! component does not authenticate selected kernels or validate a full package.

use crate::{
    physical_binding_v2::BindingCodecError, physical_semantics_v2::SemanticsCodecError,
    physical_type_v2::TypeCodecError,
};
use novarocks_type_contract::CompileControlError;
use std::fmt;

mod kind;
mod namespace;

pub use namespace::{
    ExpressionNamespaceWriteFacts, ExpressionProjectionLimits, ExpressionTypeIds,
    PreparedExpressionNamespaceWrite, encode_expression_definitions,
    prepare_expression_definitions,
};

#[derive(Debug)]
pub enum ExpressionCodecError {
    InvalidShape(&'static str),
    Control(CompileControlError),
    Type(TypeCodecError),
    Semantics(SemanticsCodecError),
    Binding(BindingCodecError),
}
impl fmt::Display for ExpressionCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidShape(message) => f.write_str(message),
            Self::Control(error) => error.fmt(f),
            Self::Type(error) => error.fmt(f),
            Self::Semantics(error) => error.fmt(f),
            Self::Binding(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for ExpressionCodecError {}
impl From<CompileControlError> for ExpressionCodecError {
    fn from(value: CompileControlError) -> Self {
        Self::Control(value)
    }
}
impl From<TypeCodecError> for ExpressionCodecError {
    fn from(value: TypeCodecError) -> Self {
        match value {
            TypeCodecError::Control(error) => Self::Control(error),
            value => Self::Type(value),
        }
    }
}
impl From<SemanticsCodecError> for ExpressionCodecError {
    fn from(value: SemanticsCodecError) -> Self {
        match value {
            SemanticsCodecError::Control(error) => Self::Control(error),
            value => Self::Semantics(value),
        }
    }
}
impl From<BindingCodecError> for ExpressionCodecError {
    fn from(value: BindingCodecError) -> Self {
        match value {
            BindingCodecError::Control(error) => Self::Control(error),
            value => Self::Binding(value),
        }
    }
}

/// Internal IDs already bound to this definition's original immutable owners.
/// The kind leaf never chooses these IDs or authors another type namespace.
struct PreparedExpressionIds<'a> {
    root_carrier_type_id: Option<u32>,
    lambda_parameter_type_ids: &'a [u32],
    function_binding_id: Option<u32>,
    aggregate_binding_id: Option<u32>,
}
