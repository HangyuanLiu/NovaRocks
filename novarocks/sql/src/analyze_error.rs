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

//! SQL analyze-domain user errors and their stable descriptors.

use std::fmt;

use novarocks_parser::Span;
use novarocks_user_error::{
    ErrorCodeDescriptor, ErrorCodeId, ErrorCodeStatus, ErrorPhase, RetryClass, UserError,
};

const UNKNOWN_TABLE: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.unknown_table"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const UNKNOWN_COLUMN: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.unknown_column"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const UNKNOWN_FUNCTION: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.unknown_function"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const TYPE_MISMATCH: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.type_mismatch"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const INVALID_LITERAL: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.invalid_literal"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const INVALID_ARGUMENT: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.invalid_argument"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const INVALID_QUERY_SHAPE: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.invalid_query_shape"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const UNSUPPORTED_EXPRESSION: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.unsupported_expression"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const UNSUPPORTED_QUERY_SHAPE: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.unsupported_query_shape"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const INTERNAL: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.internal"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};

/// All SQL analyze-domain descriptors. The independent manifest tool is their
/// only permitted aggregate owner.
pub const ERROR_CODE_DESCRIPTORS: &[ErrorCodeDescriptor] = &[
    UNKNOWN_TABLE,
    UNKNOWN_COLUMN,
    UNKNOWN_FUNCTION,
    TYPE_MISMATCH,
    INVALID_LITERAL,
    INVALID_ARGUMENT,
    INVALID_QUERY_SHAPE,
    UNSUPPORTED_EXPRESSION,
    UNSUPPORTED_QUERY_SHAPE,
    INTERNAL,
];

/// The semantic category owned by SQL analysis.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnalyzeErrorKind {
    UnknownTable,
    UnknownColumn,
    UnknownFunction,
    TypeMismatch,
    InvalidLiteral,
    InvalidArgument,
    InvalidQueryShape,
    UnsupportedExpression,
    UnsupportedQueryShape,
    Internal,
}

impl AnalyzeErrorKind {
    /// Returns the sole descriptor for this semantic category.
    pub const fn descriptor(self) -> ErrorCodeDescriptor {
        match self {
            Self::UnknownTable => UNKNOWN_TABLE,
            Self::UnknownColumn => UNKNOWN_COLUMN,
            Self::UnknownFunction => UNKNOWN_FUNCTION,
            Self::TypeMismatch => TYPE_MISMATCH,
            Self::InvalidLiteral => INVALID_LITERAL,
            Self::InvalidArgument => INVALID_ARGUMENT,
            Self::InvalidQueryShape => INVALID_QUERY_SHAPE,
            Self::UnsupportedExpression => UNSUPPORTED_EXPRESSION,
            Self::UnsupportedQueryShape => UNSUPPORTED_QUERY_SHAPE,
            Self::Internal => INTERNAL,
        }
    }
}

/// A user-visible SQL analysis failure.
///
/// Direct failures at user AST nodes carry their parser-owned span. Synthetic
/// post-analysis invariants use `None`; callers must not fabricate a span.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyzeError {
    kind: AnalyzeErrorKind,
    message: String,
    span: Option<Span>,
}

impl AnalyzeError {
    fn at(kind: AnalyzeErrorKind, message: impl Into<String>, span: Span) -> Self {
        Self {
            kind,
            message: message.into(),
            span: Some(span),
        }
    }

    pub fn unknown_table(message: impl Into<String>, span: Span) -> Self {
        Self::at(AnalyzeErrorKind::UnknownTable, message, span)
    }

    pub fn unknown_column(message: impl Into<String>, span: Span) -> Self {
        Self::at(AnalyzeErrorKind::UnknownColumn, message, span)
    }

    pub fn unknown_function(message: impl Into<String>, span: Span) -> Self {
        Self::at(AnalyzeErrorKind::UnknownFunction, message, span)
    }

    pub fn type_mismatch(message: impl Into<String>, span: Span) -> Self {
        Self::at(AnalyzeErrorKind::TypeMismatch, message, span)
    }

    pub fn invalid_literal(message: impl Into<String>, span: Span) -> Self {
        Self::at(AnalyzeErrorKind::InvalidLiteral, message, span)
    }

    pub fn invalid_argument(message: impl Into<String>, span: Span) -> Self {
        Self::at(AnalyzeErrorKind::InvalidArgument, message, span)
    }

    pub fn invalid_query_shape(message: impl Into<String>, span: Span) -> Self {
        Self::at(AnalyzeErrorKind::InvalidQueryShape, message, span)
    }

    pub fn unsupported_expression(message: impl Into<String>, span: Span) -> Self {
        Self::at(AnalyzeErrorKind::UnsupportedExpression, message, span)
    }

    pub fn unsupported_query_shape(message: impl Into<String>, span: Span) -> Self {
        Self::at(AnalyzeErrorKind::UnsupportedQueryShape, message, span)
    }

    /// Constructs a source-less internal failure. User-AST errors must use a
    /// semantic constructor above instead.
    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            kind: AnalyzeErrorKind::Internal,
            message: message.into(),
            span: None,
        }
    }

    pub const fn kind(&self) -> AnalyzeErrorKind {
        self.kind
    }

    pub const fn code(&self) -> ErrorCodeId {
        self.kind.descriptor().code
    }

    pub const fn span(&self) -> Option<Span> {
        self.span
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    /// Converts this domain error to its transport-neutral representation.
    /// A source-less caller preserves code and message but intentionally emits
    /// no user location.
    pub fn to_user_error(&self, source: Option<&str>) -> UserError {
        let location = self
            .span
            .zip(source)
            .map(|(span, source)| span.to_user_error_location(source));
        UserError::from_descriptor(
            self.kind.descriptor(),
            self.message.clone(),
            location,
            RetryClass::Never,
        )
    }
}

impl fmt::Display for AnalyzeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for AnalyzeError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptors_are_the_frozen_analyze_contract() {
        let codes = ERROR_CODE_DESCRIPTORS
            .iter()
            .map(|descriptor| descriptor.code.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            codes,
            vec![
                "sql.analyze.unknown_table",
                "sql.analyze.unknown_column",
                "sql.analyze.unknown_function",
                "sql.analyze.type_mismatch",
                "sql.analyze.invalid_literal",
                "sql.analyze.invalid_argument",
                "sql.analyze.invalid_query_shape",
                "sql.analyze.unsupported_expression",
                "sql.analyze.unsupported_query_shape",
                "sql.analyze.internal",
            ]
        );
        assert!(
            ERROR_CODE_DESCRIPTORS
                .iter()
                .all(|descriptor| descriptor.phase == ErrorPhase::Analyze)
        );
    }

    #[test]
    fn user_ast_error_preserves_span_and_location() {
        let error = AnalyzeError::unknown_column("unknown column x", Span::new(7, 8));
        assert_eq!(error.kind(), AnalyzeErrorKind::UnknownColumn);
        assert_eq!(error.code().as_str(), "sql.analyze.unknown_column");
        assert_eq!(error.span(), Some(Span::new(7, 8)));

        let user_error = error.to_user_error(Some("SELECT x"));
        assert_eq!(user_error.phase(), ErrorPhase::Analyze);
        assert_eq!(user_error.location().expect("location").column(), 8);
    }

    #[test]
    fn source_less_internal_error_has_no_location() {
        let error = AnalyzeError::internal("unexpected resolved query invariant");
        assert_eq!(error.kind(), AnalyzeErrorKind::Internal);
        assert_eq!(error.span(), None);
        assert!(error.to_user_error(Some("SELECT 1")).location().is_none());
    }
}
