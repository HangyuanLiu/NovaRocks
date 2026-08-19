// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Transport-neutral data contracts for user-visible domain errors.
//!
//! This crate intentionally owns no domain semantics, source spans, protocol
//! mappings, diagnostics factories, or registry aggregation.

use std::fmt;

/// A stable, domain-owned identifier such as `sql.parse.unexpected_token`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ErrorCodeId(&'static str);

impl ErrorCodeId {
    /// Creates a stable error code identifier.
    pub const fn new(value: &'static str) -> Self {
        Self(value)
    }

    /// Returns the machine-readable identifier.
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for ErrorCodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

/// The owner phase in which a domain error is produced.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorPhase {
    Lex,
    Parse,
    Validate,
    Analyze,
    Admit,
}

/// Whether retrying the same user operation may be useful.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryClass {
    Never,
    Retryable,
}

/// Lifecycle state of a registered code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorCodeStatus {
    Active,
    Deprecated,
}

/// A transport-neutral entry emitted by a domain-owned code registry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ErrorCodeDescriptor {
    pub code: ErrorCodeId,
    pub phase: ErrorPhase,
    pub status: ErrorCodeStatus,
}

/// A 1-based source location using UTF-8 byte columns.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserErrorLocation {
    line: u32,
    column: u32,
    end_line: Option<u32>,
    end_column: Option<u32>,
}

impl UserErrorLocation {
    /// Creates a location. End coordinates must either both be present or both
    /// be absent.
    pub fn new(line: u32, column: u32, end_line: Option<u32>, end_column: Option<u32>) -> Self {
        assert_eq!(end_line.is_some(), end_column.is_some());
        Self {
            line,
            column,
            end_line,
            end_column,
        }
    }

    pub const fn line(&self) -> u32 {
        self.line
    }

    pub const fn column(&self) -> u32 {
        self.column
    }

    pub const fn end_line(&self) -> Option<u32> {
        self.end_line
    }

    pub const fn end_column(&self) -> Option<u32> {
        self.end_column
    }
}

/// A user-visible error whose code and phase always originate in one descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserError {
    code: ErrorCodeId,
    phase: ErrorPhase,
    location: Option<UserErrorLocation>,
    message: String,
    retry: RetryClass,
}

impl UserError {
    /// Creates an error by copying the code and phase from its descriptor.
    pub fn from_descriptor(
        descriptor: ErrorCodeDescriptor,
        message: impl Into<String>,
        location: Option<UserErrorLocation>,
        retry: RetryClass,
    ) -> Self {
        Self {
            code: descriptor.code,
            phase: descriptor.phase,
            location,
            message: message.into(),
            retry,
        }
    }

    pub const fn code(&self) -> ErrorCodeId {
        self.code
    }

    pub const fn phase(&self) -> ErrorPhase {
        self.phase
    }

    pub const fn location(&self) -> Option<&UserErrorLocation> {
        self.location.as_ref()
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub const fn retry(&self) -> RetryClass {
        self.retry
    }
}

impl fmt::Display for UserError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "[{}] {}", self.code, self.message)?;
        if let Some(location) = &self.location {
            write!(
                formatter,
                " at line {} column {}",
                location.line, location.column
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for UserError {}

#[cfg(test)]
mod tests {
    use super::*;

    const PARSE: ErrorCodeDescriptor = ErrorCodeDescriptor {
        code: ErrorCodeId::new("sql.parse.unexpected_token"),
        phase: ErrorPhase::Parse,
        status: ErrorCodeStatus::Active,
    };

    #[test]
    fn descriptor_is_the_only_constructor_for_code_and_phase() {
        let error = UserError::from_descriptor(
            PARSE,
            "unexpected token",
            Some(UserErrorLocation::new(2, 4, Some(2), Some(5))),
            RetryClass::Never,
        );

        assert_eq!(error.code().as_str(), "sql.parse.unexpected_token");
        assert_eq!(error.phase(), ErrorPhase::Parse);
        assert_eq!(
            error.to_string(),
            "[sql.parse.unexpected_token] unexpected token at line 2 column 4"
        );
    }

    #[test]
    #[should_panic]
    fn location_rejects_unpaired_end_coordinates() {
        let _ = UserErrorLocation::new(1, 1, Some(1), None);
    }
}
