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

use std::fmt;

use super::limits::PlanLimits;

/// How a validation failure must be routed by whoever receives it.
///
/// The categories do not rank severity. They record *who is at fault*, which is
/// the only thing that tells a consumer what to do next, and which a prose
/// message cannot carry without being re-parsed at every call site.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ValidationErrorCategory {
    /// The plan violates an invariant of the contract itself. A correct
    /// producer cannot emit one, so a worker observing this category has proof
    /// of a producer defect. It is never a statement about capability or
    /// capacity, and retrying elsewhere cannot help.
    StructuralInvariant,
    /// The plan is well formed but names something this target cannot honour.
    /// This is scheduling information: a different target may accept the very
    /// same plan unchanged.
    UnsupportedCapability,
    /// The plan is well formed and supported but exceeds a declared structural
    /// limit. This is admission information. It never implies the plan is
    /// wrong, and it is the category an operator raises a limit to clear.
    ResourceLimit,
}

impl ValidationErrorCategory {
    /// Stable lowercase token for logs and typed assertions.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StructuralInvariant => "structural-invariant",
            Self::UnsupportedCapability => "unsupported-capability",
            Self::ResourceLimit => "resource-limit",
        }
    }
}

impl fmt::Display for ValidationErrorCategory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationError {
    category: ValidationErrorCategory,
    path: Box<str>,
    message: Box<str>,
}

impl ValidationError {
    /// A violated contract invariant. This is the default because the vast
    /// majority of checks prove structure; capability and capacity failures are
    /// the ones that must say so explicitly.
    pub(crate) fn new(path: impl AsRef<str>, message: impl AsRef<str>) -> Self {
        Self::categorized(ValidationErrorCategory::StructuralInvariant, path, message)
    }

    /// A declared structural limit was exceeded. Every limit check reports this
    /// category so admission and backpressure can be driven without parsing the
    /// message.
    pub(crate) fn resource_limit(path: impl AsRef<str>, message: impl AsRef<str>) -> Self {
        Self::categorized(ValidationErrorCategory::ResourceLimit, path, message)
    }

    /// The target cannot honour something the plan legitimately names.
    #[expect(
        dead_code,
        reason = "consumed once T07-P lowers provider capability refusals"
    )]
    pub(crate) fn unsupported_capability(path: impl AsRef<str>, message: impl AsRef<str>) -> Self {
        Self::categorized(
            ValidationErrorCategory::UnsupportedCapability,
            path,
            message,
        )
    }

    pub(crate) fn categorized(
        category: ValidationErrorCategory,
        path: impl AsRef<str>,
        message: impl AsRef<str>,
    ) -> Self {
        Self {
            category,
            path: path.as_ref().into(),
            message: message.as_ref().into(),
        }
    }

    pub const fn category(&self) -> ValidationErrorCategory {
        self.category
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "[{}] {}: {}",
            self.category, self.path, self.message
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationErrors(Box<[ValidationError]>);

pub const MAX_VALIDATION_ERRORS: usize = 128;

/// Everything a check needs besides the plan itself: where to report a
/// failure, and the bounds it is being validated against.
///
/// Limits travel with the error sink because that is the one value already
/// threaded through every check. It also keeps the two halves of a limit
/// decision together: the bound that was applied, and the refusal it produced.
/// A `ResourceLimit` error therefore cannot be raised against a number the
/// caller did not supply.
#[derive(Default)]
pub(crate) struct ValidationContext {
    pub(crate) errors: Vec<ValidationError>,
    pub(crate) truncated: bool,
    pub(crate) limits: PlanLimits,
}

impl ValidationContext {
    pub(crate) const fn new() -> Self {
        Self::with_limits(PlanLimits::FROZEN)
    }

    pub(crate) const fn with_limits(limits: PlanLimits) -> Self {
        Self {
            errors: Vec::new(),
            truncated: false,
            limits,
        }
    }

    pub(crate) const fn limits(&self) -> &PlanLimits {
        &self.limits
    }

    pub(crate) fn push(&mut self, error: ValidationError) {
        if self.errors.len() < MAX_VALIDATION_ERRORS {
            self.errors.push(error);
        } else {
            self.mark_truncated();
        }
    }

    pub(crate) fn is_saturated(&self) -> bool {
        self.errors.len() >= MAX_VALIDATION_ERRORS
    }

    pub(crate) fn mark_truncated(&mut self) {
        if !self.truncated {
            self.truncated = true;
            self.errors.push(ValidationError::new(
                "validation",
                "additional validation errors were truncated",
            ));
        }
    }

    pub(crate) fn into_vec(self) -> Vec<ValidationError> {
        self.errors
    }
}

impl std::ops::Deref for ValidationContext {
    type Target = Vec<ValidationError>;

    fn deref(&self) -> &Self::Target {
        &self.errors
    }
}

impl ValidationErrors {
    pub fn errors(&self) -> &[ValidationError] {
        &self.0
    }

    /// True when at least one error carries `category`.
    pub fn has(&self, category: ValidationErrorCategory) -> bool {
        self.0.iter().any(|error| error.category() == category)
    }

    /// True when every error is a violated contract invariant. A worker seeing
    /// only this category has proof of a producer defect: no other target and
    /// no raised limit can make the same plan succeed, so it must not be
    /// reported as a capability or capacity refusal.
    pub fn is_producer_defect(&self) -> bool {
        !self.0.is_empty()
            && self
                .0
                .iter()
                .all(|error| error.category() == ValidationErrorCategory::StructuralInvariant)
    }

    pub(crate) fn from_collector(errors: ValidationContext) -> Self {
        Self(errors.into_vec().into_boxed_slice())
    }
}

impl fmt::Display for ValidationErrors {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "physical plan has {} validation error(s)",
            self.0.len()
        )?;
        for error in &self.0 {
            write!(formatter, "; {error}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ValidationErrors {}
