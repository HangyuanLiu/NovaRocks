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

use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, ControlOwnedResourceFacts, ControlResourceCounter,
    ControlResourceError,
    owned_resources::vec::{boxed_slice_in, push_growth, reserve_for_push_in},
};

#[cfg(test)]
mod borrowed_diagnostic_tests;

type DiagnosticAdmission<'a> =
    dyn FnMut(&ControlOwnedResourceFacts) -> Result<(), CompileControlError> + 'a;

// This policy only owns already-spelled diagnostic payloads and their actual
// collector. Formatting arguments before entry still belongs to each original
// report owner; these ports do not claim to admit that formatting or its CPU.
trait DiagnosticPolicy {
    type Error;
    fn spellings(
        &mut self,
        path: impl AsRef<str>,
        message: impl AsRef<str>,
    ) -> Result<(Box<str>, Box<str>), Self::Error>;
    fn append(
        &mut self,
        errors: &mut Vec<ValidationError>,
        error: ValidationError,
    ) -> Result<(), Self::Error>;
    fn sentinel(&mut self, errors: &mut Vec<ValidationError>) -> Result<(), Self::Error>;
    fn finish(
        &mut self,
        errors: Vec<ValidationError>,
    ) -> Result<Box<[ValidationError]>, Self::Error>;
}
struct PlainDiagnostic;
impl DiagnosticPolicy for PlainDiagnostic {
    type Error = std::convert::Infallible;
    fn spellings(
        &mut self,
        path: impl AsRef<str>,
        message: impl AsRef<str>,
    ) -> Result<(Box<str>, Box<str>), Self::Error> {
        // Preserve even the original AsRef/copy evaluation order.
        let path = path.as_ref().into();
        let message = message.as_ref().into();
        Ok((path, message))
    }
    fn append(
        &mut self,
        errors: &mut Vec<ValidationError>,
        error: ValidationError,
    ) -> Result<(), Self::Error> {
        errors.push(error);
        Ok(())
    }
    fn sentinel(&mut self, errors: &mut Vec<ValidationError>) -> Result<(), Self::Error> {
        let error = ValidationError::categorized_core(
            ValidationErrorCategory::StructuralInvariant,
            "validation",
            "additional validation errors were truncated",
            self,
        )?;
        self.append(errors, error)
    }
    fn finish(
        &mut self,
        errors: Vec<ValidationError>,
    ) -> Result<Box<[ValidationError]>, Self::Error> {
        Ok(errors.into_boxed_slice())
    }
}
struct CallerDiagnostic<'a, 'control> {
    resources: &'a mut ControlResourceCounter,
    admit: &'a mut DiagnosticAdmission<'a>,
    work: &'a mut CompileCheckpoints<'control>,
}
impl CallerDiagnostic<'_, '_> {
    fn copy_spellings(
        &mut self,
        path: &str,
        message: &str,
    ) -> Result<(Box<str>, Box<str>), ControlResourceError> {
        self.work.flush()?;
        let path = path.into();
        self.work.step()?;
        self.work.flush()?;
        let message = message.into();
        self.work.step()?;
        self.work.flush()?;
        Ok((path, message))
    }
}
impl DiagnosticPolicy for CallerDiagnostic<'_, '_> {
    type Error = ControlResourceError;
    fn spellings(
        &mut self,
        path: impl AsRef<str>,
        message: impl AsRef<str>,
    ) -> Result<(Box<str>, Box<str>), Self::Error> {
        let path = path.as_ref();
        let message = message.as_ref();
        // Both actual borrowed spellings are already known. Admit both
        // standard Box byte requests before the first pending observation.
        // No intermediate String or trim allocation is introduced.
        self.resources.buffer::<u8>(path.len(), 1)?;
        self.resources.buffer::<u8>(message.len(), 1)?;
        // Two actual Box constructions and their closed byte-payload cleanup,
        // including the empty spellings with no backing request.
        self.resources.work(4)?;
        (self.admit)(&self.resources.facts())?;
        self.copy_spellings(path, message)
    }
    fn append(
        &mut self,
        errors: &mut Vec<ValidationError>,
        error: ValidationError,
    ) -> Result<(), Self::Error> {
        self.resources.work(1)?; // The actual push also occurs without growth.
        reserve_for_push_in::<_, ControlResourceError>(
            errors,
            &mut |facts| {
                if let Some(layout) = facts.requested_backing {
                    self.resources.layout(layout, 1)?;
                }
                (self.admit)(&self.resources.facts()).map_err(Into::into)
            },
            self.work,
        )?;
        errors.push(error);
        self.work.step()?;
        Ok(())
    }
    fn sentinel(&mut self, errors: &mut Vec<ValidationError>) -> Result<(), Self::Error> {
        let path = "validation";
        let message = "additional validation errors were truncated";
        let captured = push_growth(errors)?;
        self.resources.work(5)?; // Two constructions, their cleanup and one push.
        // The sentinel's two spellings AND the actual collector growth are
        // already known at this header. Admit them together before observing
        // or making any part of the original owned sentinel operation.
        self.resources.buffer::<u8>(path.len(), 1)?;
        self.resources.buffer::<u8>(message.len(), 1)?;
        if let Some(layout) = captured.requested_backing {
            self.resources.layout(layout, 1)?;
        }
        (self.admit)(&self.resources.facts())?;
        reserve_for_push_in::<_, ControlResourceError>(
            errors,
            &mut |facts| {
                if *facts != captured {
                    return Err(ControlResourceError::SourceModel(
                        "diagnostic collector source changed",
                    ));
                }
                // This is the same prepaid actual request, not another prefix sum.
                (self.admit)(&self.resources.facts()).map_err(Into::into)
            },
            self.work,
        )?;
        let (path, message) = self.copy_spellings(path, message)?;
        let error = ValidationError::from_spellings(
            ValidationErrorCategory::StructuralInvariant,
            path,
            message,
        );
        errors.push(error);
        self.work.step()?;
        Ok(())
    }
    fn finish(
        &mut self,
        errors: Vec<ValidationError>,
    ) -> Result<Box<[ValidationError]>, Self::Error> {
        self.resources.work(1)?; // Conversion also occurs without a trim request.
        boxed_slice_in::<_, ControlResourceError>(
            errors,
            &mut |facts| {
                if let Some(layout) = facts.requested_backing {
                    self.resources.layout(layout, 1)?;
                }
                (self.admit)(&self.resources.facts()).map_err(Into::into)
            },
            self.work,
        )
    }
}

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
        Self::categorized_core(category, path, message, &mut PlainDiagnostic)
            .unwrap_or_else(|never| match never {})
    }

    /// Copy these actual spellings after caller admission. Their formatter
    /// must already have been admitted by its own source author. Standard Box
    /// allocation failure is not promised to be a recoverable control error.
    pub(crate) fn categorized_in(
        category: ValidationErrorCategory,
        path: &str,
        message: &str,
        resources: &mut ControlResourceCounter,
        admit: &mut DiagnosticAdmission<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ControlResourceError> {
        Self::categorized_core(
            category,
            path,
            message,
            &mut CallerDiagnostic {
                resources,
                admit,
                work,
            },
        )
    }

    fn categorized_core<P: DiagnosticPolicy>(
        category: ValidationErrorCategory,
        path: impl AsRef<str>,
        message: impl AsRef<str>,
        policy: &mut P,
    ) -> Result<Self, P::Error> {
        let (path, message) = policy.spellings(path, message)?;
        Ok(Self::from_spellings(category, path, message))
    }

    fn from_spellings(
        category: ValidationErrorCategory,
        path: Box<str>,
        message: Box<str>,
    ) -> Self {
        Self {
            category,
            path,
            message,
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
    construction_only: bool,
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
            construction_only: false,
        }
    }

    /// The unpublished definition still has to prove every structural fact.
    /// Invocation effects and output-property derivation are separate, later
    /// obligations of the same fragment before package publication.
    pub(crate) const fn for_construction(limits: PlanLimits) -> Self {
        let mut context = Self::with_limits(limits);
        context.construction_only = true;
        context
    }

    pub(crate) const fn checks_output_and_effect_proofs(&self) -> bool {
        !self.construction_only
    }

    pub(crate) const fn limits(&self) -> &PlanLimits {
        &self.limits
    }

    pub(crate) fn push(&mut self, error: ValidationError) {
        self.push_core(error, &mut PlainDiagnostic)
            .unwrap_or_else(|never| match never {});
    }

    /// The incoming diagnostic's spellings and all possible error cleanup
    /// must already be admitted. Saturation retains the original candidate
    /// construction/drop; it does not skip the 129th or later candidate.
    pub(crate) fn push_in(
        &mut self,
        error: ValidationError,
        resources: &mut ControlResourceCounter,
        admit: &mut DiagnosticAdmission<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ControlResourceError> {
        self.push_core(
            error,
            &mut CallerDiagnostic {
                resources,
                admit,
                work,
            },
        )
    }

    fn push_core<P: DiagnosticPolicy>(
        &mut self,
        error: ValidationError,
        policy: &mut P,
    ) -> Result<(), P::Error> {
        if self.errors.len() < MAX_VALIDATION_ERRORS {
            policy.append(&mut self.errors, error)?;
        } else {
            self.mark_truncated_core(policy)?;
        }
        Ok(())
    }

    pub(crate) fn is_saturated(&self) -> bool {
        self.errors.len() >= MAX_VALIDATION_ERRORS
    }

    pub(crate) fn mark_truncated(&mut self) {
        self.mark_truncated_core(&mut PlainDiagnostic)
            .unwrap_or_else(|never| match never {});
    }

    pub(crate) fn mark_truncated_in(
        &mut self,
        resources: &mut ControlResourceCounter,
        admit: &mut DiagnosticAdmission<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ControlResourceError> {
        self.mark_truncated_core(&mut CallerDiagnostic {
            resources,
            admit,
            work,
        })
    }

    fn mark_truncated_core<P: DiagnosticPolicy>(&mut self, policy: &mut P) -> Result<(), P::Error> {
        if !self.truncated {
            self.truncated = true;
            policy.sentinel(&mut self.errors)?;
        }
        Ok(())
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
        Self::from_collector_core(errors, &mut PlainDiagnostic)
            .unwrap_or_else(|never| match never {})
    }

    /// The original final trim on the caller's counter, with already-admitted
    /// diagnostic payload/cleanup. This leaf creates neither entry nor footer.
    pub(crate) fn from_collector_in(
        errors: ValidationContext,
        resources: &mut ControlResourceCounter,
        admit: &mut DiagnosticAdmission<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ControlResourceError> {
        Self::from_collector_core(
            errors,
            &mut CallerDiagnostic {
                resources,
                admit,
                work,
            },
        )
    }

    fn from_collector_core<P: DiagnosticPolicy>(
        errors: ValidationContext,
        policy: &mut P,
    ) -> Result<Self, P::Error> {
        Ok(Self(policy.finish(errors.into_vec())?))
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
