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

//! Original static requests, independent of runtime invocation occurrences.
//! Representation equality compares pool addresses, never selected values.
//! This table supplies no SQL provenance, effects or installed capability.

use crate::{
    ConstantPolicy, ConstantReference, ExprId, ExprKind, Fragment, FragmentId, FrozenCallError,
    PhysicalCallBinding, PhysicalCallSite, ValueType,
};
use novarocks_function_contract::{FunctionArgument, FunctionBindingError, FunctionBindingRequest};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::{collections::BTreeMap, fmt, sync::Arc};

/// The expression key names a definition, including dead and TypeOnly nodes.
/// Relational keys retain their existing lifecycle and ordered call position.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum PhysicalCallDefinition {
    Expression(ExprId),
    Relational(PhysicalCallSite),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalCallRequest {
    pub arguments: Box<[FunctionArgument<ConstantReference>]>,
    pub logical_argument_count: usize,
    /// The original request's optional constraint, never its selected result.
    pub expected_result_type: Option<ValueType>,
    /// Source admission facts, not a receiver allocation grant.
    pub constant_policy: ConstantPolicy,
}
impl PhysicalCallRequest {
    pub fn request(&self) -> FunctionBindingRequest<'_, ConstantReference> {
        FunctionBindingRequest {
            arguments: &self.arguments,
            logical_argument_count: self.logical_argument_count,
            expected_result_type: self.expected_result_type.as_ref(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FragmentCallRequests {
    fragment: FragmentId,
    entries: Arc<BTreeMap<PhysicalCallDefinition, PhysicalCallRequest>>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CallRequestError {
    Control(CompileControlError),
    Binding(FunctionBindingError),
    Calls(FrozenCallError),
    Structure(crate::ValidationErrors),
    WrongFragment,
    DuplicateDefinition(PhysicalCallDefinition),
    MissingDefinition(PhysicalCallDefinition),
    ExtraDefinition,
    InvalidArgumentCount(PhysicalCallDefinition),
    ArgumentTypeMismatch(PhysicalCallDefinition),
}
impl fmt::Display for CallRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid original call requests: {self:?}")
    }
}
impl std::error::Error for CallRequestError {}
impl From<CompileControlError> for CallRequestError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<FunctionBindingError> for CallRequestError {
    fn from(error: FunctionBindingError) -> Self {
        match error {
            FunctionBindingError::Control(cause) => Self::Control(cause),
            other => Self::Binding(other),
        }
    }
}
impl From<FrozenCallError> for CallRequestError {
    fn from(error: FrozenCallError) -> Self {
        match error {
            FrozenCallError::Control(cause) => Self::Control(cause),
            other => Self::Calls(other),
        }
    }
}
fn finish<T>(
    result: Result<T, CallRequestError>,
    work: CompileCheckpoints<'_>,
) -> Result<T, CallRequestError> {
    if matches!(result, Err(CallRequestError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
impl FragmentCallRequests {
    /// A construction input only. Publication rejects missing call records.
    pub(crate) fn unpublished_empty(fragment: FragmentId) -> Self {
        Self {
            fragment,
            entries: Arc::new(BTreeMap::new()),
        }
    }
    pub const fn fragment(&self) -> FragmentId {
        self.fragment
    }
    pub fn entries(&self) -> &BTreeMap<PhysicalCallDefinition, PhysicalCallRequest> {
        &self.entries
    }
    pub fn get(&self, definition: PhysicalCallDefinition) -> Option<&PhysicalCallRequest> {
        self.entries.get(&definition)
    }
    pub fn try_new(
        fragment: &Fragment,
        entries: Vec<(PhysicalCallDefinition, PhysicalCallRequest)>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, CallRequestError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = (|| {
            crate::resource::validate_call_request_source_observed(
                &entries,
                entries.capacity(),
                &mut work,
            )?;
            let mut table = BTreeMap::new();
            for (definition, request) in entries {
                work.flush()?;
                let previous = table.insert(definition, request);
                work.step()?;
                work.flush()?;
                if previous.is_some() {
                    return Err(CallRequestError::DuplicateDefinition(definition));
                }
            }
            let table = Self {
                fragment: fragment.id(),
                entries: Arc::new(table),
            };
            work.flush()?;
            table.validate_fragment(fragment, control)?;
            Ok(table)
        })();
        finish(result, work)
    }
    pub fn validate_fragment(
        &self,
        fragment: &Fragment,
        control: &dyn PureCompileControl,
    ) -> Result<(), CallRequestError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = (|| {
            let wrong_fragment = fragment.id() != self.fragment;
            work.step()?;
            if wrong_fragment {
                return Err(CallRequestError::WrongFragment);
            }
            crate::resource::validate_call_request_table_observed(self, &mut work)?;
            let mut visited = 0usize;
            let mut check = |definition,
                             binding: PhysicalCallBinding<'_>,
                             window_logical_count: Option<usize>,
                             work: &mut CompileCheckpoints<'_>|
             -> Result<(), CallRequestError> {
                let request = self.entries.get(&definition);
                work.step()?;
                let request = request.ok_or(CallRequestError::MissingDefinition(definition))?;
                validate_request_binding(definition, request, binding, window_logical_count, work)?;
                visited = visited
                    .checked_add(1)
                    .ok_or(CompileControlError::ResourceExhausted)?;
                work.step()?;
                Ok(())
            };
            for (&id, expression) in fragment.expressions().iter() {
                work.step()?;
                let (binding, window_logical_count) = match &expression.kind {
                    ExprKind::FunctionCall { function, .. } => {
                        (PhysicalCallBinding::Scalar(function), None)
                    }
                    ExprKind::WindowCall {
                        function,
                        aggregate_binding,
                        args,
                        ..
                    } => (
                        PhysicalCallBinding::Window {
                            function,
                            aggregate: aggregate_binding.as_deref(),
                        },
                        aggregate_binding.is_none().then_some(args.len()),
                    ),
                    _ => continue,
                };
                check(
                    PhysicalCallDefinition::Expression(id),
                    binding,
                    window_logical_count,
                    &mut work,
                )?;
            }
            crate::visit_relational_calls_observed(fragment, &mut work, |site, binding, work| {
                check(
                    PhysicalCallDefinition::Relational(site),
                    binding,
                    None,
                    work,
                )
            })?;
            let extra = visited != self.entries.len();
            work.step()?;
            if extra {
                return Err(CallRequestError::ExtraDefinition);
            }
            Ok(())
        })();
        finish(result, work)
    }
}
fn validate_request_binding(
    definition: PhysicalCallDefinition,
    request: &PhysicalCallRequest,
    binding: PhysicalCallBinding<'_>,
    window_logical_count: Option<usize>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), CallRequestError> {
    let (types, logical) = match binding {
        PhysicalCallBinding::Scalar(function) => (
            &*function.argument_types,
            Some(function.argument_types.len()),
        ),
        PhysicalCallBinding::Window {
            function,
            aggregate,
        } => (
            &*function.argument_types,
            aggregate
                .map(|value| value.logical_argument_count as usize)
                .or(window_logical_count),
        ),
        PhysicalCallBinding::Aggregate(binding) => (
            &*binding.function.argument_types,
            Some(binding.logical_argument_count as usize),
        ),
        PhysicalCallBinding::Table(function) => (
            &*function.argument_types,
            Some(function.argument_types.len()),
        ),
    };
    let invalid_count = request.arguments.len() != types.len()
        || request.logical_argument_count > request.arguments.len()
        || logical.is_some_and(|logical| logical != request.logical_argument_count);
    work.step()?;
    if invalid_count {
        return Err(CallRequestError::InvalidArgumentCount(definition));
    }
    for (actual, expected) in request.arguments.iter().zip(types) {
        if !actual.matches_type_observed(expected, work)? {
            return Err(CallRequestError::ArgumentTypeMismatch(definition));
        }
        work.step()?;
    }
    Ok(())
}
impl Fragment {
    /// Attach explicit original requests before executable publication. Nothing
    /// is recovered from constants, selected results, names or runtime uses.
    pub fn with_call_requests_observed(
        mut self,
        entries: Vec<(PhysicalCallDefinition, PhysicalCallRequest)>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, CallRequestError> {
        let requests = FragmentCallRequests::try_new(&self, entries, control)?;
        self.call_requests = requests;
        Ok(self)
    }
}

#[cfg(test)]
mod tests;
