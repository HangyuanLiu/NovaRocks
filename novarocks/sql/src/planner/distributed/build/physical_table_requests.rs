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

//! Static relation requests from the original table-function node.

use std::sync::Arc;

use novarocks_functions::{
    ConstantPolicy, FunctionArgument, FunctionBindingRequest, FunctionBindingSelection,
    FunctionResultType, MAX_CALL_EFFECT_ARGUMENTS,
};
use novarocks_physical_plan::{
    BoundTableFunction, ConstantPools, ExprId, Fragment, NodeKind, PhysicalNode,
};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, CompilePhase};

use super::physical_call_arguments::{PhysicalArgumentError, author_physical_argument_observed};

#[derive(Debug)]
pub(crate) enum PhysicalTableRequestError {
    Control(CompileControlError),
    Argument(PhysicalArgumentError),
    InvalidSource(&'static str),
    MissingArgument(ExprId),
}
impl From<CompileControlError> for PhysicalTableRequestError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<PhysicalArgumentError> for PhysicalTableRequestError {
    fn from(error: PhysicalArgumentError) -> Self {
        match error {
            PhysicalArgumentError::Control(cause) => Self::Control(cause),
            other => Self::Argument(other),
        }
    }
}

/// One original node loan and the complete selected relation signature.
/// The selected Arc feeds both CallEffectInput and actual fresh preparation.
/// It authenticates neither installed capability nor complete call effects.
#[derive(Debug)]
pub(crate) struct AuthoredPhysicalTableRequest<'a> {
    source: &'a PhysicalNode,
    function: &'a BoundTableFunction,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
}
impl AuthoredPhysicalTableRequest<'_> {
    pub const fn source(&self) -> &PhysicalNode {
        self.source
    }
    pub const fn function(&self) -> &BoundTableFunction {
        self.function
    }
    pub const fn selected(&self) -> &Arc<FunctionBindingSelection> {
        &self.selected
    }
    pub fn request(&self) -> FunctionBindingRequest<'_> {
        FunctionBindingRequest {
            arguments: &self.arguments,
            logical_argument_count: self.arguments.len(),
            // A scalar result constraint cannot represent the selected relation.
            // The exact installed table owner validates all result columns.
            expected_result_type: None,
        }
    }
}

/// Preserve ordered actual argument occurrences and the original full relation
/// signature separately. Outer pass-through columns, LEFT assembly and node
/// output roles remain with the mandatory physical node validator. No scalar
/// result projection, name resolution or legacy effect metadata is used here.
/// The actual installed table owner validates these selected/source types,
/// including argument shape and its precise UNNEST specialization domain.
///
/// Caller admission covers the source and opaque nested signature/type clones.
/// Existing call bounds precede expansion and fallible parameter reservation.
/// The caller owns entry, the ordinary/success footer, exact per-use policy and
/// environment, and subsequent fresh preparation. Originating control/resource
/// refusals return directly on the same meter without a later observation.
pub(crate) fn author_physical_table_request_observed<'a>(
    source: &'a PhysicalNode,
    fragment: &Fragment,
    pools: &ConstantPools,
    literal_policy: ConstantPolicy,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AuthoredPhysicalTableRequest<'a>, PhysicalTableRequestError> {
    work.flush()?;
    let original = fragment.nodes().get(&source.id);
    work.step()?;
    work.flush()?;
    if !original.is_some_and(|node| std::ptr::eq(node, source)) {
        return Err(PhysicalTableRequestError::InvalidSource(
            "table request source is not the original fragment node",
        ));
    }
    let NodeKind::TableFunction {
        function,
        arguments: ids,
        ..
    } = &source.kind
    else {
        return Err(PhysicalTableRequestError::InvalidSource(
            "physical table request requires an actual table-function node",
        ));
    };
    let bounded = ids.len() <= MAX_CALL_EFFECT_ARGUMENTS
        && function.argument_types.len() <= MAX_CALL_EFFECT_ARGUMENTS
        && function.result_types.len() <= MAX_CALL_EFFECT_ARGUMENTS;
    work.step()?;
    if !bounded {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    let exact_count = ids.len() == function.argument_types.len();
    work.step()?;
    if !exact_count {
        return Err(PhysicalTableRequestError::InvalidSource(
            "physical table argument count differs from its selected signature",
        ));
    }
    let has_results = !function.result_types.is_empty();
    work.step()?;
    if !has_results {
        return Err(PhysicalTableRequestError::InvalidSource(
            "physical table request has no selected relation columns",
        ));
    }
    work.flush()?;
    let mut arguments = Vec::new();
    arguments
        .try_reserve_exact(ids.len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    for &id in ids {
        let argument = fragment.expressions().get(id);
        work.step()?;
        let argument = argument.ok_or(PhysicalTableRequestError::MissingArgument(id))?;
        arguments.push(author_physical_argument_observed(
            argument,
            pools,
            literal_policy,
            CompilePhase::FunctionSpecialization,
            work,
        )?);
        work.step()?;
    }
    work.flush()?;
    let selected = Arc::new(FunctionBindingSelection {
        overload: function.overload.clone(),
        argument_types: function.argument_types.clone(),
        result_type: FunctionResultType::Relation(function.result_types.clone()),
        aggregate: None,
    });
    work.flush()?;
    Ok(AuthoredPhysicalTableRequest {
        source,
        function,
        selected,
        arguments,
    })
}
