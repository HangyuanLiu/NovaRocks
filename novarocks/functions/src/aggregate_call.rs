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

//! Exact aggregate call phase and separate selected update/merge channels.
//! State placement, group mapping and lineage remain execution/program owners.

use crate::kernel_control::{compile_failure, invalid};
use crate::kernel_input::{validate_argument_observed, validate_type_observed};
use crate::{
    AggregateOrderKey, AggregateStateFormatIdentity, EvaluatedArgument, FunctionArgumentType,
    FunctionCallContract, FunctionResultType, KernelEvaluationControl, KernelFailure, Selection,
};
use novarocks_type_contract::{
    ArgumentControl, CompileCheckpoints, CompilePhase, FunctionKind, FunctionValueType,
    PureCompileControl,
};
use std::sync::Arc;

/// Per-call phase; a node may mix update and merge calls. Sequence identity and
/// complete/partial responsibility remain in the checked program lineage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateKernelPhase {
    Single,
    Partial,
    Intermediate,
    Final,
}
impl AggregateKernelPhase {
    pub const fn consumes_logical_arguments(self) -> bool {
        matches!(self, Self::Single | Self::Partial)
    }
    pub const fn produces_final_result(self) -> bool {
        matches!(self, Self::Single | Self::Final)
    }
}

/// The sole state-domain check borrows its exact immutable inputs. This proof
/// is local preparation evidence, not a graph or wire identity.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ValidatedAggregateMergeState<'a> {
    input: &'a FunctionValueType,
    intermediate: &'a FunctionValueType,
}

/// The options own the same full type that was borrowed by the runtime demand.
/// Only the observed exact comparison below can construct this token.
pub(crate) struct AlignedAggregateMergeState<'a> {
    input: &'a FunctionValueType,
    owned: FunctionValueType,
}

pub(crate) fn align_aggregate_merge_state_observed<'a>(
    input: &'a FunctionValueType,
    owned: FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AlignedAggregateMergeState<'a>, KernelFailure> {
    if !input
        .exactly_equals_observed::<KernelFailure>(&owned, || work.step().map_err(compile_failure))?
    {
        return Err(invalid(
            "aggregate options differ from the actual merge state type",
        ));
    }
    Ok(AlignedAggregateMergeState { input, owned })
}

pub(crate) fn validate_aggregate_merge_state_observed<'a>(
    input: &'a FunctionValueType,
    intermediate: &'a FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ValidatedAggregateMergeState<'a>, KernelFailure> {
    validate_type_observed(input, work)?;
    if !input.same_value_domain_observed::<KernelFailure>(intermediate, || {
        work.step().map_err(compile_failure)
    })? || (intermediate.nullable && !input.nullable)
    {
        return Err(invalid(
            "aggregate merge state layout differs from its selected intermediate domain",
        ));
    }
    Ok(ValidatedAggregateMergeState {
        input,
        intermediate,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateCallContract {
    call: Arc<FunctionCallContract>,
    phase: AggregateKernelPhase,
    distinct: bool,
    order_keys: Arc<[AggregateOrderKey]>,
    state_input_type: Option<FunctionValueType>,
}
impl AggregateCallContract {
    /// Preserve the same logical binding in every phase. Merge input layout
    /// may widen the emission's top-level nullability, never its value domain.
    /// Bind semantic options such as output lengths from frozen parameters in
    /// the exact implementation; this contract mints no memory allowance.
    pub fn try_new(
        call: Arc<FunctionCallContract>,
        phase: AggregateKernelPhase,
        distinct: bool,
        order_keys: Arc<[AggregateOrderKey]>,
        state_input_type: Option<FunctionValueType>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, KernelFailure> {
        Self::try_new_impl(
            call,
            crate::AggregatePreparationOptions {
                phase,
                distinct,
                order_keys,
                state_input_type,
            },
            None,
            control,
        )
    }

    pub(crate) fn try_new_refined(
        call: Arc<FunctionCallContract>,
        phase: AggregateKernelPhase,
        distinct: bool,
        order_keys: Arc<[AggregateOrderKey]>,
        state: Option<AlignedAggregateMergeState<'_>>,
        proof: Option<ValidatedAggregateMergeState<'_>>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, KernelFailure> {
        let validated = match (state, proof) {
            (None, None) => None,
            (Some(state), Some(proof)) if std::ptr::eq(state.input, proof.input) => {
                Some((state.owned, proof))
            }
            _ => {
                return Err(invalid(
                    "aggregate merge receipt differs from its actual state options",
                ));
            }
        };
        let (state_input_type, proof) = match validated {
            Some((state, proof)) => (Some(state), Some(proof)),
            None => (None, None),
        };
        Self::try_new_impl(
            call,
            crate::AggregatePreparationOptions {
                phase,
                distinct,
                order_keys,
                state_input_type,
            },
            proof,
            control,
        )
    }

    fn try_new_impl(
        call: Arc<FunctionCallContract>,
        options: crate::AggregatePreparationOptions,
        proof: Option<ValidatedAggregateMergeState<'_>>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, KernelFailure> {
        let crate::AggregatePreparationOptions {
            phase,
            distinct,
            order_keys,
            state_input_type,
        } = options;
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        let result = (|| {
            if call.kind() != FunctionKind::Aggregate
                || call.effects().argument_control != ArgumentControl::Aggregate
            {
                return Err(invalid(
                    "aggregate preparation requires its exact aggregate control",
                ));
            }
            let Some(state) = &call.selected().aggregate else {
                return Err(invalid("aggregate binding has no state contract"));
            };
            if !matches!(call.selected().result_type, FunctionResultType::Scalar(_)) {
                return Err(invalid("aggregate binding requires a scalar final result"));
            }
            let channels = &call.selected().argument_types;
            let logical = call.logical_argument_count();
            if logical > channels.len() || order_keys.len() > crate::MAX_CALL_EFFECT_ARGUMENTS {
                return Err(invalid("aggregate logical/order channel shape differs"));
            }
            for channel in channels {
                if !matches!(channel, FunctionArgumentType::Value(_)) {
                    return Err(invalid("aggregate channel cannot be a lambda"));
                }
                work.step().map_err(compile_failure)?;
            }
            if phase.consumes_logical_arguments() {
                if state_input_type.is_some() || order_keys.len() != channels.len() - logical {
                    return Err(invalid(
                        "aggregate update channels differ from its selected logical/order signature",
                    ));
                }
            } else {
                if distinct || !order_keys.is_empty() {
                    return Err(invalid(
                        "aggregate merge cannot repeat DISTINCT or function ORDER BY",
                    ));
                }
                let Some(input) = &state_input_type else {
                    return Err(invalid("aggregate merge has no exact state input layout"));
                };
                match proof {
                    Some(proof) if std::ptr::eq(proof.intermediate, &state.intermediate_type) => {
                        // The exact runtime source was checked once by refinement;
                        // its owned options were aligned before that refinement.
                        work.step().map_err(compile_failure)?;
                    }
                    Some(_) => {
                        return Err(invalid(
                            "aggregate state receipt has a different selected owner",
                        ));
                    }
                    None => {
                        validate_aggregate_merge_state_observed(
                            input,
                            &state.intermediate_type,
                            &mut work,
                        )?;
                    }
                }
            }
            Ok(())
        })();
        match result {
            Err(
                error @ (KernelFailure::Cancelled
                | KernelFailure::DeadlineExceeded
                | KernelFailure::ResourceExhausted),
            ) => return Err(error),
            result => {
                work.finish().map_err(compile_failure)?;
                result?;
            }
        }
        Ok(Self {
            call,
            phase,
            distinct,
            order_keys,
            state_input_type,
        })
    }
    pub fn call(&self) -> &Arc<FunctionCallContract> {
        &self.call
    }
    pub const fn phase(&self) -> AggregateKernelPhase {
        self.phase
    }
    pub const fn distinct(&self) -> bool {
        self.distinct
    }
    pub fn order_keys(&self) -> &[AggregateOrderKey] {
        &self.order_keys
    }
    pub fn logical_argument_types(&self) -> impl ExactSizeIterator<Item = &FunctionValueType> {
        self.call.selected().argument_types[..self.call.logical_argument_count()]
            .iter()
            .map(value_type)
    }
    pub fn order_argument_types(&self) -> impl ExactSizeIterator<Item = &FunctionValueType> {
        self.call.selected().argument_types[self.call.logical_argument_count()..]
            .iter()
            .map(value_type)
    }
    pub fn intermediate_type(&self) -> &FunctionValueType {
        &self
            .call
            .selected()
            .aggregate
            .as_ref()
            .expect("checked aggregate")
            .intermediate_type
    }
    pub fn state_format(&self) -> &AggregateStateFormatIdentity {
        &self
            .call
            .selected()
            .aggregate
            .as_ref()
            .expect("checked aggregate")
            .state_format
    }
    pub fn final_type(&self) -> &FunctionValueType {
        match &self.call.selected().result_type {
            FunctionResultType::Scalar(value) => value,
            _ => unreachable!("checked scalar result"),
        }
    }
    pub const fn state_input_type(&self) -> Option<&FunctionValueType> {
        self.state_input_type.as_ref()
    }
}
fn value_type(value: &FunctionArgumentType) -> &FunctionValueType {
    match value {
        FunctionArgumentType::Value(value) => value,
        _ => unreachable!("checked aggregate value channel"),
    }
}

/// Evaluated logical and function ORDER BY channels stay independent; no
/// packed Struct carrier is guessed. Empty selections invoke no state update.
/// This input does not sort, authorize reordering, or choose a group state.
#[derive(Clone, Copy, Debug)]
pub struct SelectedAggregateUpdateInput<'call, 'a> {
    contract: &'call AggregateCallContract,
    selection: Selection<'a>,
    logical_arguments: &'a [EvaluatedArgument<'a>],
    order_arguments: &'a [EvaluatedArgument<'a>],
}
impl<'call, 'a> SelectedAggregateUpdateInput<'call, 'a> {
    pub fn try_new(
        contract: &'call AggregateCallContract,
        selection: Selection<'a>,
        logical_arguments: &'a [EvaluatedArgument<'a>],
        order_arguments: &'a [EvaluatedArgument<'a>],
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        if !contract.phase.consumes_logical_arguments()
            || logical_arguments.len() != contract.logical_argument_types().len()
            || order_arguments.len() != contract.order_argument_types().len()
        {
            return Err(invalid(
                "aggregate update input differs from its exact call phase/channels",
            ));
        }
        for (argument, value_type) in logical_arguments
            .iter()
            .zip(contract.logical_argument_types())
            .chain(order_arguments.iter().zip(contract.order_argument_types()))
        {
            validate_argument_observed(*argument, selection, value_type, control)?;
        }
        Ok(Self {
            contract,
            selection,
            logical_arguments,
            order_arguments,
        })
    }
    pub const fn contract(self) -> &'call AggregateCallContract {
        self.contract
    }
    pub const fn selection(self) -> Selection<'a> {
        self.selection
    }
    pub const fn logical_arguments(self) -> &'a [EvaluatedArgument<'a>] {
        self.logical_arguments
    }
    pub const fn order_arguments(self) -> &'a [EvaluatedArgument<'a>] {
        self.order_arguments
    }
}

/// Merge retains the logical binding and frozen state format. Its one state
/// carrier is validated against the checked input layout, whose nullability
/// may be wider than intermediate emission; the implementation owns NULL rules.
#[derive(Clone, Copy, Debug)]
pub struct SelectedAggregateMergeInput<'call, 'a> {
    contract: &'call AggregateCallContract,
    selection: Selection<'a>,
    state: EvaluatedArgument<'a>,
}
impl<'call, 'a> SelectedAggregateMergeInput<'call, 'a> {
    pub fn try_new(
        contract: &'call AggregateCallContract,
        selection: Selection<'a>,
        state: EvaluatedArgument<'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        if contract.phase.consumes_logical_arguments() {
            return Err(invalid(
                "aggregate update phase cannot consume a merge input",
            ));
        }
        let value_type = contract
            .state_input_type
            .as_ref()
            .expect("checked merge layout");
        validate_argument_observed(state, selection, value_type, control)?;
        Ok(Self {
            contract,
            selection,
            state,
        })
    }
    pub const fn contract(self) -> &'call AggregateCallContract {
        self.contract
    }
    pub const fn selection(self) -> Selection<'a> {
        self.selection
    }
    pub const fn state(self) -> EvaluatedArgument<'a> {
        self.state
    }
}

#[cfg(test)]
mod tests;
