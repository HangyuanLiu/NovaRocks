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

//! Pure immutable scalar preparation and instance-owned batch invocation.
//! An ordinary kernel receives evaluated values, never an expression arena.

use std::{fmt, sync::Arc, time::Duration};

use arrow_array::types::{
    Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, DictionaryArray, RunArray, UnionArray, new_empty_array};
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CompileCheckpoints, CompileControlError, CompilePhase,
    DecimalOverflowPolicy, ExpressionEffectContext, FunctionIntrinsicRowError, FunctionKind,
    FunctionValueType, PureCompileControl, SemanticParameters,
};

use crate::{
    CallEffectInput, CallEffectRefinementError, EvaluatedArgument, FunctionArgumentType,
    FunctionBindingError, FunctionBindingResolver, FunctionBindingSelection, FunctionEffectOwner,
    FunctionId, FunctionResultType, RefinedCallEffects, SelectedValues, Selection,
};

/// Bounded diagnostics on outer failures. Only RowDataError enters the
/// maskable row channel; this carrier cannot be converted to it implicitly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KernelDiagnostic(Box<str>);
impl KernelDiagnostic {
    pub fn new(message: &str) -> Self {
        let mut end = message.len().min(crate::MAX_ROW_ERROR_MESSAGE_BYTES);
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        Self(message[..end].into())
    }
    pub fn message(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScalarKernelFailure {
    Cancelled,
    DeadlineExceeded,
    ResourceExhausted,
    InvalidProgram(KernelDiagnostic),
    Internal(KernelDiagnostic),
    Operational(KernelDiagnostic),
    InstanceFailed,
}
impl fmt::Display for ScalarKernelFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InstanceFailed => f.write_str("scalar instance has already failed"),
            Self::Cancelled => f.write_str("scalar evaluation was cancelled"),
            Self::DeadlineExceeded => f.write_str("scalar evaluation deadline was exceeded"),
            Self::ResourceExhausted => f.write_str("scalar evaluation resources were exhausted"),
            Self::InvalidProgram(message) => {
                write!(f, "invalid scalar program: {}", message.message())
            }
            Self::Internal(message) => write!(f, "scalar internal failure: {}", message.message()),
            Self::Operational(message) => {
                write!(f, "scalar operational failure: {}", message.message())
            }
        }
    }
}
impl std::error::Error for ScalarKernelFailure {}

pub const MAX_UNOBSERVED_SCALAR_WORK: u32 = 256;

/// Runtime-owned interruption/work control, independent of statement semantic
/// time. Waiting is the exact sleep implementation's observable operation.
/// The host installs its formal memory scopes and authorizes known allocation
/// steps before invocation; this interface does not mint a second budget.
pub trait ScalarEvaluationControl: Send + Sync {
    fn checkpoint(&self, work_units: u32) -> Result<(), ScalarKernelFailure>;
    fn wait(&self, duration: Duration) -> Result<(), ScalarKernelFailure>;
}

/// One locally verified call's immutable public facts. Constants needed by a
/// specialization live in its prepared implementation, not in a live service.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalarCallContract {
    function_id: FunctionId,
    context: ExpressionEffectContext,
    decimal_overflow_policy: DecimalOverflowPolicy,
    selected: Arc<FunctionBindingSelection>,
    effects: CallEffects,
    parameters: SemanticParameters,
}
impl ScalarCallContract {
    pub fn from_refined(
        input: CallEffectInput<'_>,
        receipt: &RefinedCallEffects<'_>,
        selected: Arc<FunctionBindingSelection>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ScalarKernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        receipt
            .validate_input(input)
            .map_err(|_| invalid("call refinement receipt differs from exact input"))?;
        if !std::ptr::eq(input.selected, selected.as_ref()) {
            return Err(invalid(
                "scalar signature owner differs from the exact input borrow",
            ));
        }
        if input.kind != FunctionKind::Scalar || input.selected.aggregate.is_some() {
            return Err(invalid(
                "ordinary scalar preparation requires a scalar binding",
            ));
        }
        if !matches!(
            receipt.facts().argument_control,
            ArgumentControl::Eager | ArgumentControl::TypeOnly
        ) {
            return Err(invalid(
                "guarded, higher-order and relational calls require their own control ABI",
            ));
        }
        let FunctionResultType::Scalar(result) = &input.selected.result_type else {
            return Err(invalid("ordinary scalar binding cannot produce a relation"));
        };
        validate_type_observed(result, &mut work)?;
        for argument in &input.selected.argument_types {
            let FunctionArgumentType::Value(value) = argument else {
                return Err(invalid(
                    "ordinary scalar binding cannot consume lambda arguments",
                ));
            };
            validate_type_observed(value, &mut work)?;
            work.step().map_err(compile_failure)?;
        }
        let parameters = input
            .parameters
            .project(receipt.facts().environment.iter().copied())
            .map_err(|_| invalid("scalar environment is not frozen"))?;
        work.finish().map_err(compile_failure)?;
        Ok(Self {
            function_id: input.function_id.clone(),
            context: input.context,
            decimal_overflow_policy: input.decimal_overflow_policy,
            selected,
            effects: receipt.facts().clone(),
            parameters,
        })
    }
    pub const fn function_id(&self) -> &FunctionId {
        &self.function_id
    }
    pub const fn context(&self) -> ExpressionEffectContext {
        self.context
    }
    pub const fn decimal_overflow_policy(&self) -> DecimalOverflowPolicy {
        self.decimal_overflow_policy
    }
    pub fn selected(&self) -> &FunctionBindingSelection {
        &self.selected
    }
    pub const fn effects(&self) -> &CallEffects {
        &self.effects
    }
    pub const fn parameters(&self) -> &SemanticParameters {
        &self.parameters
    }
    pub fn result_type(&self) -> &FunctionValueType {
        match &self.selected.result_type {
            FunctionResultType::Scalar(result) => result,
            FunctionResultType::Relation(_) => {
                unreachable!("constructor accepts scalar results only")
            }
        }
    }
    pub fn value_argument_types(&self) -> impl ExactSizeIterator<Item = &FunctionValueType> {
        let arguments = if self.effects.argument_control == ArgumentControl::TypeOnly {
            &[][..]
        } else {
            self.selected.argument_types.as_ref()
        };
        arguments.iter().map(|argument| match argument {
            FunctionArgumentType::Value(value) => value,
            FunctionArgumentType::Lambda { .. } => {
                unreachable!("constructor accepts value arguments only")
            }
        })
    }
}

/// Exact ordered selected arguments. Logical identities remain in the checked
/// contract; equal Arrow carriers never reselect an overload or erase them.
#[derive(Clone, Copy, Debug)]
pub struct ScalarCallInput<'call, 'a> {
    contract: &'call ScalarCallContract,
    selection: Selection<'a>,
    arguments: &'a [EvaluatedArgument<'a>],
}
impl<'call, 'a> ScalarCallInput<'call, 'a> {
    pub const fn contract(self) -> &'call ScalarCallContract {
        self.contract
    }
    pub const fn selection(self) -> Selection<'a> {
        self.selection
    }
    pub const fn arguments(self) -> &'a [EvaluatedArgument<'a>] {
        self.arguments
    }
}

/// Immutable implementation preparation can be shared by drivers. It owns no
/// mutable instance, task, connector, credentials or expression lookup. The
/// exact implementation owner supplies this object after pure specialization.
/// Dispatch is once per selected batch, not once per row.
pub trait PreparedScalarKernel: Send + Sync + fmt::Debug {
    fn contract(&self) -> &Arc<ScalarCallContract>;
    /// Complete lifetime retained bound of one boxed instance: inline body and
    /// all bounded owned heap, including growth on error exits. The host
    /// authorizes construction and remaining mutation headroom before any
    /// allocation; post-call checking cannot recover an exceeded hard limit.
    fn instance_retained_upper_bound(&self) -> usize;
    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, ScalarKernelFailure>;
}

/// One exact immutable owner supplies binding validation, effect refinement
/// and pure preparation. Invalid constant row data remains a delayed recipe;
/// cancellation, resource and internal failures are immediate outer failures.
pub trait PureScalarImplementation:
    FunctionBindingResolver + FunctionEffectOwner<Error = FunctionBindingError>
{
    fn prepare_scalar(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedScalarKernel>, ScalarKernelFailure>;
}

#[derive(Debug)]
pub enum ScalarSpecializationFailure {
    Binding(FunctionBindingError),
    Effects(novarocks_type_contract::EffectContractError),
    Control(CompileControlError),
    Kernel(ScalarKernelFailure),
    InvalidInput(&'static str),
}
impl fmt::Display for ScalarSpecializationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Binding(e) => fmt::Display::fmt(e, f),
            Self::Effects(e) => fmt::Display::fmt(e, f),
            Self::Control(e) => fmt::Display::fmt(e, f),
            Self::Kernel(e) => fmt::Display::fmt(e, f),
            Self::InvalidInput(e) => f.write_str(e),
        }
    }
}
impl std::error::Error for ScalarSpecializationFailure {}

/// The compiler's narrow exact-owner entry: no name resolution or runtime
/// capability acquisition. One owner validates, refines and specializes.
pub fn specialize_scalar<O: PureScalarImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    control: &dyn PureCompileControl,
) -> Result<Arc<dyn PreparedScalarKernel>, ScalarSpecializationFailure> {
    control
        .checkpoint(CompilePhase::FunctionSpecialization, 0)
        .map_err(ScalarSpecializationFailure::Control)?;
    let receipt =
        crate::refine_call_effects(owner, input, control).map_err(|error| match error {
            CallEffectRefinementError::Owner(error) => ScalarSpecializationFailure::Binding(error),
            CallEffectRefinementError::Control(error) => {
                ScalarSpecializationFailure::Control(error)
            }
            CallEffectRefinementError::Contract(error) => {
                ScalarSpecializationFailure::Effects(error)
            }
            CallEffectRefinementError::InvalidInput(error) => {
                ScalarSpecializationFailure::InvalidInput(error)
            }
        })?;
    let contract = Arc::new(
        ScalarCallContract::from_refined(input, &receipt, selected, control)
            .map_err(ScalarSpecializationFailure::Kernel)?,
    );
    let prepared = owner
        .prepare_scalar(input, contract.clone(), control)
        .map_err(ScalarSpecializationFailure::Kernel)?;
    if !Arc::ptr_eq(prepared.contract(), &contract) {
        return Err(ScalarSpecializationFailure::Kernel(internal(
            "scalar preparation replaced its exact immutable contract",
        )));
    }
    control
        .checkpoint(CompilePhase::FunctionSpecialization, 0)
        .map_err(ScalarSpecializationFailure::Control)?;
    Ok(prepared)
}

/// Mutable state belongs to one expression use in one evaluation instance.
/// FE folding and each driver instantiate separately; cloning preparation
/// cannot clone or replay successful RNG/warning/wait effects.
pub trait ScalarKernelInstance: Send {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn ScalarEvaluationControl,
    ) -> Result<SelectedValues<'a>, ScalarKernelFailure>;
    /// O(1), exact known retained bytes for host reconciliation. The host
    /// supplies headroom for a bounded mutation before calling evaluate.
    fn retained_bytes(&self) -> usize;
}

/// Runtime wrapper retains the exact preparation and validates the selected
/// carrier on both sides of the implementation. Child errors must already be
/// resolved by the expression controller. Strict NULL demand is also decided
/// there; a strict kernel never receives rows skipped for SQL NULL inputs.
pub struct ScalarEvaluationInstance {
    _prepared: Arc<dyn PreparedScalarKernel>,
    contract: Arc<ScalarCallContract>,
    retained_upper_bound: usize,
    instance: Box<dyn ScalarKernelInstance>,
    failed: bool,
}
impl ScalarEvaluationInstance {
    pub fn instantiate(
        prepared: Arc<dyn PreparedScalarKernel>,
    ) -> Result<Self, ScalarKernelFailure> {
        let contract = Arc::clone(prepared.contract());
        let retained_upper_bound = prepared.instance_retained_upper_bound();
        let instance = prepared.create_instance()?;
        if instance.retained_bytes() > retained_upper_bound {
            return Err(internal(
                "new scalar instance exceeded its authorized bound",
            ));
        }
        Ok(Self {
            _prepared: prepared,
            contract,
            retained_upper_bound,
            instance,
            failed: false,
        })
    }
    pub fn contract(&self) -> &ScalarCallContract {
        &self.contract
    }
    pub fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.instance.retained_bytes()
    }
    /// Shared preparation backing is accounted by its immutable owner. This
    /// bound covers this wrapper's inline body plus its one owned boxed state.
    pub fn retained_upper_bound(&self) -> usize {
        std::mem::size_of::<Self>() + self.retained_upper_bound
    }
    pub fn evaluate<'a>(
        &mut self,
        selection: Selection<'a>,
        arguments: &'a [EvaluatedArgument<'a>],
        control: &dyn ScalarEvaluationControl,
    ) -> Result<SelectedValues<'a>, ScalarKernelFailure> {
        if self.failed {
            return Err(ScalarKernelFailure::InstanceFailed);
        }
        let result = self.evaluate_once(selection, arguments, control);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn evaluate_once<'a>(
        &mut self,
        selection: Selection<'a>,
        arguments: &'a [EvaluatedArgument<'a>],
        control: &dyn ScalarEvaluationControl,
    ) -> Result<SelectedValues<'a>, ScalarKernelFailure> {
        control.checkpoint(0)?;
        let contract = &self.contract;
        let expected = contract.value_argument_types();
        if arguments.len() != expected.len() {
            return Err(invalid(
                "evaluated scalar arguments differ from the exact call shape",
            ));
        }
        let mut work = EvaluationCheckpoints::new(control);
        for (argument, ty) in arguments.iter().zip(expected) {
            argument.validate(selection, &ty.data_type).map_err(|_| {
                invalid("evaluated scalar argument violates its exact selected carrier")
            })?;
            work.step()?;
            if !ty.nullable {
                for (ordinal, row) in selection.iter().enumerate() {
                    if logical_is_null(
                        argument.array().as_ref(),
                        argument.value_row(ordinal, row),
                        1,
                        &mut work,
                    )? {
                        return Err(invalid(
                            "non-null scalar argument contains a selected SQL NULL",
                        ));
                    }
                }
            }
        }
        work.finish()?;
        if selection.is_empty() {
            return SelectedValues::try_new(
                selection,
                &contract.result_type().data_type,
                new_empty_array(&contract.result_type().data_type),
                Box::default(),
            )
            .map_err(|_| internal("empty scalar result violates its contract"));
        }
        let result = self.instance.evaluate(
            ScalarCallInput {
                contract,
                selection,
                arguments,
            },
            control,
        );
        if self.instance.retained_bytes() > self.retained_upper_bound {
            return Err(internal(
                "scalar instance exceeded its lifetime retained bound",
            ));
        }
        let output = result?;
        if !output.errors().is_empty()
            && contract.effects().own_row_error != FunctionIntrinsicRowError::MayRaise
        {
            return Err(internal(
                "never-failing scalar implementation returned a row data error",
            ));
        }
        if output.selection() != selection
            || !novarocks_type_contract::arrow_data_types_exact(
                output.values().data_type(),
                &contract.result_type().data_type,
            )
        {
            return Err(internal(
                "scalar implementation returned an unrelated selection or type",
            ));
        }
        if !contract.result_type().nullable {
            let mut work = EvaluationCheckpoints::new(control);
            let mut errors = output.errors().iter().peekable();
            for row in 0..output.values().len() {
                if errors
                    .peek()
                    .is_some_and(|error| error.selected_ordinal() == row)
                {
                    errors.next();
                    work.step()?;
                } else if logical_is_null(output.values().as_ref(), row, 1, &mut work)? {
                    return Err(internal(
                        "non-null scalar implementation returned a successful SQL NULL",
                    ));
                }
            }
            work.finish()?;
        }
        control.checkpoint(0)?;
        Ok(output)
    }
}
fn invalid(message: &str) -> ScalarKernelFailure {
    ScalarKernelFailure::InvalidProgram(KernelDiagnostic::new(message))
}
fn internal(message: &str) -> ScalarKernelFailure {
    ScalarKernelFailure::Internal(KernelDiagnostic::new(message))
}

fn compile_failure(error: CompileControlError) -> ScalarKernelFailure {
    match error {
        CompileControlError::Cancelled => ScalarKernelFailure::Cancelled,
        CompileControlError::DeadlineExceeded => ScalarKernelFailure::DeadlineExceeded,
        CompileControlError::ResourceExhausted => ScalarKernelFailure::ResourceExhausted,
    }
}
fn type_failure(error: novarocks_type_contract::ValueTypeError) -> ScalarKernelFailure {
    match error {
        novarocks_type_contract::ValueTypeError::TooDeep
        | novarocks_type_contract::ValueTypeError::TooManyNodes => {
            ScalarKernelFailure::ResourceExhausted
        }
        _ => invalid("scalar binding has invalid exact logical type"),
    }
}
struct EvaluationCheckpoints<'a> {
    control: &'a dyn ScalarEvaluationControl,
    pending: u32,
}
impl<'a> EvaluationCheckpoints<'a> {
    fn new(control: &'a dyn ScalarEvaluationControl) -> Self {
        Self {
            control,
            pending: 0,
        }
    }
    fn step(&mut self) -> Result<(), ScalarKernelFailure> {
        self.pending += 1;
        if self.pending == MAX_UNOBSERVED_SCALAR_WORK {
            self.control.checkpoint(self.pending)?;
            self.pending = 0;
        }
        Ok(())
    }
    fn finish(self) -> Result<(), ScalarKernelFailure> {
        self.control.checkpoint(self.pending)
    }
}

/// Logical NULL checks never materialize Arrow's dictionary/union/run-end
/// logical-null bitmap. Every visited row/type node has bounded work control.
fn logical_is_null(
    array: &dyn Array,
    row: usize,
    depth: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<bool, ScalarKernelFailure> {
    work.step()?;
    if depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH || row >= array.len() {
        return Err(internal("scalar result has invalid nested row addressing"));
    }
    if array.is_null(row) {
        return Ok(true);
    }
    match array.data_type() {
        DataType::Null => Ok(true),
        DataType::Dictionary(key, _) => {
            macro_rules! dictionary {
                ($key:ty) => {{
                    let array = array
                        .as_any()
                        .downcast_ref::<DictionaryArray<$key>>()
                        .ok_or_else(|| {
                            internal("scalar dictionary carrier differs from its type")
                        })?;
                    match array.key(row) {
                        Some(key) => logical_is_null(array.values().as_ref(), key, depth + 1, work),
                        None => Ok(true),
                    }
                }};
            }
            match key.as_ref() {
                DataType::Int8 => dictionary!(Int8Type),
                DataType::Int16 => dictionary!(Int16Type),
                DataType::Int32 => dictionary!(Int32Type),
                DataType::Int64 => dictionary!(Int64Type),
                DataType::UInt8 => dictionary!(UInt8Type),
                DataType::UInt16 => dictionary!(UInt16Type),
                DataType::UInt32 => dictionary!(UInt32Type),
                DataType::UInt64 => dictionary!(UInt64Type),
                _ => Err(internal("scalar dictionary key type is invalid")),
            }
        }
        DataType::RunEndEncoded(run_ends, _) => {
            macro_rules! run {
                ($key:ty) => {{
                    let array = array
                        .as_any()
                        .downcast_ref::<RunArray<$key>>()
                        .ok_or_else(|| internal("scalar run-end carrier differs from its type"))?;
                    logical_is_null(
                        array.values().as_ref(),
                        array.get_physical_index(row),
                        depth + 1,
                        work,
                    )
                }};
            }
            match run_ends.data_type() {
                DataType::Int16 => run!(Int16Type),
                DataType::Int32 => run!(Int32Type),
                DataType::Int64 => run!(Int64Type),
                _ => Err(internal("scalar run-end index type is invalid")),
            }
        }
        DataType::Union(fields, _) => {
            let array = array
                .as_any()
                .downcast_ref::<UnionArray>()
                .ok_or_else(|| internal("scalar union carrier differs from its type"))?;
            let type_id = array.type_id(row);
            if !fields.iter().any(|(id, _)| id == type_id) {
                return Err(internal("scalar union type id is invalid"));
            }
            logical_is_null(
                array.child(type_id).as_ref(),
                array.value_offset(row),
                depth + 1,
                work,
            )
        }
        _ => Ok(false),
    }
}

impl From<novarocks_type_contract::ValueTypeError> for ScalarKernelFailure {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        type_failure(error)
    }
}
fn validate_type_observed(
    value: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ScalarKernelFailure> {
    value
        .logical_type
        .validate_carrier(&value.data_type)
        .map_err(type_failure)?;
    novarocks_type_contract::validate_nested_logical_types_observed(&value.data_type, || {
        work.step().map_err(compile_failure)
    })
}

#[cfg(test)]
mod tests;
