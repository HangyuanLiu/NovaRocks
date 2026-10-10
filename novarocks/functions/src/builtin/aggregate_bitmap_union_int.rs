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
//! Real selected/phase host consumer over the ONE original bitmap union mathematics.
use crate::aggregate_format::AggregateFailureStage;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_invocation_backing::HostDiagnostic;
use crate::aggregate_scalar::ScalarStateError;
use crate::bitmap_aggregate_core::{self as core, BitmapAggregatePort, InputFailure};
use crate::bitmap_decode_resources::BitmapDecodeResources;
use crate::bitmap_value::{BitmapAggregateEncodePort, BitmapDecodePort};
use crate::kernel_control::{compile_failure, internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::*;
use allocator_api2::vec::Vec as HostVec;
use arrow_array::{
    Array, ArrayRef, BinaryArray,
    builder::{BinaryBuilder, Int64Builder},
};
use arrow_schema::DataType;
use novarocks_type_contract::CompileCheckpoints;
use std::{fmt, sync::Arc};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BitmapResult {
    Cardinality,
    Encoded,
}
impl BitmapResult {
    fn operation(self) -> core::BitmapOperation {
        match self {
            Self::Cardinality => core::BitmapOperation::Count,
            Self::Encoded => core::BitmapOperation::BitmapProjection,
        }
    }
    fn output_type(self) -> FunctionValueType {
        FunctionValueType::new(
            match self {
                Self::Cardinality => DataType::Int64,
                Self::Encoded => DataType::Binary,
            },
            true,
        )
    }
}
#[derive(Debug)]
pub(super) struct BitmapKernel {
    pub(super) contract: Arc<AggregateCallContract>,
    pub(super) projection: BitmapResult,
}
pub(super) struct BitmapState {
    core: core::BitmapState<HostAggregateAllocator>,
    allocator: HostAggregateAllocator,
    failed: bool,
}
impl BitmapState {
    fn latch(&mut self) {
        self.failed = true;
        self.core = core::BitmapState::new(self.allocator.clone());
    }
}
pub(super) struct BitmapUpdate<'a> {
    input: SelectedAggregateUpdateInput<'a, 'a>,
    mapping: HostVec<usize, HostAggregateAllocator>,
    allocator: HostAggregateAllocator,
    host: Arc<dyn AggregateStateAllocator>,
}
pub(super) struct BitmapMerge<'a> {
    input: SelectedAggregateMergeInput<'a, 'a>,
    mapping: HostVec<usize, HostAggregateAllocator>,
    allocator: HostAggregateAllocator,
    host: Arc<dyn AggregateStateAllocator>,
}
#[derive(Debug)]
enum Failure {
    IgnoredDecoderData,
    DecoderData(HostDiagnostic),
    Kernel(KernelFailure),
}
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IgnoredDecoderData => f.write_str("unpublished original decoder data"),
            Self::DecoderData(data) => f.write_str(data.message()),
            Self::Kernel(cause) => cause.fmt(f),
        }
    }
}
struct Port<'a, 'w, 'c> {
    stage: Option<AggregateFailureStage>,
    allocator: &'a HostAggregateAllocator,
    resources: BitmapDecodeResources,
    work: &'w mut EvaluationCheckpoints<'c>,
}
impl Port<'_, '_, '_> {
    fn diagnostic<T: fmt::Display + ?Sized>(&mut self, message: &T) -> Failure {
        let Some(stage) = self.stage else {
            return Failure::IgnoredDecoderData;
        };
        match HostDiagnostic::prepare(self.allocator, self.work, |writer| {
            write!(writer, "{}", stage.message(message))
        }) {
            Ok(data) => Failure::DecoderData(data),
            Err(cause) => Failure::Kernel(cause),
        }
    }
}
impl BitmapDecodePort for Port<'_, '_, '_> {
    type Error = Failure;
    fn data(&mut self, message: fmt::Arguments<'_>) -> Failure {
        self.diagnostic(&message)
    }
    fn is_data(error: &Failure) -> bool {
        !matches!(error, Failure::Kernel(_))
    }
    fn step(&mut self) -> Result<(), Failure> {
        self.work.step().map_err(Failure::Kernel)
    }
    fn boundary(&mut self) -> Result<(), Failure> {
        self.work.flush().map_err(Failure::Kernel)
    }
    fn before_tree_insert(&mut self, n: usize) -> Result<(), Failure> {
        self.resources
            .before_tree_insert(n, self.work)
            .map_err(Failure::Kernel)
    }
    fn before_tree_collection(&mut self, n: usize) -> Result<(), Failure> {
        self.resources
            .before_tree_collection(n, self.work)
            .map_err(Failure::Kernel)
    }
    fn before_roaring(&mut self, n: usize) -> Result<(), Failure> {
        self.resources
            .before_roaring(n, self.work)
            .map_err(Failure::Kernel)
    }
    fn before_u32_collection(&mut self, n: u64) -> Result<(), Failure> {
        self.resources
            .before_u32_collection(n, self.work)
            .map_err(Failure::Kernel)
    }
    fn before_render(&mut self, n: usize) -> Result<(), Failure> {
        self.resources
            .before_render(n, self.work)
            .map_err(Failure::Kernel)
    }
}
impl BitmapAggregateEncodePort for Port<'_, '_, '_> {
    fn before_aggregate_buffer(&mut self, n: usize) -> Result<(), Failure> {
        self.resources
            .before_aggregate_buffer(n, self.work)
            .map_err(Failure::Kernel)
    }
    fn before_aggregate_singleton(&mut self, n: u64) -> Result<(), Failure> {
        self.resources
            .before_aggregate_singleton(n, self.work)
            .map_err(Failure::Kernel)
    }
}
struct Mutation<'a, 'w, 'c> {
    state: &'a mut core::BitmapState<HostAggregateAllocator>,
    port: Port<'a, 'w, 'c>,
}
impl BitmapDecodePort for Mutation<'_, '_, '_> {
    type Error = Failure;
    fn data(&mut self, m: fmt::Arguments<'_>) -> Failure {
        self.port.data(m)
    }
    fn is_data(error: &Failure) -> bool {
        Port::is_data(error)
    }
    fn step(&mut self) -> Result<(), Failure> {
        self.port.step()
    }
    fn boundary(&mut self) -> Result<(), Failure> {
        self.port.boundary()
    }
    fn before_tree_insert(&mut self, n: usize) -> Result<(), Failure> {
        self.port.before_tree_insert(n)
    }
    fn before_tree_collection(&mut self, n: usize) -> Result<(), Failure> {
        self.port.before_tree_collection(n)
    }
    fn before_roaring(&mut self, n: usize) -> Result<(), Failure> {
        self.port.before_roaring(n)
    }
    fn before_u32_collection(&mut self, n: u64) -> Result<(), Failure> {
        self.port.before_u32_collection(n)
    }
    fn before_render(&mut self, n: usize) -> Result<(), Failure> {
        self.port.before_render(n)
    }
}
impl BitmapAggregatePort for Mutation<'_, '_, '_> {
    fn input_failure(&mut self, recipe: InputFailure<'_>) -> Failure {
        // Carrier failures escape update; only decoder-format Data is ignored there.
        let previous = self.port.stage;
        self.port.stage = Some(AggregateFailureStage::Update);
        let failure = self.port.diagnostic(&recipe);
        self.port.stage = previous;
        failure
    }
    fn observe(&mut self, _: usize) -> Result<(), Failure> {
        self.state.observe();
        Ok(())
    }
    fn insert(&mut self, _: usize, value: u64) -> Result<(), Failure> {
        self.state.insert(value).map_err(|error| match error {
            ScalarStateError::Kernel(cause) => Failure::Kernel(cause),
            _ => Failure::Kernel(internal(
                "bitmap real host emitted a foreign allocation failure",
            )),
        })
    }
}
fn observed<T>(
    control: &dyn KernelEvaluationControl,
    operation: impl FnOnce(&mut EvaluationCheckpoints<'_>) -> Result<T, EvaluationFailure>,
) -> Result<T, EvaluationFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = operation(&mut work);
    if result.is_ok() {
        work.finish()?;
    }
    result
}
fn mapped_domain(
    mapping: &[usize],
    selection: Selection<'_>,
    host: Option<Arc<dyn AggregateStateAllocator>>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<
    (
        HostAggregateAllocator,
        HostVec<usize, HostAggregateAllocator>,
        Arc<dyn AggregateStateAllocator>,
    ),
    KernelFailure,
> {
    if mapping.len() != selection.len() {
        return Err(invalid("bitmap mapping differs from actual selection"));
    }
    let host = host.ok_or_else(|| invalid("bitmap requires its actual aggregate host"))?;
    if host.opaque_allocation_host().is_none() {
        return Err(invalid("bitmap requires its actual opaque authority"));
    }
    let allocator = HostAggregateAllocator::try_new(Arc::clone(&host))?;
    let mut owned = HostVec::new_in(allocator.clone());
    if !mapping.is_empty() {
        work.flush()?;
        owned
            .try_reserve_exact(mapping.len())
            .map_err(|_| allocator.take_failure())?;
        work.flush()?;
    }
    for state in mapping {
        owned.push(*state);
        work.step()?;
    }
    Ok((allocator, owned, host))
}
fn publish_mutation(
    failure: Failure,
    allocator: &HostAggregateAllocator,
    contract: &Arc<AggregateCallContract>,
    phase: AggregateInvocationPhase,
    selection: Selection<'_>,
    mapping: &[usize],
    work: &mut EvaluationCheckpoints<'_>,
) -> EvaluationFailure {
    match failure {
        Failure::Kernel(cause) => cause.into(),
        Failure::IgnoredDecoderData => {
            internal("bitmap ignored decoder Data escaped its original update fallback").into()
        }
        Failure::DecoderData(message) => match InvocationData::prepare_aggregate(
            allocator,
            Arc::clone(contract),
            phase,
            selection,
            mapping,
            message,
            work,
        ) {
            Ok(data) => data.into(),
            Err(cause) => cause.into(),
        },
    }
}
fn publish_emission(
    failure: Failure,
    allocator: &HostAggregateAllocator,
    context: &AggregateEmissionContext<'_>,
    work: &mut EvaluationCheckpoints<'_>,
) -> EvaluationFailure {
    match failure {
        Failure::Kernel(cause) => cause.into(),
        Failure::IgnoredDecoderData => {
            internal("bitmap emission discarded its original Data recipe").into()
        }
        Failure::DecoderData(message) => {
            match InvocationData::prepare_emission(allocator, context, message, work) {
                Ok(data) => data.into(),
                Err(cause) => cause.into(),
            }
        }
    }
}
pub(super) fn original_native_profile(
    selected: &FunctionBindingSelection,
    count: usize,
    projection: BitmapResult,
) -> bool {
    let [FunctionArgumentType::Value(source)] = selected.argument_types.as_ref() else {
        return false;
    };
    let FunctionResultType::Scalar(output) = &selected.result_type else {
        return false;
    };
    // The original reader accepts these carriers regardless of their valid nominal domain.
    count == 1
        && matches!(
            source.data_type,
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary
        )
        && output == &projection.output_type()
}
pub(super) fn validate_contract(
    contract: &AggregateCallContract,
    projection: BitmapResult,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    work.step().map_err(compile_failure)?;
    if !original_native_profile(
        contract.call().selected(),
        contract.call().logical_argument_count(),
        projection,
    ) || !contract.order_keys().is_empty()
        || !FunctionValueType::new(DataType::Binary, true)
            .exactly_equals_observed::<KernelFailure>(contract.intermediate_type(), || {
                work.step().map_err(compile_failure)
            })?
    {
        return Err(invalid(
            "bitmap_union_int differs from its original fixed full signature",
        ));
    }
    Ok(())
}
impl BitmapKernel {
    fn check_emit(
        &self,
        context: &AggregateEmissionContext<'_>,
        count: usize,
    ) -> Result<(), KernelFailure> {
        if !Arc::ptr_eq(context.contract(), &self.contract)
            || context.state_indices().len() != count
        {
            return Err(invalid(
                "bitmap emission differs from actual prepared contract or state order",
            ));
        }
        Ok(())
    }
    fn emit<'s, I>(
        &self,
        states: I,
        context: &AggregateEmissionContext<'_>,
        control: &dyn KernelEvaluationControl,
        intermediate: bool,
    ) -> Result<ArrayRef, EvaluationFailure>
    where
        I: ExactSizeIterator<Item = &'s BitmapState>,
    {
        // One original iterator, one lookahead; known failed first state wins
        // before this kernel's entry callback or any output/diagnostic backing.
        let mut states = states.peekable();
        if states.peek().is_some_and(|state| state.failed) {
            return Err(KernelFailure::InstanceFailed.into());
        }
        observed(control, |work| {
            let expected = states.len();
            self.check_emit(context, expected)?;
            if context.contract().phase().produces_final_result() == intermediate {
                return Err(invalid("bitmap emission method differs from its actual phase").into());
            }
            let host = Arc::clone(
                context
                    .allocator()
                    .ok_or_else(|| invalid("bitmap emission requires its actual host"))?,
            );
            let mut allocator: Option<HostAggregateAllocator> = None;
            let stage = if intermediate {
                AggregateFailureStage::BuildIntermediate
            } else {
                AggregateFailureStage::BuildFinal
            };
            let mut count = 0;
            if !intermediate && self.projection == BitmapResult::Cardinality {
                let mut builder = Int64Builder::with_capacity(expected);
                for state in states {
                    if state.failed {
                        return Err(KernelFailure::InstanceFailed.into());
                    }
                    if count >= expected {
                        return Err(internal("bitmap final iterator exceeded actual extent").into());
                    }
                    work.step()?;
                    match state.core.cardinality() {
                        Ok(Some(value)) => builder.append_value(value),
                        Ok(None) => builder.append_null(),
                        Err(recipe) => {
                            if allocator.is_none() {
                                allocator =
                                    Some(HostAggregateAllocator::try_new(Arc::clone(&host))?);
                            }
                            let allocator =
                                allocator.as_ref().expect("admitted emission allocator");
                            let mut port = Port {
                                stage: Some(stage),
                                allocator,
                                resources: BitmapDecodeResources::try_new(Arc::clone(&host))?,
                                work,
                            };
                            let failure = port.diagnostic(&recipe);
                            drop(port);
                            return Err(publish_emission(failure, allocator, context, work));
                        }
                    }
                    count += 1;
                }
                if count != expected {
                    return Err(internal("bitmap final iterator shortened actual extent").into());
                }
                work.flush()?;
                let result = Arc::new(builder.finish()) as ArrayRef;
                work.flush()?;
                return Ok(result);
            }
            let mut builder = BinaryBuilder::new();
            for state in states {
                if state.failed {
                    return Err(KernelFailure::InstanceFailed.into());
                }
                if count >= expected {
                    return Err(
                        internal("bitmap intermediate iterator exceeded actual extent").into(),
                    );
                }
                work.step()?;
                if !state.core.has_value {
                    builder.append_null();
                    count += 1;
                    continue;
                }
                if allocator.is_none() {
                    allocator = Some(HostAggregateAllocator::try_new(Arc::clone(&host))?);
                }
                let allocator = allocator.as_ref().expect("admitted emission allocator");
                let mut port = Port {
                    stage: Some(stage),
                    allocator,
                    resources: BitmapDecodeResources::try_new(Arc::clone(&host))?,
                    work,
                };
                // The original empty FromIterator creates no tree/sort backing.
                if state.core.value_count() != 0 {
                    port.before_tree_collection(state.core.value_count())
                        .map_err(|failure| {
                            publish_emission(failure, allocator, context, port.work)
                        })?;
                }
                let sorted = state.core.encoded_values();
                let encoded =
                    crate::bitmap_value::encode_bitmap_aggregate_with_port(&sorted, &mut port);
                // Source sorted projection dies before its admitted opaque charge.
                drop(sorted);
                let bytes = match encoded {
                    Ok(bytes) => bytes,
                    Err(failure) => {
                        drop(port);
                        return Err(publish_emission(failure, allocator, context, work));
                    }
                };
                port.boundary()
                    .map_err(|failure| publish_emission(failure, allocator, context, port.work))?;
                builder.append_value(&bytes);
                // Original fixed-width payload dies before its operation grant.
                drop(bytes);
                drop(port);
                work.flush()?;
                count += 1;
            }
            if count != expected {
                return Err(
                    internal("bitmap intermediate iterator shortened actual extent").into(),
                );
            }
            work.flush()?;
            let output = Arc::new(builder.finish()) as ArrayRef;
            work.flush()?;
            Ok(output)
        })
    }
}

impl PreparedAggregateKernel for BitmapKernel {
    fn clone_for_local_phase(
        &self,
        contract: Arc<AggregateCallContract>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Arc<Self>, KernelFailure> {
        control
            .checkpoint(
                novarocks_type_contract::CompilePhase::FunctionSpecialization,
                0,
            )
            .map_err(crate::kernel_control::compile_failure)?;
        Ok(Arc::new(Self {
            contract,
            projection: self.projection,
        }))
    }
    type State = BitmapState;
    type PreparedUpdateBatch<'a> = BitmapUpdate<'a>;
    type PreparedMergeBatch<'a> = BitmapMerge<'a>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::AllocationTracked
    }
    fn retained_bytes(&self, state: &Self::State) -> usize {
        state.allocator.metadata_bytes()
    }
    fn has_invocation_data(&self) -> bool {
        true
    }
    fn requires_emission_context(&self) -> bool {
        true
    }
    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        control.checkpoint(0)?;
        Err(invalid("bitmap_union_int requires its host allocator"))
    }
    fn create_state_with_allocator(
        &self,
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        control.checkpoint(0)?;
        let host = allocator.ok_or_else(|| invalid("bitmap requires its actual aggregate host"))?;
        if host.opaque_allocation_host().is_none() {
            return Err(invalid("bitmap requires its actual opaque authority"));
        }
        let allocator = HostAggregateAllocator::try_new(host)?;
        Ok(BitmapState {
            core: core::BitmapState::new(allocator.clone()),
            allocator,
            failed: false,
        })
    }

    fn prepare_update<'a>(
        &'a self,
        _input: SelectedAggregateUpdateInput<'a, 'a>,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'a>, KernelFailure> {
        Err(invalid(
            "bitmap_union_int requires the lossless aggregate update port",
        ))
    }
    fn update_row<'a>(
        &self,
        _state: &mut Self::State,
        _input: &Self::PreparedUpdateBatch<'a>,
        _ordinal: usize,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        Err(invalid(
            "bitmap_union_int requires the lossless aggregate update port",
        ))
    }
    fn prepare_merge<'a>(
        &'a self,
        _input: SelectedAggregateMergeInput<'a, 'a>,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'a>, KernelFailure> {
        Err(invalid(
            "bitmap_union_int requires the lossless aggregate merge port",
        ))
    }
    fn merge_row<'a>(
        &self,
        _state: &mut Self::State,
        _input: &Self::PreparedMergeBatch<'a>,
        _ordinal: usize,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        Err(invalid(
            "bitmap_union_int requires the lossless aggregate merge port",
        ))
    }
    fn build_intermediate<'s, I>(
        &self,
        _states: I,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        Self::State: 's,
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        Err(invalid(
            "bitmap_union_int requires its real emission context",
        ))
    }
    fn build_final<'s, I>(
        &self,
        _states: I,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        Self::State: 's,
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        Err(invalid(
            "bitmap_union_int requires its real emission context",
        ))
    }
    fn prepare_update_evaluation<'a>(
        &'a self,
        input: SelectedAggregateUpdateInput<'a, 'a>,
        mapping: &[usize],
        host: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<BitmapUpdate<'a>, EvaluationFailure> {
        observed(control, |work| {
            if !std::ptr::eq(input.contract(), self.contract.as_ref())
                || !self.contract.phase().consumes_logical_arguments()
                || input.logical_arguments().len() != 1
                || !input.order_arguments().is_empty()
            {
                return Err(
                    invalid("bitmap update differs from actual phase or logical channels").into(),
                );
            }
            work.step()?;
            let (allocator, mapping, host) = mapped_domain(mapping, input.selection(), host, work)?;
            Ok(BitmapUpdate {
                input,
                mapping,
                allocator,
                host,
            })
        })
    }
    fn prepare_merge_evaluation<'a>(
        &'a self,
        input: SelectedAggregateMergeInput<'a, 'a>,
        mapping: &[usize],
        host: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<BitmapMerge<'a>, EvaluationFailure> {
        observed(control, |work| {
            if !std::ptr::eq(input.contract(), self.contract.as_ref())
                || self.contract.phase().consumes_logical_arguments()
                || input.state().array().data_type() != &DataType::Binary
            {
                return Err(
                    invalid("bitmap merge differs from actual phase or Binary state").into(),
                );
            }
            work.step()?;
            let (allocator, mapping, host) = mapped_domain(mapping, input.selection(), host, work)?;
            Ok(BitmapMerge {
                input,
                mapping,
                allocator,
                host,
            })
        })
    }
    fn prepared_update_retained_bytes(&self, p: &BitmapUpdate<'_>) -> usize {
        p.allocator.metadata_bytes() + p.mapping.capacity() * std::mem::size_of::<usize>()
    }
    fn prepared_merge_retained_bytes(&self, p: &BitmapMerge<'_>) -> usize {
        p.allocator.metadata_bytes() + p.mapping.capacity() * std::mem::size_of::<usize>()
    }
    fn update_row_evaluation<'a>(
        &self,
        state: &mut BitmapState,
        p: &BitmapUpdate<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), EvaluationFailure> {
        if state.failed {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let result = observed(control, |work| {
            let row = p
                .input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("bitmap update selected ordinal is absent"))?;
            work.step()?;
            let argument = p.input.logical_arguments()[0];
            let address = argument.value_row(ordinal, row);
            if address >= argument.array().len() {
                return Err(
                    internal("bitmap update selected source address exceeds carrier").into(),
                );
            }
            let mut mutation = Mutation {
                state: &mut state.core,
                port: Port {
                    stage: None,
                    allocator: &p.allocator,
                    resources: BitmapDecodeResources::try_new(Arc::clone(&p.host))?,
                    work,
                },
            };
            let result = core::update(
                self.projection.operation(),
                argument.array(),
                std::iter::once(address),
                &mut mutation,
            );
            drop(mutation);
            result.map_err(|failure| {
                publish_mutation(
                    failure,
                    &p.allocator,
                    &self.contract,
                    AggregateInvocationPhase::Update,
                    p.input.selection(),
                    &p.mapping,
                    work,
                )
            })
        });
        if result.is_err() {
            state.latch();
        }
        result
    }
    fn merge_row_evaluation<'a>(
        &self,
        state: &mut BitmapState,
        p: &BitmapMerge<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), EvaluationFailure> {
        if state.failed {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let result = observed(control, |work| {
            let row = p
                .input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("bitmap merge selected ordinal is absent"))?;
            work.step()?;
            let argument = p.input.state();
            let address = argument.value_row(ordinal, row);
            if address >= argument.array().len() {
                return Err(
                    internal("bitmap merge selected source address exceeds carrier").into(),
                );
            }
            let array = argument
                .array()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    internal("bitmap admitted merge Binary carrier has another shape")
                })?;
            let mut mutation = Mutation {
                state: &mut state.core,
                port: Port {
                    stage: Some(AggregateFailureStage::Merge),
                    allocator: &p.allocator,
                    resources: BitmapDecodeResources::try_new(Arc::clone(&p.host))?,
                    work,
                },
            };
            let result = core::merge(array, std::iter::once(address), &mut mutation);
            drop(mutation);
            result.map_err(|failure| {
                publish_mutation(
                    failure,
                    &p.allocator,
                    &self.contract,
                    AggregateInvocationPhase::Merge,
                    p.input.selection(),
                    &p.mapping,
                    work,
                )
            })
        });
        if result.is_err() {
            state.latch();
        }
        result
    }
    fn build_intermediate_evaluation_with_context<'s, I>(
        &self,
        states: I,
        context: &AggregateEmissionContext<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, EvaluationFailure>
    where
        I: ExactSizeIterator<Item = &'s BitmapState>,
    {
        self.emit(states, context, control, true)
    }
    fn build_final_evaluation_with_context<'s, I>(
        &self,
        states: I,
        context: &AggregateEmissionContext<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, EvaluationFailure>
    where
        I: ExactSizeIterator<Item = &'s BitmapState>,
    {
        self.emit(states, context, control, false)
    }
}
#[cfg(test)]
#[path = "aggregate_bitmap_union_int_tests.rs"]
mod tests;
