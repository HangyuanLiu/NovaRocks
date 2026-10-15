// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Borrowed fragment-local execution type roots. Source addresses are only
//! local lookup keys while the original immutable plan remains borrowed; they
//! never enter wire identity, semantic equality or access authorization.

#[path = "fragment_type_owner/cte_views.rs"]
pub(crate) mod cte_views;
#[path = "fragment_type_owner/original_requests.rs"]
pub(crate) mod original_requests;
#[path = "fragment_type_owner/output_names.rs"]
pub(crate) mod output_names;
#[path = "fragment_type_owner/root_output.rs"]
pub(crate) mod root_output;
#[path = "fragment_type_owner/scan_source_copy.rs"]
pub(crate) mod scan_source_copy;

use crate::execution_type_table::{
    EncodedExecutionTypeTable, ExecutionTypeRoot, ExecutionTypeTableBuilder,
    ExecutionTypeTableLimits,
};
use crate::host_projection_v2::{AdmissionRefusal, ProjectionFailure};
use crate::value_type_table::LogicalTypeCodecError;
use crate::{PhysicalEncodeError, physical_encode::PhysicalV1PrivateFacts};
use novarocks_physical_plan::{
    AggregateBinding, BoundFunction, ExprKind, Fragment, FunctionArgumentType, NodeKind,
    ResultPort, ValueType,
};
use novarocks_proto_models::{common, plan};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, ValueTypeStorageLifetime};
use std::alloc::Layout;

pub(crate) type TypeEncodingResult<T, H> = Result<T, ProjectionFailure<PhysicalEncodeError, H>>;

impl<H> From<novarocks_type_contract::ValueTypeError>
    for ProjectionFailure<PhysicalEncodeError, H>
{
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        invalid(&error.to_string())
    }
}
impl<H> From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for ProjectionFailure<PhysicalEncodeError, H>
{
    fn from(error: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        match error {
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel => {
                invalid("synthetic type storage source model drift")
            }
            _ => control(CompileControlError::ResourceExhausted),
        }
    }
}
impl<H> From<novarocks_physical_plan::RuntimeFilterBindingError>
    for ProjectionFailure<PhysicalEncodeError, H>
{
    fn from(error: novarocks_physical_plan::RuntimeFilterBindingError) -> Self {
        Self::Codec(PhysicalEncodeError::Invalid(error.to_string()))
    }
}
impl<H> From<crate::physical_v1::PhysicalV1PreflightError>
    for ProjectionFailure<PhysicalEncodeError, H>
{
    fn from(error: crate::physical_v1::PhysicalV1PreflightError) -> Self {
        Self::Codec(PhysicalEncodeError::Invalid(error.to_string()))
    }
}
impl<H> From<crate::WireLayoutError> for ProjectionFailure<PhysicalEncodeError, H> {
    fn from(error: crate::WireLayoutError) -> Self {
        match error {
            crate::WireLayoutError::Control(error) => {
                Self::Codec(PhysicalEncodeError::Control(error))
            }
            error => Self::Codec(PhysicalEncodeError::Invalid(error.to_string())),
        }
    }
}
impl<H> From<String> for ProjectionFailure<PhysicalEncodeError, H> {
    fn from(error: String) -> Self {
        Self::Codec(PhysicalEncodeError::Invalid(error))
    }
}
impl<H> From<&str> for ProjectionFailure<PhysicalEncodeError, H> {
    fn from(error: &str) -> Self {
        Self::Codec(PhysicalEncodeError::Invalid(error.into()))
    }
}
impl<H> From<CompileControlError> for ProjectionFailure<PhysicalEncodeError, H> {
    fn from(error: CompileControlError) -> Self {
        Self::Codec(PhysicalEncodeError::Control(error))
    }
}
/// Legacy inline expansion, pure FE grammar shaping, and an explicitly
/// profiled host share one creator body. None of these tags grants capacity.
#[derive(Clone, Copy)]
pub(crate) enum ExecutionTypeEncodingPolicy {
    LegacyInline,
    PureGrammar,
    Explicit(ExecutionTypeTableLimits),
}
impl ExecutionTypeEncodingPolicy {
    pub(crate) fn complete(self) -> bool {
        !matches!(self, Self::LegacyInline)
    }
    pub(crate) fn profile(
        self,
        roots: usize,
    ) -> Result<Option<ExecutionTypeTableLimits>, CompileControlError> {
        match self {
            Self::LegacyInline => Ok(None),
            Self::PureGrammar => {
                ExecutionTypeTableLimits::for_validated_root_count(roots).map(Some)
            }
            Self::Explicit(limits) => Ok(Some(limits)),
        }
    }
}
pub(crate) struct TypeEncodingResources<'c, H> {
    pub(crate) request: &'c mut dyn FnMut(Layout) -> Result<(), AdmissionRefusal<H>>,
    pub(crate) work: CompileCheckpoints<'c>,
    pub(crate) type_policy: ExecutionTypeEncodingPolicy,
    pub(crate) storage: ValueTypeStorageLifetime,
}
impl<H> TypeEncodingResources<'_, H> {
    pub(crate) fn request_fixed<T>(&mut self) -> TypeEncodingResult<(), H> {
        let layout = Layout::new::<T>();
        if layout.size() != 0 {
            (self.request)(layout).map_err(|error| match error {
                AdmissionRefusal::Host(error) => ProjectionFailure::Host(error),
                AdmissionRefusal::Control(error) => control(error),
            })?;
        }
        self.work.flush().map_err(control)
    }

    pub(crate) fn vector<T>(&mut self, count: usize) -> TypeEncodingResult<Vec<T>, H> {
        vector(count, &mut |layout| (self.request)(layout), &mut self.work)
    }
    pub(crate) fn copy_string(&mut self, text: &str) -> TypeEncodingResult<String, H> {
        let mut bytes = self.vector::<u8>(text.len())?;
        for byte in text.bytes() {
            self.work.step().map_err(control)?;
            bytes.push(byte);
        }
        // The copied bytes originate from UTF-8 and the allocation is moved.
        Ok(String::from_utf8(bytes).expect("borrowed UTF-8 remains valid"))
    }
    pub(crate) fn copy_slice<T: Copy>(&mut self, source: &[T]) -> TypeEncodingResult<Vec<T>, H> {
        let mut output = self.vector(source.len())?;
        for value in source {
            self.work.step()?;
            output.push(*value);
        }
        Ok(output)
    }
    pub(crate) fn boxed<T>(&mut self, value: T) -> TypeEncodingResult<Box<T>, H> {
        let layout = Layout::new::<T>();
        if layout.size() != 0 {
            (self.request)(layout).map_err(|e| match e {
                AdmissionRefusal::Host(e) => ProjectionFailure::Host(e),
                AdmissionRefusal::Control(e) => control(e),
            })?;
        }
        self.work.flush().map_err(control)?;
        let result = Box::new(value);
        self.work.step().map_err(control)?;
        Ok(result)
    }
}

type Failure<H> = ProjectionFailure<PhysicalEncodeError, H>;
fn control<H>(error: CompileControlError) -> Failure<H> {
    ProjectionFailure::Codec(PhysicalEncodeError::Control(error))
}
fn invalid<H>(message: &str) -> Failure<H> {
    ProjectionFailure::Codec(PhysicalEncodeError::Invalid(message.into()))
}
pub(crate) fn project<H>(error: ProjectionFailure<LogicalTypeCodecError, H>) -> Failure<H> {
    match error {
        ProjectionFailure::Host(error) => ProjectionFailure::Host(error),
        ProjectionFailure::Codec(LogicalTypeCodecError::Control(error)) => control(error),
        ProjectionFailure::Codec(error) => invalid(&error.to_string()),
    }
}
fn vector<T, H>(
    count: usize,
    request: &mut impl FnMut(Layout) -> Result<(), AdmissionRefusal<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<T>, Failure<H>> {
    let mut context = crate::value_type_table::resources::Context {
        observe_scope: None,
        request,
        work,
    };
    context.vector(count).map_err(project)
}

pub(crate) struct PreparedFragmentTypes<'a> {
    // ID assignment follows source traversal, independently of address order.
    roots: Vec<ExecutionTypeRoot<'a>>,
    lookup: Vec<(usize, u32)>,
    table: Option<EncodedExecutionTypeTable>,
    synthetic: &'a PreparedSyntheticTypes,
    original: &'a original_requests::PreparedOriginalRequests<'a>,
}
impl<'a> PreparedFragmentTypes<'a> {
    pub(crate) fn prepare<H>(
        fragment: &'a Fragment,
        synthetic: &'a PreparedSyntheticTypes,
        original: &'a original_requests::PreparedOriginalRequests<'a>,
        result: Option<&'a ResultPort>,
        facts: &'a impl PhysicalV1PrivateFacts,
        policy: ExecutionTypeEncodingPolicy,
        request: &mut impl FnMut(Layout) -> Result<(), AdmissionRefusal<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, Failure<H>> {
        let mut count = 0usize;
        visit(
            fragment,
            synthetic,
            original,
            result,
            facts,
            work,
            &mut |_, _| {
                count = count
                    .checked_add(1)
                    .ok_or_else(|| control(CompileControlError::ResourceExhausted))?;
                Ok(())
            },
        )?;
        let limits = policy
            .profile(count)?
            .ok_or_else(|| invalid("fragment type owner requires complete encoding policy"))?;
        if count > limits.max_bindings || count > u32::MAX as usize {
            return Err(invalid(
                "fragment execution type roots exceed caller profile",
            ));
        }
        let mut roots = vector(count, request, work)?;
        let mut lookup = vector(count, request, work)?;
        visit(
            fragment,
            synthetic,
            original,
            result,
            facts,
            work,
            &mut |value, work| {
                work.step().map_err(control)?;
                let depth = crate::physical_type::value_layout_wire_depth_observed(
                    value.data_type(),
                    &mut || work.step().map_err(control),
                )?;
                // DistributedPlan / PlanFragment / ExecutionTypeTable /
                // ExecutionValueBinding precede its ArrowPhysicalType.
                if depth.saturating_add(4) > crate::physical_expr::NATIVE_V1_MAX_WIRE_NESTING {
                    return Err(invalid(
                        "fragment value layout exceeds protobuf nesting profile",
                    ));
                }
                let id = roots.len() as u32;
                roots.push(ExecutionTypeRoot { id, value });
                lookup.push((value as *const ValueType as usize, id));
                Ok(())
            },
        )?;
        sort_ord_observed(&mut lookup, work).map_err(control)?;
        let table = ExecutionTypeTableBuilder::new(&roots)
            .encode_observed(limits, request, work)
            .map_err(project)?;
        Ok(Self {
            roots,
            lookup,
            table: Some(table),
            synthetic,
            original,
        })
    }
    pub(crate) fn reference<E: From<PhysicalEncodeError>>(
        &self,
        value: &ValueType,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<common::TypeDesc, E> {
        let address = value as *const ValueType as usize;
        let (mut low, mut high) = (0, self.lookup.len());
        while low < high {
            observe()?;
            let middle = low + (high - low) / 2;
            if self.lookup[middle].0 < address {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        observe()?;
        let Some((actual, id)) = self.lookup.get(low) else {
            return Err(PhysicalEncodeError::Invalid(
                "execution type source is absent from its fragment owner".into(),
            )
            .into());
        };
        if *actual != address {
            return Err(PhysicalEncodeError::Invalid(
                "execution type source is absent from its fragment owner".into(),
            )
            .into());
        }
        debug_assert!(std::ptr::eq(self.roots[*id as usize].value, value));
        Ok(common::TypeDesc {
            value_binding_id: Some(*id),
        })
    }
    pub(crate) fn request_reference<H>(
        &self,
        definition: novarocks_physical_plan::PhysicalCallDefinition,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<u32, H> {
        self.original.reference(definition, resources)
    }
    pub(crate) fn emit_original_requests<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<novarocks_proto_models::call_request::FragmentCallRequests, H> {
        self.original.emit(self, resources)
    }
    pub(crate) fn emit_pure_constants<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<
        (
            Vec<novarocks_proto_models::call_request::IpcConstantPool>,
            novarocks_proto_models::physical_type_v2::SourceFieldTable,
        ),
        H,
    > {
        self.original.emit_pure_constants(self, resources)
    }
    pub(crate) fn take_wire(&mut self) -> plan::ExecutionTypeTable {
        self.table
            .take()
            .expect("one fragment owns one type table")
            .into_wire()
    }
    pub(crate) fn predicate_type<E: From<PhysicalEncodeError>>(
        &self,
        node: novarocks_physical_plan::NodeId,
        observe: impl FnMut() -> Result<(), E>,
    ) -> Result<&ValueType, E> {
        self.synthetic.predicate_type(node, observe)
    }
}

pub(crate) fn sort_ord_observed<T: Ord>(
    values: &mut [T],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), CompileControlError> {
    fn sift<T: Ord>(
        values: &mut [T],
        mut root: usize,
        end: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), CompileControlError> {
        loop {
            work.step()?;
            let Some(mut child) = root.checked_mul(2).and_then(|v| v.checked_add(1)) else {
                break;
            };
            if child >= end {
                break;
            }
            if child + 1 < end {
                work.step()?;
                if values[child] < values[child + 1] {
                    child += 1;
                }
            }
            work.step()?;
            if values[root] >= values[child] {
                break;
            }
            values.swap(root, child);
            root = child;
        }
        Ok(())
    }
    for root in (0..values.len() / 2).rev() {
        sift(values, root, values.len(), work)?;
    }
    for end in (1..values.len()).rev() {
        work.step()?;
        values.swap(0, end);
        sift(values, 0, end, work)?;
    }
    Ok(())
}

fn arguments<'a, H>(
    args: &'a [FunctionArgumentType],
    work: &mut CompileCheckpoints<'_>,
    emit: &mut impl FnMut(&'a ValueType, &mut CompileCheckpoints<'_>) -> Result<(), Failure<H>>,
) -> Result<(), Failure<H>> {
    for arg in args {
        work.step().map_err(control)?;
        match arg {
            FunctionArgumentType::Value(value) => emit(value, work)?,
            FunctionArgumentType::Lambda {
                parameter_types,
                result_type,
            } => {
                for value in parameter_types {
                    work.step().map_err(control)?;
                    emit(value, work)?;
                }
                emit(result_type, work)?;
            }
        }
    }
    Ok(())
}
fn function<'a, H>(
    function: &'a BoundFunction,
    work: &mut CompileCheckpoints<'_>,
    emit: &mut impl FnMut(&'a ValueType, &mut CompileCheckpoints<'_>) -> Result<(), Failure<H>>,
) -> Result<(), Failure<H>> {
    arguments(&function.argument_types, work, emit)?;
    emit(&function.result_type, work)
}
fn aggregate<'a, H>(
    binding: &'a AggregateBinding,
    work: &mut CompileCheckpoints<'_>,
    emit: &mut impl FnMut(&'a ValueType, &mut CompileCheckpoints<'_>) -> Result<(), Failure<H>>,
) -> Result<(), Failure<H>> {
    function(&binding.function, work, emit)?;
    emit(&binding.intermediate_type, work)
}
fn visit<'a, H>(
    fragment: &'a Fragment,
    synthetic: &'a PreparedSyntheticTypes,
    original: &original_requests::PreparedOriginalRequests<'a>,
    result: Option<&'a ResultPort>,
    facts: &'a impl PhysicalV1PrivateFacts,
    work: &mut CompileCheckpoints<'_>,
    emit: &mut impl FnMut(&'a ValueType, &mut CompileCheckpoints<'_>) -> Result<(), Failure<H>>,
) -> Result<(), Failure<H>> {
    for value in fragment.values().values() {
        work.step().map_err(control)?;
        emit(&value.ty, work)?;
    }
    for (_, expression) in fragment.expressions().iter() {
        work.step().map_err(control)?;
        emit(&expression.ty, work)?;
        match &expression.kind {
            ExprKind::FunctionCall {
                function: bound, ..
            } => function(bound, work, emit)?,
            ExprKind::WindowCall {
                function: bound,
                aggregate_binding,
                ..
            } => {
                function(bound, work, emit)?;
                if let Some(binding) = aggregate_binding {
                    aggregate(binding, work, emit)?;
                }
            }
            ExprKind::Lambda {
                parameter_types, ..
            } => {
                for value in parameter_types {
                    work.step().map_err(control)?;
                    emit(value, work)?;
                }
            }
            _ => {}
        }
    }
    for node in fragment.nodes().values() {
        work.step().map_err(control)?;
        if let Some((_, calls)) = node.kind.aggregate_contract() {
            for call in calls {
                work.step().map_err(control)?;
                aggregate(&call.binding, work, emit)?;
            }
        }
        match &node.kind {
            NodeKind::Scan { relation, .. } => {
                for field in relation.schema() {
                    work.step().map_err(control)?;
                    emit(&field.ty, work)?;
                }
                // Exact frozen private projections remain genuine borrowed
                // type authors; provider attributes never become semantics.
                work.flush().map_err(control)?;
                let scan = facts.scan_fact(fragment.id(), node.id);
                work.step().map_err(control)?;
                work.flush().map_err(control)?;
                if let Some(scan) = scan {
                    for column in scan.columns.iter().chain(scan.table_columns.iter()) {
                        work.step().map_err(control)?;
                        emit(&column.ty, work)?;
                    }
                }
            }
            NodeKind::TableFunction { function, .. } => {
                arguments(&function.argument_types, work, emit)?;
                for value in &function.result_types {
                    work.step().map_err(control)?;
                    emit(value, work)?;
                }
            }
            NodeKind::TableWriter { target } => {
                for field in &target.target_fields {
                    work.step().map_err(control)?;
                    emit(&field.ty, work)?;
                }
                for field in &target.output_schema.fields {
                    work.step().map_err(control)?;
                    emit(&field.ty, work)?;
                }
                for call in &target.partial_aggregates {
                    work.step().map_err(control)?;
                    aggregate(&call.binding, work, emit)?;
                }
            }
            NodeKind::TableFinish(spec) => {
                for field in spec
                    .input_schema
                    .fields
                    .iter()
                    .chain(spec.output_schema.fields.iter())
                {
                    work.step().map_err(control)?;
                    emit(&field.ty, work)?;
                }
                for call in &spec.final_aggregates {
                    work.step().map_err(control)?;
                    aggregate(&call.binding, work, emit)?;
                }
            }
            _ => {}
        }
    }
    original.visit_types(work, emit)?;
    for (_, value) in &synthetic.predicates {
        work.step().map_err(control)?;
        emit(value, work)?;
    }
    if let Some(result) = result.filter(|r| r.fragment == fragment.id()) {
        for field in &result.fields {
            work.step().map_err(control)?;
            emit(&field.ty, work)?;
        }
    }
    Ok(())
}

/// An independently decoded schema owns its local semantic namespace. The
/// input iterator borrows genuine type authors; no Arrow field or presentation
/// domain is used to reconstruct a semantic root.
pub(crate) fn encode_schema<'a, H>(
    fields: impl ExactSizeIterator<Item = (u32, &'a str, &'a ValueType)> + Clone,
    internal: bool,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<plan::ArrowPhysicalSchema, H> {
    let mut limits = resources
        .type_policy
        .profile(fields.len())?
        .ok_or_else(|| invalid("typed schema has no complete encoding policy"))?;
    if fields.len() > limits.max_bindings {
        return Err(invalid("schema roots exceed caller binding profile"));
    }
    let mut name_bytes = 0usize;
    for (_, name, value) in fields.clone() {
        resources.work.step()?;
        if name.len() > novarocks_type_contract::MAX_ARROW_FIELD_NAME_BYTES {
            return Err(invalid("schema field name exceeds exact source profile"));
        }
        name_bytes = name_bytes
            .checked_add(name.len())
            .ok_or(CompileControlError::ResourceExhausted)?;
        for _ in name.bytes() {
            resources.work.step()?;
        }
        let depth =
            crate::physical_type::value_layout_wire_depth_observed(value.data_type(), &mut || {
                resources.work.step()
            })?;
        if depth.saturating_add(4) > crate::physical_expr::NATIVE_V1_MAX_WIRE_NESTING {
            return Err(invalid(
                "schema value layout exceeds protobuf nesting profile",
            ));
        }
    }
    limits.max_layout_string_bytes = limits
        .max_layout_string_bytes
        .checked_sub(name_bytes)
        .ok_or_else(|| invalid("schema names exceed caller layout string profile"))?;
    let mut roots = resources.vector(fields.len())?;
    for (id, (_, _, value)) in fields.clone().enumerate() {
        resources.work.step().map_err(control)?;
        roots.push(ExecutionTypeRoot {
            id: u32::try_from(id).map_err(|_| control(CompileControlError::ResourceExhausted))?,
            value,
        });
    }
    let table = ExecutionTypeTableBuilder::new(&roots)
        .encode_observed(
            limits,
            &mut |layout| (resources.request)(layout),
            &mut resources.work,
        )
        .map_err(project)?
        .into_wire();
    let mut columns = resources.vector::<plan::ArrowPhysicalColumn>(fields.len())?;
    for ((slot_id, name, value), binding) in fields.zip(table.value_bindings) {
        resources.work.step().map_err(control)?;
        columns.push(plan::ArrowPhysicalColumn {
            slot_id,
            is_internal: internal,
            field: Some(plan::ArrowPhysicalField {
                name: resources.copy_string(name)?,
                nullable: value.nullable(),
                r#type: binding
                    .value_layout
                    .map(|layout| resources.boxed(layout))
                    .transpose()?,

                logical_type_id: binding.logical_type_id,
            }),
        });
    }
    Ok(plan::ArrowPhysicalSchema {
        columns,

        logical_types: table.logical_types,
    })
}

/// Wire shaping introduces Boolean roots for multi-predicate filters. These
/// sources are owned and frozen before the borrowed reference table is built.
/// Their creator is the explicit caller storage, while their layout remains
/// shared with the validated original Boolean author.
pub(crate) struct PreparedSyntheticTypes {
    predicates: Vec<(novarocks_physical_plan::NodeId, ValueType)>,
}
impl PreparedSyntheticTypes {
    pub(crate) fn prepare<H>(
        fragment: &Fragment,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        let mut count = 0usize;
        for node in fragment.nodes().values() {
            resources.work.step()?;
            if matches!(&node.kind,NodeKind::Filter{predicates} if predicates.len()>1) {
                count = count
                    .checked_add(1)
                    .ok_or(CompileControlError::ResourceExhausted)?;
            }
        }
        let mut predicates = resources.vector(count)?;
        for node in fragment.nodes().values() {
            resources.work.step()?;
            let NodeKind::Filter {
                predicates: arguments,
            } = &node.kind
            else {
                continue;
            };
            if arguments.len() < 2 {
                continue;
            }
            let first = fragment
                .expressions()
                .get(arguments[0])
                .ok_or_else(|| invalid("filter Boolean author is missing"))?;
            let mut nullable = false;
            for argument in arguments {
                resources.work.step()?;
                let value = fragment
                    .expressions()
                    .get(*argument)
                    .ok_or_else(|| invalid("filter Boolean author is missing"))?;
                if value.ty.data_type() != &arrow::datatypes::DataType::Boolean {
                    return Err(invalid(
                        "filter Boolean author has a different exact carrier",
                    ));
                }
                nullable |= value.ty.nullable();
            }
            resources.work.step()?;
            let storage = resources.storage.clone();
            let TypeEncodingResources { request, work, .. } = resources;
            let value = first.ty.with_nullable_observed(
                nullable,
                storage,
                &mut |layout| {
                    request(layout).map_err(|e| match e {
                        AdmissionRefusal::Host(e) => ProjectionFailure::Host(e),
                        AdmissionRefusal::Control(e) => control(e),
                    })
                },
                &mut |units| {
                    for _ in 0..units {
                        work.step().map_err(control)?;
                    }
                    Ok(())
                },
            )?;
            predicates.push((node.id, value));
        }
        Ok(Self { predicates })
    }
    fn predicate_type<E: From<PhysicalEncodeError>>(
        &self,
        node: novarocks_physical_plan::NodeId,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<&ValueType, E> {
        let (mut low, mut high) = (0, self.predicates.len());
        while low < high {
            observe()?;
            let middle = low + (high - low) / 2;
            if self.predicates[middle].0 < node {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        observe()?;
        self.predicates
            .get(low)
            .filter(|(id, _)| *id == node)
            .map(|(_, value)| value)
            .ok_or_else(|| {
                PhysicalEncodeError::Invalid(
                    "filter Boolean source is absent from its preparation owner".into(),
                )
                .into()
            })
    }
}

macro_rules! admitted_box {
    ($resources:expr,$ty:ty,$value:expr) => {{
        $resources.request_fixed::<$ty>()?;
        let value = Box::new($value);
        $resources.work.step()?;
        value
    }};
}
pub(crate) use admitted_box;
macro_rules! admitted_vec {
    ($resources:expr,$count:expr,$iterator:expr) => {{
        let mut output = $resources.vector($count)?;
        for entry in $iterator {
            let result: TypeEncodingResult<_, H> = entry;
            output.push(result?);
        }
        output
    }};
}
pub(crate) use admitted_vec;
