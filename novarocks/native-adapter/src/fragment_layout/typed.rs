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

//! Explicit owner-local binding projection. Inline types remain a separate
//! expansion adapter; this path never imports metadata or retags nullability.

use super::NativeOutputLayout;
use arrow::datatypes::Field;
use novarocks_execution::exec::{
    chunk::{ArrayNormalizationStep, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema},
    fragment::program::{ExchangeInputContract, FragmentNodeId},
};
use novarocks_plan_codec::{
    execution_type_table::DecodedExecutionTypeTable,
    host_projection_v2::{AdmissionRefusal, ProjectionFailure},
    value_type_table::LogicalTypeCodecError,
};
use novarocks_proto_codec::{FieldPath, FieldPathSegment, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::{common, plan};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, MAX_ARROW_FIELD_NAME_BYTES, MAX_VALUE_TYPE_DEPTH,
    MAX_VALUE_TYPE_NODES, TypedValueBinding, ValueTypeStorageLifetime,
    owned_resources::layout::{LayoutResourceError, arc_layout},
};
use novarocks_types::SlotId;
use std::{alloc::Layout, cell::RefCell, fmt, sync::Arc};

#[derive(Debug)]
pub enum NativeLayoutProjectionError {
    Protocol(ProtocolError),
    TypeTable {
        path: FieldPath,
        source: LogicalTypeCodecError,
    },
    Control(CompileControlError),
    Resource(LayoutResourceError),
}
impl fmt::Display for NativeLayoutProjectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(e) => e.fmt(f),
            Self::TypeTable { path, source } => {
                write!(f, "native execution binding at {path}: {source}")
            }
            Self::Control(e) => e.fmt(f),
            Self::Resource(e) => write!(f, "native layout resource source: {e:?}"),
        }
    }
}
impl std::error::Error for NativeLayoutProjectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Protocol(e) => Some(e),
            Self::TypeTable { source, .. } => Some(source),
            Self::Control(e) => Some(e),
            Self::Resource(_) => None,
        }
    }
}
type Failure<H> = ProjectionFailure<NativeLayoutProjectionError, H>;
fn control<H>(error: CompileControlError) -> Failure<H> {
    ProjectionFailure::Codec(NativeLayoutProjectionError::Control(error))
}
fn protocol<H>(path: FieldPath, kind: ProtocolErrorKind, detail: impl Into<String>) -> Failure<H> {
    ProjectionFailure::Codec(NativeLayoutProjectionError::Protocol(ProtocolError::new(
        path, kind, detail,
    )))
}
fn resource<H>(error: LayoutResourceError) -> Failure<H> {
    ProjectionFailure::Codec(NativeLayoutProjectionError::Resource(error))
}
// FieldPath::field/index clone its exact-length Vec then push one segment.
// Observe that locked source operation before invoking it; MapKey strings in
// the caller's prefix are cloned independently. Derived segments own no text.
fn path_child<H>(
    path: &FieldPath,
    child: FieldPathSegment,
    port: &mut impl FnMut(Layout) -> Result<(), AdmissionRefusal<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FieldPath, Failure<H>> {
    if !novarocks_type_contract::owned_resources::profile::LOCKED_TOOLCHAIN {
        return Err(resource(LayoutResourceError::SourceModel));
    }
    let count = path.segments().len();
    if count != 0 {
        request(
            Layout::array::<FieldPathSegment>(count)
                .map_err(|_| control(CompileControlError::ResourceExhausted))?,
            port,
            work,
        )?;
    }
    for segment in path.segments() {
        work.step().map_err(control)?;
        if let FieldPathSegment::MapKey(key) = segment
            && !key.is_empty()
        {
            request(
                Layout::array::<u8>(key.len())
                    .map_err(|_| control(CompileControlError::ResourceExhausted))?,
                port,
                work,
            )?;
        }
    }
    let capacity = count
        .checked_mul(2)
        .and_then(|n| count.checked_add(1).map(|next| n.max(next).max(4)))
        .ok_or_else(|| control(CompileControlError::ResourceExhausted))?;
    request(
        Layout::array::<FieldPathSegment>(capacity)
            .map_err(|_| control(CompileControlError::ResourceExhausted))?,
        port,
        work,
    )?;
    Ok(match child {
        FieldPathSegment::Field(name) => path.field(name),
        FieldPathSegment::Index(index) => path.index(index),
        FieldPathSegment::MapKey(_) => unreachable!("derived native paths contain no owned labels"),
    })
}
fn request<H>(
    layout: Layout,
    port: &mut impl FnMut(Layout) -> Result<(), AdmissionRefusal<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure<H>> {
    work.flush().map_err(control)?;
    port(layout).map_err(|e| match e {
        AdmissionRefusal::Control(e) => control(e),
        AdmissionRefusal::Host(e) => ProjectionFailure::Host(e),
    })
}
fn vector<T, H>(
    count: usize,
    port: &mut impl FnMut(Layout) -> Result<(), AdmissionRefusal<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<T>, Failure<H>> {
    let layout =
        Layout::array::<T>(count).map_err(|_| control(CompileControlError::ResourceExhausted))?;
    if layout.size() != 0 {
        request(layout, port, work)?;
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| control(CompileControlError::ResourceExhausted))?;
    Ok(values)
}

// This local bridge keeps caller failures typed through Chunk's generic
// constructor. Strings are only genuine source-shape diagnostics from Chunk.
enum BuildFailure<H> {
    Shape(String),
    Refusal(Failure<H>),
}
impl<H> From<String> for BuildFailure<H> {
    fn from(value: String) -> Self {
        Self::Shape(value)
    }
}
fn build_failure<H>(error: BuildFailure<H>, path: &FieldPath) -> Failure<H> {
    match error {
        BuildFailure::Shape(detail) => {
            protocol(path.clone(), ProtocolErrorKind::InvalidValue, detail)
        }
        BuildFailure::Refusal(error) => error,
    }
}
fn steps<H>(count: usize, work: &mut CompileCheckpoints<'_>) -> Result<(), Failure<H>> {
    if count == 0 {
        return work.flush().map_err(control);
    }
    for _ in 0..count {
        work.step().map_err(control)?;
    }
    Ok(())
}

/// Materialize one top occurrence from its original complete binding. The
/// caller supplies its real source path and creating lifetime; shared semantic
/// and layout references retain their original creator without a table owner.
pub fn slot_schema_from_binding_typed_observed<H>(
    slot_id: SlotId,
    name: &str,
    binding: &TypedValueBinding,
    unique_id: Option<i32>,
    path: &FieldPath,
    storage: ValueTypeStorageLifetime,
    port: &mut impl FnMut(Layout) -> Result<(), AdmissionRefusal<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ChunkSlotSchema, Failure<H>> {
    work.flush().map_err(control)?;
    if name.len() > MAX_ARROW_FIELD_NAME_BYTES {
        return Err(protocol(
            path.clone().field("name"),
            ProtocolErrorKind::Capacity,
            "native field name exceeds profile",
        ));
    }
    // A ValueLayout contains no owned Dictionary Box or REE projection.
    // DataType::clone shares all Fields/UnionFields/timezone backing here;
    // only this fresh top label and Field holder allocate.
    if !name.is_empty() {
        request(
            Layout::array::<u8>(name.len())
                .map_err(|_| control(CompileControlError::ResourceExhausted))?,
            port,
            work,
        )?;
    }
    request(
        arc_layout(Layout::new::<Field>()).map_err(resource)?,
        port,
        work,
    )?;
    steps(name.len(), work)?;
    work.flush().map_err(control)?;
    let mut owned_name = String::new();
    owned_name
        .try_reserve_exact(name.len())
        .map_err(|_| control(CompileControlError::ResourceExhausted))?;
    owned_name.push_str(name);
    let field = Arc::new(Field::new(
        owned_name,
        binding.data_type().clone(),
        binding.nullable(),
    ));
    let binding = binding.clone_observed(|| work.step().map_err(control))?;
    let slot = ChunkSlotSchema::try_new_typed_observed(
        slot_id,
        field,
        binding,
        unique_id,
        storage.clone(),
        |event| match event {
            ArrayNormalizationStep::InspectConstructionWork { units } => {
                steps(units, work).map_err(BuildFailure::Refusal)
            }
            ArrayNormalizationStep::InspectType(_) => steps(1, work).map_err(BuildFailure::Refusal),
            ArrayNormalizationStep::InspectField { source, name } => {
                let units = source.name().len().checked_add(name.len()).ok_or_else(|| {
                    BuildFailure::Refusal(control(CompileControlError::ResourceExhausted))
                })?;
                steps(units, work).map_err(BuildFailure::Refusal)
            }
            _ => Err(BuildFailure::Shape(
                "validated value layout unexpectedly requires representation reconstruction".into(),
            )),
        },
    )
    .map_err(|error| build_failure(error, path))?;
    Ok(slot)
}

/// The complete cached binding comes solely from this exact explicit owner.
/// New top fields and schema scaffolds retain the caller's creation lifetime;
/// their shared children retain their original immutable binding lifetimes.
pub fn decode_output_layout_typed_observed<H>(
    columns: &[common::OutputColumn],
    path: FieldPath,
    types: &DecodedExecutionTypeTable,
    storage: ValueTypeStorageLifetime,
    port: &mut impl FnMut(Layout) -> Result<(), AdmissionRefusal<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<NativeOutputLayout, Failure<H>> {
    decode_output_layout_typed_core::<H, NoFactory>(
        columns, path, types, storage, None, port, work, false,
    )
}

/// Actual Native callers give the scratch index, each top field, and the
/// schema container independent construction lifetimes on their same tracker.
/// Limits and checkpoints remain on the one original projection body.
pub fn decode_output_layout_typed_with_factory_in<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    columns: &[common::OutputColumn],
    path: FieldPath,
    types: &DecodedExecutionTypeTable,
    factory: &F,
    work: &mut CompileCheckpoints<'_>,
) -> Result<NativeOutputLayout, Failure<H>> {
    use novarocks_type_contract::ValueSchemaCreationScope;
    check_output_header(columns, &path, work)?;
    let mut container = factory.begin_scope().map_err(ProjectionFailure::Host)?;
    let storage = container.storage_lifetime();
    let mut port = |layout| {
        container
            .request_layout(layout)
            .map_err(AdmissionRefusal::Host)
    };
    decode_output_layout_typed_core(
        columns,
        path,
        types,
        storage,
        Some(factory),
        &mut port,
        work,
        true,
    )
}

// Only a compile-time choice for the old explicit unscoped port. It has no
// value and cannot manufacture a scope, control, storage or successful grant.
enum NoFactory {}
impl<H> novarocks_type_contract::ValueSchemaCreationScope<H> for NoFactory {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        match *self {}
    }
    fn request_layout(&mut self, _: Layout) -> Result<(), H> {
        match *self {}
    }
    fn observe_opaque_upper_bound(&mut self, _: usize) -> Result<(), H> {
        match *self {}
    }
}
impl<H> novarocks_type_contract::ValueSchemaCreationFactory<H> for NoFactory {
    type Scope<'a> = NoFactory;
    fn begin_scope(&self) -> Result<Self::Scope<'_>, H> {
        match *self {}
    }
    fn begin_scope_observed<O>(
        &self,
        _: &mut dyn FnMut(Layout) -> Result<(), O>,
    ) -> Result<Self::Scope<'_>, novarocks_type_contract::ValueSchemaScopeBeginFailure<O, H>> {
        match *self {}
    }
}

fn check_output_header<H>(
    columns: &[common::OutputColumn],
    path: &FieldPath,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure<H>> {
    work.flush().map_err(control)?;
    if columns.len() > MAX_VALUE_TYPE_NODES {
        return Err(protocol(
            path.clone(),
            ProtocolErrorKind::Capacity,
            "native output column count exceeds profile",
        ));
    }
    Ok(())
}

fn decode_output_layout_typed_core<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    columns: &[common::OutputColumn],
    path: FieldPath,
    types: &DecodedExecutionTypeTable,
    storage: ValueTypeStorageLifetime,
    factory: Option<&F>,
    port: &mut impl FnMut(Layout) -> Result<(), AdmissionRefusal<H>>,
    work: &mut CompileCheckpoints<'_>,
    header_checked: bool,
) -> Result<NativeOutputLayout, Failure<H>> {
    if !header_checked {
        check_output_header(columns, &path, work)?;
    }
    // Sparse IDs never determine allocation size. Sorting precedes all field
    // construction, so duplicate occurrence errors preserve the wire index.
    use novarocks_type_contract::ValueSchemaCreationScope;
    let mut scratch = match factory {
        Some(factory) => {
            work.flush().map_err(control)?;
            Some(factory.begin_scope().map_err(ProjectionFailure::Host)?)
        }
        None => None,
    };
    let mut ordered = match scratch.as_mut() {
        Some(scope) => vector::<(SlotId, usize), H>(
            columns.len(),
            &mut |layout| scope.request_layout(layout).map_err(AdmissionRefusal::Host),
            work,
        )?,
        None => vector::<(SlotId, usize), H>(columns.len(), port, work)?,
    };
    for (index, column) in columns.iter().enumerate() {
        work.step().map_err(control)?;
        ordered.push((SlotId::new(column.column_id), index));
    }
    sort_observed(&mut ordered, |v| (v.0, v.1), work)?;
    for pair in ordered.windows(2) {
        work.step().map_err(control)?;
        if pair[0].0 == pair[1].0 {
            let index = pair[0].1.max(pair[1].1);
            return Err(protocol(
                path.clone().index(index).field("column_id"),
                ProtocolErrorKind::InconsistentFields,
                format!("duplicate OutputColumn.column_id {}", pair[0].0),
            ));
        }
    }
    drop(ordered);
    drop(scratch);
    let mut slots = vector::<ChunkSlotSchema, H>(columns.len(), port, work)?;
    for (index, column) in columns.iter().enumerate() {
        work.step().map_err(control)?;
        if column.name.len() > MAX_ARROW_FIELD_NAME_BYTES {
            return Err(protocol(
                path.clone().index(index).field("name"),
                ProtocolErrorKind::Capacity,
                "output field name exceeds profile",
            ));
        }
        let reference = column.r#type.as_ref().ok_or_else(|| {
            protocol(
                path.clone().index(index).field("type"),
                ProtocolErrorKind::MissingField,
                "OutputColumn.type missing",
            )
        })?;
        let value = types
            .type_reference_observed(reference, work)
            .map_err(|error| {
                ProjectionFailure::Codec(NativeLayoutProjectionError::TypeTable {
                    path: path.clone().index(index).field("type"),
                    source: error,
                })
            })?;
        if column.nullable != value.nullable() {
            return Err(protocol(
                path.clone().index(index).field("nullable"),
                ProtocolErrorKind::InconsistentFields,
                "output nullable disagrees with its explicit binding",
            ));
        }
        let slot = match factory {
            Some(factory) => {
                work.flush().map_err(control)?;
                let mut field = factory.begin_scope().map_err(ProjectionFailure::Host)?;
                let field_storage = field.storage_lifetime();
                slot_schema_from_binding_typed_observed(
                    SlotId::new(column.column_id),
                    &column.name,
                    value.typed_binding(),
                    None,
                    &path,
                    field_storage,
                    &mut |layout| field.request_layout(layout).map_err(AdmissionRefusal::Host),
                    work,
                )?
            }
            None => slot_schema_from_binding_typed_observed(
                SlotId::new(column.column_id),
                &column.name,
                value.typed_binding(),
                None,
                &path,
                storage.clone(),
                port,
                work,
            )?,
        };
        slots.push(slot);
    }
    let mut slot_ids = vector::<SlotId, H>(slots.len(), port, work)?;
    let mut schema_owner = match factory {
        Some(factory) => {
            work.flush().map_err(control)?;
            Some(factory.begin_scope().map_err(ProjectionFailure::Host)?)
        }
        None => None,
    };
    let schema_storage = schema_owner
        .as_ref()
        .map_or_else(|| storage.clone(), |owner| owner.storage_lifetime());
    let mut schema_port = |layout| match schema_owner.as_mut() {
        Some(owner) => owner.request_layout(layout).map_err(AdmissionRefusal::Host),
        None => port(layout),
    };
    let mut schema_slots = vector::<ChunkSlotSchema, H>(slots.len(), &mut schema_port, work)?;
    for slot in &slots {
        steps(3, work)?;
        slot_ids.push(slot.slot_id());
        schema_slots.push(slot.clone());
    }
    // Both borrowed callbacks run sequentially inside the constructor. A
    // local loan shares the original checkpoints without a second budget or
    // a no-op work port; no context or control is retained by the result.
    let checkpoints = RefCell::new(&mut *work);
    let schema = ChunkSchema::try_new_typed_observed(
        schema_slots,
        schema_storage,
        |layout| {
            request(layout, &mut schema_port, &mut checkpoints.borrow_mut())
                .map_err(BuildFailure::Refusal)
        },
        |units| steps(units, &mut checkpoints.borrow_mut()).map_err(BuildFailure::Refusal),
    )
    .map_err(|error| build_failure(error, &path))?;
    drop(checkpoints);
    request(
        arc_layout(Layout::new::<ChunkSchema>()).map_err(resource)?,
        &mut schema_port,
        work,
    )?;
    drop(schema_port);
    drop(schema_owner);
    request(
        NativeOutputLayout::allocation_layout().map_err(resource)?,
        port,
        work,
    )?;
    work.flush().map_err(control)?;
    Ok(NativeOutputLayout::new_with_storage(
        slot_ids,
        Arc::new(schema),
        slots,
        storage,
    ))
}

pub fn decode_fragment_output_layout_typed_observed<H>(
    columns: &[common::OutputColumn],
    path: FieldPath,
    types: &DecodedExecutionTypeTable,
    storage: ValueTypeStorageLifetime,
    port: &mut impl FnMut(Layout) -> Result<(), AdmissionRefusal<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<NativeOutputLayout, Failure<H>> {
    decode_output_layout_typed_observed(columns, path, types, storage, port, work)
}
pub fn chunk_schema_from_output_columns_typed_observed<H>(
    columns: &[common::OutputColumn],
    path: FieldPath,
    types: &DecodedExecutionTypeTable,
    storage: ValueTypeStorageLifetime,
    port: &mut impl FnMut(Layout) -> Result<(), AdmissionRefusal<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ChunkSchemaRef, Failure<H>> {
    decode_output_layout_typed_observed(columns, path, types, storage, port, work)
        .map(|layout| layout.chunk_schema())
}

/// Exact-sized ordered exchange contracts retain their own creator separately
/// from every projected schema's original type and scaffold lifetimes.
pub struct NativeExchangeContracts {
    entries: Vec<(FragmentNodeId, ExchangeInputContract)>,
    _storage: ValueTypeStorageLifetime,
}
impl NativeExchangeContracts {
    /// Transfer the existing ordered allocation with its actual creator.
    /// This creates neither a second index nor another ownership container.
    pub fn into_parts(
        self,
    ) -> (
        Vec<(FragmentNodeId, ExchangeInputContract)>,
        ValueTypeStorageLifetime,
    ) {
        (self.entries, self._storage)
    }
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&FragmentNodeId, &ExchangeInputContract)> {
        self.entries.iter().map(|(id, value)| (id, value))
    }
    pub fn get_observed(
        &self,
        id: FragmentNodeId,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&ExchangeInputContract>, CompileControlError> {
        let (mut low, mut high) = (0, self.entries.len());
        while low < high {
            work.step()?;
            let middle = low + (high - low) / 2;
            match self.entries[middle].0.cmp(&id) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Ok(Some(&self.entries[middle].1)),
            }
        }
        Ok(None)
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

pub fn decode_exchange_contracts_typed_observed<H>(
    root: &plan::DistributedNode,
    path: FieldPath,
    types: &DecodedExecutionTypeTable,
    storage: ValueTypeStorageLifetime,
    port: &mut impl FnMut(Layout) -> Result<(), AdmissionRefusal<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<NativeExchangeContracts, Failure<H>> {
    fn count<H>(
        node: &plan::DistributedNode,
        depth: usize,
        nodes: &mut usize,
        exchanges: &mut usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Failure<H>> {
        work.step().map_err(control)?;
        *nodes = nodes
            .checked_add(1)
            .ok_or_else(|| control(CompileControlError::ResourceExhausted))?;
        if depth > MAX_VALUE_TYPE_DEPTH || *nodes > MAX_VALUE_TYPE_NODES {
            return Err(control(CompileControlError::ResourceExhausted));
        }
        if matches!(
            node.payload,
            Some(plan::distributed_node::Payload::Exchange(_))
        ) {
            *exchanges += 1;
        }
        for child in &node.children {
            count(child, depth + 1, nodes, exchanges, work)?;
        }
        Ok(())
    }
    fn visit<H>(
        node: &plan::DistributedNode,
        path: &FieldPath,
        types: &DecodedExecutionTypeTable,
        storage: &ValueTypeStorageLifetime,
        port: &mut impl FnMut(Layout) -> Result<(), AdmissionRefusal<H>>,
        work: &mut CompileCheckpoints<'_>,
        entries: &mut Vec<(FragmentNodeId, ExchangeInputContract)>,
    ) -> Result<(), Failure<H>> {
        work.step().map_err(control)?;
        if let Some(plan::distributed_node::Payload::Exchange(exchange)) = node.payload.as_ref() {
            let payload_path = path_child(path, FieldPathSegment::Field("payload"), port, work)?;
            let exchange_path = path_child(
                &payload_path,
                FieldPathSegment::Field("exchange"),
                port,
                work,
            )?;
            let columns_path = path_child(
                &exchange_path,
                FieldPathSegment::Field("output_columns"),
                port,
                work,
            )?;
            let schema = chunk_schema_from_output_columns_typed_observed(
                &exchange.output_columns,
                columns_path,
                types,
                storage.clone(),
                port,
                work,
            )?;
            entries.push((
                FragmentNodeId::new(node.node_id),
                ExchangeInputContract::new(schema),
            ));
        }
        for (index, child) in node.children.iter().enumerate() {
            let children_path = path_child(path, FieldPathSegment::Field("children"), port, work)?;
            let child_path =
                path_child(&children_path, FieldPathSegment::Index(index), port, work)?;
            visit(child, &child_path, types, storage, port, work, entries)?;
        }
        Ok(())
    }
    fn duplicate_path<H>(
        node: &plan::DistributedNode,
        target: FragmentNodeId,
        depth: usize,
        indices: &mut [usize; MAX_VALUE_TYPE_DEPTH],
        seen: &mut usize,
        prefix: &FieldPath,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<FieldPath>, Failure<H>> {
        work.step().map_err(control)?;
        if FragmentNodeId::new(node.node_id) == target
            && matches!(
                node.payload,
                Some(plan::distributed_node::Payload::Exchange(_))
            )
        {
            *seen += 1;
            if *seen == 2 {
                // Diagnostic materialization occurs only on the rejected path;
                // successful traversal uses the explicitly observed path port.
                let mut path = prefix.clone();
                for index in &indices[..depth] {
                    path = path.field("children").index(*index);
                }
                return Ok(Some(path.field("node_id")));
            }
        }
        for (index, child) in node.children.iter().enumerate() {
            indices[depth] = index;
            if let Some(path) =
                duplicate_path(child, target, depth + 1, indices, seen, prefix, work)?
            {
                return Ok(Some(path));
            }
        }
        Ok(None)
    }
    work.flush().map_err(control)?;
    let mut exchanges = 0;
    count(root, 1, &mut 0, &mut exchanges, work)?;
    let mut entries = vector(exchanges, port, work)?;
    visit(root, &path, types, &storage, port, work, &mut entries)?;
    sort_observed(&mut entries, |v| v.0, work)?;
    for pair in entries.windows(2) {
        work.step().map_err(control)?;
        if pair[0].0 == pair[1].0 {
            return Err(protocol(
                duplicate_path(
                    root,
                    pair[0].0,
                    0,
                    &mut [0; MAX_VALUE_TYPE_DEPTH],
                    &mut 0,
                    &path,
                    work,
                )?
                .expect("sorted duplicate has a second source occurrence"),
                ProtocolErrorKind::InconsistentFields,
                format!("duplicate exchange node_id={:?}", pair[0].0),
            ));
        }
    }
    work.flush().map_err(control)?;
    Ok(NativeExchangeContracts {
        entries,
        _storage: storage,
    })
}

// Bounded O(n log n), in-place and cooperative at each comparison/swap.
fn sort_observed<T, K: Ord + Copy, H>(
    values: &mut [T],
    key: impl Fn(&T) -> K,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure<H>> {
    fn sift<T, K: Ord + Copy, H>(
        values: &mut [T],
        mut root: usize,
        end: usize,
        key: &impl Fn(&T) -> K,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Failure<H>> {
        loop {
            work.step().map_err(control)?;
            let left = root * 2 + 1;
            if left >= end {
                return Ok(());
            }
            let right = left + 1;
            let child = if right < end && key(&values[left]) < key(&values[right]) {
                right
            } else {
                left
            };
            work.step().map_err(control)?;
            if key(&values[root]) >= key(&values[child]) {
                return Ok(());
            }
            values.swap(root, child);
            root = child;
        }
    }
    for root in (0..values.len() / 2).rev() {
        sift(values, root, values.len(), &key, work)?;
    }
    for end in (1..values.len()).rev() {
        values.swap(0, end);
        sift(values, 0, end, &key, work)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use novarocks_plan_codec::execution_type_table::{
        ExecutionTypeRoot, ExecutionTypeTableBuilder, ExecutionTypeTableLimits,
        decode_execution_type_table_observed,
    };
    use novarocks_plan_codec::value_type_table::LogicalTypeTableLimits;
    use novarocks_type_contract::{
        CompilePhase, FunctionValueType, PureCompileControl, ValueField, ValueKind, ValueLayout,
        ValueLogicalType, ValueTypeRef, ValueTypeStorageHold,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Control;
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }
    struct Hold(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Hold {}
    impl Drop for Hold {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn held(drops: &Arc<AtomicUsize>) -> ValueTypeStorageLifetime {
        ValueTypeStorageLifetime::held(Arc::new(Hold(drops.clone())))
    }
    fn limits() -> ExecutionTypeTableLimits {
        ExecutionTypeTableLimits {
            logical: LogicalTypeTableLimits {
                max_definitions: 4096,
                max_expanded_nodes: 4096 * 4096,
                max_string_bytes: 1048576,
            },
            max_bindings: 4096,
            max_layout_nodes: 4096 * 4096,
            max_layout_string_bytes: 1048576,
        }
    }
    fn owner(storage: ValueTypeStorageLifetime) -> DecodedExecutionTypeTable {
        let pure = ValueTypeStorageLifetime::pure();
        let json = ValueTypeRef::try_new(
            ValueKind::Nominal(ValueLogicalType::Json),
            true,
            pure.clone(),
        )
        .unwrap();
        let list = ValueTypeRef::try_new(ValueKind::List(json), true, pure.clone()).unwrap();
        let value = ValueTypeRef::try_new(
            ValueKind::Struct(
                vec![ValueField::new("values", list, pure.clone())].into_boxed_slice(),
            ),
            false,
            pure.clone(),
        )
        .unwrap();
        let carrier = DataType::Struct(
            vec![Arc::new(Field::new(
                "values",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ))]
            .into(),
        );
        let source =
            FunctionValueType::try_from_value(value, ValueLayout::try_new(&carrier, pure).unwrap())
                .unwrap();
        let control = Control;
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let encoded = ExecutionTypeTableBuilder::new(&[ExecutionTypeRoot {
            id: u32::MAX,
            value: &source,
        }])
        .encode_observed(
            limits(),
            &mut |_| Ok::<_, AdmissionRefusal<()>>(()),
            &mut work,
        )
        .unwrap();
        decode_execution_type_table_observed(
            encoded.wire(),
            limits(),
            storage,
            &mut |_| Ok::<_, AdmissionRefusal<()>>(()),
            &mut work,
        )
        .unwrap()
    }
    fn column(id: u32) -> common::OutputColumn {
        common::OutputColumn {
            column_id: id,
            name: "output".into(),
            nullable: false,
            r#type: Some(common::TypeDesc {
                value_binding_id: Some(u32::MAX),
            }),
            is_internal: false,
        }
    }
    #[test]
    fn projected_nested_nominal_and_source_hold_survive_owner_and_layout_drop() {
        let original_drops = Arc::new(AtomicUsize::new(0));
        let creation_drops = Arc::new(AtomicUsize::new(0));
        let owner = owner(held(&original_drops));
        let control = Control;
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let mut requests = 0;
        let layout = decode_output_layout_typed_observed(
            &[column(u32::MAX)],
            FieldPath::root("columns"),
            &owner,
            held(&creation_drops),
            &mut |_| {
                requests += 1;
                Ok::<_, AdmissionRefusal<()>>(())
            },
            &mut work,
        )
        .unwrap();
        assert!(requests > 0);
        assert_eq!(layout.slot_ids(), &[SlotId::new(u32::MAX)]);
        assert!(layout.chunk_schema().is_typed());
        assert_eq!(
            layout
                .chunk_schema()
                .arrow_schema_ref()
                .metadata()
                .capacity(),
            0
        );
        let value = layout.slot_schemas()[0].field_schema().value_type();
        let leaf = value
            .child_type_at(0)
            .unwrap()
            .child_type_at(0)
            .unwrap()
            .clone();
        assert_eq!(leaf.domain(), Some(ValueLogicalType::Json));
        assert!(value.shares_storage(owner.iter().next().unwrap().1.value_type()));
        let clone = layout.clone();
        assert!(Arc::ptr_eq(&layout.0, &clone.0));
        drop(owner);
        drop(layout);
        assert_eq!(original_drops.load(Ordering::SeqCst), 0);
        assert_eq!(creation_drops.load(Ordering::SeqCst), 0);
        drop(clone);
        assert_eq!(creation_drops.load(Ordering::SeqCst), 1);
        assert_eq!(original_drops.load(Ordering::SeqCst), 0);
        drop(leaf);
        assert_eq!(original_drops.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn nullable_missing_inline_and_dangling_references_are_never_retagged() {
        let owner = owner(ValueTypeStorageLifetime::pure());
        let control = Control;
        for case in 0..4 {
            let mut column = column(1);
            match case {
                0 => column.nullable = true,
                1 => column.r#type = None,
                2 => column.r#type.as_mut().unwrap().value_binding_id = Some(1),
                _ => column.r#type.as_mut().unwrap().value_binding_id = None,
            }
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let failure = decode_output_layout_typed_observed(
                &[column],
                FieldPath::root("columns"),
                &owner,
                ValueTypeStorageLifetime::pure(),
                &mut |_| Ok::<_, AdmissionRefusal<()>>(()),
                &mut work,
            )
            .unwrap_err();
            assert!(matches!(failure, ProjectionFailure::Codec(_)));
        }
    }
    #[test]
    fn every_structure_request_preserves_original_host_refusal_and_zero_columns_work() {
        let owner = owner(ValueTypeStorageLifetime::pure());
        let control = Control;
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let mut requests = 0;
        decode_output_layout_typed_observed(
            &[column(1)],
            FieldPath::root("columns"),
            &owner,
            ValueTypeStorageLifetime::pure(),
            &mut |_| {
                requests += 1;
                Ok::<_, AdmissionRefusal<usize>>(())
            },
            &mut work,
        )
        .unwrap();
        for stop in 0..requests {
            let mut calls = 0;
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let failure = decode_output_layout_typed_observed(
                &[column(1)],
                FieldPath::root("columns"),
                &owner,
                ValueTypeStorageLifetime::pure(),
                &mut |_| {
                    let current = calls;
                    calls += 1;
                    if current == stop {
                        Err(AdmissionRefusal::Host(stop))
                    } else {
                        Ok(())
                    }
                },
                &mut work,
            )
            .unwrap_err();
            assert!(matches!(failure, ProjectionFailure::Host(value) if value == stop));
            assert_eq!(calls, stop + 1);
        }
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let zero = decode_output_layout_typed_observed(
            &[],
            FieldPath::root("columns"),
            &owner,
            ValueTypeStorageLifetime::pure(),
            &mut |_| Ok::<_, AdmissionRefusal<()>>(()),
            &mut work,
        )
        .unwrap();
        assert!(zero.slot_ids().is_empty());
        assert!(zero.chunk_schema().is_typed());
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        assert!(matches!(
            decode_output_layout_typed_observed(
                &[column(1), column(1)],
                FieldPath::root("columns"),
                &owner,
                ValueTypeStorageLifetime::pure(),
                &mut |_| Ok::<_, AdmissionRefusal<()>>(()),
                &mut work
            ),
            Err(ProjectionFailure::Codec(
                NativeLayoutProjectionError::Protocol(_)
            ))
        ));
    }
    #[test]
    fn sparse_exchange_ids_sort_lookup_and_duplicate_validation_are_observed() {
        let owner = owner(ValueTypeStorageLifetime::pure());
        let control = Control;
        let exchange = |id| plan::DistributedNode {
            node_id: id,
            payload: Some(plan::distributed_node::Payload::Exchange(
                plan::ExchangeReceiver {
                    output_columns: vec![column(1)],
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        let root = plan::DistributedNode {
            node_id: 0,
            children: vec![exchange(i32::MAX), exchange(-7), exchange(0)],
            ..Default::default()
        };
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let contracts = decode_exchange_contracts_typed_observed(
            &root,
            FieldPath::root("root"),
            &owner,
            ValueTypeStorageLifetime::pure(),
            &mut |_| Ok::<_, AdmissionRefusal<()>>(()),
            &mut work,
        )
        .unwrap();
        assert_eq!(
            contracts.iter().map(|(id, _)| id.get()).collect::<Vec<_>>(),
            vec![-7, 0, i32::MAX]
        );
        assert!(
            contracts
                .get_observed(FragmentNodeId::new(i32::MAX), &mut work)
                .unwrap()
                .is_some()
        );
        assert!(
            contracts
                .get_observed(FragmentNodeId::new(19), &mut work)
                .unwrap()
                .is_none()
        );
        let duplicate = plan::DistributedNode {
            children: vec![exchange(7), exchange(7)],
            ..Default::default()
        };
        assert!(matches!(
            decode_exchange_contracts_typed_observed(
                &duplicate,
                FieldPath::root("root"),
                &owner,
                ValueTypeStorageLifetime::pure(),
                &mut |_| Ok::<_, AdmissionRefusal<()>>(()),
                &mut work
            ),
            Err(ProjectionFailure::Codec(
                NativeLayoutProjectionError::Protocol(_)
            ))
        ));
    }
    #[test]
    fn original_control_failure_precedes_any_structure_request() {
        use std::sync::atomic::AtomicBool;
        struct Stopped(AtomicBool, CompileControlError);
        impl PureCompileControl for Stopped {
            fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
                if self.0.load(Ordering::SeqCst) {
                    Err(self.1)
                } else {
                    Ok(())
                }
            }
        }
        let owner = owner(ValueTypeStorageLifetime::pure());
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Stopped(AtomicBool::new(false), cause);
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            control.0.store(true, Ordering::SeqCst);
            let mut requests = 0;
            let result = decode_output_layout_typed_observed(
                &[column(1)],
                FieldPath::root("columns"),
                &owner,
                ValueTypeStorageLifetime::pure(),
                &mut |_| {
                    requests += 1;
                    Ok::<_, AdmissionRefusal<()>>(())
                },
                &mut work,
            );
            assert!(
                matches!(result, Err(ProjectionFailure::Codec(NativeLayoutProjectionError::Control(value))) if value == cause)
            );
            assert_eq!(requests, 0);
        }
    }
}
