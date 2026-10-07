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

//! Lossless Arrow physical schema encoding for internal execution relations.
//!
//! This is deliberately separate from the SQL `TypeDesc` codec. SQL types are
//! semantic and normalize several Arrow representations; this carrier freezes
//! names, nullability, metadata, nesting, offset widths, map ordering, and the
//! exact physical type selected by the planner.
//!
//! The field tree of every column is carried flat (see `ArrowPhysicalColumn`
//! in `plan.proto`): one node array in canonical preorder, validated by
//! [`crate::flat_type_tree`] before any Arrow value is built. Protobuf nesting
//! is constant whatever the Arrow nesting depth.
//!
//! A column is validated in one of two roles, chosen by its frozen
//! `is_internal` bit:
//!
//! - an internal column belongs to an auxiliary execution relation and keeps
//!   the relation's physical depth budget, every nesting step counting one
//!   level;
//! - a visible column carries a user-visible value and is bounded by the
//!   logical type budget (depth, nodes and text of `LogicalTypeLimits`),
//!   where physical wrappers -- a map's entries struct, a dictionary's key and
//!   value types and a run-end encoding's children -- are not levels of their
//!   own.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{
    DataType, Field, Fields, IntervalUnit, Schema, SchemaRef, TimeUnit, UnionFields, UnionMode,
};
use novarocks_proto_models::plan;
use novarocks_spi::connector::write_stack::{
    MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES, MAX_WRITE_RELATION_FIELD_NAME_BYTES,
    MAX_WRITE_RELATION_METADATA_ENTRIES_PER_FIELD, MAX_WRITE_RELATION_METADATA_KEY_BYTES,
    MAX_WRITE_RELATION_METADATA_VALUE_BYTES, MAX_WRITE_RELATION_TYPE_DEPTH,
    MAX_WRITER_AUXILIARY_CHANNELS, WRITE_RELATION_COLUMN_COUNT,
};
use novarocks_type_contract::LogicalTypeLimits;

use crate::flat_type_tree::{FlatEdge, FlatTreeLimits, FlatTreeViolation, validate_preorder};
use crate::{FieldPath, ProtocolError, ProtocolErrorKind};

const FIELD_CHARGE: usize = 128;
const TYPE_CHARGE: usize = 64;
const MAX_COLUMNS: usize = WRITE_RELATION_COLUMN_COUNT + MAX_WRITER_AUXILIARY_CHANNELS;

/// Every node is charged at least `TYPE_CHARGE` against the decoded schema
/// budget, so no internal column can hold more nodes than this.
const MAX_INTERNAL_COLUMN_NODES: usize = MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES / TYPE_CHARGE;

/// The encoder refuses a type nested deeper than this before it recurses any
/// further. It only bounds the encoder's own stack; the admitted depth of each
/// role is enforced by the self-validating decode that follows.
const MAX_ENCODE_NESTING: usize = 4 * MAX_WRITE_RELATION_TYPE_DEPTH + 2 * 64;

#[derive(Clone, Debug)]
pub struct DecodedArrowPhysicalSchema {
    schema: SchemaRef,
    slot_ids: Vec<u32>,
    internal: Vec<bool>,
}

impl DecodedArrowPhysicalSchema {
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn slot_ids(&self) -> &[u32] {
        &self.slot_ids
    }

    pub fn internal(&self) -> &[bool] {
        &self.internal
    }
}

/// The budget one column's field tree is validated against.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ColumnRole {
    Internal,
    Visible,
}

impl ColumnRole {
    fn of(is_internal: bool) -> Self {
        if is_internal {
            Self::Internal
        } else {
            Self::Visible
        }
    }

    fn tree_limits(self) -> FlatTreeLimits {
        match self {
            Self::Internal => FlatTreeLimits {
                max_depth: MAX_WRITE_RELATION_TYPE_DEPTH,
                max_nodes: MAX_INTERNAL_COLUMN_NODES,
            },
            // A logical node is at most one physical node plus one wrapper,
            // so twice the logical budget bounds the physical array before
            // the exact logical count is taken.
            Self::Visible => {
                let limits = LogicalTypeLimits::default();
                FlatTreeLimits {
                    max_depth: limits.max_depth,
                    max_nodes: limits.max_nodes.saturating_mul(2),
                }
            }
        }
    }

    /// Levels one edge adds. Wrappers are not logical levels.
    fn edge_depth(self, wrapper: bool) -> u8 {
        match self {
            Self::Internal => 1,
            Self::Visible => u8::from(!wrapper),
        }
    }
}

/// Most nodes one column's flat field tree may hold in its role. Raw-byte
/// preflight applies this bound before protobuf decoding allocates the nodes.
pub fn column_node_limit(is_internal: bool) -> usize {
    ColumnRole::of(is_internal).tree_limits().max_nodes
}

/// Encode and immediately decode the descriptor before returning it. This
/// makes a lossy or incomplete FE mapping fail where the plan is produced.
pub fn encode_schema(
    schema: &Schema,
    slot_ids: &[u32],
    internal: bool,
    path: FieldPath,
) -> Result<
    (
        Vec<plan::ArrowPhysicalColumn>,
        Vec<plan::ArrowFieldMetadataEntry>,
    ),
    ProtocolError,
> {
    encode_schema_with_internal(schema, slot_ids, &vec![internal; slot_ids.len()], path)
}

/// Encode a schema whose columns have independently frozen visibility bits.
pub fn encode_schema_with_internal(
    schema: &Schema,
    slot_ids: &[u32],
    internal: &[bool],
    path: FieldPath,
) -> Result<
    (
        Vec<plan::ArrowPhysicalColumn>,
        Vec<plan::ArrowFieldMetadataEntry>,
    ),
    ProtocolError,
> {
    if schema.fields().len() != slot_ids.len() {
        return Err(error(
            path.clone(),
            ProtocolErrorKind::InconsistentFields,
            "Arrow schema field count does not match slot-id count",
        ));
    }
    if schema.fields().len() != internal.len() {
        return Err(error(
            path,
            ProtocolErrorKind::InconsistentFields,
            "Arrow schema field count does not match internal-flag count",
        ));
    }
    if schema.fields().len() > MAX_COLUMNS {
        return Err(error(
            path,
            ProtocolErrorKind::Capacity,
            "Arrow schema exceeds the internal relation column limit",
        ));
    }
    let columns = schema
        .fields()
        .iter()
        .zip(slot_ids.iter().copied())
        .zip(internal.iter().copied())
        .enumerate()
        .map(|(index, ((field, slot_id), is_internal))| {
            encode_column(
                field.as_ref(),
                slot_id,
                is_internal,
                path.clone().field("columns").index(index),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let schema_metadata = encode_metadata(schema.metadata());
    let decoded = decode_schema(&columns, &schema_metadata, path.clone())?;
    let round_trip_columns = decoded
        .schema()
        .fields()
        .iter()
        .zip(decoded.slot_ids().iter().copied())
        .zip(decoded.internal().iter().copied())
        .enumerate()
        .map(|(index, ((field, slot_id), is_internal))| {
            encode_column(
                field.as_ref(),
                slot_id,
                is_internal,
                path.clone().field("columns").index(index),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let round_trip_metadata = encode_metadata(decoded.schema().metadata());
    if decoded.schema().as_ref() != schema
        || decoded.slot_ids() != slot_ids
        || decoded.internal() != internal
        || round_trip_columns != columns
        || round_trip_metadata != schema_metadata
    {
        return Err(error(
            path,
            ProtocolErrorKind::InconsistentFields,
            "Arrow physical schema failed exact encoder self-validation",
        ));
    }
    Ok((columns, schema_metadata))
}

/// Decode an untrusted physical schema. Each column's flat tree is validated
/// against its role's limits, and the decoded allocation budget is charged,
/// before any Arrow field is constructed.
pub fn decode_schema(
    columns: &[plan::ArrowPhysicalColumn],
    schema_metadata: &[plan::ArrowFieldMetadataEntry],
    path: FieldPath,
) -> Result<DecodedArrowPhysicalSchema, ProtocolError> {
    if columns.len() > MAX_COLUMNS {
        return Err(error(
            path.clone().field("columns"),
            ProtocolErrorKind::Capacity,
            "Arrow schema exceeds the internal relation column limit",
        ));
    }
    let mut budget = DecodeBudget::default();
    budget.charge(
        columns.len().saturating_mul(FIELD_CHARGE),
        path.clone().field("columns"),
    )?;
    let metadata = decode_metadata(
        schema_metadata,
        path.clone().field("schema_metadata"),
        &mut budget,
    )?;
    let mut fields = Vec::with_capacity(columns.len());
    let mut slot_ids = Vec::with_capacity(columns.len());
    let mut internal = Vec::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        let column_path = path.clone().field("columns").index(index);
        fields.push(Arc::new(decode_column(column, column_path, &mut budget)?));
        slot_ids.push(column.slot_id);
        internal.push(column.is_internal);
    }
    Ok(DecodedArrowPhysicalSchema {
        schema: Arc::new(Schema::new_with_metadata(fields, metadata)),
        slot_ids,
        internal,
    })
}

fn encode_column(
    field: &Field,
    slot_id: u32,
    is_internal: bool,
    path: FieldPath,
) -> Result<plan::ArrowPhysicalColumn, ProtocolError> {
    let mut nodes = Vec::new();
    encode_field_node(field, &mut nodes, 1, path.field("nodes"))?;
    Ok(plan::ArrowPhysicalColumn {
        slot_id,
        is_internal,
        nodes,
    })
}

/// Append `field` and its subtree in canonical preorder; returns its index.
fn encode_field_node(
    field: &Field,
    nodes: &mut Vec<plan::ArrowPhysicalNode>,
    nesting: usize,
    path: FieldPath,
) -> Result<u32, ProtocolError> {
    let index = push_placeholder(nodes, nesting, path.clone())?;
    check_string(
        field.name(),
        MAX_WRITE_RELATION_FIELD_NAME_BYTES,
        path.clone()
            .index(index as usize)
            .field("field")
            .field("name"),
        "Arrow field name",
    )?;
    let kind = encode_kind(field.data_type(), nodes, nesting, path)?;
    #[allow(deprecated)]
    let dictionary_id = field.dict_id();
    nodes[index as usize] = plan::ArrowPhysicalNode {
        field: Some(plan::ArrowPhysicalFieldFacts {
            name: field.name().clone(),
            nullable: field.is_nullable(),
            metadata: encode_metadata(field.metadata()),
            dictionary_id,
            dictionary_is_ordered: field.dict_is_ordered(),
        }),
        kind: Some(kind),
    };
    Ok(index)
}

/// Append a bare type node (a dictionary key or value) and its subtree.
fn encode_type_node(
    data_type: &DataType,
    nodes: &mut Vec<plan::ArrowPhysicalNode>,
    nesting: usize,
    path: FieldPath,
) -> Result<u32, ProtocolError> {
    let index = push_placeholder(nodes, nesting, path.clone())?;
    let kind = encode_kind(data_type, nodes, nesting, path)?;
    nodes[index as usize] = plan::ArrowPhysicalNode {
        field: None,
        kind: Some(kind),
    };
    Ok(index)
}

fn push_placeholder(
    nodes: &mut Vec<plan::ArrowPhysicalNode>,
    nesting: usize,
    path: FieldPath,
) -> Result<u32, ProtocolError> {
    if nesting > MAX_ENCODE_NESTING {
        return Err(error(
            path,
            ProtocolErrorKind::Capacity,
            "Arrow type exceeds the nesting depth limit",
        ));
    }
    let index = u32::try_from(nodes.len()).map_err(|_| {
        error(
            path,
            ProtocolErrorKind::Capacity,
            "Arrow field tree exceeds the node index range",
        )
    })?;
    nodes.push(plan::ArrowPhysicalNode::default());
    Ok(index)
}

fn encode_kind(
    data_type: &DataType,
    nodes: &mut Vec<plan::ArrowPhysicalNode>,
    nesting: usize,
    path: FieldPath,
) -> Result<plan::arrow_physical_node::Kind, ProtocolError> {
    use plan::arrow_physical_node::Kind;
    let child = nesting + 1;
    Ok(match data_type {
        DataType::Null => primitive(plan::ArrowPrimitiveType::Null),
        DataType::Boolean => primitive(plan::ArrowPrimitiveType::Boolean),
        DataType::Int8 => primitive(plan::ArrowPrimitiveType::Int8),
        DataType::Int16 => primitive(plan::ArrowPrimitiveType::Int16),
        DataType::Int32 => primitive(plan::ArrowPrimitiveType::Int32),
        DataType::Int64 => primitive(plan::ArrowPrimitiveType::Int64),
        DataType::UInt8 => primitive(plan::ArrowPrimitiveType::Uint8),
        DataType::UInt16 => primitive(plan::ArrowPrimitiveType::Uint16),
        DataType::UInt32 => primitive(plan::ArrowPrimitiveType::Uint32),
        DataType::UInt64 => primitive(plan::ArrowPrimitiveType::Uint64),
        DataType::Float16 => primitive(plan::ArrowPrimitiveType::Float16),
        DataType::Float32 => primitive(plan::ArrowPrimitiveType::Float32),
        DataType::Float64 => primitive(plan::ArrowPrimitiveType::Float64),
        DataType::Date32 => primitive(plan::ArrowPrimitiveType::Date32),
        DataType::Date64 => primitive(plan::ArrowPrimitiveType::Date64),
        DataType::Binary => primitive(plan::ArrowPrimitiveType::Binary),
        DataType::BinaryView => primitive(plan::ArrowPrimitiveType::BinaryView),
        DataType::LargeBinary => primitive(plan::ArrowPrimitiveType::LargeBinary),
        DataType::Utf8 => primitive(plan::ArrowPrimitiveType::Utf8),
        DataType::Utf8View => primitive(plan::ArrowPrimitiveType::Utf8View),
        DataType::LargeUtf8 => primitive(plan::ArrowPrimitiveType::LargeUtf8),
        DataType::Timestamp(unit, timezone) => {
            if let Some(timezone) = timezone {
                check_string(
                    timezone,
                    MAX_WRITE_RELATION_FIELD_NAME_BYTES,
                    path.clone().field("timestamp").field("timezone"),
                    "Arrow timestamp timezone",
                )?;
            }
            Kind::Timestamp(plan::ArrowTimestampType {
                unit: encode_time_unit(*unit) as i32,
                timezone: timezone.as_ref().map(|value| value.to_string()),
            })
        }
        DataType::Time32(unit) => Kind::Time32(plan::ArrowTimeType {
            unit: encode_time_unit(*unit) as i32,
        }),
        DataType::Time64(unit) => Kind::Time64(plan::ArrowTimeType {
            unit: encode_time_unit(*unit) as i32,
        }),
        DataType::Duration(unit) => Kind::Duration(plan::ArrowTimeType {
            unit: encode_time_unit(*unit) as i32,
        }),
        DataType::Interval(unit) => Kind::Interval(encode_interval_unit(*unit) as i32),
        DataType::FixedSizeBinary(width) => Kind::FixedSizeBinary(*width),
        DataType::Decimal32(precision, scale) => Kind::Decimal32(decimal(*precision, *scale)),
        DataType::Decimal64(precision, scale) => Kind::Decimal64(decimal(*precision, *scale)),
        DataType::Decimal128(precision, scale) => Kind::Decimal128(decimal(*precision, *scale)),
        DataType::Decimal256(precision, scale) => Kind::Decimal256(decimal(*precision, *scale)),
        DataType::List(field) => Kind::List(encode_field_node(field, nodes, child, path)?),
        DataType::ListView(field) => Kind::ListView(encode_field_node(field, nodes, child, path)?),
        DataType::FixedSizeList(field, length) => {
            Kind::FixedSizeList(plan::ArrowFixedSizeListNode {
                item: encode_field_node(field, nodes, child, path)?,
                length: *length,
            })
        }
        DataType::LargeList(field) => {
            Kind::LargeList(encode_field_node(field, nodes, child, path)?)
        }
        DataType::LargeListView(field) => {
            Kind::LargeListView(encode_field_node(field, nodes, child, path)?)
        }
        DataType::Struct(fields) => {
            check_repeated_len(fields.len(), path.clone().field("struct_type"))?;
            let mut children = Vec::with_capacity(fields.len());
            for field in fields {
                children.push(encode_field_node(field, nodes, child, path.clone())?);
            }
            Kind::StructType(plan::ArrowStructNode { fields: children })
        }
        DataType::Union(fields, mode) => {
            check_repeated_len(fields.len(), path.clone().field("union_type"))?;
            let mut children = Vec::with_capacity(fields.len());
            for (type_id, field) in fields.iter() {
                children.push(plan::ArrowUnionChild {
                    type_id: i32::from(type_id),
                    field: encode_field_node(field, nodes, child, path.clone())?,
                });
            }
            Kind::UnionType(plan::ArrowUnionNode {
                mode: match mode {
                    UnionMode::Sparse => plan::ArrowUnionMode::Sparse as i32,
                    UnionMode::Dense => plan::ArrowUnionMode::Dense as i32,
                },
                fields: children,
            })
        }
        DataType::Dictionary(key, value) => {
            let key = encode_type_node(key, nodes, child, path.clone())?;
            let value = encode_type_node(value, nodes, child, path)?;
            Kind::Dictionary(plan::ArrowDictionaryNode { key, value })
        }
        DataType::Map(entries, ordered) => Kind::Map(plan::ArrowMapNode {
            entries: encode_field_node(entries, nodes, child, path)?,
            ordered: *ordered,
        }),
        DataType::RunEndEncoded(run_ends, values) => {
            let run_ends = encode_field_node(run_ends, nodes, child, path.clone())?;
            let values = encode_field_node(values, nodes, child, path)?;
            Kind::RunEndEncoded(plan::ArrowRunEndEncodedNode { run_ends, values })
        }
    })
}

fn encode_metadata(metadata: &HashMap<String, String>) -> Vec<plan::ArrowFieldMetadataEntry> {
    let mut entries = metadata
        .iter()
        .map(|(key, value)| plan::ArrowFieldMetadataEntry {
            key: key.clone(),
            value: value.clone(),
        })
        .collect::<Vec<_>>();
    entries.sort_unstable_by(|left, right| left.key.cmp(&right.key));
    entries
}

fn primitive(value: plan::ArrowPrimitiveType) -> plan::arrow_physical_node::Kind {
    plan::arrow_physical_node::Kind::Primitive(value as i32)
}

fn decimal(precision: u8, scale: i8) -> plan::ArrowDecimalType {
    plan::ArrowDecimalType {
        precision: u32::from(precision),
        scale: i32::from(scale),
    }
}

fn encode_time_unit(unit: TimeUnit) -> plan::ArrowTimeUnit {
    match unit {
        TimeUnit::Second => plan::ArrowTimeUnit::Second,
        TimeUnit::Millisecond => plan::ArrowTimeUnit::Millisecond,
        TimeUnit::Microsecond => plan::ArrowTimeUnit::Microsecond,
        TimeUnit::Nanosecond => plan::ArrowTimeUnit::Nanosecond,
    }
}

fn encode_interval_unit(unit: IntervalUnit) -> plan::ArrowIntervalUnit {
    match unit {
        IntervalUnit::YearMonth => plan::ArrowIntervalUnit::YearMonth,
        IntervalUnit::DayTime => plan::ArrowIntervalUnit::DayTime,
        IntervalUnit::MonthDayNano => plan::ArrowIntervalUnit::MonthDayNano,
    }
}

/// Validate one column's flat tree in its role, then build its root field.
fn decode_column(
    column: &plan::ArrowPhysicalColumn,
    path: FieldPath,
    budget: &mut DecodeBudget,
) -> Result<Field, ProtocolError> {
    let role = ColumnRole::of(column.is_internal);
    let nodes = &column.nodes;
    let nodes_path = path.field("nodes");
    validate_preorder(nodes.len(), role.tree_limits(), |index, edges| {
        node_edges(&nodes[index], role, edges, nodes_path.clone().index(index))
    })
    .map_err(|violation: TreeError| violation.into_protocol(nodes_path.clone()))?;
    if role == ColumnRole::Visible {
        check_logical_budget(nodes, nodes_path.clone())?;
    }
    build_column(nodes, nodes_path, budget)
}

/// A kernel violation, or a node rejected while its edges were listed.
enum TreeError {
    Violation(FlatTreeViolation),
    Node(ProtocolError),
}

impl From<FlatTreeViolation> for TreeError {
    fn from(violation: FlatTreeViolation) -> Self {
        Self::Violation(violation)
    }
}

impl From<ProtocolError> for TreeError {
    fn from(error: ProtocolError) -> Self {
        Self::Node(error)
    }
}

impl TreeError {
    fn into_protocol(self, path: FieldPath) -> ProtocolError {
        match self {
            Self::Node(error) => error,
            Self::Violation(violation) => {
                let kind = match violation {
                    FlatTreeViolation::Empty => ProtocolErrorKind::MissingField,
                    FlatTreeViolation::TooManyNodes { .. } | FlatTreeViolation::TooDeep { .. } => {
                        ProtocolErrorKind::Capacity
                    }
                    FlatTreeViolation::ChildOutOfRange { .. }
                    | FlatTreeViolation::NonCanonical { .. }
                    | FlatTreeViolation::Unreachable { .. }
                    | FlatTreeViolation::TooManyEdges { .. } => ProtocolErrorKind::InvalidValue,
                };
                error(path, kind, format!("Arrow field tree: {violation}"))
            }
        }
    }
}

fn node_edges(
    node: &plan::ArrowPhysicalNode,
    role: ColumnRole,
    edges: &mut Vec<FlatEdge>,
    path: FieldPath,
) -> Result<(), TreeError> {
    use plan::arrow_physical_node::Kind;
    let kind = node.kind.as_ref().ok_or_else(|| {
        error(
            path.clone().field("kind"),
            ProtocolErrorKind::MissingField,
            "Arrow physical node kind is required",
        )
    })?;
    let level = role.edge_depth(false);
    let wrapper = role.edge_depth(true);
    let mut push = |child: u32, depth: u8| edges.push(FlatEdge { child, depth });
    match kind {
        Kind::List(child)
        | Kind::ListView(child)
        | Kind::LargeList(child)
        | Kind::LargeListView(child) => push(*child, level),
        Kind::FixedSizeList(value) => push(value.item, level),
        Kind::StructType(value) => {
            check_repeated_len(
                value.fields.len(),
                path.field("struct_type").field("fields"),
            )?;
            for child in &value.fields {
                push(*child, level);
            }
        }
        Kind::UnionType(value) => {
            check_repeated_len(value.fields.len(), path.field("union_type").field("fields"))?;
            for child in &value.fields {
                push(child.field, level);
            }
        }
        Kind::Dictionary(value) => {
            push(value.key, wrapper);
            push(value.value, wrapper);
        }
        Kind::Map(value) => push(value.entries, wrapper),
        Kind::RunEndEncoded(value) => {
            push(value.run_ends, wrapper);
            push(value.values, wrapper);
        }
        Kind::Primitive(_)
        | Kind::Timestamp(_)
        | Kind::Time32(_)
        | Kind::Time64(_)
        | Kind::Duration(_)
        | Kind::Interval(_)
        | Kind::FixedSizeBinary(_)
        | Kind::Decimal32(_)
        | Kind::Decimal64(_)
        | Kind::Decimal128(_)
        | Kind::Decimal256(_) => {}
    }
    Ok(())
}

/// The logical node and text budget of a visible column. Wrapper nodes --
/// a map's entries struct, a dictionary's key and value, a run-end encoding's
/// children -- are not logical nodes, and only struct and union member names
/// are logical text; a map's synthetic key/value names are not. Called only
/// after the tree itself has been validated, so every index is in range.
fn check_logical_budget(
    nodes: &[plan::ArrowPhysicalNode],
    path: FieldPath,
) -> Result<(), ProtocolError> {
    use plan::arrow_physical_node::Kind;
    let limits = LogicalTypeLimits::default();
    let mut wrapper = vec![false; nodes.len()];
    for node in nodes {
        match node.kind.as_ref() {
            Some(Kind::Map(value)) => wrapper[value.entries as usize] = true,
            Some(Kind::Dictionary(value)) => {
                wrapper[value.key as usize] = true;
                wrapper[value.value as usize] = true;
            }
            Some(Kind::RunEndEncoded(value)) => {
                wrapper[value.run_ends as usize] = true;
                wrapper[value.values as usize] = true;
            }
            _ => {}
        }
    }
    let logical_nodes = wrapper.iter().filter(|wrapped| !**wrapped).count();
    if logical_nodes > limits.max_nodes {
        return Err(error(
            path,
            ProtocolErrorKind::Capacity,
            format!(
                "visible Arrow column has {logical_nodes} logical nodes, above the limit {}",
                limits.max_nodes
            ),
        ));
    }
    let name_len = |member: u32| {
        nodes[member as usize]
            .field
            .as_ref()
            .map_or(0, |facts| facts.name.len())
    };
    let mut text = 0_usize;
    for (index, node) in nodes.iter().enumerate() {
        match node.kind.as_ref() {
            Some(Kind::StructType(value)) if !wrapper[index] => {
                for member in &value.fields {
                    text = text.saturating_add(name_len(*member));
                }
            }
            Some(Kind::UnionType(value)) => {
                for member in &value.fields {
                    text = text.saturating_add(name_len(member.field));
                }
            }
            _ => {}
        }
    }
    if text > limits.max_text_bytes {
        return Err(error(
            path,
            ProtocolErrorKind::Capacity,
            format!(
                "visible Arrow column has {text} bytes of member names, above the limit {}",
                limits.max_text_bytes
            ),
        ));
    }
    Ok(())
}

/// A constructed node waiting for its parent.
enum Built {
    Field(Field),
    Type(DataType),
}

/// Build the column bottom-up in reverse preorder. Validation already proved
/// every child index is larger than its parent's and owned by that parent
/// alone, so each child is complete and unclaimed when its parent is built.
fn build_column(
    nodes: &[plan::ArrowPhysicalNode],
    path: FieldPath,
    budget: &mut DecodeBudget,
) -> Result<Field, ProtocolError> {
    budget.charge(
        nodes.len().saturating_mul(size_of::<Option<Built>>()),
        path.clone(),
    )?;
    let mut built: Vec<Option<Built>> = Vec::with_capacity(nodes.len());
    built.resize_with(nodes.len(), || None);
    for index in (0..nodes.len()).rev() {
        let node_path = path.clone().index(index);
        let node = &nodes[index];
        let data_type = build_type(node, &mut built, node_path.clone(), budget)?;
        built[index] = Some(match &node.field {
            Some(facts) => Built::Field(build_field(facts, data_type, node_path, budget)?),
            None => Built::Type(data_type),
        });
    }
    match built.into_iter().next().flatten() {
        Some(Built::Field(field)) => Ok(field),
        _ => Err(error(
            path.index(0).field("field"),
            ProtocolErrorKind::MissingField,
            "Arrow physical column root must be a field node",
        )),
    }
}

fn take_field(
    built: &mut [Option<Built>],
    child: u32,
    path: FieldPath,
) -> Result<Arc<Field>, ProtocolError> {
    match built[child as usize].take() {
        Some(Built::Field(field)) => Ok(Arc::new(field)),
        _ => Err(error(
            path,
            ProtocolErrorKind::InconsistentFields,
            "Arrow nested child must be a field node",
        )),
    }
}

fn take_type(
    built: &mut [Option<Built>],
    child: u32,
    path: FieldPath,
) -> Result<DataType, ProtocolError> {
    match built[child as usize].take() {
        Some(Built::Type(data_type)) => Ok(data_type),
        _ => Err(error(
            path,
            ProtocolErrorKind::InconsistentFields,
            "Arrow dictionary child must be a bare type node",
        )),
    }
}

fn build_field(
    facts: &plan::ArrowPhysicalFieldFacts,
    data_type: DataType,
    path: FieldPath,
    budget: &mut DecodeBudget,
) -> Result<Field, ProtocolError> {
    let path = path.field("field");
    check_string(
        &facts.name,
        MAX_WRITE_RELATION_FIELD_NAME_BYTES,
        path.clone().field("name"),
        "Arrow field name",
    )?;
    budget.charge(FIELD_CHARGE + facts.name.len(), path.clone())?;
    let metadata = decode_metadata(&facts.metadata, path.clone().field("metadata"), budget)?;
    let field = if matches!(data_type, DataType::Dictionary(_, _)) {
        let dictionary_id = facts.dictionary_id.ok_or_else(|| {
            error(
                path.clone().field("dictionary_id"),
                ProtocolErrorKind::MissingField,
                "Arrow dictionary field id is required",
            )
        })?;
        let dictionary_is_ordered = facts.dictionary_is_ordered.ok_or_else(|| {
            error(
                path.clone().field("dictionary_is_ordered"),
                ProtocolErrorKind::MissingField,
                "Arrow dictionary field ordering is required",
            )
        })?;
        #[allow(deprecated)]
        Field::new_dict(
            &facts.name,
            data_type,
            facts.nullable,
            dictionary_id,
            dictionary_is_ordered,
        )
    } else {
        if facts.dictionary_id.is_some() || facts.dictionary_is_ordered.is_some() {
            return Err(error(
                path,
                ProtocolErrorKind::InconsistentFields,
                "Arrow dictionary field attributes require Dictionary type",
            ));
        }
        Field::new(&facts.name, data_type, facts.nullable)
    };
    Ok(field.with_metadata(metadata))
}

fn build_type(
    node: &plan::ArrowPhysicalNode,
    built: &mut [Option<Built>],
    path: FieldPath,
    budget: &mut DecodeBudget,
) -> Result<DataType, ProtocolError> {
    budget.charge(TYPE_CHARGE, path.clone())?;
    use plan::arrow_physical_node::Kind;
    let kind = node.kind.as_ref().ok_or_else(|| {
        error(
            path.clone().field("kind"),
            ProtocolErrorKind::MissingField,
            "Arrow physical node kind is required",
        )
    })?;
    match kind {
        Kind::Primitive(value) => decode_primitive(*value, path.field("primitive")),
        Kind::Timestamp(value) => {
            if let Some(timezone) = &value.timezone {
                check_string(
                    timezone,
                    MAX_WRITE_RELATION_FIELD_NAME_BYTES,
                    path.clone().field("timestamp").field("timezone"),
                    "Arrow timestamp timezone",
                )?;
                budget.charge(timezone.len(), path.clone())?;
            }
            Ok(DataType::Timestamp(
                decode_time_unit(value.unit, path.clone().field("timestamp").field("unit"))?,
                value.timezone.as_deref().map(Arc::<str>::from),
            ))
        }
        Kind::Time32(value) => Ok(DataType::Time32(decode_time_unit(
            value.unit,
            path.field("time32").field("unit"),
        )?)),
        Kind::Time64(value) => Ok(DataType::Time64(decode_time_unit(
            value.unit,
            path.field("time64").field("unit"),
        )?)),
        Kind::Duration(value) => Ok(DataType::Duration(decode_time_unit(
            value.unit,
            path.field("duration").field("unit"),
        )?)),
        Kind::Interval(value) => Ok(DataType::Interval(decode_interval_unit(
            *value,
            path.field("interval"),
        )?)),
        Kind::FixedSizeBinary(width) => Ok(DataType::FixedSizeBinary(*width)),
        Kind::Decimal32(value) => {
            decode_decimal(value, path.field("decimal32"), DataType::Decimal32)
        }
        Kind::Decimal64(value) => {
            decode_decimal(value, path.field("decimal64"), DataType::Decimal64)
        }
        Kind::Decimal128(value) => {
            decode_decimal(value, path.field("decimal128"), DataType::Decimal128)
        }
        Kind::Decimal256(value) => {
            decode_decimal(value, path.field("decimal256"), DataType::Decimal256)
        }
        Kind::List(child) => Ok(DataType::List(take_field(
            built,
            *child,
            path.field("list"),
        )?)),
        Kind::ListView(child) => Ok(DataType::ListView(take_field(
            built,
            *child,
            path.field("list_view"),
        )?)),
        Kind::FixedSizeList(value) => Ok(DataType::FixedSizeList(
            take_field(
                built,
                value.item,
                path.field("fixed_size_list").field("item"),
            )?,
            value.length,
        )),
        Kind::LargeList(child) => Ok(DataType::LargeList(take_field(
            built,
            *child,
            path.field("large_list"),
        )?)),
        Kind::LargeListView(child) => Ok(DataType::LargeListView(take_field(
            built,
            *child,
            path.field("large_list_view"),
        )?)),
        Kind::StructType(value) => {
            let fields = value
                .fields
                .iter()
                .enumerate()
                .map(|(index, child)| {
                    take_field(
                        built,
                        *child,
                        path.clone()
                            .field("struct_type")
                            .field("fields")
                            .index(index),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(DataType::Struct(Fields::from(fields)))
        }
        Kind::UnionType(value) => {
            let mode = match plan::ArrowUnionMode::try_from(value.mode) {
                Ok(plan::ArrowUnionMode::Sparse) => UnionMode::Sparse,
                Ok(plan::ArrowUnionMode::Dense) => UnionMode::Dense,
                Ok(plan::ArrowUnionMode::Unspecified) | Err(_) => {
                    return Err(error(
                        path.field("union_type").field("mode"),
                        ProtocolErrorKind::InvalidEnum,
                        "Arrow union mode is unknown or unspecified",
                    ));
                }
            };
            let mut type_ids = Vec::with_capacity(value.fields.len());
            let mut fields = Vec::with_capacity(value.fields.len());
            for (index, child) in value.fields.iter().enumerate() {
                let child_path = path
                    .clone()
                    .field("union_type")
                    .field("fields")
                    .index(index);
                let type_id = i8::try_from(child.type_id).map_err(|_| {
                    error(
                        child_path.clone().field("type_id"),
                        ProtocolErrorKind::OutOfRange,
                        "Arrow union type id does not fit i8",
                    )
                })?;
                type_ids.push(type_id);
                fields.push(take_field(built, child.field, child_path.field("field"))?);
            }
            let fields = UnionFields::try_new(type_ids, fields).map_err(|err| {
                error(
                    path.field("union_type").field("fields"),
                    ProtocolErrorKind::InvalidValue,
                    format!("invalid Arrow union fields: {err}"),
                )
            })?;
            Ok(DataType::Union(fields, mode))
        }
        Kind::Dictionary(value) => {
            let key = take_type(
                built,
                value.key,
                path.clone().field("dictionary").field("key"),
            )?;
            let dictionary_value =
                take_type(built, value.value, path.field("dictionary").field("value"))?;
            Ok(DataType::Dictionary(
                Box::new(key),
                Box::new(dictionary_value),
            ))
        }
        Kind::Map(value) => Ok(DataType::Map(
            take_field(built, value.entries, path.field("map").field("entries"))?,
            value.ordered,
        )),
        Kind::RunEndEncoded(value) => {
            let run_ends = take_field(
                built,
                value.run_ends,
                path.clone().field("run_end_encoded").field("run_ends"),
            )?;
            let values = take_field(
                built,
                value.values,
                path.field("run_end_encoded").field("values"),
            )?;
            Ok(DataType::RunEndEncoded(run_ends, values))
        }
    }
}

fn decode_primitive(value: i32, path: FieldPath) -> Result<DataType, ProtocolError> {
    match plan::ArrowPrimitiveType::try_from(value) {
        Ok(plan::ArrowPrimitiveType::Null) => Ok(DataType::Null),
        Ok(plan::ArrowPrimitiveType::Boolean) => Ok(DataType::Boolean),
        Ok(plan::ArrowPrimitiveType::Int8) => Ok(DataType::Int8),
        Ok(plan::ArrowPrimitiveType::Int16) => Ok(DataType::Int16),
        Ok(plan::ArrowPrimitiveType::Int32) => Ok(DataType::Int32),
        Ok(plan::ArrowPrimitiveType::Int64) => Ok(DataType::Int64),
        Ok(plan::ArrowPrimitiveType::Uint8) => Ok(DataType::UInt8),
        Ok(plan::ArrowPrimitiveType::Uint16) => Ok(DataType::UInt16),
        Ok(plan::ArrowPrimitiveType::Uint32) => Ok(DataType::UInt32),
        Ok(plan::ArrowPrimitiveType::Uint64) => Ok(DataType::UInt64),
        Ok(plan::ArrowPrimitiveType::Float16) => Ok(DataType::Float16),
        Ok(plan::ArrowPrimitiveType::Float32) => Ok(DataType::Float32),
        Ok(plan::ArrowPrimitiveType::Float64) => Ok(DataType::Float64),
        Ok(plan::ArrowPrimitiveType::Date32) => Ok(DataType::Date32),
        Ok(plan::ArrowPrimitiveType::Date64) => Ok(DataType::Date64),
        Ok(plan::ArrowPrimitiveType::Binary) => Ok(DataType::Binary),
        Ok(plan::ArrowPrimitiveType::BinaryView) => Ok(DataType::BinaryView),
        Ok(plan::ArrowPrimitiveType::LargeBinary) => Ok(DataType::LargeBinary),
        Ok(plan::ArrowPrimitiveType::Utf8) => Ok(DataType::Utf8),
        Ok(plan::ArrowPrimitiveType::Utf8View) => Ok(DataType::Utf8View),
        Ok(plan::ArrowPrimitiveType::LargeUtf8) => Ok(DataType::LargeUtf8),
        Ok(plan::ArrowPrimitiveType::Unspecified) | Err(_) => Err(error(
            path,
            ProtocolErrorKind::InvalidEnum,
            "Arrow primitive type is unknown or unspecified",
        )),
    }
}

fn decode_time_unit(value: i32, path: FieldPath) -> Result<TimeUnit, ProtocolError> {
    match plan::ArrowTimeUnit::try_from(value) {
        Ok(plan::ArrowTimeUnit::Second) => Ok(TimeUnit::Second),
        Ok(plan::ArrowTimeUnit::Millisecond) => Ok(TimeUnit::Millisecond),
        Ok(plan::ArrowTimeUnit::Microsecond) => Ok(TimeUnit::Microsecond),
        Ok(plan::ArrowTimeUnit::Nanosecond) => Ok(TimeUnit::Nanosecond),
        Ok(plan::ArrowTimeUnit::Unspecified) | Err(_) => Err(error(
            path,
            ProtocolErrorKind::InvalidEnum,
            "Arrow time unit is unknown or unspecified",
        )),
    }
}

fn decode_interval_unit(value: i32, path: FieldPath) -> Result<IntervalUnit, ProtocolError> {
    match plan::ArrowIntervalUnit::try_from(value) {
        Ok(plan::ArrowIntervalUnit::YearMonth) => Ok(IntervalUnit::YearMonth),
        Ok(plan::ArrowIntervalUnit::DayTime) => Ok(IntervalUnit::DayTime),
        Ok(plan::ArrowIntervalUnit::MonthDayNano) => Ok(IntervalUnit::MonthDayNano),
        Ok(plan::ArrowIntervalUnit::Unspecified) | Err(_) => Err(error(
            path,
            ProtocolErrorKind::InvalidEnum,
            "Arrow interval unit is unknown or unspecified",
        )),
    }
}

fn decode_decimal(
    value: &plan::ArrowDecimalType,
    path: FieldPath,
    build: fn(u8, i8) -> DataType,
) -> Result<DataType, ProtocolError> {
    let precision = u8::try_from(value.precision).map_err(|_| {
        error(
            path.clone().field("precision"),
            ProtocolErrorKind::OutOfRange,
            "Arrow decimal precision does not fit u8",
        )
    })?;
    let scale = i8::try_from(value.scale).map_err(|_| {
        error(
            path.field("scale"),
            ProtocolErrorKind::OutOfRange,
            "Arrow decimal scale does not fit i8",
        )
    })?;
    Ok(build(precision, scale))
}

fn decode_metadata(
    entries: &[plan::ArrowFieldMetadataEntry],
    path: FieldPath,
    budget: &mut DecodeBudget,
) -> Result<HashMap<String, String>, ProtocolError> {
    if entries.len() > MAX_WRITE_RELATION_METADATA_ENTRIES_PER_FIELD {
        return Err(error(
            path,
            ProtocolErrorKind::Capacity,
            "Arrow metadata exceeds the entry limit",
        ));
    }
    let mut previous: Option<&str> = None;
    for (index, entry) in entries.iter().enumerate() {
        let entry_path = path.clone().index(index);
        check_string(
            &entry.key,
            MAX_WRITE_RELATION_METADATA_KEY_BYTES,
            entry_path.clone().field("key"),
            "Arrow metadata key",
        )?;
        check_string(
            &entry.value,
            MAX_WRITE_RELATION_METADATA_VALUE_BYTES,
            entry_path.clone().field("value"),
            "Arrow metadata value",
        )?;
        if previous.is_some_and(|key| key >= entry.key.as_str()) {
            return Err(error(
                entry_path.field("key"),
                if previous == Some(entry.key.as_str()) {
                    ProtocolErrorKind::DuplicateField
                } else {
                    ProtocolErrorKind::InvalidValue
                },
                "Arrow metadata keys must be unique and strictly sorted",
            ));
        }
        budget.charge(
            entry.key.len() + entry.value.len() + 2 * size_of::<String>(),
            entry_path,
        )?;
        previous = Some(&entry.key);
    }
    Ok(entries
        .iter()
        .map(|entry| (entry.key.clone(), entry.value.clone()))
        .collect())
}

fn check_repeated_len(length: usize, path: FieldPath) -> Result<(), ProtocolError> {
    if length > MAX_COLUMNS {
        Err(error(
            path,
            ProtocolErrorKind::Capacity,
            "Arrow nested field count exceeds the relation limit",
        ))
    } else {
        Ok(())
    }
}

fn check_string(
    value: &str,
    limit: usize,
    path: FieldPath,
    label: &'static str,
) -> Result<(), ProtocolError> {
    if value.len() > limit {
        Err(error(
            path,
            ProtocolErrorKind::Capacity,
            format!("{label} exceeds the byte limit"),
        ))
    } else {
        Ok(())
    }
}

#[derive(Default)]
struct DecodeBudget {
    bytes: usize,
}

impl DecodeBudget {
    fn charge(&mut self, amount: usize, path: FieldPath) -> Result<(), ProtocolError> {
        self.bytes = self.bytes.checked_add(amount).ok_or_else(|| {
            error(
                path.clone(),
                ProtocolErrorKind::Capacity,
                "Arrow schema decoded allocation charge overflowed",
            )
        })?;
        if self.bytes > MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES {
            return Err(error(
                path,
                ProtocolErrorKind::Capacity,
                "Arrow schema exceeds the decoded allocation limit",
            ));
        }
        Ok(())
    }
}

fn error(path: FieldPath, kind: ProtocolErrorKind, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, kind, detail)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nested_field(name: &str, data_type: DataType, nullable: bool) -> Arc<Field> {
        Arc::new(
            Field::new(name, data_type, nullable)
                .with_metadata(HashMap::from([("owner".to_string(), "test".to_string())])),
        )
    }

    #[test]
    fn exact_physical_schema_round_trips_without_sql_normalization() {
        let map_entries = nested_field(
            "kv",
            DataType::Struct(Fields::from(vec![
                nested_field("key", DataType::LargeUtf8, false),
                nested_field("value", DataType::LargeBinary, false),
            ])),
            false,
        );
        let schema = Schema::new_with_metadata(
            vec![
                Field::new(
                    "list32",
                    DataType::List(nested_field("element32", DataType::Utf8, false)),
                    true,
                ),
                Field::new(
                    "list64",
                    DataType::LargeList(nested_field("element64", DataType::LargeUtf8, true)),
                    false,
                ),
                Field::new(
                    "fixed",
                    DataType::FixedSizeList(nested_field("fixed_item", DataType::Binary, false), 7),
                    true,
                ),
                Field::new("map", DataType::Map(map_entries, true), true),
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("+08:00".into())),
                    false,
                ),
                #[allow(deprecated)]
                Field::new_dict(
                    "dictionary",
                    DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::LargeUtf8)),
                    false,
                    41,
                    true,
                ),
            ],
            HashMap::from([("schema".to_string(), "physical".to_string())]),
        );
        let slot_ids = [1, 2, 3, 4, 5, 6];
        let path = FieldPath::root("schema");
        let (columns, metadata) = encode_schema(&schema, &slot_ids, true, path.clone()).unwrap();
        let decoded = decode_schema(&columns, &metadata, path).unwrap();
        assert_eq!(decoded.schema().as_ref(), &schema);
        assert_eq!(decoded.slot_ids(), slot_ids);
        assert_eq!(decoded.internal(), &[true; 6]);
        #[allow(deprecated)]
        {
            assert_eq!(decoded.schema().field(5).dict_id(), Some(41));
        }
        assert_eq!(decoded.schema().field(5).dict_is_ordered(), Some(true));
    }

    #[test]
    fn every_arrow_58_physical_variant_round_trips_exactly() {
        let item = || nested_field("item", DataType::Int32, false);
        let union_fields = UnionFields::try_new(
            [1, 7],
            [
                nested_field("left", DataType::Int64, true),
                nested_field("right", DataType::LargeUtf8, false),
            ],
        )
        .expect("union fields");
        let map_entries = nested_field(
            "entries",
            DataType::Struct(Fields::from(vec![
                nested_field("key", DataType::Utf8, false),
                nested_field("value", DataType::Binary, false),
            ])),
            false,
        );
        let data_types = vec![
            DataType::Null,
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Duration(TimeUnit::Millisecond),
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::Binary,
            DataType::FixedSizeBinary(17),
            DataType::LargeBinary,
            DataType::BinaryView,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::List(item()),
            DataType::ListView(item()),
            DataType::FixedSizeList(item(), 5),
            DataType::LargeList(item()),
            DataType::LargeListView(item()),
            DataType::Struct(Fields::from(vec![item()])),
            DataType::Union(union_fields, UnionMode::Dense),
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::LargeUtf8)),
            DataType::Decimal32(7, -2),
            DataType::Decimal64(16, 3),
            DataType::Decimal128(38, 8),
            DataType::Decimal256(76, 12),
            DataType::Map(map_entries, true),
            DataType::RunEndEncoded(
                nested_field("run_ends", DataType::Int32, false),
                nested_field("values", DataType::Utf8, true),
            ),
        ];
        let fields = data_types
            .into_iter()
            .enumerate()
            .map(|(index, data_type)| Field::new(format!("c{index}"), data_type, index % 2 == 0))
            .collect::<Vec<_>>();
        let schema = Schema::new(fields);
        let slot_ids = (0..schema.fields().len() as u32).collect::<Vec<_>>();
        let path = FieldPath::root("schema");
        let (columns, metadata) = encode_schema(&schema, &slot_ids, false, path.clone()).unwrap();
        let decoded = decode_schema(&columns, &metadata, path).unwrap();
        assert_eq!(decoded.schema().as_ref(), &schema);
        assert_eq!(decoded.slot_ids(), slot_ids);
        assert!(decoded.internal().iter().all(|internal| !internal));
    }

    #[test]
    fn mixed_internal_flags_and_nested_nullability_round_trip_exactly() {
        let schema = Schema::new(vec![
            Field::new(
                "field_ids",
                DataType::List(nested_field("item", DataType::Int32, false)),
                false,
            ),
            Field::new("value", DataType::Binary, false),
        ]);
        let path = FieldPath::root("schema");
        let (columns, metadata) =
            encode_schema_with_internal(&schema, &[10, 11], &[true, false], path.clone()).unwrap();
        let decoded = decode_schema(&columns, &metadata, path).unwrap();
        assert_eq!(decoded.schema().as_ref(), &schema);
        assert_eq!(decoded.internal(), &[true, false]);
    }

    #[test]
    fn decoder_rejects_missing_type_duplicate_metadata_and_excessive_depth() {
        let path = FieldPath::root("schema");
        let schema = Schema::new(vec![Field::new("x", DataType::Int32, false)]);
        let (mut columns, metadata) = encode_schema(&schema, &[1], true, path.clone()).unwrap();
        columns[0].nodes[0].kind = None;
        assert_eq!(
            decode_schema(&columns, &metadata, path.clone())
                .unwrap_err()
                .kind(),
            ProtocolErrorKind::MissingField
        );

        let (mut columns, metadata) = encode_schema(&schema, &[1], true, path.clone()).unwrap();
        let field = columns[0].nodes[0].field.as_mut().unwrap();
        field.metadata = vec![
            plan::ArrowFieldMetadataEntry {
                key: "a".into(),
                value: "1".into(),
            },
            plan::ArrowFieldMetadataEntry {
                key: "a".into(),
                value: "2".into(),
            },
        ];
        assert_eq!(
            decode_schema(&columns, &metadata, path.clone())
                .unwrap_err()
                .kind(),
            ProtocolErrorKind::DuplicateField
        );

        let dictionary = Schema::new(vec![Field::new(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            false,
        )]);
        let (mut columns, metadata) = encode_schema(&dictionary, &[1], true, path.clone()).unwrap();
        columns[0].nodes[0]
            .field
            .as_mut()
            .expect("field")
            .dictionary_is_ordered = None;
        assert_eq!(
            decode_schema(&columns, &metadata, path.clone())
                .unwrap_err()
                .kind(),
            ProtocolErrorKind::MissingField
        );

        let (mut columns, metadata) = encode_schema(&schema, &[1], true, path.clone()).unwrap();
        columns[0].nodes[0]
            .field
            .as_mut()
            .expect("field")
            .dictionary_id = Some(7);
        assert_eq!(
            decode_schema(&columns, &metadata, path.clone())
                .unwrap_err()
                .kind(),
            ProtocolErrorKind::InconsistentFields
        );

        let mut nested = DataType::Int32;
        for depth in 0..MAX_WRITE_RELATION_TYPE_DEPTH {
            nested = DataType::List(Arc::new(Field::new(format!("d{depth}"), nested, false)));
        }
        let schema = Schema::new(vec![Field::new("too_deep", nested, false)]);
        assert_eq!(
            encode_schema(&schema, &[1], true, path).unwrap_err().kind(),
            ProtocolErrorKind::Capacity
        );
    }

    fn nested_structs(levels: usize) -> DataType {
        let mut data_type = DataType::Int32;
        for level in 0..levels {
            data_type = DataType::Struct(Fields::from(vec![Field::new(
                format!("s{level}"),
                data_type,
                true,
            )]));
        }
        data_type
    }

    fn nested_maps(levels: usize) -> DataType {
        let mut data_type = DataType::Int32;
        for _ in 0..levels {
            let entries = Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", data_type, true),
                ])),
                false,
            );
            data_type = DataType::Map(Arc::new(entries), false);
        }
        data_type
    }

    fn encode_one(
        data_type: DataType,
        internal: bool,
    ) -> Result<
        (
            Vec<plan::ArrowPhysicalColumn>,
            Vec<plan::ArrowFieldMetadataEntry>,
        ),
        ProtocolError,
    > {
        let schema = Schema::new(vec![Field::new("column", data_type, true)]);
        encode_schema(&schema, &[1], internal, FieldPath::root("schema"))
    }

    fn decode_kind(columns: &[plan::ArrowPhysicalColumn]) -> ProtocolErrorKind {
        decode_schema(columns, &[], FieldPath::root("schema"))
            .unwrap_err()
            .kind()
    }

    #[test]
    fn internal_columns_keep_the_physical_depth_budget() {
        // The root plus 31 lists puts the leaf at the 32nd level.
        let mut nested = DataType::Int32;
        for depth in 0..MAX_WRITE_RELATION_TYPE_DEPTH - 1 {
            nested = DataType::List(Arc::new(Field::new(format!("d{depth}"), nested, false)));
        }
        encode_one(nested.clone(), true).expect("32 physical levels fit an internal column");
        let deeper = DataType::List(Arc::new(Field::new("d", nested, false)));
        assert_eq!(
            encode_one(deeper, true).unwrap_err().kind(),
            ProtocolErrorKind::Capacity
        );
    }

    #[test]
    fn visible_columns_admit_logical_depth_64_and_reject_65() {
        let limit = LogicalTypeLimits::default().max_depth;
        let (columns, metadata) =
            encode_one(nested_structs(limit - 1), false).expect("64 logical levels");
        let decoded = decode_schema(&columns, &metadata, FieldPath::root("schema")).unwrap();
        assert_eq!(
            decoded.schema().field(0).data_type(),
            &nested_structs(limit - 1)
        );
        assert_eq!(
            encode_one(nested_structs(limit), false).unwrap_err().kind(),
            ProtocolErrorKind::Capacity
        );
        // The same 64-level tree exceeds an internal column's physical budget.
        assert_eq!(
            encode_one(nested_structs(limit - 1), true)
                .unwrap_err()
                .kind(),
            ProtocolErrorKind::Capacity
        );
    }

    #[test]
    fn visible_map_entries_are_not_logical_levels() {
        let limit = LogicalTypeLimits::default().max_depth;
        // 63 maps nest the innermost value at logical depth 64 while the
        // physical tree is nearly twice as deep.
        encode_one(nested_maps(limit - 1), false).expect("64 logical levels through maps");
        assert_eq!(
            encode_one(nested_maps(limit), false).unwrap_err().kind(),
            ProtocolErrorKind::Capacity
        );
    }

    #[test]
    fn visible_columns_bound_logical_nodes_excluding_wrappers() {
        let limit = LogicalTypeLimits::default().max_nodes;
        let struct_of = |members: usize, member: &dyn Fn(usize) -> Field| {
            DataType::Struct(Fields::from((0..members).map(member).collect::<Vec<_>>()))
        };
        let int_member = |index: usize| Field::new(format!("f{index}"), DataType::Int32, true);
        encode_one(struct_of(limit - 1, &int_member), false).expect("root plus 4095 members");
        assert_eq!(
            encode_one(struct_of(limit, &int_member), false)
                .unwrap_err()
                .kind(),
            ProtocolErrorKind::Capacity
        );
        // Each map member is a map, its entries wrapper, a key and a value:
        // four physical nodes but three logical ones.
        let map_member = |index: usize| {
            let entries = Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int32, true),
                ])),
                false,
            );
            Field::new(
                format!("m{index}"),
                DataType::Map(Arc::new(entries), false),
                true,
            )
        };
        let fitting = (limit - 1) / 3;
        encode_one(struct_of(fitting, &map_member), false).expect("wrappers are not counted");
        assert_eq!(
            encode_one(struct_of(fitting + 1, &map_member), false)
                .unwrap_err()
                .kind(),
            ProtocolErrorKind::Capacity
        );
    }

    #[test]
    fn visible_columns_bound_member_name_text() {
        let limit = LogicalTypeLimits::default().max_text_bytes;
        let long = MAX_WRITE_RELATION_FIELD_NAME_BYTES;
        let members = |extra: usize| {
            let mut fields = (0..limit / long)
                .map(|index| {
                    Field::new(
                        format!("{index:0>width$}", width = long),
                        DataType::Int32,
                        true,
                    )
                })
                .collect::<Vec<_>>();
            if extra > 0 {
                fields.push(Field::new("x".repeat(extra), DataType::Int32, true));
            }
            DataType::Struct(Fields::from(fields))
        };
        encode_one(members(0), false).expect("exactly the text budget");
        assert_eq!(
            encode_one(members(1), false).unwrap_err().kind(),
            ProtocolErrorKind::Capacity
        );
    }

    #[test]
    fn decoder_rejects_every_malformed_tree_shape() {
        let two_members = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let (columns, _) = encode_one(two_members, false).unwrap();
        let struct_children = |columns: &mut Vec<plan::ArrowPhysicalColumn>, children: Vec<u32>| {
            let Some(plan::arrow_physical_node::Kind::StructType(node)) =
                columns[0].nodes[0].kind.as_mut()
            else {
                panic!("root is a struct");
            };
            node.fields = children;
        };

        let mut empty = columns.clone();
        empty[0].nodes.clear();
        assert_eq!(decode_kind(&empty), ProtocolErrorKind::MissingField);

        let mut out_of_range = columns.clone();
        struct_children(&mut out_of_range, vec![1, 99]);
        assert_eq!(decode_kind(&out_of_range), ProtocolErrorKind::InvalidValue);

        let mut reused = columns.clone();
        struct_children(&mut reused, vec![1, 1]);
        assert_eq!(decode_kind(&reused), ProtocolErrorKind::InvalidValue);

        let mut swapped = columns.clone();
        struct_children(&mut swapped, vec![2, 1]);
        assert_eq!(decode_kind(&swapped), ProtocolErrorKind::InvalidValue);

        let mut back_edge = columns.clone();
        struct_children(&mut back_edge, vec![1, 0]);
        assert_eq!(decode_kind(&back_edge), ProtocolErrorKind::InvalidValue);

        let mut orphan = columns.clone();
        let extra = orphan[0].nodes[1].clone();
        orphan[0].nodes.push(extra);
        assert_eq!(decode_kind(&orphan), ProtocolErrorKind::InvalidValue);

        let mut bare_root = columns.clone();
        bare_root[0].nodes[0].field = None;
        assert_eq!(decode_kind(&bare_root), ProtocolErrorKind::MissingField);

        let mut bare_member = columns.clone();
        bare_member[0].nodes[2].field = None;
        assert_eq!(
            decode_kind(&bare_member),
            ProtocolErrorKind::InconsistentFields
        );

        #[allow(deprecated)]
        let dictionary_field = Field::new_dict(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            false,
            3,
            false,
        );
        let (dictionary, _) = encode_schema(
            &Schema::new(vec![dictionary_field]),
            &[1],
            true,
            FieldPath::root("schema"),
        )
        .unwrap();
        let mut fielded_key = dictionary.clone();
        fielded_key[0].nodes[1].field = fielded_key[0].nodes[0].field.clone();
        assert_eq!(
            decode_kind(&fielded_key),
            ProtocolErrorKind::InconsistentFields
        );
    }

    #[test]
    fn deep_schema_decodes_under_the_default_protobuf_recursion_limit() {
        use prost::Message;
        let limit = LogicalTypeLimits::default().max_depth;
        let (columns, schema_metadata) =
            encode_one(nested_maps(limit - 1), false).expect("64 logical levels");
        let bytes = plan::ArrowPhysicalSchema {
            columns,
            schema_metadata,
        }
        .encode_to_vec();
        let decoded = plan::ArrowPhysicalSchema::decode(bytes.as_slice())
            .expect("flat carrier nesting is constant");
        decode_schema(
            &decoded.columns,
            &decoded.schema_metadata,
            FieldPath::root("schema"),
        )
        .expect("decoded schema validates");
    }
}
