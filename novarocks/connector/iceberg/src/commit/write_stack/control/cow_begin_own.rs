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

// Private COW constructor specialization. Ordinary constructors remain unchanged.
// No capacity acquisition; every total includes the actual caller's existing E.
use crate::commit::write_stack::domain::{IcebergDataBranchRecipe, IcebergWriteTableFacts};
use crate::iceberg::spec::TableMetadata;
use crate::metadata::existing_serde_projection::{self as projection, OriginalScope};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use std::{
    fmt,
    mem::{align_of, size_of},
};

pub(super) enum Failure<E> {
    Original(E),
    Provider(ConnectorError),
    Projection(projection::Error<E>),
    Overflow,
    CapacityChanged,
}
impl<E> fmt::Debug for Failure<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Original(_) => "CowBeginOwn::Original",
            Self::Provider(_) => "CowBeginOwn::Provider",
            Self::Projection(_) => "CowBeginOwn::Projection",
            Self::Overflow => "CowBeginOwn::Overflow",
            Self::CapacityChanged => "CowBeginOwn::CapacityChanged",
        })
    }
}
impl<E> fmt::Display for Failure<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
fn add<E>(a: u64, b: u64) -> Result<u64, Failure<E>> {
    a.checked_add(b).ok_or(Failure::Overflow)
}
fn mul<E>(a: u64, b: u64) -> Result<u64, Failure<E>> {
    a.checked_mul(b).ok_or(Failure::Overflow)
}
fn check<E>(scope: &dyn OriginalScope<E>, total: u64) -> Result<(), Failure<E>> {
    scope.check_active().map_err(Failure::Original)?;
    scope.check_total(total).map_err(Failure::Original)
}
fn exact<E>(text: &str) -> Result<String, Failure<E>> {
    let mut out = String::with_capacity(text.len());
    if out.capacity() != text.len() {
        return Err(Failure::CapacityChanged);
    }
    out.push_str(text);
    Ok(out)
}
fn arc_str<E>(n: usize) -> Result<u64, Failure<E>> {
    let a = align_of::<usize>() as u64;
    let raw = add(2 * size_of::<usize>() as u64, n as u64)?;
    Ok(add(raw, a - 1)? / a * a)
}
// CatalogAdmissionRequest owns no additional heap beyond its target's TWO
// Arc<str> allocations; initiation and operation are scalar. Catalog runtime
// graph ownership is independent and not converted into an asserted zero.
pub(super) fn entry_upper<E>(namespace: &str, table: &str) -> Result<u64, Failure<E>> {
    add(arc_str(namespace.len())?, arc_str(table.len())?)
}

// Nova-owned diagnostic receipt for the unchanged location validator.
// Public URL parse/query decoding internals retain the original call owner;
// they are not represented as a proved zero-byte library heap.
pub(super) struct LocationValidationUpper {
    pub(super) maximum_whole_validator_temporary: u64,
}

pub(super) struct LoadedFacts {
    facts: IcebergWriteTableFacts,
    retained_upper: u64,
    // Same neutral holder; fields above exit before it. No activity lease.
    _original: novarocks_spi::connector::ConnectorPayloadRetentionGuard,
}
impl LoadedFacts {
    pub(super) fn facts(&self) -> &IcebergWriteTableFacts {
        &self.facts
    }
    pub(super) fn retained_upper(&self) -> u64 {
        self.retained_upper
    }
    pub(super) fn into_facts(self) -> IcebergWriteTableFacts {
        self.facts
    }
}
pub(super) fn loaded_facts<E>(
    metadata: &TableMetadata,
    namespace: &str,
    table: &str,
    target_ref: &str,
    base_snapshot_id: Option<i64>,
    base_sequence_number: i64,
    schema_id: i32,
    spec_id: i32,
    format_version: u8,
    original_caller_existing: u64,
    validation: &LocationValidationUpper,
    scope: &dyn OriginalScope<E>,
) -> Result<LoadedFacts, Failure<E>> {
    let mut uuid_buf = [0u8; 36];
    let uuid = metadata.uuid().hyphenated().encode_lower(&mut uuid_buf);
    let explicit_data = metadata.properties().get("write.data.path");
    let prefix = metadata.location().trim_end_matches('/');
    let data_len = match explicit_data {
        Some(value) => value.len(),
        None => prefix.len().checked_add(5).ok_or(Failure::Overflow)?,
    };
    let mut retained = size_of::<LoadedFacts>() as u64;
    for n in [
        uuid.len(),
        namespace.len(),
        table.len(),
        metadata.location().len(),
        data_len,
        target_ref.len(),
    ] {
        retained = add(retained, n as u64)?;
    }
    check(
        scope,
        add(
            add(original_caller_existing, retained)?,
            validation.maximum_whole_validator_temporary,
        )?,
    )?;
    let guard = scope.original_guard();
    let data = match explicit_data {
        Some(value) => exact(value)?,
        None => {
            let mut out = String::with_capacity(data_len);
            if out.capacity() != data_len {
                return Err(Failure::CapacityChanged);
            }
            out.push_str(prefix);
            out.push_str("/data");
            out
        }
    };
    // Original constructor preserves UUID/ref/location/sequence/version order.
    let facts = IcebergWriteTableFacts::try_new(
        exact(uuid)?,
        exact(namespace)?,
        exact(table)?,
        exact(metadata.location())?,
        data,
        exact(target_ref)?,
        base_snapshot_id,
        base_sequence_number,
        schema_id,
        spec_id,
        format_version,
    )
    .map_err(Failure::Provider)?;
    scope.check_active().map_err(Failure::Original)?;
    Ok(LoadedFacts {
        facts,
        retained_upper: retained,
        _original: guard,
    })
}

struct Count(usize);
impl fmt::Write for Count {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.0 = self.0.checked_add(text.len()).ok_or(fmt::Error)?;
        Ok(())
    }
}
fn transform_len<E>(transform: &crate::iceberg::spec::Transform) -> Result<usize, Failure<E>> {
    let mut count = Count(0);
    fmt::write(&mut count, format_args!("{transform}")).map_err(|_| Failure::Overflow)?;
    Ok(count.0)
}
fn transform<E>(value: &crate::iceberg::spec::Transform) -> Result<String, Failure<E>> {
    let n = transform_len(value)?;
    let mut text = String::with_capacity(n);
    if text.capacity() != n {
        return Err(Failure::CapacityChanged);
    }
    fmt::write(&mut text, format_args!("{value}")).map_err(|_| Failure::CapacityChanged)?;
    if text.len() != n {
        return Err(Failure::CapacityChanged);
    }
    Ok(text)
}

// Caller has already performed the ORIGINAL signed-input validation and
// Parquet parameter validation, in that order. existing_current includes
// caller E + actual LoadedFacts + actual signed input + data-output holder.
// No schema Serialize is moved before those original semantic first errors.
pub(super) fn data_recipe<E>(
    metadata: &TableMetadata,
    row_lineage: bool,
    existing_current: u64,
    validation: &LocationValidationUpper,
    scope: &dyn OriginalScope<E>,
) -> Result<IcebergDataBranchRecipe, Failure<E>> {
    let schema = metadata.current_schema();
    let fields = metadata.default_partition_spec().fields();
    let mut arrays = add(
        size_of::<IcebergDataBranchRecipe>() as u64,
        mul(fields.len() as u64, 3 * size_of::<String>() as u64)?,
    )?;
    // The missing-source error remains at its exact original row. This borrowed
    // pass only computes safe prefix allocations, never copies a name/field.
    // Include the original dynamic diagnostic's formatted bytes before emit.
    let mut missing_diagnostic = 0;
    for field in fields {
        scope.check_active().map_err(Failure::Original)?;
        let Some(source) = schema.field_by_id(field.source_id) else {
            let mut count = Count(0);
            fmt::write(
                &mut count,
                format_args!(
                    "Iceberg partition field {} names unknown source id {}",
                    field.name, field.source_id
                ),
            )
            .map_err(|_| Failure::Overflow)?;
            // Original format! can retain output capacity and its predecessor.
            // This diagnostic will instead use checked exact formatting below.
            missing_diagnostic = count.0 as u64;
            break;
        };
        arrays = add(arrays, source.name.len() as u64)?;
        arrays = add(arrays, field.name.len() as u64)?;
        arrays = add(arrays, transform_len(&field.transform)? as u64)?;
    }
    let total = add(add(existing_current, arrays)?, missing_diagnostic)?;
    check(scope, total)?;
    let mut sources = Vec::with_capacity(fields.len());
    let mut names = Vec::with_capacity(fields.len());
    let mut transforms = Vec::with_capacity(fields.len());
    if sources.capacity() != fields.len()
        || names.capacity() != fields.len()
        || transforms.capacity() != fields.len()
    {
        return Err(Failure::CapacityChanged);
    }
    for field in fields {
        scope.check_active().map_err(Failure::Original)?;
        let source = match schema.field_by_id(field.source_id) {
            Some(source) => source,
            None => {
                let mut count = Count(0);
                fmt::write(
                    &mut count,
                    format_args!(
                        "Iceberg partition field {} names unknown source id {}",
                        field.name, field.source_id
                    ),
                )
                .map_err(|_| Failure::Overflow)?;
                let mut text = String::with_capacity(count.0);
                if text.capacity() != count.0 {
                    return Err(Failure::CapacityChanged);
                }
                fmt::write(
                    &mut text,
                    format_args!(
                        "Iceberg partition field {} names unknown source id {}",
                        field.name, field.source_id
                    ),
                )
                .map_err(|_| Failure::CapacityChanged)?;
                return Err(Failure::Provider(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    text,
                )));
            }
        };
        sources.push(exact(&source.name)?);
        names.push(exact(&field.name)?);
        transforms.push(transform(&field.transform)?);
    }
    // All partition lookup errors still precede the same public default codec.
    let plan = projection::SchemaOnlyPlan::inspect(schema, scope).map_err(Failure::Projection)?;
    let validation_total = add(total, validation.maximum_whole_validator_temporary)?;
    // Existing try_new constructs a BTreeSet<&String> for duplicate names;
    // its cumulative split-node recipe is a separately required current input.
    // It cannot be asserted zero merely because the Strings already exist.
    let btree_temporary = duplicate_name_tree_upper::<E>(fields.len())?;
    let checked = plan
        .authorize(add(validation_total, btree_temporary)?, scope)
        .map_err(Failure::Projection)?;
    let backing = checked.build().map_err(Failure::Projection)?;
    scope.check_active().map_err(Failure::Original)?;
    let recipe =
        IcebergDataBranchRecipe::try_new_cow(backing, sources, names, transforms, row_lineage)
            .map_err(Failure::Provider)?;
    if let Err(original) = scope.check_active() {
        drop(recipe); // Actual unclaimed graph exits BEFORE returning original cause.
        return Err(Failure::Original(original));
    }
    Ok(recipe)
}
// Pinned std BTree B=6, a leaf/internal upper: parent pointer, two u16,
// 11 reference keys, 11 unit values, 12 edge pointers, all-field alignment pad.
// Inserting n references allocates <=2n+1 nodes cumulatively; this safe upper
// includes nodes before/after root split, rather than counting only live nodes.
fn duplicate_name_tree_upper<E>(n: usize) -> Result<u64, Failure<E>> {
    if n == 0 {
        return Ok(0);
    }
    let a = align_of::<usize>() as u64;
    let node = add(
        add(size_of::<usize>() as u64, 4)?,
        add(23 * size_of::<usize>() as u64, mul(9, a - 1)?)?,
    )?;
    mul(add(mul(2, n as u64)?, 1)?, node)
}

// Exact Field.clone ownership: Field name + metadata HashMap + cloned Strings;
// DataType only Dictionary has owned Box recursion in Arrow 58.4. Other nested
// variants share FieldRef/Fields/UnionFields; Timestamp timezone is Arc<str>.
fn datatype_clone_heap<E>(
    ty: &arrow::datatypes::DataType,
    scope: &dyn OriginalScope<E>,
) -> Result<u64, Failure<E>> {
    use arrow::datatypes::DataType::*;
    scope.check_active().map_err(Failure::Original)?;
    match ty {
        Dictionary(key, value) => add(
            2 * size_of::<arrow::datatypes::DataType>() as u64,
            add(
                datatype_clone_heap(key, scope)?,
                datatype_clone_heap(value, scope)?,
            )?,
        ),
        Null | Boolean | Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64
        | Float16 | Float32 | Float64 | Timestamp(..) | Date32 | Date64 | Time32(..)
        | Time64(..) | Duration(..) | Interval(..) | Binary | FixedSizeBinary(..) | LargeBinary
        | BinaryView | Utf8 | LargeUtf8 | Utf8View | List(..) | ListView(..)
        | FixedSizeList(..) | LargeList(..) | LargeListView(..) | Struct(..) | Union(..)
        | Decimal32(..) | Decimal64(..) | Decimal128(..) | Decimal256(..) | Map(..)
        | RunEndEncoded(..) => Ok(0),
    }
}
pub(super) fn field_clone_heap<E>(
    field: &arrow::datatypes::Field,
    scope: &dyn OriginalScope<E>,
) -> Result<u64, Failure<E>> {
    let mut total = add(
        field.name().len() as u64,
        datatype_clone_heap(field.data_type(), scope)?,
    )?;
    // Pinned std/hashbrown clone keeps the original raw bucket count.
    // Recover that layout from public capacity, controls and aligned pair slots.
    let layout = crate::read_snapshot::cow_capture::geometry::hash_actual::<
        novarocks_spi::connector::ConnectorCowBeginCause,
        (String, String),
    >(field.metadata().capacity())
    .map_err(|_| Failure::Overflow)?;
    total = add(total, layout)?;
    for (key, value) in field.metadata() {
        total = add(total, add(key.len() as u64, value.len() as u64)?)?;
    }
    Ok(total)
}
pub(super) struct SignedInput {
    shape: novarocks_spi::connector::ConnectorWriteInputShape,
    retained_upper: u64,
    _original: novarocks_spi::connector::ConnectorPayloadRetentionGuard,
}
impl SignedInput {
    pub(super) fn shape(&self) -> &novarocks_spi::connector::ConnectorWriteInputShape {
        &self.shape
    }
    pub(super) fn retained_upper(&self) -> u64 {
        self.retained_upper
    }
    pub(super) fn into_shape(self) -> novarocks_spi::connector::ConnectorWriteInputShape {
        self.shape
    }
}
pub(super) fn signed_input<E>(
    facts: &IcebergWriteTableFacts,
    request: &novarocks_spi::connector::ConnectorWriteInputRequest,
    existing_current: u64,
    scope: &dyn OriginalScope<E>,
) -> Result<SignedInput, Failure<E>> {
    use novarocks_spi::connector::{
        ConnectorWriteFieldBinding, ConnectorWriteFieldRequest, ConnectorWriteFieldToken,
        ConnectorWriteInputRequest, ConnectorWriteInputShape,
    };
    use sha2::{Digest, Sha256};
    let (first, second): (&[ConnectorWriteFieldRequest], &[ConnectorWriteFieldRequest]) =
        match request {
            ConnectorWriteInputRequest::Data { fields } => (fields, &[]),
            ConnectorWriteInputRequest::RowLineage {
                data_fields,
                row_identity_fields,
            } => (data_fields, row_identity_fields),
            ConnectorWriteInputRequest::PositionDelete {
                identity_fields,
                partition_source_fields,
            }
            | ConnectorWriteInputRequest::DeletionVector {
                identity_fields,
                partition_source_fields,
            } => (identity_fields, partition_source_fields),
            ConnectorWriteInputRequest::EqualityDelete { equality_fields } => {
                (equality_fields, &[])
            }
        };
    let n = first
        .len()
        .checked_add(second.len())
        .ok_or(Failure::Overflow)?;
    let mut retained = add(
        size_of::<SignedInput>() as u64,
        mul(n as u64, size_of::<ConnectorWriteFieldBinding>() as u64)?,
    )?;
    let mut duplicated_names = 0;
    for field in first.iter().chain(second) {
        scope.check_active().map_err(Failure::Original)?;
        retained = add(retained, field_clone_heap(field.field(), scope)?)?;
        duplicated_names = add(duplicated_names, field.field().name().len() as u64)?;
    }
    // Original validate() keeps Vec<&Binding>, two incrementally grown HashSets,
    // and duplicated name Strings. Pinned raw bucket/load/group geometry below
    // covers both current and predecessor tables during resize.
    use crate::read_snapshot::cow_capture::geometry;
    let token_table = geometry::hash_max::<
        novarocks_spi::connector::ConnectorCowBeginCause,
        ConnectorWriteFieldToken,
    >(n)
    .map_err(|_| Failure::Overflow)?;
    let name_table =
        geometry::hash_max::<novarocks_spi::connector::ConnectorCowBeginCause, String>(n)
            .map_err(|_| Failure::Overflow)?;
    let tables = mul(add(token_table, name_table)?, 2)?;
    let diagnostic = "connector write input shape must contain at least one field"
        .len()
        .max("connector write input shape contains a duplicate field token or name".len())
        as u64;
    let scratch = add(
        add(add(tables, duplicated_names)?, diagnostic)?,
        mul(n as u64, size_of::<usize>() as u64)?,
    )?;
    check(scope, add(add(existing_current, retained)?, scratch)?)?;
    let guard = scope.original_guard();
    struct HashWrite<'a>(&'a mut Sha256);
    impl fmt::Write for HashWrite<'_> {
        fn write_str(&mut self, text: &str) -> fmt::Result {
            self.0.update(text.as_bytes());
            Ok(())
        }
    }
    let sign = |tag: &str,
                fields: &[ConnectorWriteFieldRequest]|
     -> Result<Vec<ConnectorWriteFieldBinding>, Failure<E>> {
        let mut output = Vec::with_capacity(fields.len());
        if output.capacity() != fields.len() {
            return Err(Failure::CapacityChanged);
        }
        for (index, field) in fields.iter().enumerate() {
            scope.check_active().map_err(Failure::Original)?;
            let mut hasher = Sha256::new();
            hasher.update(b"novarocks.iceberg.write-stack.field.v1\0");
            hasher.update(facts.table_uuid().as_bytes());
            hasher.update([0]);
            hasher.update(facts.target_ref().as_bytes());
            hasher.update([0]);
            hasher.update(tag.as_bytes());
            hasher.update([0]);
            hasher.update(index.to_be_bytes());
            hasher.update(field.field().name().as_bytes());
            hasher.update([0]);
            fmt::write(
                &mut HashWrite(&mut hasher),
                format_args!("{:?}", field.field().data_type()),
            )
            .map_err(|_| Failure::CapacityChanged)?;
            hasher.update([u8::from(field.field().is_nullable())]);
            let token = ConnectorWriteFieldToken::from_bytes(hasher.finalize().into());
            output.push(ConnectorWriteFieldBinding::new(
                token,
                field.field().clone(),
            ));
        }
        Ok(output)
    };
    let shape = match request {
        ConnectorWriteInputRequest::Data { fields } => ConnectorWriteInputShape::Data {
            fields: sign("data", fields)?,
        },
        ConnectorWriteInputRequest::RowLineage {
            data_fields,
            row_identity_fields,
        } => ConnectorWriteInputShape::RowLineage {
            data_fields: sign("row-lineage-data", data_fields)?,
            row_identity_fields: sign("row-lineage-identity", row_identity_fields)?,
        },
        ConnectorWriteInputRequest::PositionDelete {
            identity_fields,
            partition_source_fields,
        } => ConnectorWriteInputShape::PositionDelete {
            identity_fields: sign("position-delete-identity", identity_fields)?,
            partition_source_fields: sign("position-delete-partition", partition_source_fields)?,
        },
        ConnectorWriteInputRequest::DeletionVector {
            identity_fields,
            partition_source_fields,
        } => ConnectorWriteInputShape::DeletionVector {
            identity_fields: sign("deletion-vector-identity", identity_fields)?,
            partition_source_fields: sign("deletion-vector-partition", partition_source_fields)?,
        },
        ConnectorWriteInputRequest::EqualityDelete { equality_fields } => {
            ConnectorWriteInputShape::EqualityDelete {
                equality_fields: sign("equality-delete", equality_fields)?,
            }
        }
    };
    shape.validate().map_err(Failure::Provider)?;
    scope.check_active().map_err(Failure::Original)?;
    Ok(SignedInput {
        shape,
        retained_upper: retained,
        _original: guard,
    })
}
