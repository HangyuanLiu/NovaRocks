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

//! Before-allocation bounds for the closed signed COW cast vocabulary.
//!
//! This preflight does not perform casts or change their values. The caller must
//! still invoke execution's canonical cast_array_to_target. Re-audit these
//! formulas when types::arrow_cast or Arrow 58.2 changes. Input capacity is a
//! declaration from a bounded source; this does not certify custom buffer owners.
//!
//! Arrow unary/parse kernels allocate round64(rows * native_width) values and
//! round64(ceil(rows / 8)) validity. Canonical decimal/largeint kernels additionally
//! hold concrete Vec<Option<Native>> buffers. Default primitive builders start at
//! 1024 elements and double: both generations are charged before growth. Decimal256
//! narrowing has two concrete Option vectors. Timestamp conversion can retain its
//! microsecond intermediate and an additional timezone-adjustment array. Utf8
//! decimal parsing also retains normalized strings and a split-parts vector.
//! Each column's output, temporary and copy bounds are summed, conservatively
//! covering earlier columns while the current column is cast. No rows * guessed
//! expansion factor is used. Only fixed header allowances scale with type nodes.

use std::collections::HashMap;
use std::mem::size_of;

use arrow::array::{Array, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit, i256};
use arrow::record_batch::RecordBatch;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorRowConversionFootprint,
};

const INPUT_BYTES: usize = 32 * 1024 * 1024;
const SELECTION_BYTES: usize = 64 * 1024 * 1024;
const TRANSFORM_BYTES: usize = 256 * 1024 * 1024;
const METADATA_BYTES: usize = 16 * 1024 * 1024;
const MAX_NODES: usize = 4096;
const MAX_DEPTH: usize = 64;
const MAX_ROWS: usize = 1_048_576;
// ArrayData/builder/Arc headers, their short vectors and numeric error strings.
// This allowance contains no values, offsets, Option vectors or payload copies.
const NODE_HEADERS: usize = 2048;

#[derive(Clone, Copy, Debug)]
pub(super) struct CowSignedCastFootprint {
    pub input_bytes: usize,
    pub output_bytes: usize,
    pub temporary_bytes: usize,
    pub copy_peak_bytes: usize,
    pub peak_bytes: usize,
}

impl CowSignedCastFootprint {
    /// `source_scratch_bytes` is decoder scratch under Input32M. An assembled
    /// record has its separate Assembly32M bound and belongs in `other_live_bytes`,
    /// alongside retained converters, key tables and any other live owners.
    pub(super) fn for_batch(
        schema: &Schema,
        batch: &RecordBatch,
        retained_selection_bytes: usize,
        source_scratch_bytes: usize,
        other_live_bytes: usize,
    ) -> Result<Self, ConnectorError> {
        Self::with_limit(
            schema,
            batch,
            retained_selection_bytes,
            source_scratch_bytes,
            other_live_bytes,
            TRANSFORM_BYTES,
        )
    }

    fn with_limit(
        schema: &Schema,
        batch: &RecordBatch,
        retained_selection_bytes: usize,
        source_scratch_bytes: usize,
        other_live_bytes: usize,
        limit: usize,
    ) -> Result<Self, ConnectorError> {
        if batch.num_rows() > MAX_ROWS || retained_selection_bytes > SELECTION_BYTES {
            return Err(exhausted());
        }
        // Bound every tree before PartialEq, SPI traversal, cloning or Debug.
        let target = TypeWalk::schema(schema)?;
        let source = TypeWalk::schema(batch.schema_ref())?;
        let mut arrays = TypeWalk::default();
        for array in batch.columns() {
            arrays.data_type(array.data_type(), 0)?;
        }
        if batch.num_columns() != schema.fields().len() {
            return Err(invalid(
                "COW match query output width differs from its signed contract",
            ));
        }
        let input_bytes = add(
            ConnectorRowConversionFootprint::retained_batch_bytes(batch)?,
            source_scratch_bytes,
        )?;
        if input_bytes > INPUT_BYTES {
            return Err(exhausted());
        }
        let mut total = Kernel::default();
        for (array, field) in batch.columns().iter().zip(schema.fields()) {
            let kernel = kernel(array.as_ref(), field.data_type())?;
            total.output = add(total.output, kernel.output)?;
            total.temporary = add(total.temporary, kernel.temporary)?;
            total.copy = add(total.copy, kernel.copy)?;
        }
        let nodes = add(add(target.nodes, source.nodes)?, arrays.nodes)?;
        // Debug escapes need at most six bytes per source byte. Retain the
        // originating error while formatting Arrow Display and the COW ordinal
        // context, including old/new String capacities (six escaped lengths).
        // This also covers metadata retags and short schema/array-data copies.
        let metadata = add(add(target.metadata, source.metadata)?, arrays.metadata)?;
        total.temporary = add(total.temporary, mul(nodes, NODE_HEADERS)?)?;
        total.temporary = add(total.temporary, mul(metadata, 36)?)?;
        let peak_bytes = add(
            add(
                add(input_bytes, retained_selection_bytes)?,
                other_live_bytes,
            )?,
            add(add(total.output, total.temporary)?, total.copy)?,
        )?;
        if peak_bytes > limit {
            return Err(exhausted());
        }
        Ok(Self {
            input_bytes,
            output_bytes: total.output,
            temporary_bytes: total.temporary,
            copy_peak_bytes: total.copy,
            peak_bytes,
        })
    }
}

fn exhausted() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "COW signed cast exceeds its fixed input, metadata or transform workspace",
    )
}
fn invalid(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}
fn unsupported() -> ConnectorError {
    invalid("COW signed cast has no closed allocation bound for this type pair")
}
fn add(a: usize, b: usize) -> Result<usize, ConnectorError> {
    a.checked_add(b).ok_or_else(exhausted)
}
fn mul(a: usize, b: usize) -> Result<usize, ConnectorError> {
    a.checked_mul(b).ok_or_else(exhausted)
}
fn round64(bytes: usize) -> Result<usize, ConnectorError> {
    Ok(add(bytes, 63)? / 64 * 64)
}
fn bitmap(rows: usize) -> Result<usize, ConnectorError> {
    round64(add(rows, 7)? / 8)
}
fn geometric(items: usize, initial: usize) -> Result<usize, ConnectorError> {
    let mut capacity = initial;
    while capacity < items {
        capacity = mul(capacity, 2)?;
    }
    Ok(capacity)
}

#[derive(Default)]
struct Kernel {
    output: usize,
    temporary: usize,
    copy: usize,
}
impl Kernel {
    fn fixed(rows: usize, ty: &DataType) -> Result<Self, ConnectorError> {
        let values = if ty == &DataType::Boolean {
            bitmap(rows)?
        } else {
            round64(mul(rows, fixed_width(ty).ok_or_else(unsupported)?)?)?
        };
        Ok(Self {
            output: add(values, bitmap(rows)?)?,
            ..Self::default()
        })
    }
    fn option<T>(mut self, rows: usize) -> Result<Self, ConnectorError> {
        self.temporary = add(self.temporary, mul(rows, size_of::<Option<T>>())?)?;
        Ok(self)
    }
    fn default_builder(rows: usize, ty: &DataType) -> Result<Self, ConnectorError> {
        let capacity = geometric(rows, 1024)?;
        let mut result = Self::fixed(capacity, ty)?;
        // Vec and MutableBuffer growth both retain the old allocation until
        // realloc returns. The old capacity is at most the counted new capacity.
        result.copy = result.output;
        Ok(result)
    }
}

fn numeric(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
    )
}
fn signed_integer(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}
fn datetime_source(ty: &DataType) -> bool {
    (numeric(ty) && ty != &DataType::Float16)
        || matches!(
            ty,
            DataType::Boolean | DataType::Decimal128(_, _) | DataType::FixedSizeBinary(16)
        )
}
fn fixed_width(ty: &DataType) -> Option<usize> {
    match ty {
        DataType::Int8 | DataType::UInt8 => Some(1),
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => Some(2),
        DataType::Int32
        | DataType::UInt32
        | DataType::Float32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Decimal32(_, _) => Some(4),
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Decimal64(_, _) => Some(8),
        DataType::Decimal128(_, _) => Some(16),
        DataType::Decimal256(_, _) => Some(32),
        DataType::FixedSizeBinary(width) => usize::try_from(*width).ok(),
        DataType::Interval(_) => ty.primitive_width(),
        _ => None,
    }
}

fn kernel(array: &dyn Array, target: &DataType) -> Result<Kernel, ConnectorError> {
    let source = array.data_type();
    let rows = array.len();
    if source == target || target == &DataType::Null {
        return Ok(Kernel::default());
    }
    if source == &DataType::Null {
        // Null is physically tiny but its target can expand by an arbitrary
        // fixed-binary width. Count that width before new_null_array allocates.
        return Ok(Kernel {
            output: null_output(rows, target)?,
            ..Kernel::default()
        });
    }
    let fixed = || Kernel::fixed(rows, target);
    match (source, target) {
        (DataType::List(source), DataType::List(target))
            if source.data_type() == target.data_type() =>
        {
            if !target.is_nullable() && source.is_nullable() {
                // A narrowing retag can make ListArray::new panic. Its generic
                // logical-null validation is outside this metadata-only proof.
                return Err(unsupported());
            }
            // execution's explicit metadata-only List branch reuses offsets,
            // child and validity. A new ListArray header is charged by TypeWalk.
            Ok(Kernel::default())
        }
        (DataType::Struct(source), DataType::Struct(target)) => {
            let array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(unsupported)?;
            if source.len() != target.len() {
                return Err(unsupported());
            }
            // Match the canonical HashMap's last duplicate-name winner without
            // building that map before admission. If any name is missing, the
            // kernel uses ordinals for every field.
            let by_name = target
                .iter()
                .all(|t| source.iter().any(|s| s.name() == t.name()));
            for (index, field) in target.iter().enumerate() {
                let source_index = if by_name {
                    source
                        .iter()
                        .rposition(|s| s.name() == field.name())
                        .expect("all names exist")
                } else {
                    index
                };
                if array.column(source_index).data_type() != field.data_type() {
                    // This also refuses the canonical all-null child branch:
                    // new_null_array(target, child.len()) can expand arbitrarily.
                    return Err(unsupported());
                }
                if !field.is_nullable() {
                    let child = array.column(source_index);
                    if matches!(
                        child.data_type(),
                        DataType::Null
                            | DataType::Dictionary(_, _)
                            | DataType::Union(_, _)
                            | DataType::RunEndEncoded(_, _)
                    ) {
                        // These logical-null computations can allocate another
                        // bitmap/child workspace; do not invoke them to preflight.
                        return Err(unsupported());
                    }
                    if let Some(nulls) = child.nulls() {
                        if nulls.null_count() != 0
                            && !array.nulls().is_some_and(|parent| parent.contains(nulls))
                        {
                            return Err(invalid(
                                "COW signed Struct retag has unmasked nonnullable child nulls",
                            ));
                        }
                    }
                }
            }
            // HashMap<&str, usize>, ArrayRef/Field vectors and Fields Vec -> Arc
            // copies are each below the explicit per-node 2048-byte allowance.
            Ok(Kernel::default())
        }
        // Canonical relaxed kernels hold their Option vector through Arrow's
        // values/validity construction, then retag shared buffers.
        (DataType::Decimal128(_, a), DataType::Decimal128(_, b)) => {
            b.checked_sub(*a).ok_or_else(exhausted)?;
            a.checked_sub(*b).ok_or_else(exhausted)?;
            fixed()?.option::<i128>(rows)
        }
        (DataType::Decimal256(_, a), DataType::Decimal256(_, b)) => {
            b.checked_sub(*a).ok_or_else(exhausted)?;
            a.checked_sub(*b).ok_or_else(exhausted)?;
            fixed()?.option::<i256>(rows)
        }
        (DataType::Boolean | DataType::FixedSizeBinary(16), DataType::Decimal128(_, _)) => {
            fixed()?.option::<i128>(rows)
        }
        (DataType::Boolean | DataType::FixedSizeBinary(16), DataType::Decimal256(_, _)) => {
            fixed()?.option::<i256>(rows)
        }
        (s, DataType::Decimal128(_, _))
            if signed_integer(s) || matches!(s, DataType::Float32 | DataType::Float64) =>
        {
            fixed()?.option::<i128>(rows)
        }
        (DataType::Decimal256(_, _), t) if signed_integer(t) => {
            // Both vectors may coexist during IntoIter::collect. No reliance
            // on Rust's optional in-place Vec collection optimization.
            let result = fixed()?.option::<i128>(rows)?;
            match t {
                DataType::Int8 => result.option::<i8>(rows),
                DataType::Int16 => result.option::<i16>(rows),
                DataType::Int32 => result.option::<i32>(rows),
                DataType::Int64 => result.option::<i64>(rows),
                _ => unreachable!(),
            }
        }
        (DataType::Decimal256(_, _), DataType::Float32) => fixed()?.option::<f32>(rows),
        (DataType::Decimal256(_, _), DataType::Float64) => fixed()?.option::<f64>(rows),
        (DataType::Decimal256(_, _), DataType::Boolean) => fixed()?.option::<bool>(rows),
        (DataType::Decimal256(_, _), DataType::FixedSizeBinary(16)) => {
            fixed()?.option::<i128>(rows)
        }
        (s, DataType::FixedSizeBinary(16)) if datetime_source(s) || s == &DataType::Utf8 => {
            fixed()?.option::<i128>(rows)
        }
        (DataType::FixedSizeBinary(16), t) if signed_integer(t) || t == &DataType::Boolean => {
            Kernel::default_builder(rows, target)
        }
        (s, DataType::Date32) if datetime_source(s) || s == &DataType::Utf8 => {
            Kernel::default_builder(rows, target)
        }
        (s, DataType::Timestamp(unit, _)) if datetime_source(s) || s == &DataType::Utf8 => {
            let mut result = fixed()?.option::<i64>(rows)?;
            if s == &DataType::Utf8 && *unit == TimeUnit::Nanosecond {
                // Result collection has a zero lower size hint and can grow.
                // Count old/new Vec<Option<i64>> rather than its final length.
                result.temporary = mul(2 * size_of::<Option<i64>>(), geometric(rows, 4)?)?;
            }
            if *unit != TimeUnit::Nanosecond {
                // Microsecond array, then possible unit conversion and timezone
                // adjustment. All three fixed arrays may transiently coexist.
                result.temporary = add(result.temporary, mul(2, result.output)?)?;
            }
            if s == &DataType::Utf8 {
                result.temporary = add(result.temporary, utf8_error_peak(array)?)?;
            }
            Ok(result)
        }
        (DataType::Timestamp(_, _), DataType::Timestamp(_, _)) => {
            let mut result = fixed()?;
            // Converted values can coexist with timezone-adjusted values.
            result.temporary = result.output;
            Ok(result)
        }
        (DataType::Date32 | DataType::Date64, DataType::Timestamp(_, _)) => {
            let mut result = fixed()?;
            // Date -> timestamp unary, then a possible timezone conversion and
            // adjustment under Arrow's cast_with_options.
            result.temporary = mul(2, result.output)?;
            Ok(result)
        }
        (DataType::Timestamp(_, _), DataType::Date32 | DataType::Date64) => fixed(),
        (DataType::Timestamp(_, _), t) if numeric(t) => fixed(),
        (DataType::Timestamp(_, _), DataType::Decimal128(_, _) | DataType::Decimal256(_, _)) => {
            fixed()
        }
        (DataType::Date32, DataType::Date64 | DataType::Int32 | DataType::Int64)
        | (DataType::Date64, DataType::Date32 | DataType::Int32 | DataType::Int64)
        | (DataType::Int32 | DataType::Int64, DataType::Date64) => fixed(),
        (DataType::Utf8, DataType::Boolean) => Kernel::default_builder(rows, target),
        (DataType::Utf8, DataType::Decimal128(_, _)) => utf8_decimal(array, target, true),
        (DataType::Utf8, DataType::Decimal256(_, _)) => utf8_decimal(array, target, false),
        (DataType::Utf8, t) if numeric(t) => fixed(),
        (s, t)
            if (numeric(s) || s == &DataType::Boolean)
                && (numeric(t) || t == &DataType::Boolean) =>
        {
            fixed()
        }
        // Arrow's decimal cross-width, integer/float to decimal256 and unsigned
        // integer to decimal128 paths use unary/unary_opt, without Option Vecs.
        (DataType::Decimal128(_, a), DataType::Decimal256(_, b))
        | (DataType::Decimal256(_, a), DataType::Decimal128(_, b)) => {
            b.checked_sub(*a).ok_or_else(exhausted)?;
            a.checked_sub(*b).ok_or_else(exhausted)?;
            fixed()
        }
        (s, DataType::Decimal128(_, _) | DataType::Decimal256(_, _))
            if numeric(s) && s != &DataType::Float16 =>
        {
            fixed()
        }
        (DataType::Decimal128(_, _), t) if numeric(t) && t != &DataType::Float16 => fixed(),
        (DataType::Decimal256(_, _), t)
            if matches!(
                t,
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
            ) =>
        {
            fixed()
        }
        // Exact nested identity and borrowed List/Struct retags are above.
        // Recursive child conversion, JSON/container conversion,
        // variable output, dictionaries and unlisted pairs cannot fall back.
        _ => Err(unsupported()),
    }
}

/// ArrayData::new_null allocates all declared values/offsets plus one validity
/// bitmap per node. List children are empty, Struct children have the parent
/// length, and FixedSizeList multiplies it. Buffer payload is never shared with
/// input. Private ArrayData/child vectors are in the pre-counted node allowance.
fn null_output(rows: usize, target: &DataType) -> Result<usize, ConnectorError> {
    if rows > MAX_ROWS {
        return Err(exhausted());
    }
    if target == &DataType::Null {
        return Ok(0);
    }
    if fixed_width(target).is_some() || target == &DataType::Boolean {
        return Ok(Kernel::fixed(rows, target)?.output);
    }
    let validity = bitmap(rows)?;
    let offsets = |width| round64(mul(add(rows, 1)?, width)?);
    let values = match target {
        DataType::Utf8 | DataType::Binary => offsets(4)?,
        DataType::LargeUtf8 | DataType::LargeBinary => offsets(8)?,
        DataType::Utf8View | DataType::BinaryView => round64(mul(rows, 16)?)?,
        DataType::List(field) => add(offsets(4)?, null_output(0, field.data_type())?)?,
        DataType::LargeList(field) => add(offsets(8)?, null_output(0, field.data_type())?)?,
        DataType::ListView(field) => add(
            mul(2, round64(mul(rows, 4)?)?)?,
            null_output(0, field.data_type())?,
        )?,
        DataType::LargeListView(field) => add(
            mul(2, round64(mul(rows, 8)?)?)?,
            null_output(0, field.data_type())?,
        )?,
        DataType::FixedSizeList(field, width) => null_output(
            mul(rows, usize::try_from(*width).map_err(|_| exhausted())?)?,
            field.data_type(),
        )?,
        DataType::Struct(fields) => {
            let mut bytes = 0;
            for field in fields {
                bytes = add(bytes, null_output(rows, field.data_type())?)?;
            }
            bytes
        }
        _ => return Err(unsupported()),
    };
    add(values, validity)
}

fn utf8_error_peak(array: &dyn Array) -> Result<usize, ConnectorError> {
    let strings = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(unsupported)?;
    let largest = strings.iter().flatten().map(str::len).max().unwrap_or(0);
    // Debug can escape each byte to six characters. An originating Arrow error
    // remains alive during Display -> String and the caller's ordinal context.
    // Six escaped lengths cover these retained and old/new String generations.
    mul(6, add(mul(largest, 6)?, 256)?)
}

fn utf8_decimal(
    array: &dyn Array,
    target: &DataType,
    normalize: bool,
) -> Result<Kernel, ConnectorError> {
    let strings = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(unsupported)?;
    let mut result = Kernel::fixed(array.len(), target)?;
    let mut largest_parts = 1;
    let mut copied_bytes = 0;
    for value in strings.iter().flatten() {
        largest_parts = largest_parts.max(add(value.bytes().filter(|b| *b == b'.').count(), 1)?);
        if !value.trim().is_empty() {
            copied_bytes = add(copied_bytes, value.len())?;
        }
    }
    // split('.').collect::<Vec<&str>>() starts at four slots and doubles.
    result.temporary = mul(2 * size_of::<&str>(), geometric(largest_parts, 4)?)?;
    result.temporary = add(result.temporary, utf8_error_peak(array)?)?;
    if normalize {
        // types::arrow_cast normalizes empty Utf8 to null with StringBuilder::new:
        // values start at 1024 bytes; offsets start at 1025 i32; validity starts
        // at 1024 bits. Count retained buffers plus their old growth generations.
        let values = geometric(copied_bytes, 1024)?;
        let offsets = mul(geometric(add(array.len(), 1)?, 1025)?, size_of::<i32>())?;
        let validity = bitmap(geometric(array.len(), 1024)?)?;
        result.temporary = add(
            result.temporary,
            mul(2, add(add(values, offsets)?, validity)?)?,
        )?;
    }
    Ok(result)
}

#[derive(Default)]
struct TypeWalk {
    nodes: usize,
    metadata: usize,
}
impl TypeWalk {
    fn schema(schema: &Schema) -> Result<Self, ConnectorError> {
        if schema.fields().len() > MAX_NODES {
            return Err(exhausted());
        }
        let mut walk = Self::default();
        walk.metadata(schema.metadata())?;
        for field in schema.fields() {
            walk.field(field, 0)?;
        }
        Ok(walk)
    }
    fn bytes(&mut self, bytes: usize) -> Result<(), ConnectorError> {
        self.metadata = add(self.metadata, bytes)?;
        if self.metadata > METADATA_BYTES {
            return Err(exhausted());
        }
        Ok(())
    }
    fn metadata(&mut self, values: &HashMap<String, String>) -> Result<(), ConnectorError> {
        if values.capacity() > MAX_NODES {
            return Err(exhausted());
        }
        self.bytes(mul(values.capacity(), size_of::<(String, String)>())?)?;
        for (key, value) in values {
            self.bytes(add(key.capacity(), value.capacity())?)?;
        }
        Ok(())
    }
    fn field(&mut self, field: &Field, depth: usize) -> Result<(), ConnectorError> {
        self.bytes(field.name().capacity())?;
        self.metadata(field.metadata())?;
        self.data_type(field.data_type(), depth)
    }
    fn data_type(&mut self, ty: &DataType, depth: usize) -> Result<(), ConnectorError> {
        if depth > MAX_DEPTH || self.nodes == MAX_NODES {
            return Err(exhausted());
        }
        self.nodes += 1;
        match ty {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
            | DataType::FixedSizeList(field, _)
            | DataType::Map(field, _) => {
                if matches!(ty, DataType::FixedSizeList(_, n) if *n < 0) {
                    return Err(invalid("COW signed cast has a negative fixed list width"));
                }
                self.field(field, depth + 1)?;
            }
            DataType::Struct(fields) => {
                for field in fields {
                    self.field(field, depth + 1)?;
                }
            }
            DataType::Union(fields, _) => {
                for (_, field) in fields.iter() {
                    self.field(field, depth + 1)?;
                }
            }
            DataType::Dictionary(key, value) => {
                self.data_type(key, depth + 1)?;
                self.data_type(value, depth + 1)?;
            }
            DataType::RunEndEncoded(run, value) => {
                self.field(run, depth + 1)?;
                self.field(value, depth + 1)?;
            }
            DataType::Timestamp(_, Some(zone)) => self.bytes(zone.len())?,
            DataType::FixedSizeBinary(n) if *n < 0 => {
                return Err(invalid("COW signed cast has a negative fixed binary width"));
            }
            DataType::Decimal128(p, s) if *p == 0 || *p > 38 || !(-38..=38).contains(s) => {
                return Err(invalid("COW signed cast has invalid decimal128 metadata"));
            }
            DataType::Decimal256(p, s) if *p == 0 || *p > 76 || !(-76..=76).contains(s) => {
                return Err(invalid("COW signed cast has invalid decimal256 metadata"));
            }
            DataType::Decimal32(p, s) if *p == 0 || *p > 9 || !(-9..=9).contains(s) => {
                return Err(invalid("COW signed cast has invalid decimal32 metadata"));
            }
            DataType::Decimal64(p, s) if *p == 0 || *p > 18 || !(-18..=18).contains(s) => {
                return Err(invalid("COW signed cast has invalid decimal64 metadata"));
            }
            DataType::Time32(TimeUnit::Microsecond | TimeUnit::Nanosecond)
            | DataType::Time64(TimeUnit::Second | TimeUnit::Millisecond) => {
                return Err(invalid("COW signed cast has invalid time unit metadata"));
            }
            _ => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Decimal128Array, Int8Array, Int64Array, ListArray, NullArray};
    use arrow::buffer::OffsetBuffer;
    use std::sync::Arc;

    fn batch(columns: Vec<ArrayRef>) -> RecordBatch {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .enumerate()
                .map(|(n, a)| Field::new(n.to_string(), a.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        RecordBatch::try_new(schema, columns).unwrap()
    }
    fn target(types: Vec<DataType>) -> Schema {
        Schema::new(
            types
                .into_iter()
                .enumerate()
                .map(|(n, t)| Field::new(n.to_string(), t, true))
                .collect::<Vec<_>>(),
        )
    }
    #[test]
    fn null_fixed_width_expansion_is_refused_without_constructing_target() {
        let input = batch(vec![Arc::new(NullArray::new(1024))]);
        let schema = target(vec![DataType::FixedSizeBinary(1024 * 1024)]);
        let error = CowSignedCastFootprint::for_batch(&schema, &input, 0, 0, 0).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    }
    #[test]
    fn numeric_narrowing_preserves_canonical_values_and_nulls() {
        let input = batch(vec![Arc::new(Int64Array::from(vec![
            Some(1),
            Some(128),
            None,
        ]))]);
        let schema = target(vec![DataType::Int8]);
        let p = CowSignedCastFootprint::for_batch(&schema, &input, 0, 0, 0).unwrap();
        let cast = novarocks_execution::exec::expr::cast_array_to_target(
            &input.columns()[0],
            &DataType::Int8,
        )
        .unwrap();
        assert_eq!(
            cast.as_any().downcast_ref::<Int8Array>().unwrap(),
            &Int8Array::from(vec![Some(1), None, None])
        );
        assert!(cast.get_array_memory_size() <= p.output_bytes + NODE_HEADERS);
    }
    #[test]
    fn decimal_option_vectors_and_fixed_expansion_are_explicit() {
        let input = batch(vec![Arc::new(Int8Array::from(vec![1; 4096]))]);
        let narrow = target(vec![DataType::Int64]);
        let wide = target(vec![DataType::Decimal128(20, 4)]);
        let a = CowSignedCastFootprint::for_batch(&narrow, &input, 0, 0, 0).unwrap();
        let b = CowSignedCastFootprint::for_batch(&wide, &input, 0, 0, 0).unwrap();
        assert!(b.output_bytes > a.output_bytes);
        assert!(b.temporary_bytes >= 4096 * size_of::<Option<i128>>());
        let array: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(123), None])
                .with_precision_and_scale(20, 2)
                .unwrap(),
        );
        let input = batch(vec![array]);
        let p = CowSignedCastFootprint::for_batch(
            &target(vec![DataType::Decimal256(30, 4)]),
            &input,
            0,
            0,
            0,
        )
        .unwrap();
        assert!(p.output_bytes >= 2 * size_of::<i256>());
    }
    #[test]
    fn columns_are_summed_before_first_cast_and_live_owners_share_peak() {
        let input = batch(vec![
            Arc::new(NullArray::new(1024)),
            Arc::new(NullArray::new(1024)),
        ]);
        let schema = target(vec![
            DataType::FixedSizeBinary(131072),
            DataType::FixedSizeBinary(131072),
        ]);
        assert!(CowSignedCastFootprint::for_batch(&schema, &input, 0, 0, 0).is_err());
        let input = batch(vec![Arc::new(Int64Array::from(vec![1]))]);
        let schema = target(vec![DataType::Int8]);
        let p = CowSignedCastFootprint::for_batch(&schema, &input, SELECTION_BYTES, 0, 0).unwrap();
        assert!(
            CowSignedCastFootprint::with_limit(
                &schema,
                &input,
                SELECTION_BYTES,
                0,
                0,
                p.peak_bytes - 1
            )
            .is_err()
        );
        assert!(CowSignedCastFootprint::for_batch(&schema, &input, 0, INPUT_BYTES, 0).is_err());
        assert!(CowSignedCastFootprint::for_batch(&schema, &input, 0, 0, TRANSFORM_BYTES).is_err());
    }
    #[test]
    fn json_container_and_deep_type_are_refused_before_generic_cast() {
        let input = batch(vec![Arc::new(StringArray::from(vec!["[1,2]"]))]);
        let mut ty = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let error = CowSignedCastFootprint::for_batch(&target(vec![ty.clone()]), &input, 0, 0, 0)
            .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        for _ in 0..MAX_DEPTH {
            ty = DataType::List(Arc::new(Field::new("item", ty, true)));
        }
        assert!(CowSignedCastFootprint::for_batch(&target(vec![ty]), &input, 0, 0, 0).is_err());
    }
    #[test]
    fn utf8_numeric_and_decimal_keep_canonical_parser_semantics() {
        let input = batch(vec![Arc::new(StringArray::from(vec![
            Some("12"),
            Some("bad"),
            Some(""),
            None,
        ]))]);
        let schema = target(vec![DataType::Int64]);
        CowSignedCastFootprint::for_batch(&schema, &input, 0, 0, 0).unwrap();
        let cast = novarocks_execution::exec::expr::cast_array_to_target(
            &input.columns()[0],
            &DataType::Int64,
        )
        .unwrap();
        assert_eq!(
            cast.as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(vec![Some(12), None, None, None])
        );
        let ty = DataType::Decimal128(20, 2);
        let p =
            CowSignedCastFootprint::for_batch(&target(vec![ty.clone()]), &input, 0, 0, 0).unwrap();
        let cast = novarocks_execution::exec::expr::cast_array_to_target(&input.columns()[0], &ty)
            .unwrap();
        assert!(cast.get_array_memory_size() <= p.output_bytes + NODE_HEADERS);
        assert_eq!(cast.null_count(), 3);
    }
    #[test]
    fn nested_identity_and_metadata_only_struct_retag_share_payload() {
        let child: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let fields = vec![Arc::new(Field::new("old", DataType::Int64, true))];
        let nested: ArrayRef = Arc::new(StructArray::new(
            fields.into(),
            vec![Arc::clone(&child)],
            None,
        ));
        let input = batch(vec![nested]);
        let exact = CowSignedCastFootprint::for_batch(input.schema_ref(), &input, 0, 0, 0).unwrap();
        assert_eq!(exact.output_bytes, 0);
        let ty = DataType::Struct(vec![Arc::new(Field::new("new", DataType::Int64, true))].into());
        let p =
            CowSignedCastFootprint::for_batch(&target(vec![ty.clone()]), &input, 0, 0, 0).unwrap();
        assert_eq!(p.output_bytes, 0);
        let cast = novarocks_execution::exec::expr::cast_array_to_target(&input.columns()[0], &ty)
            .unwrap();
        assert!(Arc::ptr_eq(
            cast.as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .column(0),
            &child
        ));
    }
    #[test]
    fn all_null_nested_changed_child_types_refuse_but_exact_retags_share() {
        let child: ArrayRef = Arc::new(NullArray::new(4));
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("old", DataType::Null, true)),
            OffsetBuffer::new(vec![0_i32, 4].into()),
            Arc::clone(&child),
            None,
        ));
        let input = batch(vec![list]);
        let changed = DataType::List(Arc::new(Field::new(
            "new",
            DataType::FixedSizeBinary(i32::MAX),
            true,
        )));
        let error =
            CowSignedCastFootprint::for_batch(&target(vec![changed]), &input, 0, 0, 0).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        let exact = DataType::List(Arc::new(Field::new("new", DataType::Null, true)));
        let p = CowSignedCastFootprint::for_batch(&target(vec![exact.clone()]), &input, 0, 0, 0)
            .unwrap();
        assert_eq!(p.output_bytes, 0);
        let cast =
            novarocks_execution::exec::expr::cast_array_to_target(&input.columns()[0], &exact)
                .unwrap();
        assert!(Arc::ptr_eq(
            cast.as_any().downcast_ref::<ListArray>().unwrap().values(),
            &child
        ));

        let nested: ArrayRef = Arc::new(StructArray::new(
            vec![Arc::new(Field::new("old", DataType::Null, true))].into(),
            vec![Arc::clone(&child)],
            None,
        ));
        let input = batch(vec![nested]);
        let changed = DataType::Struct(
            vec![Arc::new(Field::new(
                "new",
                DataType::FixedSizeBinary(i32::MAX),
                true,
            ))]
            .into(),
        );
        let error =
            CowSignedCastFootprint::for_batch(&target(vec![changed]), &input, 0, 0, 0).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        let exact =
            DataType::Struct(vec![Arc::new(Field::new("new", DataType::Null, true))].into());
        let p = CowSignedCastFootprint::for_batch(&target(vec![exact.clone()]), &input, 0, 0, 0)
            .unwrap();
        assert_eq!(p.output_bytes, 0);
        let cast =
            novarocks_execution::exec::expr::cast_array_to_target(&input.columns()[0], &exact)
                .unwrap();
        assert!(Arc::ptr_eq(
            cast.as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .column(0),
            &child
        ));
    }
    #[test]
    fn borrowed_metadata_and_node_limits_precede_type_formatting() {
        let input = batch(vec![Arc::new(Int64Array::from(vec![1]))]);
        let mut metadata = HashMap::new();
        metadata.insert("large".to_string(), "x".repeat(METADATA_BYTES + 1));
        let schema = Schema::new(vec![
            Field::new("x", DataType::Int8, true).with_metadata(metadata),
        ]);
        assert!(CowSignedCastFootprint::for_batch(&schema, &input, 0, 0, 0).is_err());
        let ty = DataType::Struct(
            (0..=MAX_NODES)
                .map(|_| Arc::new(Field::new("x", DataType::Int8, true)))
                .collect::<Vec<_>>()
                .into(),
        );
        assert!(CowSignedCastFootprint::for_batch(&target(vec![ty]), &input, 0, 0, 0).is_err());
    }
    #[test]
    fn null_string_and_nested_targets_have_closed_offsets_and_child_lengths() {
        let input = batch(vec![Arc::new(NullArray::new(1024))]);
        let string = target(vec![DataType::Utf8]);
        let p = CowSignedCastFootprint::for_batch(&string, &input, 0, 0, 0).unwrap();
        assert_eq!(
            p.output_bytes,
            round64(1025 * 4).unwrap() + bitmap(1024).unwrap()
        );
        let cast = novarocks_execution::exec::expr::cast_array_to_target(
            &input.columns()[0],
            &DataType::Utf8,
        )
        .unwrap();
        assert_eq!(cast.null_count(), 1024);
        assert!(cast.get_array_memory_size() <= p.output_bytes + NODE_HEADERS);
        let ty = DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Decimal256(50, 2), true)),
            16,
        );
        let p =
            CowSignedCastFootprint::for_batch(&target(vec![ty.clone()]), &input, 0, 0, 0).unwrap();
        assert!(p.output_bytes >= 1024 * 16 * size_of::<i256>());
        let ty =
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Null, true)), i32::MAX);
        assert!(CowSignedCastFootprint::for_batch(&target(vec![ty]), &input, 0, 0, 0).is_err());
    }
}
