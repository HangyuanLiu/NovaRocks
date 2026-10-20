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

//! Before-allocation bounds for signed COW layout normalization.
//! No value conversion is admitted here. Dictionary expansion is measured by
//! reading actual keys and UTF8 offsets; all other payload is borrowed until
//! the existing source owner copies it. Tree/header bounds precede any cloning.
use arrow::{
    array::{Array, ArrayRef},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorRowConversionFootprint,
};
use std::collections::HashMap;
use std::mem::size_of;
const INPUT_BYTES: usize = 32 * 1024 * 1024;
const SELECTION_BYTES: usize = 64 * 1024 * 1024;
const TRANSFORM_BYTES: usize = 256 * 1024 * 1024;
const METADATA_BYTES: usize = 16 * 1024 * 1024;
const MAX_NODES: usize = 4096;
const MAX_DEPTH: usize = 64;
const MAX_ROWS: usize = 1_048_576;
// ArrayData, builders, Arc headers, child vectors and bounded error formatting.
// This fixed allowance never stands in for value/offset/payload allocations.
const NODE_HEADERS: usize = 2048;
#[derive(Clone, Copy, Debug)]
pub(super) struct CowSignedLayoutFootprint {
    pub input_bytes: usize,
    pub output_bytes: usize,
    pub temporary_bytes: usize,
    pub copy_peak_bytes: usize,
    pub peak_bytes: usize,
}
impl CowSignedLayoutFootprint {
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
        let target = TypeWalk::schema(schema)?;
        let source = TypeWalk::schema(batch.schema_ref())?;
        let mut arrays = TypeWalk::default();
        for array in batch.columns() {
            arrays.data_type(array.data_type(), 0)?;
        }
        if batch.num_columns() != schema.fields().len() {
            return Err(invalid("COW output width differs from its signed contract"));
        }
        let input_bytes = add(
            ConnectorRowConversionFootprint::retained_batch_bytes(batch)?,
            source_scratch_bytes,
        )?;
        if input_bytes > INPUT_BYTES {
            return Err(exhausted());
        }
        let mut output_bytes = 0;
        let mut copy_peak_bytes = 0;
        for (array, field) in batch.columns().iter().zip(schema.fields()) {
            let (expanded, copied) = layout(array, field)?;
            output_bytes = add(output_bytes, expanded)?;
            copy_peak_bytes = add(copy_peak_bytes, copied)?;
        }
        let nodes = add(add(target.nodes, source.nodes)?, arrays.nodes)?;
        let metadata = add(add(target.metadata, source.metadata)?, arrays.metadata)?;
        let temporary_bytes = add(mul(nodes, NODE_HEADERS)?, mul(metadata, 36)?)?;
        let peak_bytes = add(
            add(
                add(input_bytes, retained_selection_bytes)?,
                other_live_bytes,
            )?,
            add(add(output_bytes, temporary_bytes)?, copy_peak_bytes)?,
        )?;
        if peak_bytes > limit {
            return Err(exhausted());
        }
        Ok(Self {
            input_bytes,
            output_bytes,
            temporary_bytes,
            copy_peak_bytes,
            peak_bytes,
        })
    }
}
fn exhausted() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "COW signed layout exceeds its fixed input, metadata or transform workspace",
    )
}
fn invalid(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
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
fn layout(array: &ArrayRef, field: &Field) -> Result<(usize, usize), ConnectorError> {
    let data = array.to_data();
    if data.offset() != 0 {
        return Err(invalid("COW signed layout requires unsliced source arrays"));
    }
    let rows = array.len();
    if rows > MAX_ROWS {
        return Err(exhausted());
    }
    let validity = if field.is_nullable() && data.nulls().is_some() {
        bitmap(rows)?
    } else {
        0
    };
    match (array.data_type(), field.data_type()) {
        (DataType::Dictionary(key, value), DataType::Utf8)
            if **key == DataType::Int32 && **value == DataType::Utf8 =>
        {
            let bytes = super::signed_layout::decoded_dictionary_bytes(array)?;
            let expanded = add(
                add(round64(bytes)?, round64(mul(add(rows, 1)?, 4)?)?)?,
                bitmap(rows)?,
            )?;
            Ok((expanded, expanded))
        }
        (DataType::Struct(source), DataType::Struct(target)) if source.len() == target.len() => {
            let structure = array
                .as_any()
                .downcast_ref::<arrow::array::StructArray>()
                .ok_or_else(|| invalid("COW Struct carrier differs from its declaration"))?;
            let (mut output, mut copy) = (0, validity);
            for (child, target) in structure.columns().iter().zip(target) {
                let (e, c) = layout(child, target)?;
                output = add(output, e)?;
                copy = add(copy, c)?;
            }
            Ok((output, copy))
        }
        (DataType::List(_), DataType::List(target)) => {
            let list = array
                .as_any()
                .downcast_ref::<arrow::array::ListArray>()
                .ok_or_else(|| invalid("COW List carrier differs from its declaration"))?;
            let (e, c) = layout(list.values(), target)?;
            Ok((e, add(add(validity, round64(mul(add(rows, 1)?, 4)?)?)?, c)?))
        }
        (DataType::LargeList(_), DataType::LargeList(target)) => {
            let list = array
                .as_any()
                .downcast_ref::<arrow::array::LargeListArray>()
                .ok_or_else(|| invalid("COW LargeList carrier differs from its declaration"))?;
            let (e, c) = layout(list.values(), target)?;
            Ok((e, add(add(validity, round64(mul(add(rows, 1)?, 8)?)?)?, c)?))
        }
        (source, target) if source == target => {
            // Copy each logical buffer separately: the source owner does so even
            // if two columns share backing. Full buffer lengths conservatively
            // include unused payload; no retained-source deduplication is used.
            let mut copied = validity;
            for buffer in data.buffers() {
                copied = add(copied, round64(buffer.len())?)?;
            }
            if !data.child_data().is_empty() {
                return Err(invalid(
                    "COW signed layout has an unsupported nested carrier",
                ));
            }
            Ok((0, copied))
        }
        _ => Err(invalid(
            "COW root carrier differs beyond admitted layout differences",
        )),
    }
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
mod tests;
