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

//! Write-side glue for Iceberg v3 variant columns.
//!
//! NovaRocks carries variants in execution as a single `LargeBinary`
//! per column whose bytes are `[size:u32 LE | metadata | value]` (the
//! `VariantValue::serialize` form). Iceberg parquet writers expect the
//! parent column to be a `StructArray { metadata: BinaryArray (req),
//! value: BinaryArray (req) }`; this module bridges the two right
//! before `iceberg::ParquetWriter::write`.

use iceberg::spec::SchemaRef;

/// Returns the offsets within a `[metadata|value]` payload at which the
/// metadata segment ends. Mirrors the parsing in
/// `src/exec/variant.rs::load_metadata` but only computes the length —
/// it deliberately does not validate the value segment.
///
/// `payload` must be the bytes AFTER the leading `u32` size header.
pub(crate) fn metadata_byte_len(payload: &[u8]) -> Result<usize, String> {
    // Mirror src/exec/variant.rs::load_metadata, but stop at the metadata
    // segment instead of returning the full slice.
    const HEADER: usize = 1;
    const VERSION_MASK: u8 = 0b0000_1111;
    const OFFSET_SIZE_MASK: u8 = 0b1100_0000;
    const OFFSET_SIZE_SHIFT: u8 = 6;

    if payload.len() < HEADER + 1 {
        return Err(format!(
            "variant metadata too short: {} bytes",
            payload.len()
        ));
    }
    let header = payload[0];
    let version = header & VERSION_MASK;
    if version != 1 {
        return Err(format!("unsupported variant metadata version: {version}"));
    }
    let offset_size = 1 + ((header & OFFSET_SIZE_MASK) >> OFFSET_SIZE_SHIFT);
    if !(1..=4).contains(&offset_size) {
        return Err(format!("invalid variant metadata offset size: {offset_size}"));
    }
    if payload.len() < HEADER + offset_size as usize {
        return Err("variant metadata too short to contain dict_size".to_string());
    }
    let dict_size = read_le_u32(&payload[HEADER..], offset_size)? as usize;
    let offset_list_offset = HEADER + offset_size as usize;
    let last_offset_pos = offset_list_offset + dict_size * offset_size as usize;
    if last_offset_pos + offset_size as usize > payload.len() {
        return Err("variant metadata too short to contain offset list".to_string());
    }
    let last_data_size = read_le_u32(&payload[last_offset_pos..], offset_size)? as usize;
    let data_offset = offset_list_offset + (1 + dict_size) * offset_size as usize;
    let end = data_offset + last_data_size;
    if end > payload.len() {
        return Err(format!(
            "variant metadata end {end} exceeds payload {}",
            payload.len()
        ));
    }
    Ok(end)
}

fn read_le_u32(data: &[u8], size: u8) -> Result<u32, String> {
    if size == 0 || size > 4 {
        return Err("invalid little-endian size".to_string());
    }
    if data.len() < size as usize {
        return Err("variant metadata: not enough bytes for u32 read".to_string());
    }
    let mut out: u32 = 0;
    for (i, byte) in data.iter().copied().enumerate().take(size as usize) {
        out |= (byte as u32) << (8 * i);
    }
    Ok(out)
}

/// Returns the *top-level* arrow indices in the iceberg current
/// schema that correspond to `PrimitiveType::Variant` fields. Order
/// matches `iceberg_schema.as_struct().fields()`.
pub(crate) fn variant_field_indices(iceberg_schema: &SchemaRef) -> Vec<usize> {
    use iceberg::spec::{PrimitiveType, Type};
    iceberg_schema
        .as_struct()
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, f)| match f.field_type.as_ref() {
            Type::Primitive(PrimitiveType::Variant) => Some(idx),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_metadata_empty() -> Vec<u8> {
        // Version 1, dict_size = 0, single offset entry of 0.
        vec![0x01, 0x00, 0x00]
    }

    #[test]
    fn metadata_byte_len_empty_dict() {
        let m = build_metadata_empty();
        let payload: Vec<u8> = m.iter().copied().chain([/* value */ 0x00].iter().copied()).collect();
        assert_eq!(metadata_byte_len(&payload).expect("ok"), m.len());
    }

    #[test]
    fn metadata_byte_len_rejects_short_input() {
        let err = metadata_byte_len(&[0x01]).expect_err("must reject");
        assert!(
            err.to_lowercase().contains("metadata") || err.to_lowercase().contains("short"),
            "{err}"
        );
    }

    #[test]
    fn variant_field_indices_finds_variant_columns() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
        use std::sync::Arc;
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Variant)).into(),
                    NestedField::optional(3, "s", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(4, "v2", Type::Primitive(PrimitiveType::Variant)).into(),
                ])
                .build()
                .expect("schema"),
        );
        assert_eq!(variant_field_indices(&schema), vec![1, 3]);
    }

    #[test]
    fn variant_field_indices_returns_empty_when_no_variants() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
        use std::sync::Arc;
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .expect("schema"),
        );
        assert!(variant_field_indices(&schema).is_empty());
    }
}
