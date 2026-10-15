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

// the ORIGINAL validated sealed route, never reconstructed token authorities.
use super::borrowed_value_footprint::Failure;
use super::recipe::SkeletonEvidence;
use super::skeleton_shape::{self as shape, Expression as E, Factor};
use super::type_footprint::exact_type_heap;
use arrow::datatypes::Field;
use novarocks_type_contract::FunctionValueType;
type Result<T> = std::result::Result<T, Failure>;
fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b).ok_or(Failure::ResourceExhausted)
}

pub struct AppendColumn<'a> {
    pub writer: &'a Field,
    pub writer_type: &'a FunctionValueType,
    pub signed_selection_type: &'a FunctionValueType,
}
pub fn append(
    rows: u64,
    columns: &[AppendColumn<'_>],
    owned_target_metadata_upper: u64,
    construction_views_upper: u64,
) -> Result<SkeletonEvidence> {
    append_in(
        rows,
        columns,
        owned_target_metadata_upper,
        construction_views_upper,
        false,
    )
}

pub fn append_in(
    rows: u64,
    columns: &[AppendColumn<'_>],
    owned_target_metadata_upper: u64,
    construction_views_upper: u64,
    materialized: bool,
) -> Result<SkeletonEvidence> {
    if rows == 0 || columns.is_empty() {
        return Err(Failure::OriginalSemantic("COW empty VALUES relation"));
    }
    let mut projection_heap = 0;
    let mut alias_bytes = 0;
    let mut selected_type_heap = 0;
    for (ordinal, column) in columns.iter().enumerate() {
        let alias_len = shape::generated_value_alias_bytes(ordinal as u64);
        alias_bytes = add(alias_bytes, alias_len)?;
        let value = E::column("__nr_values".len() as u64, alias_len)?;
        let expression = if materialized {
            value
        } else {
            E::cast(value, exact_type_heap(column.writer_type)?)?
        };
        projection_heap = add(
            projection_heap,
            shape::item_heap(expression, column.writer.name().len() as u64)?,
        )?;
        if !materialized {
            selected_type_heap = add(
                selected_type_heap,
                exact_type_heap(column.signed_selection_type)?,
            )?;
        }
    }
    // Zero cell sum means omit only row Expr storage, not row Vec headers.
    let from = Factor::derived_values(
        if materialized { 0 } else { rows },
        0,
        "__nr_values".len() as u64,
        columns.len() as u64,
        alias_bytes,
    )?;
    let skeleton = shape::query_owned(columns.len() as u64, projection_heap, from, None, None)?;
    Ok(SkeletonEvidence {
        ast_owned: skeleton,
        clone_owned_upper: skeleton,
        owned_target_metadata_upper,
        rows: if materialized { 0 } else { rows },
        width: columns.len() as u64,
        selection_cast_width: if materialized {
            0
        } else {
            columns.len() as u64
        },
        one_row_selection_cast_type_heap: if materialized { 0 } else { selected_type_heap },
        // Original usize decimal alias formatting: 7 prefix + at most 20
        // digits on the frozen 64-bit target, coexisting with its final Ident.
        constructor_transient_upper: add(
            construction_views_upper,
            shape::generated_value_alias_bytes(usize::MAX as u64),
        )?,
    })
}

#[derive(Clone, Copy)]
pub enum RewriteValue {
    Identity,
    AfterImage { values_ordinal: u64 },
    InheritWrittenVersion,
}
pub struct RewriteWriter<'a> {
    pub writer: &'a Field,
    pub writer_type: &'a FunctionValueType,
    pub original_scan_field: &'a Field,
    pub value: RewriteValue,
}
pub struct MatchKey<'a> {
    pub original_scan_field: &'a Field,
    pub values_ordinal: u64,
}

pub struct Rewrite<'a> {
    pub rows: u64,
    pub original_values_types: &'a [&'a FunctionValueType],
    pub writers: &'a [RewriteWriter<'a>],
    pub original_match_keys: &'a [MatchKey<'a>],
    pub frozen_catalog: &'a str,
    pub frozen_namespace: &'a str,
    pub frozen_table: &'a str,
    pub owned_target_metadata_upper: u64,
    pub construction_views_upper: u64,
}

pub fn rewrite(spec: Rewrite<'_>) -> Result<SkeletonEvidence> {
    rewrite_in(spec, false)
}
pub fn rewrite_in(spec: Rewrite<'_>, materialized: bool) -> Result<SkeletonEvidence> {
    if spec.rows == 0 {
        return Err(Failure::OriginalSemantic("COW empty VALUES relation"));
    }
    if spec.original_match_keys.is_empty() {
        return Err(Failure::OriginalSemantic(
            "COW rewrite branch carries no match key",
        ));
    }
    let matched = || {
        E::is_null(E::column(
            "__nr_match".len() as u64,
            "__nr_matched".len() as u64,
        )?)
    };
    let effect = || E::column("__nr_match".len() as u64, "__nr_effect".len() as u64);
    let mut projection_heap = 0;
    for column in spec.writers {
        let scan = E::column(
            "__nr_scan".len() as u64,
            column.original_scan_field.name().len() as u64,
        )?;
        let value = match column.value {
            RewriteValue::Identity => scan,
            RewriteValue::AfterImage { values_ordinal } => E::case(
                matched()?,
                E::column(
                    "__nr_match".len() as u64,
                    shape::generated_value_alias_bytes(values_ordinal),
                )?,
                scan,
            )?,
            RewriteValue::InheritWrittenVersion => E::case(matched()?, E::literal(0)?, scan)?,
        };
        let cast = if materialized {
            value
        } else {
            E::cast(value, exact_type_heap(column.writer_type)?)?
        };
        projection_heap = add(
            projection_heap,
            shape::item_heap(cast, column.writer.name().len() as u64)?,
        )?;
    }
    let mut on = None;
    for key in spec.original_match_keys {
        let equal = E::binary(
            E::column(
                "__nr_scan".len() as u64,
                key.original_scan_field.name().len() as u64,
            )?,
            E::column(
                "__nr_match".len() as u64,
                shape::generated_value_alias_bytes(key.values_ordinal),
            )?,
        )?;
        on = Some(match on {
            None => equal,
            Some(left) => E::binary(left, equal)?,
        });
    }
    // Existing Delete enum is 1; original grammar stores one digit Number.
    // The integration must use that original enum rather than a guessed tag.
    let delete = novarocks_spi::connector::ConnectorRowMutationEffect::Delete as i8;
    let delete_len =
        shape::generated_value_alias_bytes(delete.unsigned_abs() as u64) - "__nr_v_".len() as u64;
    let delete_shape = if delete < 0 {
        E::unary(E::literal(delete_len)?)?
    } else {
        E::literal(delete_len)?
    };
    let where_clause = E::binary(E::is_null(effect()?)?, E::binary(effect()?, delete_shape)?)?;
    let mut selected_type_heap = super::type_footprint::effect_type_heap()?;
    let mut alias_bytes = add("__nr_matched".len() as u64, "__nr_effect".len() as u64)?;
    for (ordinal, field) in spec.original_values_types.iter().enumerate() {
        if !materialized {
            selected_type_heap = add(selected_type_heap, exact_type_heap(field)?)?;
        }
        alias_bytes = add(
            alias_bytes,
            shape::generated_value_alias_bytes(ordinal as u64),
        )?;
    }
    let width = add(spec.original_values_types.len() as u64, 2)?;
    let values = Factor::derived_values(
        if materialized { 0 } else { spec.rows },
        0,
        "__nr_match".len() as u64,
        width,
        alias_bytes,
    )?;
    let scan = Factor::table(
        spec.frozen_catalog.len() as u64,
        spec.frozen_namespace.len() as u64,
        spec.frozen_table.len() as u64,
        "__nr_scan".len() as u64,
    )?;
    let skeleton = shape::query_owned(
        spec.writers.len() as u64,
        projection_heap,
        scan,
        Some((
            values,
            on.ok_or(Failure::InvalidSource("COW missing join shape"))?,
        )),
        Some(where_clause),
    )?;
    Ok(SkeletonEvidence {
        ast_owned: skeleton,
        clone_owned_upper: skeleton,
        owned_target_metadata_upper: spec.owned_target_metadata_upper,
        rows: if materialized { 0 } else { spec.rows },
        width,
        selection_cast_width: if materialized { 0 } else { width - 1 },
        one_row_selection_cast_type_heap: if materialized { 0 } else { selected_type_heap },
        constructor_transient_upper: add(
            add(
                spec.construction_views_upper,
                (spec.original_values_types.len() as u64)
                    .checked_mul(std::mem::size_of::<
                        novarocks_spi::connector::ConnectorWriteFieldToken,
                    >() as u64)
                    .ok_or(Failure::ResourceExhausted)?,
            )?,
            shape::generated_value_alias_bytes(usize::MAX as u64),
        )?,
    })
}
