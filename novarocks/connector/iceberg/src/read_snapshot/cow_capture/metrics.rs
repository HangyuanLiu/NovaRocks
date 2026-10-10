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

// Private read_snapshot child: prospective NEW FieldMetrics construction only.
use super::geometry::*;
use crate::delete_semantics::{FieldMetrics, FileMetrics, POSITION_FILE_PATH_FIELD_ID};
use crate::iceberg::spec::{DataContentType, DataFile, Datum, PrimitiveLiteral, Schema, Type};

#[derive(Clone, Copy, Debug)]
pub(crate) struct MetricsUpper {
    pub constructor_upper: u64,
    pub retained_upper: u64,
}
fn heap(d: &Datum) -> u64 {
    match d.literal() {
        PrimitiveLiteral::String(v) => v.len() as u64,
        PrimitiveLiteral::Binary(v) => v.len() as u64,
        _ => 0,
    }
}
fn buffer(d: &Datum) -> u64 {
    match d.literal() {
        PrimitiveLiteral::Boolean(_) => 1,
        PrimitiveLiteral::Int(_) | PrimitiveLiteral::Float(_) => 4,
        PrimitiveLiteral::Long(_) | PrimitiveLiteral::Double(_) => 8,
        PrimitiveLiteral::String(v) => v.len() as u64,
        PrimitiveLiteral::Binary(v) => v.len() as u64,
        _ => 16,
    }
}
fn unique<E>(
    df: &DataFile,
    mut f: impl FnMut(i32) -> Result<(), Failure<E>>,
) -> Result<(), Failure<E>> {
    let v = df.value_counts();
    let n = df.null_value_counts();
    let a = df.nan_value_counts();
    let l = df.lower_bounds();
    let u = df.upper_bounds();
    for &id in v.keys() {
        f(id)?;
    }
    for &id in n.keys() {
        if !v.contains_key(&id) {
            f(id)?;
        }
    }
    for &id in a.keys() {
        if !v.contains_key(&id) && !n.contains_key(&id) {
            f(id)?;
        }
    }
    for &id in l.keys() {
        if !v.contains_key(&id) && !n.contains_key(&id) && !a.contains_key(&id) {
            f(id)?;
        }
    }
    for &id in u.keys() {
        if !v.contains_key(&id)
            && !n.contains_key(&id)
            && !a.contains_key(&id)
            && !l.contains_key(&id)
        {
            f(id)?;
        }
    }
    Ok(())
}
// Recognized exactly by the ORIGINAL manifest_metrics lookup. This does not
// predict legal promotions or change .ok() / unknown-bound policy.
fn recognized(df: &DataFile, schema: &Schema, id: i32) -> bool {
    if df.content_type() == DataContentType::PositionDeletes
        && (id == POSITION_FILE_PATH_FIELD_ID || id == POSITION_FILE_PATH_FIELD_ID - 1)
    {
        return true;
    }
    schema
        .field_by_id(id)
        .is_some_and(|f| matches!(f.field_type.as_ref(), Type::Primitive(_)))
}
pub(crate) fn prospective<E>(
    df: &DataFile,
    schema: &Schema,
    active: &mut impl FnMut() -> Result<(), E>,
) -> Result<MetricsUpper, Failure<E>> {
    active().map_err(Failure::Original)?;
    let mut ids = 0usize;
    let mut fields = 0usize;
    let mut nested = 0u64;
    let mut max_buffer = 0u64;
    let mut max_owned = 0u64;
    unique(df, |id| {
        active().map_err(Failure::Original)?;
        ids = ids.checked_add(1).ok_or(Failure::Overflow)?;
        if recognized(df, schema, id) {
            fields = fields.checked_add(1).ok_or(Failure::Overflow)?;
            for d in [df.lower_bounds().get(&id), df.upper_bounds().get(&id)]
                .into_iter()
                .flatten()
            {
                let own = heap(d);
                nested = add(nested, own)?;
                max_owned = max_owned.max(own);
                max_buffer = max_buffer.max(buffer(d));
            }
        }
        Ok(())
    })?;
    let id_tree = btree_construct::<E, i32, ()>(ids)?;
    let map = btree_construct::<E, i32, FieldMetrics>(fields)?;
    let final_map = btree_retained::<E, i32, FieldMetrics>(fields)?;
    // FileMetrics::new consumes map into a TrustedLen Map<BTreeMap::IntoIter>:
    // one exact input Vec, stable-sort scratch uses max(n,48) elements upper, then bulk
    // map build. This conservative sum covers their overlap with old map nodes.
    let vector = slots::<E, (i32, FieldMetrics)>(fields)?;
    let sort = stable_sort_scratch::<E, (i32, FieldMetrics)>(fields)?;
    // Original clone -> ByteBuf -> returned Datum can all coexist. No legal
    // promotion expands String/Binary. One complete final nested set plus ONE
    // old current Datum and ONE ByteBuf covers both bind_bounds passes.
    let bound_transient = add(max_owned, max_buffer)?;
    let retained = add(final_map, nested)?;
    let upper = add(
        add(id_tree, add(mul(2, map)?, add(vector, sort)?)?)?,
        add(nested, bound_transient)?,
    )?;
    active().map_err(Failure::Original)?;
    Ok(MetricsUpper {
        constructor_upper: upper,
        retained_upper: retained,
    })
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct MetricsRetained {
    pub own_upper: u64,
}
pub(crate) fn retained<E>(actual: &FileMetrics) -> Result<MetricsRetained, Failure<E>> {
    let mut n = btree_retained::<E, i32, FieldMetrics>(actual.fields().len())?;
    for field in actual.fields().values() {
        for d in [&field.lower_bound, &field.upper_bound]
            .into_iter()
            .flatten()
        {
            n = add(
                n,
                match d.literal() {
                    PrimitiveLiteral::String(v) => v.capacity() as u64,
                    PrimitiveLiteral::Binary(v) => v.capacity() as u64,
                    _ => 0,
                },
            )?;
        }
    }
    Ok(MetricsRetained { own_upper: n })
}
