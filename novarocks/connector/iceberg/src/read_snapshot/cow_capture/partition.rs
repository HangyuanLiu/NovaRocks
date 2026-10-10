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

// Private manifest/read_snapshot recipe. No literal/Struct/Canonical copy.
use super::geometry::*;
use crate::delete_semantics::CanonicalScalar;
use crate::iceberg::spec::{
    Literal, PrimitiveLiteral, PrimitiveType, Schema, Struct, StructType, TableMetadata, Transform,
};
use crate::scan_model::IcebergPartitionFieldValue;

#[derive(Clone, Copy, Debug)]
pub(crate) struct PartitionUpper {
    pub canonical_constructor_upper: u64,
    pub resolved_struct_upper: u64,
    pub debug_key_upper: u64,
    pub has_nonprimitive: bool,
}
pub(crate) fn prospective<E>(
    tuple: &Struct,
    partition_type: &StructType,
    active: &mut impl FnMut() -> Result<(), E>,
) -> Result<PartitionUpper, Failure<E>> {
    active().map_err(Failure::Original)?;
    let n = tuple.fields().len();
    let mut arcs = 0u64;
    let mut cloned = 0u64;
    let mut printed = 64u64;
    let mut nonprimitive = false;
    for value in tuple.fields() {
        active().map_err(Failure::Original)?;
        // A nested literal never reaches canonical/resolved copies: original
        // TypedPartition rejects it. We mark that precondition, never traverse
        // or clone a potentially huge nested graph before the same validator.
        let literal = match value {
            Some(Literal::Primitive(p)) => Some(p),
            None => None,
            Some(_) => {
                nonprimitive = true;
                None
            }
        };
        printed = add(printed, 192)?; // closed wrapper + any fixed primitive Debug
        if let Some(p) = literal {
            match p {
                PrimitiveLiteral::String(v) => {
                    arcs = add(arcs, arc_bytes::<E>(v.len())?)?;
                    cloned = add(cloned, v.len() as u64)?;
                    printed = add(printed, mul(6, v.len() as u64)?)?;
                }
                PrimitiveLiteral::Binary(v) => {
                    arcs = add(arcs, arc_bytes::<E>(v.len())?)?;
                    cloned = add(cloned, v.len() as u64)?;
                    printed = add(printed, mul(5, v.len() as u64)?)?;
                }
                _ => (),
            }
        }
    }
    let vecs = add(
        slots::<E, (i32, PrimitiveType)>(n)?,
        slots::<E, Option<CanonicalScalar>>(n)?,
    )?;
    let final_arcs = add(
        arc_slice::<E, (i32, PrimitiveType)>(n)?,
        arc_slice::<E, Option<CanonicalScalar>>(n)?,
    )?;
    let retained = add(final_arcs, arcs)?;
    let resolved_n = n.min(partition_type.fields().len());
    let resolved = add(slots::<E, Option<Literal>>(resolved_n)?, cloned)?;
    active().map_err(Failure::Original)?;
    Ok(PartitionUpper {
        canonical_constructor_upper: add(vecs, retained)?,
        resolved_struct_upper: resolved,
        debug_key_upper: if n == 0 { 0 } else { format_capacity(printed)? },
        has_nonprimitive: nonprimitive,
    })
}
// Only after ORIGINAL TypedPartition::bind_type has succeeded with this tuple.
// Its validator covers type/arity/compatibility; this is not a mirrored validator.
pub(crate) fn before_resolved<E>(plan: PartitionUpper) -> Result<u64, Failure<E>> {
    if plan.has_nonprimitive {
        return Err(Failure::ReceiptExceeded);
    }
    Ok(plan.resolved_struct_upper)
}
fn transform_len(t: &Transform) -> u64 {
    fn digits(mut n: u32) -> u64 {
        let mut d = 1;
        while n >= 10 {
            n /= 10;
            d += 1
        }
        d
    }
    match t {
        Transform::Identity => 8,
        Transform::Bucket(n) => 8 + digits(*n),
        Transform::Truncate(n) => 10 + digits(*n),
        Transform::Year => 4,
        Transform::Month => 5,
        Transform::Day => 3,
        Transform::Hour => 4,
        Transform::Void => 4,
        Transform::Unknown => 7,
    }
}
fn signed_digits(mut n: i32) -> u64 {
    let mut d = if n < 0 { 1 } else { 0 };
    loop {
        d += 1;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    d
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct FieldValuesUpper {
    pub constructor_upper: u64,
}
// Called with the actual resolved tuple, AFTER original spec lookup. The caller
// retains the missing-spec diagnostic order instead of raising a new error here.
pub(crate) fn field_values<E>(
    metadata: &TableMetadata,
    schema: &Schema,
    spec_id: i32,
    tuple: &Struct,
    active: &mut impl FnMut() -> Result<(), E>,
) -> Result<Option<FieldValuesUpper>, Failure<E>> {
    let Some(spec) = metadata.partition_spec_by_id(spec_id) else {
        return Ok(None);
    };
    let mut retained = slots::<E, IcebergPartitionFieldValue>(spec.fields().len())?;
    let mut maximum_extra = 0u64;
    for (i, field) in spec.fields().iter().enumerate() {
        active().map_err(Failure::Original)?;
        let source = if let Some(source) = schema.field_by_id(field.source_id) {
            source.name.len() as u64
        } else {
            let length = 1 + signed_digits(field.source_id);
            format_capacity::<E>(length)?
        };
        let tl = transform_len(&field.transform);
        // For nonidentity: Debug String then ASCII lower String coexist.
        let temp = if field.transform == Transform::Identity {
            0
        } else {
            format_capacity::<E>(tl)?
        };
        maximum_extra = maximum_extra.max(temp);
        retained = add(retained, add(source, add(field.name.len() as u64, tl)?)?)?;
        if let Some(Some(Literal::Primitive(p))) = tuple.fields().get(i) {
            let n = match p {
                PrimitiveLiteral::String(v) => v.len() as u64,
                PrimitiveLiteral::Binary(v) => v.len() as u64,
                _ => 0,
            };
            retained = add(retained, n)?;
        }
    }
    active().map_err(Failure::Original)?;
    Ok(Some(FieldValuesUpper {
        constructor_upper: add(retained, maximum_extra)?,
    }))
}
