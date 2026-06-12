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

//! The single recursive type relation for the execution layer.
//!
//! This is the keystone of the distributed-execution target architecture
//! (pillar P1, see `docs/design/2026-06-12-distributed-execution-target-architecture.md`).
//! It is the one place that answers "is column type `actual` relatable to the
//! authoritative descriptor type `expected`?". It replaces the five hand-rolled
//! copies of that predicate that drifted apart:
//!   - `exec::schema_compat::is_execution_data_type_compatible`
//!   - `exec::chunk::schema::is_compatible_chunk_field_type` / `reconcile_chunk_data_type`
//!   - `exec::operators::sort::is_compatible_sort_field_type`
//!   - `runtime::exchange::is_compatible_exchange_arrow_type` / `merge_exchange_field_type`
//!   - `exec::expr::agg::functions::array_agg::reconcile_data_type`
//!
//! Deliberate decisions encoded here (resolved divergences across those copies):
//!   - decimal is scale-strict; `SameScaleWiden` permits a precision difference
//!     only within the same physical width (never Decimal128 <-> Decimal256).
//!   - `Map` `ordered` flags must match.
//!   - `List` and `LargeList` never relate across each other.
//!   - structs relate by POSITION, ignoring field names (Arrow field names are
//!     not part of the StarRocks logical type; cf. `struct_column` serde).
//!   - the historical `List <-> Struct[len==1]` collapse is DROPPED: it papered
//!     over an aggregate-state shape inconsistency that pillar P5 makes
//!     deterministic instead.
//!
//! Type only: this relation says nothing about nullability. Field-level
//! nullability reconciliation (and the root-boundary `required -> null`
//! fail-fast) is a separate concern layered on top when call sites are rewired.
#![allow(dead_code)] // Staged foundation: wired into exchange/sort/aggregate by the
                     // descriptor-authoritative migration (P3/P5). Unused until then.

use arrow::array::{make_array, Array, ArrayData, ArrayRef};
use arrow::datatypes::{DataType, Field};

/// The compatibility policy parameter for [`relate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompatibilityPolicy {
    /// Arrow-structural identity: same discriminant and same scalar parameters
    /// (decimal precision AND scale, timestamp unit/tz), recursing children by
    /// position while ignoring field names. Used where the descriptor type must
    /// be reproduced exactly (post-materialization, final output boundary).
    ExactArrow,
    /// Internal-transport tolerance: a decimal may differ in precision at the
    /// same scale within the same physical width (never Decimal128 <-> Decimal256),
    /// a timestamp may differ in unit/tz, and Utf8 <-> Binary are interchangeable.
    /// Children recurse by position; `List` != `LargeList`; `Map` `ordered` must
    /// match. Used for the metadata-only retag the exchange receiver applies to
    /// materialize a decoded column to its registered descriptor.
    SameScaleWiden,
}

/// One step on the path from a top-level type to a nested mismatch, for
/// diagnostics that can name `col.field[2].list.item` precisely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NestedStep {
    ListItem,
    LargeListItem,
    MapKey,
    MapValue,
    StructField(usize),
}

/// Why two types do not relate. Carried so CI / engine error classification can
/// discriminate without parsing free text (pillar P8 embeds this as the
/// type-mismatch arm of the engine error enum).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TypeMismatchKind {
    /// Non-relatable scalars (e.g. Int32 vs Int64; Utf8 vs Binary under `ExactArrow`).
    ScalarMismatch,
    /// Decimal scales differ (never permitted under any policy).
    DecimalScaleMismatch,
    /// Decimal physical width differs (Decimal128 vs Decimal256).
    DecimalWidthCross,
    /// A list-kind type met a different kind (List vs LargeList, or list vs non-list).
    ListKindMismatch,
    /// `Map` `ordered` flags differ.
    MapOrderingMismatch,
    /// Struct field counts differ.
    StructArityMismatch,
}

/// A structured type mismatch produced by [`relate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TypeMismatch {
    pub nested_path: Vec<NestedStep>,
    pub expected: DataType,
    pub actual: DataType,
    pub policy: CompatibilityPolicy,
    pub kind: TypeMismatchKind,
}

/// The one recursive type relation. Returns `Ok(())` when `actual` is relatable
/// to the authoritative `expected` under `policy` (and therefore retaggable to
/// `expected`), or a structured [`TypeMismatch`] otherwise.
pub(crate) fn relate(
    expected: &DataType,
    actual: &DataType,
    policy: CompatibilityPolicy,
) -> Result<(), TypeMismatch> {
    let mut path = Vec::new();
    relate_inner(expected, actual, policy, &mut path)
}

fn relate_inner(
    expected: &DataType,
    actual: &DataType,
    policy: CompatibilityPolicy,
    path: &mut Vec<NestedStep>,
) -> Result<(), TypeMismatch> {
    use CompatibilityPolicy::{ExactArrow, SameScaleWiden};
    use DataType::*;
    use TypeMismatchKind::*;

    if expected == actual {
        return Ok(());
    }

    let mismatch = |kind: TypeMismatchKind, path: &[NestedStep]| TypeMismatch {
        nested_path: path.to_vec(),
        expected: expected.clone(),
        actual: actual.clone(),
        policy,
        kind,
    };

    match (expected, actual) {
        (Decimal128(ep, es), Decimal128(ap, as_)) | (Decimal256(ep, es), Decimal256(ap, as_)) => {
            if es != as_ {
                Err(mismatch(DecimalScaleMismatch, path))
            } else {
                match policy {
                    SameScaleWiden => Ok(()),
                    ExactArrow if ep == ap => Ok(()),
                    ExactArrow => Err(mismatch(ScalarMismatch, path)),
                }
            }
        }
        (Decimal128(..), Decimal256(..)) | (Decimal256(..), Decimal128(..)) => {
            Err(mismatch(DecimalWidthCross, path))
        }
        (Timestamp(_, _), Timestamp(_, _)) | (Utf8, Binary) | (Binary, Utf8) => match policy {
            SameScaleWiden => Ok(()),
            ExactArrow => Err(mismatch(ScalarMismatch, path)),
        },
        (List(ef), List(af)) => {
            path.push(NestedStep::ListItem);
            let r = relate_inner(ef.data_type(), af.data_type(), policy, path);
            path.pop();
            r
        }
        (LargeList(ef), LargeList(af)) => {
            path.push(NestedStep::LargeListItem);
            let r = relate_inner(ef.data_type(), af.data_type(), policy, path);
            path.pop();
            r
        }
        (List(_) | LargeList(_), _) | (_, List(_) | LargeList(_)) => {
            Err(mismatch(ListKindMismatch, path))
        }
        (Map(ef, eo), Map(af, ao)) => {
            if eo != ao {
                return Err(mismatch(MapOrderingMismatch, path));
            }
            let (ek, ev) = map_key_value(ef).ok_or_else(|| mismatch(ScalarMismatch, path))?;
            let (ak, av) = map_key_value(af).ok_or_else(|| mismatch(ScalarMismatch, path))?;
            path.push(NestedStep::MapKey);
            let rk = relate_inner(ek, ak, policy, path);
            path.pop();
            rk?;
            path.push(NestedStep::MapValue);
            let rv = relate_inner(ev, av, policy, path);
            path.pop();
            rv
        }
        (Struct(ef), Struct(af)) => {
            if ef.len() != af.len() {
                return Err(mismatch(StructArityMismatch, path));
            }
            for (idx, (e, a)) in ef.iter().zip(af.iter()).enumerate() {
                path.push(NestedStep::StructField(idx));
                let r = relate_inner(e.data_type(), a.data_type(), policy, path);
                path.pop();
                r?;
            }
            Ok(())
        }
        _ => Err(mismatch(ScalarMismatch, path)),
    }
}

/// Retag `array` so its type equals `target`, changing only metadata — never a
/// single value. This is the surviving "normalize" primitive the exchange
/// receiver uses to materialize a decoded column to its registered descriptor
/// (pillar P3). The legitimate cases are: identity; a decimal precision change
/// at the SAME scale within the same physical width (an `i128`/`i256` buffer is
/// reinterpreted, values untouched); `Utf8` <-> `Binary` (identical physical
/// layout); and recursion into `List` / `LargeList` / `Struct` / `Map` children.
/// Any difference that is not a pure relabel (e.g. a timestamp unit change, or
/// `Decimal128` <-> `Decimal256`) returns `Err`; under pillar P2 the sender and
/// receiver descriptors agree by construction so those do not arise.
pub(crate) fn retag_column(array: &ArrayRef, target: &DataType) -> Result<ArrayRef, TypeMismatch> {
    let data = retag_data(array.to_data(), target, &mut Vec::new())?;
    Ok(make_array(data))
}

fn retag_data(
    data: ArrayData,
    target: &DataType,
    path: &mut Vec<NestedStep>,
) -> Result<ArrayData, TypeMismatch> {
    use DataType::*;
    use TypeMismatchKind::*;

    if data.data_type() == target {
        return Ok(data);
    }
    let source = data.data_type().clone();

    match (&source, target) {
        (Decimal128(_, ss), Decimal128(_, ts)) | (Decimal256(_, ss), Decimal256(_, ts)) => {
            if ss != ts {
                return Err(retag_mismatch(path, target, &source, DecimalScaleMismatch));
            }
            finish_retag(data, target, Vec::new(), path, &source)
        }
        (Decimal128(..), Decimal256(..)) | (Decimal256(..), Decimal128(..)) => {
            Err(retag_mismatch(path, target, &source, DecimalWidthCross))
        }
        (Utf8, Binary) | (Binary, Utf8) => finish_retag(data, target, Vec::new(), path, &source),
        (List(_), List(tf)) => {
            path.push(NestedStep::ListItem);
            let child = retag_data(data.child_data()[0].clone(), tf.data_type(), path);
            path.pop();
            finish_retag(data, target, vec![child?], path, &source)
        }
        (LargeList(_), LargeList(tf)) => {
            path.push(NestedStep::LargeListItem);
            let child = retag_data(data.child_data()[0].clone(), tf.data_type(), path);
            path.pop();
            finish_retag(data, target, vec![child?], path, &source)
        }
        (List(_) | LargeList(_), _) | (_, List(_) | LargeList(_)) => {
            Err(retag_mismatch(path, target, &source, ListKindMismatch))
        }
        (Map(_, so), Map(tf, to)) => {
            if so != to {
                return Err(retag_mismatch(path, target, &source, MapOrderingMismatch));
            }
            // A Map's single child is the `entries` struct; recurse it as a struct.
            path.push(NestedStep::StructField(0));
            let child = retag_data(data.child_data()[0].clone(), tf.data_type(), path);
            path.pop();
            finish_retag(data, target, vec![child?], path, &source)
        }
        (Struct(_), Struct(tfields)) => {
            let n = data.child_data().len();
            if n != tfields.len() {
                return Err(retag_mismatch(path, target, &source, StructArityMismatch));
            }
            let mut children = Vec::with_capacity(n);
            for (idx, tf) in tfields.iter().enumerate() {
                path.push(NestedStep::StructField(idx));
                let c = retag_data(data.child_data()[idx].clone(), tf.data_type(), path);
                path.pop();
                children.push(c?);
            }
            finish_retag(data, target, children, path, &source)
        }
        _ => Err(retag_mismatch(path, target, &source, ScalarMismatch)),
    }
}

/// Rebuild `data` with `target` as its type, reusing the original buffers/nulls
/// (metadata-only) and substituting `children` when retagging a nested type.
fn finish_retag(
    data: ArrayData,
    target: &DataType,
    children: Vec<ArrayData>,
    path: &[NestedStep],
    source: &DataType,
) -> Result<ArrayData, TypeMismatch> {
    let mut builder = data.into_builder().data_type(target.clone());
    if !children.is_empty() {
        builder = builder.child_data(children);
    }
    builder
        .build()
        .map_err(|_| retag_mismatch(path, target, source, TypeMismatchKind::ScalarMismatch))
}

fn retag_mismatch(
    path: &[NestedStep],
    expected: &DataType,
    actual: &DataType,
    kind: TypeMismatchKind,
) -> TypeMismatch {
    TypeMismatch {
        nested_path: path.to_vec(),
        expected: expected.clone(),
        actual: actual.clone(),
        policy: CompatibilityPolicy::SameScaleWiden,
        kind,
    }
}

/// The one Field-level nullability merge rule across exchange / sort / chunk
/// boundaries: a merged field is nullable if EITHER side is (OR). This is the
/// single source of truth replacing the scattered `expected || actual` sites.
///
/// Descriptor nullability is the contract; runtime nullability may widen it
/// when a producer observes NULL values. Type selection stays descriptor-led.
pub(crate) fn merge_fields_nullability(expected: &Field, actual: &Field) -> bool {
    expected.is_nullable() || actual.is_nullable()
}

/// Extract the (key, value) child data types from a `Map` entries field, which
/// Arrow models as a 2-field `Struct<key, value>`.
fn map_key_value(entries: &arrow::datatypes::FieldRef) -> Option<(&DataType, &DataType)> {
    match entries.data_type() {
        DataType::Struct(fields) if fields.len() == 2 => {
            Some((fields[0].data_type(), fields[1].data_type()))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::CompatibilityPolicy::{ExactArrow, SameScaleWiden};
    use super::TypeMismatchKind::*;
    use super::{merge_fields_nullability, relate, retag_column, NestedStep};
    use arrow::array::{
        Array, ArrayRef, BinaryArray, Decimal128Array, Int32Array, Int64Array, ListArray,
        StringArray, StructArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
    use std::sync::Arc;

    fn list(item: DataType) -> DataType {
        DataType::List(Arc::new(Field::new("item", item, true)))
    }
    fn large_list(item: DataType) -> DataType {
        DataType::LargeList(Arc::new(Field::new("item", item, true)))
    }
    fn strukt(fields: Vec<(&str, DataType)>) -> DataType {
        DataType::Struct(Fields::from(
            fields
                .into_iter()
                .map(|(n, t)| Field::new(n, t, true))
                .collect::<Vec<_>>(),
        ))
    }
    fn map(key: DataType, value: DataType, ordered: bool) -> DataType {
        let entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", key, false),
                Field::new("value", value, true),
            ])),
            false,
        );
        DataType::Map(Arc::new(entries), ordered)
    }
    fn d128(p: u8, s: i8) -> DataType {
        DataType::Decimal128(p, s)
    }
    fn d256(p: u8, s: i8) -> DataType {
        DataType::Decimal256(p, s)
    }

    #[test]
    fn identical_scalars_relate_under_any_policy() {
        assert!(relate(&DataType::Int64, &DataType::Int64, ExactArrow).is_ok());
        assert!(relate(&DataType::Int64, &DataType::Int64, SameScaleWiden).is_ok());
    }

    #[test]
    fn distinct_scalars_are_a_scalar_mismatch() {
        let err = relate(&DataType::Int32, &DataType::Int64, SameScaleWiden).unwrap_err();
        assert_eq!(err.kind, ScalarMismatch);
        assert!(err.nested_path.is_empty());
    }

    #[test]
    fn same_scale_decimal_widens_precision_both_directions() {
        assert!(relate(&d128(20, 2), &d128(38, 2), SameScaleWiden).is_ok());
        assert!(relate(&d128(38, 2), &d128(20, 2), SameScaleWiden).is_ok());
    }

    #[test]
    fn exact_arrow_rejects_decimal_precision_difference() {
        let err = relate(&d128(20, 2), &d128(38, 2), ExactArrow).unwrap_err();
        assert_eq!(err.kind, ScalarMismatch);
    }

    #[test]
    fn decimal_scale_difference_never_relates() {
        let err = relate(&d128(20, 2), &d128(20, 3), SameScaleWiden).unwrap_err();
        assert_eq!(err.kind, DecimalScaleMismatch);
        let err = relate(&d128(20, 2), &d128(20, 3), ExactArrow).unwrap_err();
        assert_eq!(err.kind, DecimalScaleMismatch);
    }

    #[test]
    fn decimal128_and_decimal256_never_relate() {
        let err = relate(&d128(20, 2), &d256(20, 2), SameScaleWiden).unwrap_err();
        assert_eq!(err.kind, DecimalWidthCross);
        let err = relate(&d256(20, 2), &d128(20, 2), SameScaleWiden).unwrap_err();
        assert_eq!(err.kind, DecimalWidthCross);
    }

    #[test]
    fn timestamp_unit_tolerated_only_under_widen() {
        let us = DataType::Timestamp(TimeUnit::Microsecond, None);
        let ns = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
        assert!(relate(&us, &ns, SameScaleWiden).is_ok());
        assert_eq!(
            relate(&us, &ns, ExactArrow).unwrap_err().kind,
            ScalarMismatch
        );
    }

    #[test]
    fn utf8_binary_interchangeable_only_under_widen() {
        assert!(relate(&DataType::Utf8, &DataType::Binary, SameScaleWiden).is_ok());
        assert!(relate(&DataType::Binary, &DataType::Utf8, SameScaleWiden).is_ok());
        assert_eq!(
            relate(&DataType::Utf8, &DataType::Binary, ExactArrow)
                .unwrap_err()
                .kind,
            ScalarMismatch
        );
    }

    #[test]
    fn list_recurses_into_item_with_path() {
        assert!(relate(&list(d128(20, 2)), &list(d128(38, 2)), SameScaleWiden).is_ok());
        let err = relate(&list(d128(20, 2)), &list(d128(20, 3)), SameScaleWiden).unwrap_err();
        assert_eq!(err.kind, DecimalScaleMismatch);
        assert_eq!(err.nested_path, vec![NestedStep::ListItem]);
    }

    #[test]
    fn list_and_large_list_never_relate() {
        let err = relate(
            &list(DataType::Int64),
            &large_list(DataType::Int64),
            SameScaleWiden,
        )
        .unwrap_err();
        assert_eq!(err.kind, ListKindMismatch);
    }

    #[test]
    fn list_struct_collapse_is_dropped() {
        // The historical List<->Struct[len==1] tolerance is deliberately removed.
        let err = relate(
            &list(DataType::Int32),
            &strukt(vec![("f", DataType::Int32)]),
            SameScaleWiden,
        )
        .unwrap_err();
        assert_eq!(err.kind, ListKindMismatch);
    }

    #[test]
    fn struct_relates_by_position_ignoring_names() {
        // Same positions/types, different field names -> Ok even under ExactArrow.
        let a = strukt(vec![("a", DataType::Int64), ("b", d128(20, 2))]);
        let b = strukt(vec![("x", DataType::Int64), ("y", d128(38, 2))]);
        assert!(relate(&a, &b, SameScaleWiden).is_ok());
        let a2 = strukt(vec![("a", DataType::Int64)]);
        let b2 = strukt(vec![("z", DataType::Int64)]);
        assert!(relate(&a2, &b2, ExactArrow).is_ok());
    }

    #[test]
    fn struct_arity_mismatch_is_reported() {
        let a = strukt(vec![("a", DataType::Int64), ("b", DataType::Int64)]);
        let b = strukt(vec![("a", DataType::Int64)]);
        assert_eq!(
            relate(&a, &b, SameScaleWiden).unwrap_err().kind,
            StructArityMismatch
        );
    }

    #[test]
    fn struct_child_mismatch_carries_field_path() {
        let a = strukt(vec![("a", DataType::Int64), ("b", d128(20, 2))]);
        let b = strukt(vec![("a", DataType::Int64), ("b", d128(20, 3))]);
        let err = relate(&a, &b, SameScaleWiden).unwrap_err();
        assert_eq!(err.kind, DecimalScaleMismatch);
        assert_eq!(err.nested_path, vec![NestedStep::StructField(1)]);
    }

    #[test]
    fn map_ordering_must_match() {
        let ordered = map(DataType::Utf8, DataType::Int64, true);
        let unordered = map(DataType::Utf8, DataType::Int64, false);
        assert_eq!(
            relate(&ordered, &unordered, SameScaleWiden)
                .unwrap_err()
                .kind,
            MapOrderingMismatch
        );
    }

    #[test]
    fn map_recurses_into_key_and_value() {
        let a = map(DataType::Utf8, d128(20, 2), false);
        let b = map(DataType::Utf8, d128(38, 2), false);
        assert!(relate(&a, &b, SameScaleWiden).is_ok());

        let bad_value = map(DataType::Utf8, d128(20, 3), false);
        let err = relate(&a, &bad_value, SameScaleWiden).unwrap_err();
        assert_eq!(err.kind, DecimalScaleMismatch);
        assert_eq!(err.nested_path, vec![NestedStep::MapValue]);

        let bad_key = map(DataType::Int64, d128(20, 2), false);
        let err = relate(&a, &bad_key, SameScaleWiden).unwrap_err();
        assert_eq!(err.kind, ScalarMismatch);
        assert_eq!(err.nested_path, vec![NestedStep::MapKey]);
    }

    #[test]
    fn relate_ignores_field_nullability() {
        // The relation is about type structure only; child nullability differs
        // but the item types match.
        let non_null_item = DataType::List(Arc::new(Field::new("item", DataType::Int64, false)));
        let null_item = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        assert!(relate(&non_null_item, &null_item, ExactArrow).is_ok());
    }

    #[test]
    fn merge_fields_nullability_is_or_rule() {
        let req = Field::new("f", DataType::Int64, false);
        let nul = Field::new("f", DataType::Int64, true);
        assert!(!merge_fields_nullability(&req, &req));
        assert!(merge_fields_nullability(&req, &nul));
        assert!(merge_fields_nullability(&nul, &req));
        assert!(merge_fields_nullability(&nul, &nul));
    }

    fn decimal128(values: Vec<i128>, p: u8, s: i8) -> ArrayRef {
        Arc::new(
            Decimal128Array::from(values.into_iter().map(Some).collect::<Vec<_>>())
                .with_precision_and_scale(p, s)
                .expect("decimal type"),
        )
    }

    #[test]
    fn retag_column_identity_is_a_noop() {
        let arr = decimal128(vec![123], 38, 2);
        let out = retag_column(&arr, &DataType::Decimal128(38, 2)).expect("retag");
        assert_eq!(out.data_type(), &DataType::Decimal128(38, 2));
    }

    #[test]
    fn retag_column_decimal_widens_precision_keeps_values() {
        let arr = decimal128(vec![123, -45], 18, 2);
        let out = retag_column(&arr, &DataType::Decimal128(38, 2)).expect("retag");
        assert_eq!(out.data_type(), &DataType::Decimal128(38, 2));
        assert!(relate(&DataType::Decimal128(38, 2), out.data_type(), ExactArrow).is_ok());
        let d = out.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(d.value(0), 123);
        assert_eq!(d.value(1), -45);
    }

    #[test]
    fn retag_column_decimal_scale_mismatch_errors() {
        let arr = decimal128(vec![123], 18, 2);
        let err = retag_column(&arr, &DataType::Decimal128(38, 3)).unwrap_err();
        assert_eq!(err.kind, DecimalScaleMismatch);
    }

    #[test]
    fn retag_column_decimal_width_cross_errors() {
        let arr = decimal128(vec![123], 18, 2);
        let err = retag_column(&arr, &DataType::Decimal256(40, 2)).unwrap_err();
        assert_eq!(err.kind, DecimalWidthCross);
    }

    #[test]
    fn retag_column_utf8_to_binary_keeps_bytes() {
        let arr = Arc::new(StringArray::from(vec!["ab", "cd"])) as ArrayRef;
        let out = retag_column(&arr, &DataType::Binary).expect("retag");
        assert_eq!(out.data_type(), &DataType::Binary);
        let b = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(b.value(0), b"ab");
        assert_eq!(b.value(1), b"cd");
    }

    #[test]
    fn retag_column_recurses_struct_child() {
        let d = decimal128(vec![123], 18, 2);
        let i = Arc::new(Int64Array::from(vec![7_i64])) as ArrayRef;
        let src = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("d", DataType::Decimal128(18, 2), true)),
                d,
            ),
            (Arc::new(Field::new("i", DataType::Int64, true)), i),
        ])) as ArrayRef;
        let target = DataType::Struct(Fields::from(vec![
            Field::new("d", DataType::Decimal128(38, 2), true),
            Field::new("i", DataType::Int64, true),
        ]));
        let out = retag_column(&src, &target).expect("retag");
        assert_eq!(out.data_type(), &target);
        let s = out.as_any().downcast_ref::<StructArray>().unwrap();
        let dcol = s
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(dcol.data_type(), &DataType::Decimal128(38, 2));
        assert_eq!(dcol.value(0), 123);
    }

    #[test]
    fn retag_column_recurses_list_item() {
        let values = decimal128(vec![123, 456], 18, 2);
        let src = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Decimal128(18, 2), true)),
            OffsetBuffer::from_lengths([2]),
            values,
            None,
        )) as ArrayRef;
        let target = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Decimal128(38, 2),
            true,
        )));
        let out = retag_column(&src, &target).expect("retag");
        assert_eq!(out.data_type(), &target);
        let l = out.as_any().downcast_ref::<ListArray>().unwrap();
        let items = l
            .values()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(items.value(0), 123);
        assert_eq!(items.value(1), 456);
    }

    #[test]
    fn retag_column_non_retaggable_scalar_errors() {
        let arr = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        let err = retag_column(&arr, &DataType::Int64).unwrap_err();
        assert_eq!(err.kind, ScalarMismatch);
    }
}
