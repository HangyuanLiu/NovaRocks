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

use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields};

use crate::{FunctionValueType, ValueLogicalType, wider_type};

/// Common decimal type for a decimal-vs-decimal comparison / equi-join key.
/// scale = max(s1,s2); precision = max(p1-s1, p2-s2) + scale. Promotes to
/// Decimal256 when the precision exceeds 38 or either side is already 256;
/// errors when it would exceed 76 (Decimal256 max). This is the single source
/// shared by `comparison_common_type`, lower binary_pred, and lower join-key.
pub fn decimal_compare_type(left: &DataType, right: &DataType) -> Result<DataType, String> {
    let (lp, ls, left_is_256) = match left {
        DataType::Decimal128(p, s) => (*p, *s, false),
        DataType::Decimal256(p, s) => (*p, *s, true),
        _ => {
            return Err(format!(
                "decimal comparison requires decimal operands (left={left:?}, right={right:?})"
            ));
        }
    };
    let (rp, rs, right_is_256) = match right {
        DataType::Decimal128(p, s) => (*p, *s, false),
        DataType::Decimal256(p, s) => (*p, *s, true),
        _ => {
            return Err(format!(
                "decimal comparison requires decimal operands (left={left:?}, right={right:?})"
            ));
        }
    };

    let target_scale: i8 = ls.max(rs);
    let lhs_int_digits: i16 = (lp as i16) - (ls as i16);
    let rhs_int_digits: i16 = (rp as i16) - (rs as i16);
    let int_digits: i16 = lhs_int_digits.max(rhs_int_digits).max(0);
    let target_precision: i16 = int_digits + (target_scale as i16);
    if target_precision <= 0 {
        return Err(format!(
            "decimal comparison invalid precision (left={left:?}, right={right:?})"
        ));
    }
    let need_decimal256 = left_is_256 || right_is_256 || target_precision > 38;
    if need_decimal256 {
        if target_precision > 76 {
            return Err(format!(
                "decimal comparison precision overflow (left={left:?}, right={right:?}, target=Decimal256({target_precision}, {target_scale}))"
            ));
        }
        let target_precision_u8 = target_precision as u8;
        return Ok(DataType::Decimal256(target_precision_u8, target_scale));
    }
    let target_precision_u8 = target_precision as u8;
    Ok(DataType::Decimal128(target_precision_u8, target_scale))
}

/// Comparison operand common type. Single authority shared by analyzer,
/// execution `normalize_comparison_types`, and lower binary_pred / join-key.
/// `Ok(None)`: operands already equal, OR pair is out of scope (temporal,
/// largeint-decimal, cross-family) and is left to the caller.
/// `Ok(Some(t))`: nullable / numeric / decimal / string-numeric pair, including
/// same-shape complex containers whose nested scalar fields have a common type,
/// -> cast BOTH operands to `t`.
/// `Err`: decimal-compatible pair whose common precision exceeds Decimal256
/// (> 76).
pub fn comparison_common_type(
    left: &DataType,
    right: &DataType,
) -> Result<Option<DataType>, String> {
    if left == right {
        return Ok(None);
    }
    if left == &DataType::Null {
        return Ok(Some(right.clone()));
    }
    if right == &DataType::Null {
        return Ok(Some(left.clone()));
    }
    if let Some(common) = comparison_common_complex_type(left, right)? {
        return Ok(Some(common));
    }
    let is_int = |dt: &DataType| {
        matches!(
            dt,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        )
    };
    let is_bool = |dt: &DataType| matches!(dt, DataType::Boolean);
    let is_float = |dt: &DataType| matches!(dt, DataType::Float32 | DataType::Float64);
    let int_as_zero_scale_decimal = |dt: &DataType| -> Option<DataType> {
        match dt {
            DataType::Boolean => Some(DataType::Decimal128(1, 0)),
            DataType::Int8 => Some(DataType::Decimal128(3, 0)),
            DataType::Int16 => Some(DataType::Decimal128(5, 0)),
            DataType::Int32 => Some(DataType::Decimal128(10, 0)),
            DataType::Int64 => Some(DataType::Decimal128(19, 0)),
            _ => None,
        }
    };
    let is_decimal =
        |dt: &DataType| matches!(dt, DataType::Decimal128(_, _) | DataType::Decimal256(_, _));

    if (crate::is_largeint_data_type(left) && (is_int(right) || is_bool(right)))
        || ((is_int(left) || is_bool(left)) && crate::is_largeint_data_type(right))
    {
        return Ok(Some(DataType::FixedSizeBinary(crate::LARGEINT_BYTE_WIDTH)));
    }
    if (is_bool(left) && is_int(right)) || (is_int(left) && is_bool(right)) {
        return Ok(Some(DataType::Int64));
    }
    if (is_bool(left) && is_float(right)) || (is_float(left) && is_bool(right)) {
        return Ok(Some(DataType::Float64));
    }
    if is_int(left) && is_int(right) {
        return Ok(Some(DataType::Int64));
    }
    if (is_int(left) && is_float(right)) || (is_float(left) && is_int(right)) {
        return Ok(Some(DataType::Float64));
    }
    if is_float(left) && is_float(right) {
        return Ok(Some(DataType::Float64));
    }
    if is_decimal(left) && is_decimal(right) {
        return Ok(Some(decimal_compare_type(left, right)?));
    }
    if let Some(left_decimal) = int_as_zero_scale_decimal(left)
        && is_decimal(right)
    {
        return Ok(Some(decimal_compare_type(&left_decimal, right)?));
    }
    if let Some(right_decimal) = int_as_zero_scale_decimal(right)
        && is_decimal(left)
    {
        return Ok(Some(decimal_compare_type(left, &right_decimal)?));
    }
    if (is_float(left) && is_decimal(right)) || (is_decimal(left) && is_float(right)) {
        return Ok(Some(DataType::Float64));
    }
    let is_string = |dt: &DataType| matches!(dt, DataType::Utf8 | DataType::LargeUtf8);
    let is_numeric = |dt: &DataType| is_int(dt) || is_float(dt) || is_decimal(dt);
    if is_string(left) && is_numeric(right) {
        return Ok(Some(right.clone()));
    }
    if is_numeric(left) && is_string(right) {
        return Ok(Some(left.clone()));
    }

    Ok(None)
}

fn comparison_common_complex_type(
    left: &DataType,
    right: &DataType,
) -> Result<Option<DataType>, String> {
    match (left, right) {
        (DataType::List(left_item), DataType::List(right_item)) => {
            let Some((item, changed)) = comparison_common_field(left_item, right_item)? else {
                return Ok(None);
            };
            Ok(changed.then_some(DataType::List(item)))
        }
        (DataType::LargeList(left_item), DataType::LargeList(right_item)) => {
            let Some((item, changed)) = comparison_common_field(left_item, right_item)? else {
                return Ok(None);
            };
            Ok(changed.then_some(DataType::LargeList(item)))
        }
        (
            DataType::FixedSizeList(left_item, left_size),
            DataType::FixedSizeList(right_item, right_size),
        ) if left_size == right_size => {
            let Some((item, changed)) = comparison_common_field(left_item, right_item)? else {
                return Ok(None);
            };
            Ok(changed.then_some(DataType::FixedSizeList(item, *left_size)))
        }
        (DataType::Struct(left_fields), DataType::Struct(right_fields))
            if left_fields.len() == right_fields.len() =>
        {
            comparison_common_struct_type(left_fields, right_fields)
        }
        (
            DataType::Map(left_entries, left_ordered),
            DataType::Map(right_entries, right_ordered),
        ) if left_ordered == right_ordered => {
            let Some((entries, changed)) = comparison_common_field(left_entries, right_entries)?
            else {
                return Ok(None);
            };
            Ok(changed.then_some(DataType::Map(entries, *left_ordered)))
        }
        _ => Ok(None),
    }
}

fn comparison_common_struct_type(
    left_fields: &Fields,
    right_fields: &Fields,
) -> Result<Option<DataType>, String> {
    if let Some(fields) = comparison_common_struct_fields_by_name(left_fields, right_fields)? {
        return Ok(Some(DataType::Struct(fields)));
    }

    let mut fields = Vec::with_capacity(left_fields.len());
    let mut changed_any = left_fields
        .iter()
        .zip(right_fields.iter())
        .any(|(left_field, right_field)| left_field.name() != right_field.name());
    for (left_field, right_field) in left_fields.iter().zip(right_fields.iter()) {
        let Some((field, changed)) = comparison_common_field(left_field, right_field)? else {
            return Ok(None);
        };
        changed_any |= changed;
        fields.push(field);
    }
    Ok(changed_any.then(|| DataType::Struct(Fields::from(fields))))
}

fn comparison_common_struct_fields_by_name(
    left_fields: &Fields,
    right_fields: &Fields,
) -> Result<Option<Fields>, String> {
    let right_by_name = right_fields
        .iter()
        .map(|field| (field.name().as_str(), field))
        .collect::<std::collections::HashMap<_, _>>();
    if left_fields
        .iter()
        .any(|field| !right_by_name.contains_key(field.name().as_str()))
    {
        return Ok(None);
    }

    let mut fields = Vec::with_capacity(left_fields.len());
    let mut changed_any = left_fields
        .iter()
        .zip(right_fields.iter())
        .any(|(left_field, right_field)| left_field.name() != right_field.name());
    for left_field in left_fields {
        let right_field = right_by_name
            .get(left_field.name().as_str())
            .expect("right field exists by name");
        let Some((field, changed)) = comparison_common_field(left_field, right_field)? else {
            return Ok(None);
        };
        changed_any |= changed;
        fields.push(field);
    }
    Ok(changed_any.then(|| Fields::from(fields)))
}

fn comparison_common_field(
    left: &Field,
    right: &Field,
) -> Result<Option<(Arc<Field>, bool)>, String> {
    let data_type = if left.data_type() == right.data_type() {
        left.data_type().clone()
    } else if let Some(common) =
        comparison_common_nested_field_type(left.data_type(), right.data_type())?
    {
        common
    } else {
        return Ok(None);
    };
    let nullable = left.is_nullable() || right.is_nullable();
    let changed = left.name() != right.name()
        || left.is_nullable() != nullable
        || right.is_nullable() != nullable
        || left.data_type() != &data_type
        || right.data_type() != &data_type;
    Ok(Some((
        Arc::new(Field::new(left.name(), data_type, nullable)),
        changed,
    )))
}

fn comparison_common_nested_field_type(
    left: &DataType,
    right: &DataType,
) -> Result<Option<DataType>, String> {
    let common = comparison_common_type(left, right)?;
    if common.is_some() && (is_string_type(left) || is_string_type(right)) {
        return Ok(Some(wider_type(left, right)));
    }
    Ok(common)
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Utf8 | DataType::LargeUtf8)
}

/// Complete comparison operand type using the existing carrier comparison rule.
///
/// `None` retains the carrier API's out-of-scope meaning. Incompatible logical
/// identities are errors, not an invitation to erase an identity with a carrier
/// fallback. The result is a type calculation only: a caller must materialize
/// any semantic conversion through its actual function owner.
///
/// Recursive inputs must first have been admitted within the caller's type
/// resource bounds. This API does not claim cooperative compilation accounting.
pub fn comparison_common_value_type(
    left: &FunctionValueType,
    right: &FunctionValueType,
) -> Result<Option<FunctionValueType>, String> {
    validate_operands(left, right)?;
    if left.data_type == DataType::Null || right.data_type == DataType::Null {
        let value = null_common_value(left, right);
        return Ok((value != *left || value != *right).then_some(value));
    }
    let carrier = comparison_common_type(&left.data_type, &right.data_type)?;
    let common_exists = carrier.is_some()
        || crate::arrow_type_equals_ignoring_metadata(&left.data_type, &right.data_type);
    let candidate = carrier.unwrap_or_else(|| left.data_type.clone());
    let value = restore_value(candidate, left, right)?;
    if !common_exists {
        return Ok(None);
    }
    Ok((value != *left || value != *right).then_some(value))
}

/// Complete type for the existing comparison carrier fallback.
///
/// This shares `wider_type` rather than defining another carrier promotion
/// algorithm. It preserves the selected source's field facts and permits only
/// the explicitly declared JSON/text and LARGEINT/signed-integer domain pairs.
/// It does not authorize runtime casts or supply decimal conversion policy.
pub fn wider_comparison_value_type(
    left: &FunctionValueType,
    right: &FunctionValueType,
) -> Result<FunctionValueType, String> {
    validate_operands(left, right)?;
    if left.data_type == DataType::Null || right.data_type == DataType::Null {
        return Ok(null_common_value(left, right));
    }
    restore_value(wider_type(&left.data_type, &right.data_type), left, right)
}

fn validate_operands(left: &FunctionValueType, right: &FunctionValueType) -> Result<(), String> {
    left.validate()
        .and_then(|()| right.validate())
        .map_err(|error| error.to_string())
}

fn null_common_value(left: &FunctionValueType, right: &FunctionValueType) -> FunctionValueType {
    let mut result = if left.data_type == DataType::Null {
        right.clone()
    } else {
        left.clone()
    };
    result.nullable = left.nullable || right.nullable;
    result
}

fn signed_integer(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn restore_value(
    mut carrier: DataType,
    left: &FunctionValueType,
    right: &FunctionValueType,
) -> Result<FunctionValueType, String> {
    if left.data_type == DataType::Null || right.data_type == DataType::Null {
        return Ok(null_common_value(left, right));
    }
    let logical_type = if left.logical_type == right.logical_type {
        if left.logical_type == ValueLogicalType::Physical
            && ((crate::is_largeint_data_type(&left.data_type)
                && (crate::is_numeric_value_type(right) || right.data_type == DataType::Boolean))
                || (crate::is_largeint_data_type(&right.data_type)
                    && (crate::is_numeric_value_type(left) || left.data_type == DataType::Boolean)))
        {
            return Err("fixed binary has no declared numeric comparison identity".into());
        }
        left.logical_type
    } else {
        match (left.logical_type, right.logical_type) {
            (ValueLogicalType::LargeInt, ValueLogicalType::Physical)
                if signed_integer(&right.data_type) =>
            {
                carrier = left.data_type.clone();
                ValueLogicalType::LargeInt
            }
            (ValueLogicalType::Physical, ValueLogicalType::LargeInt)
                if signed_integer(&left.data_type) =>
            {
                carrier = right.data_type.clone();
                ValueLogicalType::LargeInt
            }
            (ValueLogicalType::Json, ValueLogicalType::Physical)
                if right.data_type == DataType::Utf8 =>
            {
                carrier = DataType::Utf8;
                ValueLogicalType::Physical
            }
            (ValueLogicalType::Physical, ValueLogicalType::Json)
                if left.data_type == DataType::Utf8 =>
            {
                carrier = DataType::Utf8;
                ValueLogicalType::Physical
            }
            _ => {
                return Err(format!(
                    "comparison has no declared conversion between {:?} and {:?}",
                    left.logical_type, right.logical_type
                ));
            }
        }
    };
    let data_type = restore_type(carrier, &left.data_type, &right.data_type)?;
    FunctionValueType::try_with_logical_type(
        data_type,
        left.nullable || right.nullable,
        logical_type,
    )
    .map_err(|error| error.to_string())
}

fn field_value(field: &Field) -> Result<FunctionValueType, String> {
    FunctionValueType::try_with_logical_type(
        field.data_type().clone(),
        field.is_nullable(),
        crate::field_logical_type(field).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())
}

fn restore_field(candidate: &Field, left: &Field, right: &Field) -> Result<Arc<Field>, String> {
    if left.data_type() != &DataType::Null && right.data_type() != &DataType::Null {
        #[allow(deprecated)]
        let dictionary_identity_matches =
            left.dict_id() == right.dict_id() && left.dict_is_ordered() == right.dict_is_ordered();
        if !dictionary_identity_matches {
            return Err("comparison cannot change a dictionary field identity".into());
        }
    }
    let value = restore_value(
        candidate.data_type().clone(),
        &field_value(left)?,
        &field_value(right)?,
    )?;
    // NULL has no nested identity to donate. Preserve the decided source's
    // metadata and dictionary attributes, with the existing left layout name.
    let decided = if left.data_type() == &DataType::Null {
        right
    } else {
        left
    };
    let mut metadata = decided.metadata().clone();
    match value.logical_type.metadata_value() {
        Some(domain) => {
            metadata.insert(crate::NR_LOGICAL_TYPE_KEY.into(), domain.into());
        }
        None => {
            metadata.remove(crate::NR_LOGICAL_TYPE_KEY);
        }
    }
    Ok(Arc::new(
        decided
            .clone()
            .with_name(left.name())
            .with_data_type(value.data_type)
            .with_nullable(value.nullable)
            .with_metadata(metadata),
    ))
}

fn restore_type(
    candidate: DataType,
    left: &DataType,
    right: &DataType,
) -> Result<DataType, String> {
    macro_rules! list {
        ($variant:ident, $target:ident, $lhs:ident, $rhs:ident) => {
            Ok(DataType::$variant(restore_field($target, $lhs, $rhs)?))
        };
    }
    match (&candidate, left, right) {
        (DataType::List(t), DataType::List(l), DataType::List(r)) => list!(List, t, l, r),
        (DataType::LargeList(t), DataType::LargeList(l), DataType::LargeList(r)) => {
            list!(LargeList, t, l, r)
        }
        (DataType::ListView(t), DataType::ListView(l), DataType::ListView(r)) => {
            list!(ListView, t, l, r)
        }
        (DataType::LargeListView(t), DataType::LargeListView(l), DataType::LargeListView(r)) => {
            list!(LargeListView, t, l, r)
        }
        (
            DataType::FixedSizeList(t, n),
            DataType::FixedSizeList(l, ln),
            DataType::FixedSizeList(r, rn),
        ) if n == ln && ln == rn => Ok(DataType::FixedSizeList(restore_field(t, l, r)?, *n)),
        (DataType::Map(t, sorted), DataType::Map(l, _), DataType::Map(r, _)) => {
            Ok(DataType::Map(restore_field(t, l, r)?, *sorted))
        }
        (DataType::Struct(t), DataType::Struct(l), DataType::Struct(r))
            if t.len() == l.len() && l.len() == r.len() =>
        {
            let right_by_name = r
                .iter()
                .map(|field| (field.name().as_str(), field))
                .collect::<std::collections::HashMap<_, _>>();
            let by_name = l
                .iter()
                .all(|field| right_by_name.contains_key(field.name().as_str()));
            let fields = t
                .iter()
                .zip(l.iter())
                .enumerate()
                .map(|(index, (target, left))| {
                    let right = if by_name {
                        right_by_name[left.name().as_str()]
                    } else {
                        &r[index]
                    };
                    restore_field(target, left, right)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(DataType::Struct(fields.into()))
        }
        (
            DataType::Dictionary(tk, tv),
            DataType::Dictionary(lk, lv),
            DataType::Dictionary(rk, rv),
        ) if tk == lk && lk == rk => Ok(DataType::Dictionary(
            tk.clone(),
            Box::new(restore_type((**tv).clone(), lv, rv)?),
        )),
        (
            DataType::RunEndEncoded(tre, tv),
            DataType::RunEndEncoded(lre, lv),
            DataType::RunEndEncoded(rre, rv),
        ) => Ok(DataType::RunEndEncoded(
            restore_field(tre, lre, rre)?,
            restore_field(tv, lv, rv)?,
        )),
        (DataType::Union(t, tm), DataType::Union(l, lm), DataType::Union(r, rm))
            if tm == lm && lm == rm && t.len() == l.len() && l.len() == r.len() =>
        {
            let fields = t
                .iter()
                .zip(l.iter())
                .map(|((tag, target), (left_tag, left))| {
                    if tag != left_tag {
                        return Err("comparison cannot change union field identity".into());
                    }
                    let right = r
                        .iter()
                        .find_map(|(right_tag, field)| (right_tag == tag).then_some(field))
                        .ok_or_else(|| {
                            "comparison cannot change union field identity".to_string()
                        })?;
                    Ok((tag, restore_field(target, left, right)?))
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(DataType::Union(fields.into_iter().collect(), *tm))
        }
        _ => {
            // The carrier fallback can select one whole shape. A shape change
            // may not silently discard any nested semantic or dictionary fact.
            if (contains_identity(left)? || contains_identity(right)?)
                && (!crate::arrow_data_types_exact(&candidate, left)
                    || !crate::arrow_data_types_exact(left, right))
            {
                return Err("comparison cannot erase an unsupported nested identity".into());
            }
            Ok(candidate)
        }
    }
}

fn contains_identity(data_type: &DataType) -> Result<bool, String> {
    fn field(field: &Field) -> Result<bool, String> {
        #[allow(deprecated)]
        let dictionary = field.dict_id().is_some() || field.dict_is_ordered().is_some();
        Ok(dictionary
            || crate::field_logical_type(field).map_err(|error| error.to_string())?
                != ValueLogicalType::Physical
            || contains_identity(field.data_type())?)
    }
    match data_type {
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _) => field(f),
        DataType::Struct(fields) => fields
            .iter()
            .try_fold(false, |found, f| Ok(found || field(f)?)),
        DataType::Union(fields, _) => fields
            .iter()
            .try_fold(false, |found, (_, f)| Ok(found || field(f)?)),
        DataType::RunEndEncoded(run_ends, values) => Ok(field(run_ends)? || field(values)?),
        DataType::Dictionary(..) => Ok(true),
        _ => Ok(false),
    }
}

#[cfg(test)]
mod value_tests {
    use super::*;
    use arrow_schema::TimeUnit;

    fn value(carrier: DataType, logical: ValueLogicalType, nullable: bool) -> FunctionValueType {
        FunctionValueType::try_with_logical_type(carrier, nullable, logical).unwrap()
    }
    fn physical(carrier: DataType, nullable: bool) -> FunctionValueType {
        value(carrier, ValueLogicalType::Physical, nullable)
    }
    fn field(
        name: &str,
        carrier: DataType,
        logical: ValueLogicalType,
        nullable: bool,
    ) -> Arc<Field> {
        let mut metadata = [("provider.field-id".into(), "73".into())]
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        if let Some(domain) = logical.metadata_value() {
            metadata.insert(crate::NR_LOGICAL_TYPE_KEY.into(), domain.into());
        }
        Arc::new(Field::new(name, carrier, nullable).with_metadata(metadata))
    }

    #[test]
    fn scalar_common_types_preserve_existing_decimal_and_numeric_rules() {
        let left = physical(DataType::Int32, false);
        let right = physical(DataType::Decimal128(10, 2), true);
        assert_eq!(
            comparison_common_value_type(&left, &right).unwrap(),
            Some(physical(DataType::Decimal128(12, 2), true))
        );
        assert!(
            comparison_common_value_type(
                &physical(DataType::Decimal256(76, 0), false),
                &physical(DataType::Decimal256(76, 38), false)
            )
            .is_err()
        );
        assert_eq!(comparison_common_value_type(&left, &left).unwrap(), None);
    }

    #[test]
    fn only_declared_root_pairs_get_semantic_common_types() {
        let large = value(
            DataType::FixedSizeBinary(16),
            ValueLogicalType::LargeInt,
            false,
        );
        let integer = physical(DataType::Int64, true);
        let expected = value(
            DataType::FixedSizeBinary(16),
            ValueLogicalType::LargeInt,
            true,
        );
        for (left, right) in [(&large, &integer), (&integer, &large)] {
            assert_eq!(
                comparison_common_value_type(left, right).unwrap(),
                Some(expected.clone())
            );
            assert_eq!(wider_comparison_value_type(left, right).unwrap(), expected);
        }
        let json = value(DataType::Utf8, ValueLogicalType::Json, false);
        let text = physical(DataType::Utf8, true);
        assert_eq!(
            comparison_common_value_type(&json, &text).unwrap(),
            Some(text.clone())
        );
        assert_eq!(wider_comparison_value_type(&text, &json).unwrap(), text);
        assert!(
            wider_comparison_value_type(&large, &physical(DataType::Decimal128(20, 0), true))
                .is_err()
        );
        assert!(wider_comparison_value_type(&large, &physical(DataType::Utf8, true)).is_err());
        assert!(wider_comparison_value_type(&json, &integer).is_err());
    }

    #[test]
    fn fixed_binary_and_uuid_are_never_inferred_numeric() {
        let raw = physical(DataType::FixedSizeBinary(16), false);
        let uuid = value(DataType::FixedSizeBinary(16), ValueLogicalType::Uuid, false);
        for numeric in [
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Boolean,
            DataType::Float64,
            DataType::Decimal32(7, 2),
            DataType::Decimal64(15, 2),
            DataType::Decimal128(20, 0),
            DataType::Decimal256(55, 2),
        ] {
            let numeric = physical(numeric, true);
            assert!(comparison_common_value_type(&raw, &numeric).is_err());
            assert!(wider_comparison_value_type(&raw, &numeric).is_err());
            assert!(comparison_common_value_type(&numeric, &raw).is_err());
            assert!(wider_comparison_value_type(&numeric, &raw).is_err());
            assert!(comparison_common_value_type(&uuid, &numeric).is_err());
            assert!(wider_comparison_value_type(&uuid, &numeric).is_err());
        }
        assert_eq!(comparison_common_value_type(&uuid, &uuid).unwrap(), None);
        assert_eq!(wider_comparison_value_type(&uuid, &uuid).unwrap(), uuid);
    }

    #[test]
    fn null_copies_full_decided_source_and_only_joins_nullability() {
        let source = value(DataType::Utf8, ValueLogicalType::Json, false);
        let null = physical(DataType::Null, true);
        let mut expected = source.clone();
        expected.nullable = true;
        assert_eq!(
            comparison_common_value_type(&null, &source).unwrap(),
            Some(expected.clone())
        );
        assert_eq!(
            wider_comparison_value_type(&source, &null).unwrap(),
            expected
        );
        let list = physical(
            DataType::List(field(
                "actual",
                DataType::Utf8,
                ValueLogicalType::Json,
                false,
            )),
            false,
        );
        let mut expected = list.clone();
        expected.nullable = true;
        assert_eq!(wider_comparison_value_type(&null, &list).unwrap(), expected);
    }

    #[test]
    fn nested_common_retains_selected_field_facts_and_changes_only_declared_domain() {
        let left_item = field("source_item", DataType::Utf8, ValueLogicalType::Json, false);
        let right_item = field(
            "other_item",
            DataType::Utf8,
            ValueLogicalType::Physical,
            true,
        );
        let left = physical(DataType::List(left_item.clone()), false);
        let right = physical(DataType::List(right_item), true);
        for result in [
            comparison_common_value_type(&left, &right)
                .unwrap()
                .unwrap(),
            wider_comparison_value_type(&left, &right).unwrap(),
        ] {
            let DataType::List(item) = result.data_type else {
                panic!("expected List")
            };
            assert_eq!(item.name(), "source_item");
            assert!(item.is_nullable());
            assert_eq!(item.metadata().get("provider.field-id").unwrap(), "73");
            assert!(!item.metadata().contains_key(crate::NR_LOGICAL_TYPE_KEY));
            assert!(result.nullable);
        }
        assert_eq!(
            crate::field_logical_type(&left_item).unwrap(),
            ValueLogicalType::Json
        );
    }

    #[test]
    fn nested_signed_to_largeint_retains_authoritative_domain_and_metadata() {
        let left = physical(
            DataType::Struct(
                vec![field(
                    "n",
                    DataType::FixedSizeBinary(16),
                    ValueLogicalType::LargeInt,
                    false,
                )]
                .into(),
            ),
            false,
        );
        let right = physical(
            DataType::Struct(
                vec![field(
                    "n",
                    DataType::Int32,
                    ValueLogicalType::Physical,
                    true,
                )]
                .into(),
            ),
            false,
        );
        let result = comparison_common_value_type(&left, &right)
            .unwrap()
            .unwrap();
        let DataType::Struct(fields) = result.data_type else {
            panic!("expected Struct")
        };
        assert_eq!(
            crate::field_logical_type(&fields[0]).unwrap(),
            ValueLogicalType::LargeInt
        );
        assert_eq!(fields[0].metadata().get("provider.field-id").unwrap(), "73");
        assert!(fields[0].is_nullable());
    }

    #[test]
    fn dictionary_attributes_survive_and_conflicting_identity_is_an_error() {
        #[allow(deprecated)]
        let make = |id, nullable| {
            physical(
                DataType::Struct(
                    vec![
                        Field::new_dict(
                            "dict",
                            DataType::Dictionary(
                                Box::new(DataType::Int8),
                                Box::new(DataType::Utf8),
                            ),
                            nullable,
                            id,
                            true,
                        )
                        .with_metadata([("provider.field-id".into(), "91".into())].into()),
                    ]
                    .into(),
                ),
                false,
            )
        };
        let left = make(73, false);
        let right = make(73, true);
        let expected = make(73, true);
        assert_eq!(
            wider_comparison_value_type(&left, &right).unwrap(),
            expected
        );
        assert!(comparison_common_value_type(&left, &make(74, true)).is_err());
        assert!(wider_comparison_value_type(&left, &make(74, true)).is_err());
    }

    #[test]
    fn ordinary_physical_fallback_keeps_temporal_and_text_behavior() {
        let date = physical(DataType::Date32, false);
        let time = physical(
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        );
        assert_eq!(comparison_common_value_type(&date, &time).unwrap(), None);
        assert_eq!(wider_comparison_value_type(&date, &time).unwrap(), time);
        let text = physical(DataType::Utf8, false);
        assert_eq!(
            comparison_common_value_type(&text, &physical(DataType::Int32, true)).unwrap(),
            Some(physical(DataType::Int32, true))
        );
        assert_eq!(
            wider_comparison_value_type(&text, &physical(DataType::Int32, true)).unwrap(),
            physical(DataType::Utf8, true)
        );
    }

    #[test]
    fn map_fallback_preserves_original_field_attributes() {
        let entries = field(
            "actual_entries",
            DataType::Struct(
                vec![
                    field(
                        "actual_key",
                        DataType::Int32,
                        ValueLogicalType::Physical,
                        false,
                    ),
                    field("actual_value", DataType::Utf8, ValueLogicalType::Json, true),
                ]
                .into(),
            ),
            ValueLogicalType::Physical,
            false,
        );
        let left = physical(DataType::Map(entries.clone(), true), false);
        let result = wider_comparison_value_type(&left, &left).unwrap();
        assert_eq!(result, left);
        let right = physical(
            DataType::Map(
                field(
                    "other_entries",
                    DataType::Struct(
                        vec![
                            field(
                                "actual_key",
                                DataType::Int64,
                                ValueLogicalType::Physical,
                                false,
                            ),
                            field(
                                "actual_value",
                                DataType::Utf8,
                                ValueLogicalType::Physical,
                                true,
                            ),
                        ]
                        .into(),
                    ),
                    ValueLogicalType::Physical,
                    false,
                ),
                true,
            ),
            false,
        );
        let result = wider_comparison_value_type(&left, &right).unwrap();
        let DataType::Map(fields, sorted) = result.data_type else {
            panic!("expected Map")
        };
        assert!(!sorted); // Existing wider_type carrier behavior.
        assert_eq!(fields.name(), "actual_entries");
        assert_eq!(fields.metadata(), entries.metadata());
        let DataType::Struct(children) = fields.data_type() else {
            panic!("expected entries")
        };
        assert_eq!(children[1].name(), "actual_value");
        assert_eq!(
            children[1].metadata().get("provider.field-id").unwrap(),
            "73"
        );
        assert_eq!(
            crate::field_logical_type(&children[1]).unwrap(),
            ValueLogicalType::Physical
        );
    }
}
