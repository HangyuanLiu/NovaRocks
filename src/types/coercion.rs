use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields};

use crate::types::predicate::{is_integer, is_largeint};

/// Determine the wider type for unifying two types (comparisons, CASE, UNION, etc.).
pub(crate) fn wider_type(a: &DataType, b: &DataType) -> DataType {
    if a == b {
        return a.clone();
    }
    match (a, b) {
        (DataType::Null, other) | (other, DataType::Null) => other.clone(),
        (l, r) if (is_largeint(l) && is_integer(r)) || (is_integer(l) && is_largeint(r)) => {
            DataType::FixedSizeBinary(crate::common::largeint::LARGEINT_BYTE_WIDTH)
        }
        (DataType::List(left_field), DataType::List(right_field)) => {
            DataType::List(Arc::new(Field::new(
                left_field.name(),
                wider_type(left_field.data_type(), right_field.data_type()),
                left_field.is_nullable() || right_field.is_nullable(),
            )))
        }
        (DataType::Map(left_entries, _), DataType::Map(right_entries, _)) => {
            wider_map_type(left_entries, right_entries)
        }
        (DataType::Struct(left_fields), DataType::Struct(right_fields))
            if left_fields.len() == right_fields.len() =>
        {
            if let Some(fields) = wider_struct_fields_by_name(left_fields, right_fields) {
                return DataType::Struct(fields);
            }
            DataType::Struct(Fields::from(
                left_fields
                    .iter()
                    .zip(right_fields.iter())
                    .map(|(left_field, right_field)| {
                        Arc::new(Field::new(
                            left_field.name(),
                            wider_type(left_field.data_type(), right_field.data_type()),
                            left_field.is_nullable() || right_field.is_nullable(),
                        ))
                    })
                    .collect::<Vec<_>>(),
            ))
        }
        // VARCHAR wins before DECIMAL, matching StarRocks TypeManager:
        // getAssignmentCompatibleType handles string pairs before decimal
        // pairs, and ARRAY/MAP/STRUCT common types recurse through this rule.
        (DataType::Utf8, _) | (_, DataType::Utf8) => DataType::Utf8,
        (DataType::LargeUtf8, _) | (_, DataType::LargeUtf8) => DataType::Utf8,
        // Decimal + Decimal -> wider Decimal
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
            let scale = (*s1).max(*s2);
            let precision = ((*p1 as i8 - *s1).max(*p2 as i8 - *s2) + scale).min(38) as u8;
            DataType::Decimal128(precision, scale)
        }
        // Decimal + Integer -> Decimal
        (
            DataType::Decimal128(_, _),
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8,
        )
        | (
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8,
            DataType::Decimal128(_, _),
        ) => {
            let (p, s) = match (a, b) {
                (DataType::Decimal128(p, s), _) | (_, DataType::Decimal128(p, s)) => (*p, *s),
                _ => unreachable!(),
            };
            DataType::Decimal128(p, s)
        }
        // Decimal + Float -> Float64 (StarRocks FE: promote to Double)
        (DataType::Decimal128(_, _), DataType::Float64 | DataType::Float32)
        | (DataType::Float64 | DataType::Float32, DataType::Decimal128(_, _)) => DataType::Float64,
        // Decimal + other -> Decimal
        (DataType::Decimal128(_, _), _) | (_, DataType::Decimal128(_, _)) => {
            let (p, s) = match (a, b) {
                (DataType::Decimal128(p, s), _) | (_, DataType::Decimal128(p, s)) => (*p, *s),
                _ => unreachable!(),
            };
            DataType::Decimal128(p, s)
        }
        // DATE + DATETIME -> DATETIME (StarRocks: only DATETIME signatures exist
        // for comparison/greatest/least/coalesce with mixed date+datetime input).
        (DataType::Timestamp(u, tz), DataType::Date32)
        | (DataType::Date32, DataType::Timestamp(u, tz)) => DataType::Timestamp(*u, tz.clone()),
        (DataType::Float64, _) | (_, DataType::Float64) => DataType::Float64,
        (DataType::Float32, _) | (_, DataType::Float32) => DataType::Float64,
        (DataType::Int64, _) | (_, DataType::Int64) => DataType::Int64,
        (DataType::Int32, _) | (_, DataType::Int32) => DataType::Int64,
        (DataType::Int16, _) | (_, DataType::Int16) => DataType::Int16,
        _ => a.clone(),
    }
}

fn wider_struct_fields_by_name(left_fields: &Fields, right_fields: &Fields) -> Option<Fields> {
    let right_by_name = right_fields
        .iter()
        .map(|field| (field.name().as_str(), field))
        .collect::<std::collections::HashMap<_, _>>();
    if left_fields
        .iter()
        .any(|field| !right_by_name.contains_key(field.name().as_str()))
    {
        return None;
    }
    Some(Fields::from(
        left_fields
            .iter()
            .map(|left_field| {
                let right_field = right_by_name.get(left_field.name().as_str())?;
                Some(Arc::new(Field::new(
                    left_field.name(),
                    wider_type(left_field.data_type(), right_field.data_type()),
                    left_field.is_nullable() || right_field.is_nullable(),
                )))
            })
            .collect::<Option<Vec<_>>>()?,
    ))
}

fn wider_map_type(left_entries: &Field, right_entries: &Field) -> DataType {
    let DataType::Struct(left_fields) = left_entries.data_type() else {
        return DataType::Map(Arc::new(left_entries.clone()), false);
    };
    let DataType::Struct(right_fields) = right_entries.data_type() else {
        return DataType::Map(Arc::new(left_entries.clone()), false);
    };
    if left_fields.len() != 2 || right_fields.len() != 2 {
        return DataType::Map(Arc::new(left_entries.clone()), false);
    }

    let key_type = wider_type(left_fields[0].data_type(), right_fields[0].data_type());
    let value_type = wider_type(left_fields[1].data_type(), right_fields[1].data_type());
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new(
                        "key",
                        key_type,
                        left_fields[0].is_nullable() || right_fields[0].is_nullable(),
                    )),
                    Arc::new(Field::new(
                        "value",
                        value_type,
                        left_fields[1].is_nullable() || right_fields[1].is_nullable(),
                    )),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

/// Common decimal type for a decimal-vs-decimal comparison / equi-join key.
/// scale = max(s1,s2); precision = max(p1-s1, p2-s2) + scale. Promotes to
/// Decimal256 when the precision exceeds 38 or either side is already 256;
/// errors when it would exceed 76 (Decimal256 max). This is the single source
/// shared by `comparison_common_type`, lower binary_pred, and lower join-key.
pub(crate) fn decimal_compare_type(left: &DataType, right: &DataType) -> Result<DataType, String> {
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
pub(crate) fn comparison_common_type(
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

    if (is_largeint(left) && (is_int(right) || is_bool(right)))
        || ((is_int(left) || is_bool(left)) && is_largeint(right))
    {
        return Ok(Some(DataType::FixedSizeBinary(
            crate::common::largeint::LARGEINT_BYTE_WIDTH,
        )));
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
    if let Some(left_decimal) = int_as_zero_scale_decimal(left) {
        if is_decimal(right) {
            return Ok(Some(decimal_compare_type(&left_decimal, right)?));
        }
    }
    if let Some(right_decimal) = int_as_zero_scale_decimal(right) {
        if is_decimal(left) {
            return Ok(Some(decimal_compare_type(left, &right_decimal)?));
        }
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
            Ok(changed.then(|| DataType::List(item)))
        }
        (DataType::LargeList(left_item), DataType::LargeList(right_item)) => {
            let Some((item, changed)) = comparison_common_field(left_item, right_item)? else {
                return Ok(None);
            };
            Ok(changed.then(|| DataType::LargeList(item)))
        }
        (
            DataType::FixedSizeList(left_item, left_size),
            DataType::FixedSizeList(right_item, right_size),
        ) if left_size == right_size => {
            let Some((item, changed)) = comparison_common_field(left_item, right_item)? else {
                return Ok(None);
            };
            Ok(changed.then(|| DataType::FixedSizeList(item, *left_size)))
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
            Ok(changed.then(|| DataType::Map(entries, *left_ordered)))
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Fields};
    use std::sync::Arc;

    #[test]
    fn comparison_common_type_numeric_and_decimal() {
        // equal -> Ok(None)
        assert_eq!(
            comparison_common_type(&DataType::Int32, &DataType::Int32),
            Ok(None)
        );
        // int width mismatch -> both Int64
        assert_eq!(
            comparison_common_type(&DataType::Int32, &DataType::Int64),
            Ok(Some(DataType::Int64))
        );
        assert_eq!(
            comparison_common_type(&DataType::Int16, &DataType::Int8),
            Ok(Some(DataType::Int64))
        );
        // int <-> float / float x float -> both Float64
        assert_eq!(
            comparison_common_type(&DataType::Int32, &DataType::Float64),
            Ok(Some(DataType::Float64))
        );
        assert_eq!(
            comparison_common_type(&DataType::Float32, &DataType::Float64),
            Ok(Some(DataType::Float64))
        );
        // decimal x decimal <=38 -> common Decimal128
        assert_eq!(
            comparison_common_type(&DataType::Decimal128(10, 2), &DataType::Decimal128(18, 4)),
            Ok(Some(DataType::Decimal128(18, 4)))
        );
        assert_eq!(
            comparison_common_type(&DataType::Decimal128(10, 2), &DataType::Decimal128(10, 2)),
            Ok(None)
        );
        // int x decimal: int modeled as zero-scale decimal -> common decimal
        // Int32 -> Decimal128(10,0); scale=2, int_digits=max(10,8)=10, prec=12
        assert_eq!(
            comparison_common_type(&DataType::Int32, &DataType::Decimal128(10, 2)),
            Ok(Some(DataType::Decimal128(12, 2)))
        );
        // string x numeric -> numeric operand's type
        assert_eq!(
            comparison_common_type(&DataType::Utf8, &DataType::Int32),
            Ok(Some(DataType::Int32))
        );
    }

    #[test]
    fn comparison_common_type_new_arms_s3() {
        assert_eq!(
            comparison_common_type(&DataType::Int64, &DataType::Decimal128(10, 2)),
            Ok(Some(DataType::Decimal128(21, 2)))
        );
        assert_eq!(
            comparison_common_type(&DataType::Decimal128(10, 2), &DataType::Int32),
            Ok(Some(DataType::Decimal128(12, 2)))
        );
        assert_eq!(
            comparison_common_type(&DataType::Float64, &DataType::Decimal128(10, 2)),
            Ok(Some(DataType::Float64))
        );
        assert_eq!(
            comparison_common_type(&DataType::Decimal128(10, 2), &DataType::Float32),
            Ok(Some(DataType::Float64))
        );
        assert_eq!(
            comparison_common_type(&DataType::Utf8, &DataType::Int32),
            Ok(Some(DataType::Int32))
        );
        assert_eq!(
            comparison_common_type(&DataType::LargeUtf8, &DataType::Int32),
            Ok(Some(DataType::Int32))
        );
        assert_eq!(
            comparison_common_type(&DataType::Decimal128(10, 2), &DataType::Utf8),
            Ok(Some(DataType::Decimal128(10, 2)))
        );
        assert_eq!(
            comparison_common_type(&DataType::Utf8, &DataType::Float64),
            Ok(Some(DataType::Float64))
        );
        let largeint = DataType::FixedSizeBinary(crate::common::largeint::LARGEINT_BYTE_WIDTH);
        assert_eq!(
            comparison_common_type(&largeint, &DataType::Decimal128(10, 2)),
            Ok(None)
        );
        assert_eq!(
            comparison_common_type(&DataType::Utf8, &DataType::Utf8),
            Ok(None)
        );
        assert_eq!(
            comparison_common_type(&DataType::Utf8, &DataType::Date32),
            Ok(None)
        );
    }

    #[test]
    fn comparison_common_type_recurses_into_complex_shapes() {
        assert_eq!(
            comparison_common_type(
                &DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                &DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Decimal128(10, 2),
                    true
                ))),
            ),
            Ok(Some(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Decimal128(12, 2),
                true
            )))))
        );

        assert_eq!(
            comparison_common_type(
                &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                &DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Decimal128(10, 2),
                    true
                ))),
            ),
            Ok(Some(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Utf8,
                true
            )))))
        );

        assert_eq!(
            comparison_common_type(
                &DataType::Struct(Fields::from(vec![
                    Field::new("c0", DataType::Decimal128(16, 3), true),
                    Field::new("c1", DataType::Utf8, true),
                ])),
                &DataType::Struct(Fields::from(vec![
                    Field::new("c0", DataType::Int32, true),
                    Field::new("c1", DataType::Utf8, true),
                ])),
            ),
            Ok(Some(DataType::Struct(Fields::from(vec![
                Field::new("c0", DataType::Decimal128(16, 3), true),
                Field::new("c1", DataType::Utf8, true),
            ]))))
        );

        let decimal_map = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Decimal128(16, 3), false),
                    Field::new("value", DataType::Utf8, true),
                ])),
                false,
            )),
            false,
        );
        let int_map = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Int32, false),
                    Field::new("value", DataType::Utf8, true),
                ])),
                false,
            )),
            false,
        );
        assert_eq!(
            comparison_common_type(&decimal_map, &int_map),
            Ok(Some(decimal_map))
        );
    }

    #[test]
    fn comparison_common_type_boolean_null_and_largeint_edges() {
        let largeint = DataType::FixedSizeBinary(crate::common::largeint::LARGEINT_BYTE_WIDTH);

        assert_eq!(
            comparison_common_type(&DataType::Boolean, &DataType::Int64),
            Ok(Some(DataType::Int64))
        );
        assert_eq!(
            comparison_common_type(&DataType::Int32, &DataType::Boolean),
            Ok(Some(DataType::Int64))
        );
        assert_eq!(
            comparison_common_type(&DataType::Boolean, &DataType::Float64),
            Ok(Some(DataType::Float64))
        );
        assert_eq!(
            comparison_common_type(&DataType::Boolean, &DataType::Decimal128(10, 2)),
            Ok(Some(DataType::Decimal128(10, 2)))
        );
        assert_eq!(
            comparison_common_type(&DataType::Int64, &DataType::Null),
            Ok(Some(DataType::Int64))
        );
        assert_eq!(
            comparison_common_type(&DataType::Null, &DataType::Boolean),
            Ok(Some(DataType::Boolean))
        );
        assert_eq!(
            comparison_common_type(&largeint, &DataType::Int64),
            Ok(Some(largeint.clone()))
        );
        assert_eq!(
            comparison_common_type(&DataType::Int32, &largeint),
            Ok(Some(largeint))
        );
    }

    #[test]
    fn comparison_common_type_decimal_overflow_promotes_to_256() {
        // scale=max(0,10)=10, int_digits=max(30-0,30-10)=30, precision=40 > 38 -> Decimal256
        assert_eq!(
            comparison_common_type(&DataType::Decimal128(30, 0), &DataType::Decimal128(30, 10)),
            Ok(Some(DataType::Decimal256(40, 10)))
        );
        // either side already Decimal256 -> Decimal256
        assert_eq!(
            comparison_common_type(&DataType::Decimal256(40, 2), &DataType::Decimal128(10, 2)),
            Ok(Some(DataType::Decimal256(40, 2)))
        );
    }

    #[test]
    fn comparison_common_type_decimal_overflow_beyond_256_errs() {
        // precision > 76 -> Err
        let err =
            comparison_common_type(&DataType::Decimal256(76, 0), &DataType::Decimal256(76, 38));
        let err = err.expect_err("expected overflow Err");
        assert!(
            err.contains("precision overflow"),
            "expected precision overflow Err, got {err}"
        );
    }

    #[test]
    fn wider_type_decimal_vs_float64_returns_float64() {
        let result = wider_type(&DataType::Decimal128(7, 2), &DataType::Float64);
        assert_eq!(result, DataType::Float64);
    }

    #[test]
    fn wider_type_float32_vs_decimal_returns_float64() {
        let result = wider_type(&DataType::Float32, &DataType::Decimal128(18, 6));
        assert_eq!(result, DataType::Float64);
    }

    #[test]
    fn wider_type_string_vs_decimal_returns_string() {
        let result = wider_type(&DataType::Utf8, &DataType::Decimal128(26, 2));
        assert_eq!(result, DataType::Utf8);
    }

    #[test]
    fn wider_type_array_string_vs_decimal_returns_array_string() {
        let left = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let right = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Decimal128(26, 2),
            true,
        )));

        let result = wider_type(&left, &right);
        let DataType::List(item) = result else {
            panic!("expected array type");
        };
        assert_eq!(item.data_type(), &DataType::Utf8);
    }

    #[test]
    fn wider_type_promotes_map_key_and_value_types() {
        let left = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Null, true)),
                        Arc::new(Field::new("value", DataType::Null, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let right = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Int64, true)),
                        Arc::new(Field::new("value", DataType::Int64, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );

        let widened = wider_type(&left, &right);
        let DataType::Map(entries, _) = widened else {
            panic!("expected map type");
        };
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("expected entries struct");
        };
        assert_eq!(fields[0].data_type(), &DataType::Int64);
        assert_eq!(fields[1].data_type(), &DataType::Int64);
    }
}
