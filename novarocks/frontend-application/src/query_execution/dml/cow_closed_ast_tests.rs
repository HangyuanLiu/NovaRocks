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

use super::super::cow_compile_receipt::borrowed_value_footprint::{self as footprint, Failure};
use super::*;
use crate::query_execution::internal_result_cpu::INTERNAL_PEAK_BYTES;
use std::sync::Arc;

fn map_failure(f: Failure) -> BuildError {
    match f {
        Failure::ResourceExhausted => BuildError::ResourceExhausted,
        Failure::Stopped => BuildError::InvalidSource("original COW scope stopped"),
        Failure::InvalidSource(s) => BuildError::InvalidSource(s),
        Failure::OriginalSemantic(s) => BuildError::OriginalSemantic(s.into()),
    }
}
fn preflight(a: &dyn Array, row: usize) -> Result<()> {
    footprint::borrowed_value(a, row, INTERNAL_PEAK_BYTES)
        .map(|_| ())
        .map_err(map_failure)
}
fn old_cell(column: &ArrayRef, row: usize) -> std::result::Result<String, String> {
    let literal = novarocks_sql::literal::literal_from_batch(column, row)?;
    let text = crate::query_execution::dml::iceberg_writer::literal_to_sql_for_arrow_type(
        &literal,
        column.data_type(),
    )?;
    let field = novarocks_types::schema::ColumnDef {
        name: "value".into(),
        data_type: column.data_type().clone(),
        nullable: true,
        write_default: None,
        logical_type: None,
    };
    let sql = crate::query_execution::dml::iceberg_writer::target_cast_expr_sql(&text, &field)?;
    let query = novarocks_parser::parse(&format!("SELECT {sql}")).map_err(|e| e.to_string())?;
    let [Statement::Query(q)] = query.as_slice() else {
        panic!("single old query")
    };
    let SetExpr::Select(select) = q.body.as_ref() else {
        panic!("single SELECT")
    };
    let [SelectItem::UnnamedExpr(e)] = select.projection.as_slice() else {
        panic!("single expression")
    };
    Ok(novarocks_parser::printer::print_expr(e))
}
fn new_cell(column: &ArrayRef, row: usize) -> Result<String> {
    let e = checked_value(column.as_ref(), row, &preflight, &|| Ok(()))?;
    Ok(novarocks_parser::printer::print_expr(&cast(
        e,
        column.data_type(),
    )?))
}

#[test]
fn small_values_keep_original_literal_caster_and_parser_intersection() {
    let list = ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>([
        Some(vec![Some(-2), None, Some(3)]),
        None,
    ]);
    let structure = StructArray::from(vec![
        (
            Arc::new(arrow::datatypes::Field::new("a`b", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![Some(-4), None])) as ArrayRef,
        ),
        (
            Arc::new(arrow::datatypes::Field::new("text", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("a'\\b"), None])) as ArrayRef,
        ),
    ]);
    let mut map = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    map.keys().append_value("key");
    map.values().append_value(-7);
    map.append(true).unwrap();
    map.append(false).unwrap();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(map.finish()),
        Arc::new(BooleanArray::from(vec![Some(true), None])),
        Arc::new(Int8Array::from(vec![Some(-128), None])),
        Arc::new(Int16Array::from(vec![Some(-32768), None])),
        Arc::new(Int32Array::from(vec![Some(i32::MIN), None])),
        Arc::new(Int64Array::from(vec![Some(i64::MIN), None])),
        Arc::new(Float32Array::from(vec![Some(-0.1), None])),
        Arc::new(Float64Array::from(vec![Some(-0.0), None])),
        Arc::new(
            Decimal128Array::from(vec![Some(-1001), None])
                .with_precision_and_scale(12, 3)
                .unwrap(),
        ),
        Arc::new(StringArray::from(vec![Some("a'\\b"), None])),
        Arc::new(BinaryArray::from(vec![Some(&b"\x00\xFF\x27"[..]), None])),
        Arc::new(LargeBinaryArray::from(vec![
            Some(&b"\x00\xFF\x27"[..]),
            None,
        ])),
        Arc::new(Date32Array::from(vec![Some(-1), None])),
        Arc::new(TimestampMicrosecondArray::from(vec![Some(1_234_567), None])),
        Arc::new(list),
        Arc::new(structure),
    ];
    for column in columns {
        for row in 0..column.len() {
            assert_eq!(
                new_cell(&column, row).unwrap(),
                old_cell(&column, row).unwrap(),
                "type {:?} row {row}",
                column.data_type()
            );
        }
    }
}

#[test]
fn unsupported_nonnull_values_stay_unsupported_but_null_first_is_retained() {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(LargeStringArray::from(vec![Some("x"), None])),
        Arc::new(Time64MicrosecondArray::from(vec![Some(1), None])),
        Arc::new(TimestampNanosecondArray::from(vec![Some(1), None])),
    ];
    for column in columns {
        assert!(old_cell(&column, 0).is_err());
        assert!(matches!(
            new_cell(&column, 0),
            Err(BuildError::OriginalSemantic(_))
        ));
        assert_eq!(new_cell(&column, 1).unwrap(), old_cell(&column, 1).unwrap());
    }
    for scale in [-1, 0] {
        let value = if scale == 0 { i128::MAX } else { 1 };
        let a: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(value), None])
                .with_precision_and_scale(38, scale)
                .unwrap(),
        );
        assert!(old_cell(&a, 0).is_err());
        assert!(new_cell(&a, 0).is_err());
        if scale < 0 {
            assert!(old_cell(&a, 1).is_err());
            assert!(new_cell(&a, 1).is_err());
        }
    }
    let empty_struct = DataType::Struct(Vec::<arrow::datatypes::Field>::new().into());
    assert!(matches!(
        type_name(&empty_struct),
        Err(BuildError::OriginalSemantic(_))
    ));
    assert!(matches!(
        finite_float(f64::INFINITY),
        Err(BuildError::OriginalSemantic(_))
    ));
}

#[test]
fn preflight_rejection_happens_before_nested_ast_constructor_entry() {
    let mut list = ListBuilder::new(BooleanBuilder::new());
    for _ in 0..4096 {
        list.values().append_value(true);
    }
    list.append(true);
    let array = list.finish();
    let entered = std::cell::Cell::new(0_u64);
    let result = checked_value(
        &array,
        0,
        &|a, r| {
            // Real borrowed nested lower bound; deliberately smaller COMPONENT
            // allowance to exercise growth ordering, not a production profile.
            footprint::borrowed_value(a, r, std::mem::size_of::<Expr>() as u64)
                .map(|_| ())
                .map_err(map_failure)
        },
        &|| {
            entered.set(entered.get() + 1);
            Ok(())
        },
    );
    assert!(matches!(result, Err(BuildError::ResourceExhausted)));
    assert_eq!(entered.get(), 0);
}

#[test]
fn nested_constructor_still_checks_original_scope_after_first_admission() {
    let array = ListArray::from_iter_primitive::<arrow::datatypes::Int8Type, _, _>([Some(vec![
            Some(1);
            600
        ])]);
    let checks = std::cell::Cell::new(0_u64);
    let result = checked_value(&array, 0, &preflight, &|| {
        checks.set(checks.get() + 1);
        if checks.get() == 2 {
            Err(BuildError::InvalidSource("original scope closed"))
        } else {
            Ok(())
        }
    });
    assert!(matches!(
        result,
        Err(BuildError::InvalidSource("original scope closed"))
    ));
    assert_eq!(checks.get(), 2);
}

#[test]
fn every_generated_nonempty_slot_is_exact_and_empty_values_are_rejected() {
    let a: ArrayRef = Arc::new(BinaryArray::from(vec![&b"abc"[..]]));
    let e = checked_value(a.as_ref(), 0, &preflight, &|| Ok(())).unwrap();
    let Expr::Literal(Literal {
        kind: LiteralKind::HexString(s),
        ..
    }) = e
    else {
        panic!("hex")
    };
    assert_eq!((s.as_str(), s.len(), s.capacity()), ("616263", 6, 6));
    let mut row = exact_vec(1).unwrap();
    row.push(cast(bool_value(true), &DataType::Boolean).unwrap());
    let mut rows = exact_vec(1).unwrap();
    rows.push(row);
    let mut aliases = exact_vec(1).unwrap();
    aliases.push(ident("__nr_v_0", true).unwrap());
    let from = values(rows, "__nr_values", aliases).unwrap();
    let mut projection = exact_vec(1).unwrap();
    projection.push(item(column("__nr_values", "__nr_v_0").unwrap(), "value").unwrap());
    let query = select(projection, from, None, None).unwrap();
    let measured =
        super::super::cow_compile_receipt::deep_closed_query::query_owned(&query).unwrap();
    assert!(measured > std::mem::size_of::<Query>() as u64);
    assert!(values(Vec::new(), "x", Vec::new()).is_err());
}

#[test]
fn authored_variant_fields_keep_exact_cast_identity_and_prospective_heap() {
    use super::super::cow_compile_receipt::type_footprint;
    use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
    let physical = arrow::datatypes::Field::new("value", DataType::LargeBinary, true);
    let variant = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        ValueLogicalType::Variant,
    )
    .unwrap()
    .try_to_field("value")
    .unwrap();
    for (field, expected) in [(physical, "VARBINARY"), (variant, "VARIANT")] {
        let column: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
            Some(&b"\x00\xFF\x27"[..]),
            None,
        ]));
        for row in 0..column.len() {
            let literal = novarocks_sql::literal::literal_from_batch(&column, row).unwrap();
            let literal_sql =
                crate::query_execution::dml::iceberg_writer::literal_to_sql_for_arrow_type(
                    &literal,
                    column.data_type(),
                )
                .unwrap();
            let old = crate::query_execution::dml::iceberg_writer::target_cast_expr_sql(
                &literal_sql,
                &novarocks_types::schema::ColumnDef::from_value_type(
                    field.name().clone(),
                    FunctionValueType::try_from_field(&field).unwrap(),
                    None,
                )
                .unwrap(),
            )
            .unwrap();
            let value = checked_value(column.as_ref(), row, &preflight, &|| Ok(())).unwrap();
            let new = cast_field(value, &field).unwrap();
            assert_eq!(novarocks_parser::printer::print_expr(&new), old);
            assert!(old.ends_with(&format!(" AS {expected})")));
        }
        let name = field_type_name(&field).unwrap();
        assert_eq!(
            type_footprint::exact_field_type_heap(&field).unwrap(),
            name.name.parts.capacity() as u64 * std::mem::size_of::<Ident>() as u64
                + name.name.parts[0].value.capacity() as u64,
        );
    }
    let invalid = arrow::datatypes::Field::new("value", DataType::Int64, true)
        .with_metadata([("nr_logical_type".into(), "variant".into())].into());
    assert!(field_type_name(&invalid).is_err());
    assert!(type_footprint::exact_field_type_heap(&invalid).is_err());
}

#[test]
fn declared_cow_field_types_preserve_original_caster_supported_intersection() {
    use super::super::cow_compile_receipt::type_footprint;
    use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
    let typed = |carrier, logical| {
        FunctionValueType::try_with_logical_type(carrier, true, logical)
            .unwrap()
            .try_to_field("value")
            .unwrap()
    };
    let fields = [
        typed(DataType::FixedSizeBinary(16), ValueLogicalType::Physical),
        typed(DataType::FixedSizeBinary(16), ValueLogicalType::LargeInt),
        typed(DataType::Utf8, ValueLogicalType::Json),
        typed(DataType::Binary, ValueLogicalType::Hll),
        typed(DataType::Binary, ValueLogicalType::Bitmap),
        typed(DataType::FixedSizeBinary(16), ValueLogicalType::Uuid),
        typed(DataType::LargeBinary, ValueLogicalType::Object),
        typed(DataType::Binary, ValueLogicalType::Percentile),
        arrow::datatypes::Field::new(
            "value",
            DataType::List(Arc::new(typed(
                DataType::LargeBinary,
                ValueLogicalType::Variant,
            ))),
            true,
        ),
    ];
    for field in fields {
        let old = novarocks_types::schema::ColumnDef::from_value_type(
            field.name().clone(),
            FunctionValueType::try_from_field(&field).unwrap(),
            None,
        )
        .map_err(|error| error.to_string())
        .and_then(|column| {
            crate::query_execution::dml::iceberg_writer::target_cast_expr_sql("NULL", &column)
        });
        let new = cast_field(null(), &field);
        match (old, new) {
            (Ok(old), Ok(new)) => {
                assert_eq!(novarocks_parser::printer::print_expr(&new), old);
                assert!(type_footprint::exact_field_type_heap(&field).is_ok());
            }
            (Err(_), Err(_)) => assert!(type_footprint::exact_field_type_heap(&field).is_err()),
            (old, new) => {
                panic!("original/new COW cast support differs for {field:?}: {old:?}, {new:?}")
            }
        }
    }
}
