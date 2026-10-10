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
//! Independent pre-extraction MD5 family raw oracles; no pure owner required.
use super::{ExprArena, ExprId, ExprNode, LiteralValue};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::eval_encryption_function;
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, FixedSizeBinaryArray,
    Float64Array, Int64Array, LargeBinaryArray, LargeStringArray, NullArray, StringArray,
    StructArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_types::{SlotId, largeint};
use std::sync::Arc;
fn chunk(columns: Vec<ArrayRef>) -> Chunk {
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("v{i}"), a.data_type().clone(), true))
        .collect::<Vec<_>>();
    let slots = (1..=columns.len())
        .map(|i| SlotId::new(i as u32))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    Chunk::new_with_chunk_schema(batch, schema)
}
fn invocation(columns: &[ArrayRef], target: Option<DataType>) -> (ExprArena, ExprId, Vec<ExprId>) {
    let mut arena = ExprArena::default();
    let args = columns
        .iter()
        .enumerate()
        .map(|(i, a)| {
            arena.push_typed(
                ExprNode::SlotId(SlotId::new(i as u32 + 1)),
                a.data_type().clone(),
            )
        })
        .collect();
    let expr = target
        .map(|ty| arena.push_typed(ExprNode::Literal(LiteralValue::Null), ty))
        .unwrap_or(ExprId(usize::MAX));
    (arena, expr, args)
}
fn eval(name: &str, columns: Vec<ArrayRef>, target: Option<DataType>) -> Result<ArrayRef, String> {
    let (arena, expr, args) = invocation(&columns, target);
    eval_encryption_function(name, &arena, expr, &args, &chunk(columns))
}
fn strings(out: &ArrayRef) -> Vec<Option<String>> {
    out.as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|v| v.map(str::to_owned))
        .collect()
}
fn text(v: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
const EMPTY: &str = "d41d8cd98f00b204e9800998ecf8427e";
const HELLO: &str = "5d41402abc4b2a76b9719d911017c592";
#[test]
fn legacy_md5_family_baseline_unary_four_byte_carriers_and_null() {
    let values = vec![Some(""), Some("hello"), Some("é中"), Some("a\0b"), None];
    let bytes = values
        .iter()
        .map(|v| v.map(str::as_bytes))
        .collect::<Vec<_>>();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(values.clone())),
        Arc::new(LargeStringArray::from(values)),
        Arc::new(BinaryArray::from(bytes.clone())),
        Arc::new(LargeBinaryArray::from(bytes)),
    ];
    let expected = vec![
        Some(EMPTY.into()),
        Some(HELLO.into()),
        Some("f06fd4f6fa9601b2b8d793ff0f2e65f1".into()),
        Some("70350f6027bce3713f6b76473084309b".into()),
        None,
    ];
    for a in arrays {
        assert_eq!(
            strings(&eval("md5", vec![a.clone()], Some(DataType::Int64)).unwrap()),
            expected
        );
        assert_eq!(
            strings(&eval("md5", vec![a.slice(1, 3)], None).unwrap()),
            expected[1..4]
        );
    }
    assert_eq!(
        strings(&eval("md5", vec![Arc::new(NullArray::new(3))], None).unwrap()),
        vec![None; 3]
    );
    assert_eq!(
        eval("md5", vec![Arc::new(Int64Array::from(vec![None; 2]))], None).unwrap_err(),
        "md5: arg0 must be VARCHAR or VARBINARY"
    );
}
#[test]
fn legacy_md5_family_baseline_sum_skip_null_partition_and_binary() {
    let a = text(vec![Some("a"), None, None, Some("")]);
    let b = Arc::new(BinaryArray::from(vec![
        Some(b"b".as_slice()),
        Some(b"hello".as_slice()),
        None,
        Some(b"".as_slice()),
    ])) as ArrayRef;
    assert_eq!(
        strings(&eval("md5sum", vec![a, b], Some(DataType::Int64)).unwrap()),
        vec![
            Some("187ef4436122d1cc2f40dc2b92f0eba0".into()),
            Some(HELLO.into()),
            Some(EMPTY.into()),
            Some(EMPTY.into())
        ]
    );
    for a in [
        Arc::new(BinaryArray::from(vec![b"\xff\0\xfe".as_slice()])) as ArrayRef,
        Arc::new(LargeBinaryArray::from(vec![b"\xff\0\xfe".as_slice()])) as ArrayRef,
    ] {
        assert_eq!(
            strings(&eval("md5sum", vec![a], None).unwrap()),
            vec![Some("13a18f27d9e54107c1d22c7d67f55018".into())]
        );
    }
}
#[test]
fn legacy_md5_family_baseline_sum_original_varchar_cast_not_sql_formatter() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![Some(-42), None])),
        Arc::new(BooleanArray::from(vec![Some(true), None])),
        Arc::new(Float64Array::from(vec![Some(-0.0), None])),
        Arc::new(
            Decimal128Array::from(vec![Some(1234), None])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ),
    ];
    let strings_in: Vec<ArrayRef> = vec![
        text(vec![Some("-42"), None]),
        text(vec![Some("true"), None]),
        text(vec![Some("-0.0"), None]),
        text(vec![Some("12.34"), None]),
    ];
    // Arrow owns the textual spelling. Check each exact output before its digest.
    for (a, s) in arrays.iter().zip(&strings_in) {
        assert_eq!(
            arrow::compute::cast(a, &DataType::Utf8).unwrap().to_data(),
            s.to_data()
        );
    }
    assert_eq!(
        strings(&eval("md5sum", arrays, None).unwrap()),
        vec![
            Some("2dbd307d6b5f28ac11c4e56c3ad0601b".into()),
            Some(EMPTY.into())
        ]
    );
}
#[test]
fn legacy_md5_family_baseline_numeric_big_endian_digest_and_low_i64() {
    let input = text(vec![Some(""), Some("hello"), None, Some("a\0b")]);
    let out = eval(
        "md5sum_numeric",
        vec![input.clone()],
        Some(DataType::FixedSizeBinary(16)),
    )
    .unwrap();
    let a = out.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
    let expected: [i128; 4] = [
        -58332598431525814501020785164969033090i128,
        123957004363873451094272536567338222994i128,
        -58332598431525814501020785164969033090i128,
        149149039115758847277334851244616069275i128,
    ];
    for (row, v) in expected.iter().enumerate() {
        assert_eq!(a.value(row), v.to_be_bytes());
        assert_eq!(largeint::value_at(a, row).unwrap(), *v);
    }
    assert_eq!(out.null_count(), 0);
    let out = eval("md5sum_numeric", vec![input], None).unwrap();
    assert_eq!(
        out.to_data(),
        Int64Array::from(expected.map(|v| v as i64).to_vec()).to_data()
    );
}
#[test]
fn legacy_md5_family_baseline_numeric_raw_output_cast_and_null_target_error() {
    let input = text(vec![Some("hello"), None]);
    let raw = eval("md5sum_numeric", vec![input.clone()], None).unwrap();
    for ty in [
        DataType::Utf8,
        DataType::Float64,
        DataType::Int32,
        DataType::FixedSizeBinary(8),
    ] {
        let expected = arrow::compute::cast(&raw, &ty)
            .map_err(|e| format!("md5sum_numeric: failed to cast output: {}", e));
        let actual = eval("md5sum_numeric", vec![input.clone()], Some(ty));
        match (expected, actual) {
            (Ok(a), Ok(b)) => assert_eq!(a.to_data(), b.to_data()),
            (Err(a), Err(b)) => assert_eq!(a, b),
            _ => panic!("raw cast outcome changed"),
        }
    }
}
#[test]
fn legacy_md5_family_baseline_argument_admission_precedes_later_eval_and_full_cast_errors() {
    let a: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(Field::new("x".repeat(700), DataType::Int64, true))].into(),
        vec![Arc::new(Int64Array::from(vec![1]))],
        None,
    ));
    for name in ["md5sum", "md5sum_numeric"] {
        let columns = vec![a.clone()];
        let (mut arena, expr, mut args) = invocation(&columns, None);
        args.push(arena.push_typed(ExprNode::SlotId(SlotId::new(999)), DataType::Utf8));
        assert_eq!(
            eval_encryption_function(name, &arena, expr, &args, &chunk(columns)).unwrap_err(),
            format!("{}: arg0 must be VARCHAR or VARBINARY", name)
        );
    }
    let a = text(vec![Some("x")]);
    let columns = vec![a];
    let (mut arena, expr, mut args) = invocation(&columns, None);
    let bad = arena.push_typed(ExprNode::SlotId(SlotId::new(999)), DataType::Utf8);
    args.push(bad);
    let error = arena.eval(bad, &chunk(columns.clone())).unwrap_err();
    for name in ["md5sum", "md5sum_numeric"] {
        assert_eq!(
            eval_encryption_function(name, &arena, expr, &args, &chunk(columns.clone()))
                .unwrap_err(),
            error
        );
    }
    let target = DataType::Struct(
        vec![Arc::new(Field::new(
            "long".repeat(180),
            DataType::Int64,
            true,
        ))]
        .into(),
    );
    let out = eval("md5sum_numeric", columns.clone(), None).unwrap();
    let expected = arrow::compute::cast(&out, &target)
        .map_err(|e| format!("md5sum_numeric: failed to cast output: {}", e))
        .unwrap_err();
    assert!(expected.len() > 512);
    assert_eq!(
        eval("md5sum_numeric", columns, Some(target)).unwrap_err(),
        expected
    );
}
#[test]
fn legacy_md5_family_baseline_zero_arity_tail_ignored_and_empty_batches() {
    let columns = vec![text(vec![Some("unused"), None])];
    let (mut arena, expr, _) = invocation(&columns, None);
    let c = chunk(columns.clone());
    for name in ["md5sum", "md5sum_numeric"] {
        let out = eval_encryption_function(name, &arena, expr, &[], &c).unwrap();
        if name == "md5sum" {
            assert_eq!(strings(&out), vec![Some(EMPTY.into()); 2]);
        } else {
            assert_eq!(
                out.to_data(),
                Int64Array::from(vec![-58332598431525814501020785164969033090i128 as i64; 2])
                    .to_data()
            );
        }
    }
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| eval_encryption_function(
            "md5",
            &arena,
            expr,
            &[],
            &c
        )))
        .is_err()
    );
    let first = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
    let bad = arena.push_typed(ExprNode::SlotId(SlotId::new(999)), DataType::Utf8);
    assert_eq!(
        strings(&eval_encryption_function("md5", &arena, expr, &[first, bad], &c).unwrap()),
        vec![Some("fd94c6a26d6b6571e8d9398446227ae8".into()), None]
    );
    for name in ["md5", "md5sum", "md5sum_numeric"] {
        let out = eval(name, vec![text(vec![])], None).unwrap();
        assert_eq!(out.len(), 0);
    }
}
#[test]
fn legacy_md5_family_baseline_literal_pool_constant_provenance_is_value_only() {
    for name in ["md5", "md5sum", "md5sum_numeric"] {
        for literal in [true, false] {
            let mut arena = ExprArena::default();
            let source = if literal {
                arena.push_typed(
                    ExprNode::Literal(LiteralValue::Utf8("hello".into())),
                    DataType::Utf8,
                )
            } else {
                let pool = super::pure_differential::constant(
                    novarocks_functions::FunctionValueType::new(DataType::Utf8, false),
                    text(vec![Some("hello")]),
                );
                arena.push_typed(ExprNode::Constant(pool), DataType::Utf8)
            };
            let expr = arena.push_typed(
                ExprNode::Literal(LiteralValue::Null),
                if name == "md5sum_numeric" {
                    DataType::FixedSizeBinary(16)
                } else {
                    DataType::Utf8
                },
            );
            let c = chunk(vec![text(vec![None, None, None])]);
            let out = eval_encryption_function(name, &arena, expr, &[source], &c).unwrap();
            if name == "md5sum_numeric" {
                let a = out.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
                for row in 0..3 {
                    assert_eq!(
                        a.value(row),
                        (123957004363873451094272536567338222994i128 as i128).to_be_bytes()
                    );
                }
            } else {
                assert_eq!(strings(&out), vec![Some(HELLO.into()); 3]);
            }
        }
    }
}
