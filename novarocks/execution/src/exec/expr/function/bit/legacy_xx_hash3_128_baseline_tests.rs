// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.
//! Independent raw-v1 XXH3-128 oracles; no new pure owner is involved.
use super::eval_xx_hash3_128;
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use arrow::array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, Int64Array, LargeBinaryArray,
    LargeStringArray, NullArray, StringArray,
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
    let node = ExprNode::Literal(LiteralValue::Null);
    let output = match target {
        Some(ty) => arena.push_typed(node, ty),
        None => ExprId(usize::MAX),
    };
    (arena, output, args)
}
fn evaluate(columns: Vec<ArrayRef>, target: Option<DataType>) -> Result<ArrayRef, String> {
    let (arena, expr, args) = invocation(&columns, target);
    eval_xx_hash3_128(&arena, expr, &args, &chunk(columns))
}
fn values(output: &ArrayRef) -> Vec<Option<i128>> {
    let array = output
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    (0..array.len())
        .map(|row| {
            if array.is_null(row) {
                None
            } else {
                Some(largeint::value_at(array, row).unwrap())
            }
        })
        .collect()
}
fn join(high: i64, low: u64) -> i128 {
    ((u128::from(high as u64) << 64) | u128::from(low)) as i128
}
#[test]
fn legacy_xx_hash3_128_baseline_original_known_vectors_and_big_endian() {
    let hello = Arc::new(StringArray::from(vec!["hello"])) as ArrayRef;
    let out = evaluate(vec![hello.clone()], Some(DataType::FixedSizeBinary(16))).unwrap();
    let expected = join(-5_338_522_934_378_283_393, 14_373_748_016_363_485_208);
    assert_eq!(values(&out), vec![Some(expected)]);
    assert_eq!(
        out.as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
        expected.to_be_bytes()
    );
    let starrocks = Arc::new(StringArray::from(vec!["starrocks"])) as ArrayRef;
    assert_eq!(
        values(&evaluate(vec![hello, starrocks], Some(DataType::FixedSizeBinary(16))).unwrap()),
        vec![Some(join(
            1_559_307_639_436_096_304,
            8_859_976_453_967_563_600
        ))]
    );
}
#[test]
fn legacy_xx_hash3_128_baseline_partition_boundaries_are_raw_concatenation() {
    let text = Arc::new(StringArray::from(vec![
        Some("ab"),
        Some("你好\0👩‍💻"),
        Some(""),
        None,
    ])) as ArrayRef;
    let binary = Arc::new(BinaryArray::from(vec![
        Some(b"ab".as_slice()),
        Some("你好\0👩‍💻".as_bytes()),
        Some(b"".as_slice()),
        None,
    ])) as ArrayRef;
    let expected = values(&evaluate(vec![text], None).unwrap());
    assert_eq!(values(&evaluate(vec![binary], None).unwrap()), expected);
    let left = Arc::new(StringArray::from(vec![
        Some("a"),
        Some("你好\0"),
        Some(""),
        None,
    ])) as ArrayRef;
    let right = Arc::new(BinaryArray::from(vec![
        Some(b"b".as_slice()),
        Some("👩‍💻".as_bytes()),
        Some(b"".as_slice()),
        Some(b"ignored".as_slice()),
    ])) as ArrayRef;
    assert_eq!(
        values(&evaluate(vec![left, right], None).unwrap()),
        expected
    );
}
#[test]
fn legacy_xx_hash3_128_baseline_large_layout_normalization_and_slices() {
    let source = vec![
        Some("unused"),
        Some("hello"),
        None,
        Some("é\0世界"),
        Some(""),
    ];
    let small = Arc::new(StringArray::from(source.clone())) as ArrayRef;
    let large = Arc::new(LargeStringArray::from(source.clone())) as ArrayRef;
    let binary = Arc::new(BinaryArray::from(
        source
            .iter()
            .map(|s| s.map(str::as_bytes))
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let large_binary = Arc::new(LargeBinaryArray::from(
        source
            .iter()
            .map(|s| s.map(str::as_bytes))
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let expected = values(&evaluate(vec![small.slice(1, 3)], None).unwrap());
    for array in [large, binary, large_binary] {
        assert_eq!(
            values(&evaluate(vec![array.slice(1, 3)], None).unwrap()),
            expected
        );
    }
}
#[test]
fn legacy_xx_hash3_128_baseline_strict_null_and_admission_before_null_or_zero_rows() {
    let left = Arc::new(StringArray::from(vec![None, Some("a"), Some("a")])) as ArrayRef;
    let right = Arc::new(BinaryArray::from(vec![
        Some(b"x".as_slice()),
        None,
        Some(b"".as_slice()),
    ])) as ArrayRef;
    let expected =
        values(&evaluate(vec![Arc::new(StringArray::from(vec!["a"]))], None).unwrap())[0];
    assert_eq!(
        values(&evaluate(vec![left, right], None).unwrap()),
        vec![None, None, expected]
    );
    for rows in [0, 1] {
        assert_eq!(
            evaluate(
                vec![
                    Arc::new(StringArray::from(vec![None::<&str>; rows])),
                    Arc::new(Int64Array::from(vec![None::<i64>; rows]))
                ],
                None
            )
            .unwrap_err(),
            "xx_hash3_128: arg1 must be VARCHAR or VARBINARY"
        );
        assert_eq!(
            evaluate(vec![Arc::new(NullArray::new(rows))], None).unwrap_err(),
            "xx_hash3_128: arg0 must be VARCHAR or VARBINARY"
        );
    }
}
#[test]
fn legacy_xx_hash3_128_baseline_early_admission_error_precedes_later_child_error() {
    let arrays = vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef];
    let (mut arena, expr, mut args) = invocation(&arrays, None);
    args.push(arena.push_typed(ExprNode::SlotId(SlotId::new(999)), DataType::Utf8));
    assert_eq!(
        eval_xx_hash3_128(&arena, expr, &args, &chunk(arrays)).unwrap_err(),
        "xx_hash3_128: arg0 must be VARCHAR or VARBINARY"
    );
}
#[test]
fn legacy_xx_hash3_128_baseline_raw_zero_arity_hashes_empty_public_metadata_excludes_it() {
    let arrays = vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef];
    let (arena, expr, _) = invocation(&arrays, Some(DataType::FixedSizeBinary(16)));
    let out = eval_xx_hash3_128(&arena, expr, &[], &chunk(arrays)).unwrap();
    assert_eq!(
        values(&out),
        vec![Some(0x99aa06d3014798d86001c324468d497fu128 as i128); 2]
    );
    let meta = super::super::metadata("xx_hash3_128").unwrap();
    assert_eq!(meta.min_args, 1);
    assert_eq!(meta.max_args, usize::MAX);
}
#[test]
fn legacy_xx_hash3_128_baseline_original_output_cast_and_full_error() {
    let input = Arc::new(StringArray::from(vec!["hello"])) as ArrayRef;
    let original = evaluate(vec![input.clone()], None).unwrap();
    let out = evaluate(vec![input.clone()], Some(DataType::Binary)).unwrap();
    assert_eq!(out.data_type(), &DataType::Binary);
    assert_eq!(
        out.as_any().downcast_ref::<BinaryArray>().unwrap().value(0),
        original
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0)
    );
    let field = "very_long_target_".to_string() + &"x".repeat(1025);
    let target = DataType::Struct(vec![Field::new(&field, DataType::Int64, true)].into());
    let error = evaluate(vec![input], Some(target)).unwrap_err();
    assert!(error.starts_with("xx_hash3_128: failed to cast output:"));
    assert!(error.contains(&field));
    assert!(error.len() > 512);
}
