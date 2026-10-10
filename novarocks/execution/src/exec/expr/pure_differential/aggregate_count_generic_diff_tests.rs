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

//! ANY COUNT profile: physical roots, nested values, encoded carriers and pool offsets.
use super::aggregate::{AggregateDiffSpec, assert_aggregate_matches_v1};
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Int16Type, Int64Type, UnionFields};
use std::sync::Arc;
pub(crate) fn arrays() -> Vec<ArrayRef> {
    vec![
        Arc::new(BooleanArray::from(vec![
            Some(true),
            None,
            Some(false),
            None,
            Some(true),
        ])),
        Arc::new(UInt64Array::from(vec![
            Some(u64::MAX),
            None,
            Some(0),
            None,
            Some(3),
        ])),
        Arc::new(Float64Array::from(vec![
            Some(f64::NAN),
            None,
            Some(-0.0),
            None,
            Some(f64::INFINITY),
        ])),
        Arc::new(
            Decimal128Array::from(vec![Some(123), None, Some(-9), None, Some(0)])
                .with_precision_and_scale(30, 4)
                .unwrap(),
        ),
        Arc::new(Date32Array::from(vec![
            Some(i32::MAX),
            None,
            Some(0),
            None,
            Some(i32::MIN),
        ])),
        Arc::new(
            TimestampMicrosecondArray::from(vec![
                Some(i64::MAX),
                None,
                Some(0),
                None,
                Some(i64::MIN),
            ])
            .with_timezone("a-long-unknown-count-timezone"),
        ),
        Arc::new(BinaryArray::from(vec![
            Some(&b"\xff\0"[..]),
            None,
            Some(&b""[..]),
            None,
            Some(&b"x"[..]),
        ])),
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1), None]),
            None,
            Some(vec![]),
            Some(vec![None]),
            Some(vec![Some(3)]),
        ])),
        Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("v", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![None, Some(1), None, Some(2), None])) as ArrayRef,
        )])),
        Arc::new(
            UnionArray::try_new(
                UnionFields::try_new([0], [Field::new("v", DataType::Int64, true)]).unwrap(),
                vec![0_i8; 5].into(),
                None,
                vec![Arc::new(Int64Array::from(vec![
                    None,
                    Some(1),
                    None,
                    Some(2),
                    None,
                ]))],
            )
            .unwrap(),
        ),
        Arc::new(
            RunArray::<Int16Type>::try_new(
                &Int16Array::from(vec![2, 5]),
                &Int64Array::from(vec![Some(1), None]),
            )
            .unwrap(),
        ),
    ]
}
pub(crate) fn pool(array: ArrayRef, ordinal: usize) -> novarocks_functions::ConstantValue {
    let ty = novarocks_type_contract::FunctionValueType::new(array.data_type().clone(), true);
    let pool = novarocks_functions::ConstantPool::try_new(
        Arc::new(ty.try_to_field("original-count-pool").unwrap()),
        ty,
        array.to_data(),
        super::constant_policy(),
        novarocks_type_contract::CompilePhase::Validate,
        &super::HarnessControl,
    )
    .unwrap();
    pool.value(u32::try_from(ordinal).unwrap()).unwrap()
}
#[test]
fn pure_differential_count_original_any_generic_physical_carriers() {
    for array in arrays() {
        for source in [array.clone(), array.slice(1, 3), array.slice(2, 0)] {
            let n = source.len();
            assert_aggregate_matches_v1(
                AggregateDiffSpec::new("count")
                    .column(source)
                    .grouped((0..n).map(|r| r % 2).collect(), 4)
                    .partitions(3, 21),
            );
        }
    }
}
#[test]
fn pure_differential_count_original_any_nonzero_constant_pool_ordinal() {
    for array in arrays() {
        for ordinal in [1, 4] {
            let value = pool(array.clone(), ordinal);
            for n in [0, 1, 5, 319] {
                assert_aggregate_matches_v1(
                    AggregateDiffSpec::new("count")
                        .constant(value.clone())
                        .constant_rows(n)
                        .grouped((0..n).map(|r| r % 2).collect(), 4)
                        .partitions(3, 21),
                );
            }
        }
    }
}
