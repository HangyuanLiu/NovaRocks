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
//! Additional complete Arrow shape fixtures for the accurately selected ARRAY_REPEAT overload.
use super::{FloatComparison, ScalarDiffSpec, assert_scalar_matches_v1};
use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, StringArray, StructArray};
use arrow::datatypes::{DataType, Field};
use arrow_buffer::OffsetBuffer;
use novarocks_type_contract::FunctionValueType;
use std::sync::Arc;
fn counts(_: usize) -> ArrayRef {
    Arc::new(Int64Array::from(vec![Some(2), None, Some(-1)]))
}
fn check(source: FunctionValueType, array: ArrayRef, count: FunctionValueType, c: ArrayRef) {
    let result = assert_scalar_matches_v1(
        ScalarDiffSpec::new("array_repeat")
            .typed_column(source.clone(), array)
            .typed_column(count, c)
            .float_comparison(FloatComparison::Exact)
            .sparse_selections(3, 4821),
    );
    assert_eq!(result.attributed_row_errors, 0);
    assert_eq!(result.legacy_batch_errors, 0);
    let DataType::List(item) = result.result_type.data_type else {
        panic!("accurate List result")
    };
    assert_eq!(item.data_type(), &source.data_type);
}

#[test]
fn pure_differential_repeat_actual_arrow_encoded_view_and_map_shapes() {
    use arrow::array::{
        BinaryViewArray, DictionaryArray, FixedSizeListArray, Int8Array, Int16Array,
        LargeListArray, LargeListViewArray, ListViewArray, MapArray, RunArray, StringViewArray,
        UnionArray,
    };
    use arrow::datatypes::{Int8Type, Int16Type, UnionFields};
    let int: ArrayRef = Arc::new(Int32Array::from(vec![Some(10), None, Some(30)]));
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![Some(0), None, Some(1)]),
                Arc::new(StringArray::from(vec![
                    "backing string greater than twelve bytes",
                    "other retained dictionary string",
                ])),
            )
            .unwrap(),
        ),
        Arc::new(StringViewArray::from(vec![
            Some("a long view string greater than inline width"),
            None,
            Some("short"),
        ])),
        Arc::new(BinaryViewArray::from(vec![
            Some(&b"binary view exceeding inline length"[..]),
            None,
            Some(&b"short"[..]),
        ])),
        Arc::new(LargeListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(vec![0_i64, 1, 2, 3].into()),
            int.clone(),
            None,
        )),
        Arc::new(
            FixedSizeListArray::try_new(
                Arc::new(Field::new("item", DataType::Int32, true)),
                1,
                int.clone(),
                None,
            )
            .unwrap(),
        ),
        Arc::new(
            ListViewArray::try_new(
                Arc::new(Field::new("item", DataType::Int32, true)),
                vec![0_i32, 1, 2].into(),
                vec![1_i32, 1, 1].into(),
                int.clone(),
                None,
            )
            .unwrap(),
        ),
        Arc::new(
            LargeListViewArray::try_new(
                Arc::new(Field::new("item", DataType::Int32, true)),
                vec![0_i64, 1, 2].into(),
                vec![1_i64, 1, 1].into(),
                int.clone(),
                None,
            )
            .unwrap(),
        ),
    ];
    let fields = vec![
        Arc::new(Field::new("authored-key", DataType::Int32, false)),
        Arc::new(Field::new("authored-value", DataType::Int32, true)),
    ];
    let entries = StructArray::new(
        fields.into(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3])), int.clone()],
        None,
    );
    arrays.push(Arc::new(MapArray::new(
        Arc::new(Field::new(
            "authored-entries",
            entries.data_type().clone(),
            false,
        )),
        OffsetBuffer::new(vec![0, 1, 2, 3].into()),
        entries,
        None,
        true,
    )));
    let fields = UnionFields::try_new(
        [7],
        [Arc::new(Field::new(
            "authored-union",
            DataType::Int32,
            true,
        ))],
    )
    .unwrap();
    for dense in [false, true] {
        arrays.push(Arc::new(
            UnionArray::try_new(
                fields.clone(),
                vec![7_i8, 7, 7].into(),
                dense.then(|| vec![0_i32, 1, 2].into()),
                vec![int.clone()],
            )
            .unwrap(),
        ));
    }
    arrays.push(Arc::new(
        RunArray::<Int16Type>::try_new(&Int16Array::from(vec![1, 2, 3]), int.as_ref()).unwrap(),
    ));
    for a in arrays {
        check(
            FunctionValueType::new(a.data_type().clone(), true),
            a,
            FunctionValueType::new(DataType::Int64, true),
            counts(3),
        );
    }
    let count: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), None, Some(1)]),
            Arc::new(Int64Array::from(vec![2, 1])),
        )
        .unwrap(),
    );
    check(
        FunctionValueType::new(DataType::Int32, true),
        int,
        FunctionValueType::new(count.data_type().clone(), true),
        count,
    );
}
