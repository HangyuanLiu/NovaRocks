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
//! Independent original empty-Struct window witness. No production changes.
use super::*;

#[test]
fn by_window_empty_struct_preserves_rows_nulls_and_empty_invocation() {
    for running in [false, true] {
        for null_winner in [false, true] {
            let nulls = null_winner.then(|| arrow::buffer::NullBuffer::new_null(2));
            let values: ArrayRef = Arc::new(arrow::array::StructArray::new_empty_fields(2, nulls));
            let keys: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
            let output = original_result(values, keys, running).unwrap().unwrap();
            let output = output
                .as_any()
                .downcast_ref::<arrow::array::StructArray>()
                .unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.num_columns(), 2);
            for column in output.columns() {
                let value = column
                    .as_any()
                    .downcast_ref::<arrow::array::StructArray>()
                    .unwrap();
                assert!(value.fields().is_empty());
                assert_eq!(value.len(), 2);
                assert_eq!(value.null_count(), if null_winner { 2 } else { 0 });
            }
        }
    }
    let empty: ArrayRef = Arc::new(arrow::array::StructArray::new_empty_fields(0, None));
    assert!(
        original_result(empty, Arc::new(Int32Array::from(Vec::<i32>::new())), false)
            .unwrap()
            .is_none()
    );
}
