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
use std::panic::{AssertUnwindSafe, catch_unwind};

#[test]
fn by_window_original_empty_struct_preserves_arrow_builder_panic_and_empty_invocation() {
    const ORIGINAL_PANIC: &str = "called `Result::unwrap()` on an `Err` value: InvalidArgumentError(\"use StructArray::try_new_with_length or StructArray::new_empty_fields to create a struct array with no fields so that the length can be set correctly\")";
    for running in [false, true] {
        for null_winner in [false, true] {
            let nulls = null_winner.then(|| arrow::buffer::NullBuffer::new_null(2));
            let values: ArrayRef = Arc::new(arrow::array::StructArray::new_empty_fields(2, nulls));
            assert_eq!(values.len(), 2);
            assert_eq!(values.data_type(), &DataType::Struct(Fields::empty()));
            let keys: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
            let result = catch_unwind(AssertUnwindSafe(|| original_result(values, keys, running)));
            let panic = result
                .expect_err("original empty Struct reaches the unchanged Arrow builder panic");
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .expect("original Arrow unwrap panic has a textual payload");
            assert_eq!(message, ORIGINAL_PANIC);
        }
    }
    let empty: ArrayRef = Arc::new(arrow::array::StructArray::new_empty_fields(0, None));
    assert!(
        original_result(empty, Arc::new(Int32Array::from(Vec::<i32>::new())), false)
            .unwrap()
            .is_none()
    );
}
