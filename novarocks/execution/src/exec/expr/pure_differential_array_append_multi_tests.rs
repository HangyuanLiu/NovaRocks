// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Real append dictionary/view constructor profiles through the permanent harness.
use super::{ScalarDiffSpec, assert_scalar_matches_v1};
use arrow::array::{
    Array, ArrayRef, DictionaryArray, Int8Array, ListArray, StringArray, StringViewArray,
};
use arrow::datatypes::{Field, Int8Type};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use std::sync::Arc;
const ROWS: usize = 257;
fn list(values: ArrayRef) -> ArrayRef {
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::new((0..=ROWS as i32).collect::<Vec<_>>().into()),
        values,
        Some(NullBuffer::from(
            (0..ROWS).map(|row| row % 11 != 0).collect::<Vec<_>>(),
        )),
    ))
}
#[test]
fn pure_differential_append_dictionary_same_and_different_full_backing_domains() {
    let domain: ArrayRef = Arc::new(StringArray::from(vec!["first", "unused-first", "another"]));
    let child: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(
                (0..ROWS)
                    .map(|row| {
                        if row % 7 == 0 {
                            None
                        } else {
                            Some((row % 3) as i8)
                        }
                    })
                    .collect::<Vec<_>>(),
            ),
            domain.clone(),
        )
        .unwrap(),
    );
    for domain in [
        domain,
        Arc::new(StringArray::from(vec![
            "second",
            "unused-second",
            "unused-third",
        ])) as ArrayRef,
    ] {
        let target: ArrayRef = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(
                    (0..ROWS)
                        .map(|row| {
                            if row % 13 == 0 {
                                None
                            } else {
                                Some((row % 3) as i8)
                            }
                        })
                        .collect::<Vec<_>>(),
                ),
                domain,
            )
            .unwrap(),
        );
        let result = assert_scalar_matches_v1(
            ScalarDiffSpec::new("array_append")
                .column(list(child.clone()))
                .column(target)
                .sparse_selections(7, 5381),
        );
        assert_eq!(result.attributed_row_errors, 0);
        assert_eq!(result.legacy_batch_errors, 0);
    }
}
#[test]
fn pure_differential_append_view_constructor_retains_actual_source_buffer_mapping() {
    let child: ArrayRef = Arc::new(StringViewArray::from(
        (0..ROWS)
            .map(|row| {
                if row % 7 == 0 {
                    None
                } else {
                    Some("original-child-long-view")
                }
            })
            .collect::<Vec<_>>(),
    ));
    let target: ArrayRef = Arc::new(StringViewArray::from(
        (0..ROWS)
            .map(|row| {
                if row % 13 == 0 {
                    None
                } else {
                    Some("original-target-long-view")
                }
            })
            .collect::<Vec<_>>(),
    ));
    let result = assert_scalar_matches_v1(
        ScalarDiffSpec::new("array_append")
            .column(list(child))
            .column(target)
            .sparse_selections(7, 5382),
    );
    assert_eq!(result.attributed_row_errors, 0);
    assert_eq!(result.legacy_batch_errors, 0);
}
