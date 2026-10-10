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

//! Original Binary CAST ignores invalid payload under SQL NULL.
use super::legacy_binary_text_cast_baseline_tests::actual;
use arrow::array::{Array, ArrayRef, BinaryArray, StringArray};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer};
use novarocks_type_contract::DecimalOverflowPolicy;
use std::sync::Arc;
#[test]
fn legacy_binary_text_cast_original_hidden_invalid_bytes_under_null_stay_null() {
    let input: ArrayRef = Arc::new(BinaryArray::new(
        OffsetBuffer::new(vec![0, 1, 2, 3].into()),
        Buffer::from(vec![b'a', 0xff, b'b']),
        Some(NullBuffer::from(vec![true, false, true])),
    ));
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            let out = actual(&input, policy, allow).unwrap();
            assert_eq!(
                out.as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some("a"), None, Some("b")]
            );
            let slice = actual(&input.slice(1, 1), policy, allow).unwrap();
            assert!(slice.is_null(0));
        }
    }
}
