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

//! Original full-domain scalar tuning and diagnostic source probes.
use super::*;
use arrow::array::{Array, ListArray};
use arrow::datatypes::Int64Type;

#[test]
fn original_ds_scalar_null_tuning_defaults_are_not_strict_null_propagation() {
    let values = Arc::new(Int64Array::from(vec![Some(3)])) as ArrayRef;
    for arity in 2..=3 {
        let mut args = vec![values.clone(), Arc::new(Int64Array::from(vec![None]))];
        if arity == 3 {
            args.push(Arc::new(StringArray::from(vec![None::<&str>])));
        }
        let output = evaluate(args).unwrap();
        let output = output.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert!(!output.is_null(0));
        assert_eq!(output.value(0)[3], 17);
        assert_eq!((output.value(0)[7] >> 2) & 3, 1);
    }
}

#[test]
fn original_ds_scalar_full_nested_target_diagnostic_exceeds_row_message_limit() {
    let text = "diagnostic-source-".repeat(80);
    let mut builder = arrow::array::ListBuilder::new(arrow::array::StringBuilder::new());
    builder.values().append_value(&text);
    builder.append(true);
    let target = Arc::new(builder.finish()) as ArrayRef;
    let actual = evaluate(vec![
        Arc::new(Int64Array::from(vec![1])),
        Arc::new(Int64Array::from(vec![10])),
        target.clone(),
    ])
    .unwrap_err();
    let expected = format!(
        "ds_hll_count_distinct_state target type expects string input, got List([Some(Utf8({text:?}))])"
    );
    assert_eq!(actual, expected);
    assert!(actual.len() > novarocks_functions::MAX_ROW_ERROR_MESSAGE_BYTES);
    let output = evaluate(vec![
        Arc::new(Int64Array::from(vec![None])),
        Arc::new(Int64Array::from(vec![10])),
        target,
    ])
    .unwrap();
    assert!(
        output.is_null(0),
        "the same tuning carrier is skipped for a NULL key"
    );
}

#[test]
fn original_ds_scalar_tuning_data_has_exact_single_row_source_and_first_error_order() {
    let value = Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef;
    let log = Arc::new(Int64Array::from(vec![-1, 0])) as ArrayRef;
    let target = Arc::new(StringArray::from(vec!["HLL_6", "HLL_8"])) as ArrayRef;
    let arrays = vec![value, log, target];
    assert_eq!(
        evaluate(arrays.clone()).unwrap_err(),
        "ds_hll_count_distinct_state log_k out of range: -1"
    );
    for (row, expected) in [
        "ds_hll_count_distinct_state log_k out of range: -1",
        "ds_hll_count_distinct_state log_k must be in [4, 21], got 0",
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            evaluate(arrays.iter().map(|a| a.slice(row, 1)).collect()).unwrap_err(),
            expected
        );
    }
    // The same source type is legal when the key row is NULL, without tuning reads.
    let log = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(3)]),
    ])) as ArrayRef;
    let output = evaluate(vec![Arc::new(Int64Array::from(vec![None])), log]).unwrap();
    assert!(output.is_null(0));
}
