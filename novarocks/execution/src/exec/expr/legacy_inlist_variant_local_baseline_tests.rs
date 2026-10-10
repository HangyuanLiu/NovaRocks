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

//! Original UTF8 IN Variant/local-offset dependence. No process TZ is mutated.
use super::legacy_inlist_required_baseline_tests::original;
use arrow::array::{Array, ArrayRef, BooleanArray, StringArray};
use chrono::FixedOffset;
use novarocks_types::value::variant::VariantValue;
use std::sync::Arc;
fn bools(array: &ArrayRef) -> Vec<Option<bool>> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}
fn utf8(values: Vec<String>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}
#[test]
fn inlist_original_utf8_json_objects_compare_decoded_values() {
    let left = utf8(vec!["{\"a\":1,\"b\":2}".into(), "plain".into()]);
    let right = utf8(vec!["{\"b\":2,\"a\":1}".into(), "PLAIN".into()]);
    assert_eq!(
        bools(&original(vec![left, right], false).unwrap()),
        vec![Some(true), Some(false)]
    );
}
#[test]
fn inlist_original_utf8_variant_timestamp_compares_actual_local_projection() {
    // Original serialized primitive author: TimestampTz primitive ordinal12,
    // BasicType::Primitive low bits00 and i64 epoch microseconds LE.
    let mut primitive = vec![12u8 << 2];
    primitive.extend_from_slice(&0i64.to_le_bytes());
    let value = VariantValue::create(&[1, 0, 0], &primitive).unwrap();
    let serialized = String::from_utf8(value.serialize()).unwrap();
    let zero = value
        .to_json(Some(FixedOffset::east_opt(0).unwrap()))
        .unwrap();
    let eight = value
        .to_json(Some(FixedOffset::east_opt(8 * 3600).unwrap()))
        .unwrap();
    assert_eq!(zero, "\"1970-01-01 00:00:00+00:00\"");
    assert_eq!(eight, "\"1970-01-01 08:00:00+08:00\"");
    assert_ne!(
        zero, eight,
        "one original Variant author depends on its exact offset"
    );
    // Borrow the actual original local author; this is evidence of its value,
    // never a query-frozen parameter or guessed UTC/session timezone.
    let local = value.to_json_local().unwrap();
    println!("original local Variant timestamp JSON={local:?}");
    let wanted = [&zero, &eight].map(|candidate| {
        Some(
            serde_json::from_str::<serde_json::Value>(&local).unwrap()
                == serde_json::from_str::<serde_json::Value>(candidate).unwrap(),
        )
    });
    let left = utf8(vec![serialized.clone(), serialized]);
    let right = utf8(vec![zero, eight]);
    assert_eq!(bools(&original(vec![left, right], false).unwrap()), wanted);
}
