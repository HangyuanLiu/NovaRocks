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
//! UTF-8 membership equality, including JSON and Variant value equality.

use arrow::array::{Array, ArrayRef, BooleanArray, BooleanBuilder, StringArray};
use novarocks_types::value::variant::VariantValue;
use serde_json::Value as JsonValue;

fn row_index(row: usize, len: usize) -> usize {
    if len == 1 { 0 } else { row }
}

pub(crate) fn utf8_equality_observed<E>(
    array: &ArrayRef,
    candidate: &ArrayRef,
    observe: &mut dyn FnMut(novarocks_functions::native_inlist::InObservation) -> Result<(), E>,
) -> Result<BooleanArray, novarocks_functions::native_inlist::InError<E>> {
    use novarocks_functions::native_inlist::{InError, InObservation};
    let input = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            InError::Operation(novarocks_functions::native_inlist::contract(
                "failed to downcast IN input to StringArray",
            ))
        })?;
    let values = candidate
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            InError::Operation(novarocks_functions::native_inlist::contract(
                "failed to downcast IN candidate to StringArray",
            ))
        })?;
    if values.len() != 1 && values.len() != input.len() {
        return Err(InError::Operation(
            novarocks_functions::native_inlist::contract(format_args!(
                "IN predicate value length mismatch: input has {}, value has {}",
                input.len(),
                values.len()
            )),
        ));
    }
    observe(InObservation::OpaqueBoundary).map_err(InError::Host)?;
    let mut builder = BooleanBuilder::with_capacity(input.len());
    observe(InObservation::OpaqueBoundary).map_err(InError::Host)?;
    for row in 0..input.len() {
        observe(InObservation::Step).map_err(InError::Host)?;
        let candidate_row = row_index(row, values.len());
        if input.is_null(row) || values.is_null(candidate_row) {
            builder.append_null();
            continue;
        }
        let lhs = input.value(row);
        let rhs = values.value(candidate_row);
        observe(InObservation::OpaqueBoundary).map_err(InError::Host)?;
        let equal = if let (Some(lhs_json), Some(rhs_json)) = (
            json_value_from_text_or_variant(lhs),
            json_value_from_text_or_variant(rhs),
        ) {
            lhs_json == rhs_json
        } else {
            lhs == rhs
        };
        observe(InObservation::OpaqueBoundary).map_err(InError::Host)?;
        builder.append_value(equal);
    }
    observe(InObservation::OpaqueBoundary).map_err(InError::Host)?;
    let result = builder.finish();
    observe(InObservation::OpaqueBoundary).map_err(InError::Host)?;
    Ok(result)
}

pub(crate) fn variant_json_value(bytes: &[u8]) -> Option<JsonValue> {
    let text = VariantValue::from_serialized(bytes)
        .ok()?
        .to_json_local()
        .ok()?;
    serde_json::from_str(&text).ok()
}

pub(crate) fn json_value_from_text_or_variant(text: &str) -> Option<JsonValue> {
    serde_json::from_str(text)
        .ok()
        .or_else(|| variant_json_value(text.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn observed_utf8_in_preserves_original_json_text_nulls_and_scalar_broadcast() {
        let source: ArrayRef = Arc::new(StringArray::from(vec![
            Some("{\"a\":1,\"b\":2}"),
            Some("ASIA"),
            None,
            Some("x"),
        ]));
        let column: ArrayRef = Arc::new(StringArray::from(vec![
            Some("{\"b\":2,\"a\":1}"),
            Some("AMERICA"),
            Some("ASIA"),
            None,
        ]));
        let actual = utf8_equality_observed(&source, &column, &mut |_| Ok::<_, ()>(())).unwrap();
        assert_eq!(
            actual.iter().collect::<Vec<_>>(),
            vec![Some(true), Some(false), None, None]
        );
        let scalar: ArrayRef = Arc::new(StringArray::from(vec![Some("ASIA")]));
        let actual = utf8_equality_observed(&source, &scalar, &mut |_| Ok::<_, ()>(())).unwrap();
        assert_eq!(
            actual.iter().collect::<Vec<_>>(),
            vec![Some(false), Some(true), None, Some(false)]
        );
    }

    #[test]
    fn observed_utf8_in_retains_control_cause_at_original_opaque_boundaries() {
        use novarocks_functions::native_inlist::{InError, InObservation};
        let source: ArrayRef = Arc::new(StringArray::from(vec![Some("{\"a\":1}")]));
        let mut observations = Vec::new();
        utf8_equality_observed(&source, &source, &mut |event| {
            observations.push(event);
            Ok::<_, ()>(())
        })
        .unwrap();
        for stop in 0..observations.len() {
            let mut reached = 0;
            let error = utf8_equality_observed(&source, &source, &mut |_| {
                let current = reached;
                reached += 1;
                if current == stop {
                    Err("original cancellation")
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
            assert!(matches!(error, InError::Host("original cancellation")));
            assert_eq!(reached, stop + 1);
        }
        assert!(
            observations
                .iter()
                .any(|event| matches!(event, InObservation::Step))
        );
    }
}
