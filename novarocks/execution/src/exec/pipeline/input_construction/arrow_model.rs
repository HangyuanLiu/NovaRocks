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

//! BE policy delegates to the single neutral locked-Arrow construction model.
use super::{ConstructionFailure, InputConstructionOperationModel, OpaqueConstructionBound};
use crate::exec::chunk::ArrayNormalizationStep;
use std::alloc::Layout;

pub struct LockedArrowInputConstructionModel;
impl InputConstructionOperationModel for LockedArrowInputConstructionModel {
    fn observe(
        &self,
        step: ArrayNormalizationStep<'_>,
        request: &mut dyn FnMut(Layout) -> Result<(), ConstructionFailure>,
        opaque: &mut dyn FnMut(OpaqueConstructionBound) -> Result<(), ConstructionFailure>,
        work: &mut dyn FnMut(usize) -> Result<(), ConstructionFailure>,
    ) -> Result<(), ConstructionFailure> {
        novarocks_functions::array_projection::observe_array_normalization_construction(
            step, request, opaque, work,
        )
    }
}
#[cfg(test)]
fn decode(
    source: &arrow::array::ArrayRef,
    target: &arrow::datatypes::DataType,
    request: &mut dyn FnMut(Layout) -> Result<(), ConstructionFailure>,
    opaque: &mut dyn FnMut(OpaqueConstructionBound) -> Result<(), ConstructionFailure>,
    work: &mut dyn FnMut(usize) -> Result<(), ConstructionFailure>,
) -> Result<(), ConstructionFailure> {
    novarocks_functions::array_projection::observe_array_normalization_construction(
        ArrayNormalizationStep::DecodeDictionary {
            path: &[],
            source,
            target,
        },
        request,
        opaque,
        work,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::{
        array::{Array, ArrayRef, DictionaryArray, StringArray},
        datatypes::{DataType, Int32Type},
    };

    #[test]
    fn dictionary_sizing_reads_actual_sliced_keys_and_referenced_value_nulls() {
        let source: ArrayRef = std::sync::Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(0), Some(1), None, Some(0)]),
                std::sync::Arc::new(StringArray::from(vec![Some("abcd"), None, Some("unused")])),
            )
            .unwrap(),
        );
        let mut requests = Vec::new();
        let mut bounds = Vec::new();
        decode(
            &source,
            &DataType::Utf8,
            &mut |layout| {
                requests.push(layout);
                Ok(())
            },
            &mut |bound| {
                bounds.push(bound);
                Ok(())
            },
            &mut |_| Ok(()),
        )
        .unwrap();
        assert_eq!(requests[0], Layout::array::<i32>(5).unwrap());
        assert_eq!(requests[1], Layout::array::<u8>(8).unwrap());
        assert_eq!(requests.len(), 3);
        assert_eq!(bounds.len(), 1);
        let sliced = source.slice(1, 2);
        requests.clear();
        decode(
            &sliced,
            &DataType::Utf8,
            &mut |layout| {
                requests.push(layout);
                Ok(())
            },
            &mut |_| Ok(()),
            &mut |_| Ok(()),
        )
        .unwrap();
        assert_eq!(requests[0], Layout::array::<i32>(3).unwrap());
        assert_eq!(requests.len(), 2);
    }
    #[test]
    fn decode_callback_refusal_is_not_changed_into_a_shape_failure() {
        let source: ArrayRef = std::sync::Arc::new(
            vec![Some("a")]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        );
        let error = decode(
            &source,
            &DataType::Utf8,
            &mut |_| {
                Err(ConstructionFailure::Control(
                    novarocks_type_contract::CompileControlError::Cancelled,
                ))
            },
            &mut |_| Ok(()),
            &mut |_| Ok(()),
        )
        .unwrap_err();
        assert_eq!(
            error,
            ConstructionFailure::Control(novarocks_type_contract::CompileControlError::Cancelled)
        );
    }
    #[test]
    fn slice_column_vector_requests_are_exact_and_separate_from_hydration_marker_work() {
        let mut requests = Vec::new();
        let mut opaque = 0;
        let mut work = 0;
        let model = LockedArrowInputConstructionModel;
        model
            .observe(
                ArrayNormalizationStep::CreateChunkColumns { channels: 5 },
                &mut |layout| {
                    requests.push(layout);
                    Ok(())
                },
                &mut |_| {
                    opaque += 1;
                    Ok(())
                },
                &mut |units| {
                    work += units;
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(requests, vec![Layout::array::<ArrayRef>(5).unwrap()]);
        assert_eq!(opaque, 0);
        assert_eq!(work, 5);
        requests.clear();
        model
            .observe(
                ArrayNormalizationStep::RebuildChunkScaffolds { channels: 5 },
                &mut |layout| {
                    requests.push(layout);
                    Ok(())
                },
                &mut |_| {
                    opaque += 1;
                    Ok(())
                },
                &mut |units| {
                    work += units;
                    Ok(())
                },
            )
            .unwrap();
        assert!(requests.is_empty());
        assert_eq!(opaque, 0);
        assert_eq!(work, 10);
        model
            .observe(
                ArrayNormalizationStep::CreateRecordBatchWrapper,
                &mut |layout| {
                    requests.push(layout);
                    Ok(())
                },
                &mut |_| {
                    opaque += 1;
                    Ok(())
                },
                &mut |units| {
                    work += units;
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(
            requests,
            vec![
                novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<
                    arrow::record_batch::RecordBatch,
                >())
                .unwrap()
            ]
        );
        assert_eq!(opaque, 0);
    }
}
