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
mod tests {
    use super::*;
    use arrow::array::ArrayRef;
    #[test]
    fn value_publication_observes_only_actual_array_and_private_buffer_owners() {
        use novarocks_value_arrays::{ByteLayout, control::ValueStructure};
        let model = LockedArrowInputConstructionModel;
        let mut requests = Vec::new();
        let mut opaque = Vec::new();
        model
            .observe(
                ArrayNormalizationStep::ValueStructure {
                    path: &[],
                    event: ValueStructure::ByteArray {
                        layout: ByteLayout::Utf8,
                        new_buffer_owners: 2,
                    },
                },
                &mut |layout| {
                    requests.push(layout);
                    Ok(())
                },
                &mut |bound| {
                    opaque.push(bound);
                    Ok(())
                },
                &mut |_| Ok(()),
            )
            .unwrap();
        assert_eq!(
            requests,
            vec![
                novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<
                    arrow::array::StringArray,
                >())
                .unwrap()
            ]
        );
        assert_eq!(opaque.len(), 1);
        assert_eq!(opaque[0].requests_upper, 2);
        assert_eq!(
            opaque[0].bytes_upper,
            2 * novarocks_functions::array_projection::ARROW_BUFFER_OWNER_METADATA_BOUND
        );
        let original =
            ConstructionFailure::Control(novarocks_type_contract::CompileControlError::Cancelled);
        let refusal = model
            .observe(
                ArrayNormalizationStep::ValueStructure {
                    path: &[],
                    event: ValueStructure::ByteArray {
                        layout: ByteLayout::Utf8View,
                        new_buffer_owners: 1,
                    },
                },
                &mut |_| Err(original.clone()),
                &mut |_| panic!("first refusal stops later observation"),
                &mut |_| Ok(()),
            )
            .unwrap_err();
        assert_eq!(refusal, original);
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
