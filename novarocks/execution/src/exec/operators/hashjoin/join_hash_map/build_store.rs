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
//! Single build-side chunk store for hash join.

use crate::runtime::fragment::ExecutionFailure;
#[derive(Debug)]
struct JoinStoreContract(novarocks_failure_contract::BoundedDetail);
impl std::fmt::Display for JoinStoreContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}
impl std::error::Error for JoinStoreContract {}
impl novarocks_failure_contract::FailureProjection for JoinStoreContract {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            crate::runtime::fragment::ExecutionReason::JoinStoreContract.reason(),
        )
    }
}
fn contract(detail: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(JoinStoreContract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}

use std::sync::Arc;

use arrow::compute::concat_batches;

use crate::exec::chunk::{Chunk, ChunkBatchProjection, ChunkSchemaRef};
use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionOwner};
use crate::runtime::mem_tracker::MemTracker;
use novarocks_type_contract::ValueTypeStorageLifetime;

#[derive(Debug, Clone)]
pub(crate) struct BuildStore {
    chunk: BuildStoreChunkProjection,
}

/// An independently retained build-row view keeps the actual shared header.
/// The holder owns only its data and finite creator; no artifact or program.
#[derive(Clone, Debug)]
pub(crate) struct BuildStoreChunkProjection(Arc<BuildStoreChunkBacking>);
#[derive(Clone, Debug)]
struct BuildStoreChunkBacking {
    // Data exits before the actual header construction hold.
    chunk: Chunk,
    storage: ValueTypeStorageLifetime,
}
impl AsRef<Chunk> for BuildStoreChunkProjection {
    fn as_ref(&self) -> &Chunk {
        &self.0.chunk
    }
}
impl std::ops::Deref for BuildStoreChunkProjection {
    type Target = Chunk;
    fn deref(&self) -> &Chunk {
        &self.0.chunk
    }
}

#[allow(
    dead_code,
    reason = "Store inspection helpers are retained for hash-join integration coverage."
)]
impl BuildStore {
    pub(crate) fn new(chunk: Chunk) -> Self {
        // Explicit legacy expansion construction; this is not a runtime
        // capacity author or evidence for copied payload funding.
        Self {
            chunk: BuildStoreChunkProjection(Arc::new(BuildStoreChunkBacking {
                chunk,
                storage: ValueTypeStorageLifetime::pure(),
            })),
        }
    }

    /// Only the actual shared header is new here. The Chunk's own data/type
    /// lineage is moved intact; this does not certify whole payload coverage.
    pub(crate) fn new_with_header_owner(
        chunk: Chunk,
        owner: &dyn InputConstructionOwner,
    ) -> Result<Self, ConstructionFailure> {
        owner.request(
            novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<BuildStoreChunkBacking>(),
            )?,
        )?;
        Ok(Self {
            chunk: BuildStoreChunkProjection(Arc::new(BuildStoreChunkBacking {
                chunk,
                storage: owner.storage_lifetime(),
            })),
        })
    }
    pub(crate) fn chunk(&self) -> BuildStoreChunkProjection {
        self.chunk.clone()
    }

    pub(crate) fn len(&self) -> usize {
        self.chunk.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.chunk.is_empty()
    }

    pub(crate) fn transfer_to(&mut self, tracker: &Arc<MemTracker>) {
        // Original legacy transfer policy. Its pure clone author is explicit;
        // compiled publication uses the observed header transfer below.
        Arc::make_mut(&mut self.chunk.0).chunk.transfer_to(tracker);
    }

    pub(crate) fn transfer_to_with_header_owner(
        &mut self,
        tracker: &Arc<MemTracker>,
        owner: &dyn InputConstructionOwner,
    ) -> Result<(), ConstructionFailure> {
        if Arc::get_mut(&mut self.chunk.0).is_none() {
            owner.request(
                novarocks_type_contract::owned_resources::layout::arc_layout(
                    std::alloc::Layout::new::<BuildStoreChunkBacking>(),
                )?,
            )?;
            let backing = Arc::make_mut(&mut self.chunk.0);
            // The new header replaces that header's creation responsibility.
            // Immutable Chunk data/type references retain their own origins.
            backing.storage = owner.storage_lifetime();
        }
        Arc::get_mut(&mut self.chunk.0)
            .expect("unique observed build header")
            .chunk
            .transfer_to(tracker);
        Ok(())
    }
}

pub(crate) struct BuildStoreBuilder {
    schema: Option<ChunkSchemaRef>,
    batches: Vec<ChunkBatchProjection>,
    row_count: usize,
    // The table backing dies before its actual construction hold.
    table_storage: ValueTypeStorageLifetime,
}

impl BuildStoreBuilder {
    pub(crate) fn new() -> Self {
        Self {
            schema: None,
            batches: Vec::new(),
            row_count: 0,
            table_storage: ValueTypeStorageLifetime::pure(),
        }
    }

    #[cfg(test)]
    pub(crate) fn push_chunk(&mut self, chunk: &Chunk) -> Result<(), ExecutionFailure> {
        self.push_chunk_core(chunk, None)
    }
    pub(crate) fn push_chunk_owned(
        &mut self,
        chunk: &Chunk,
        owner: &dyn InputConstructionOwner,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        self.push_chunk_core(chunk, Some(owner))
    }
    fn push_chunk_core(
        &mut self,
        chunk: &Chunk,
        owner: Option<&dyn InputConstructionOwner>,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        if let Some(schema) = self.schema.as_ref() {
            validate_chunk_schema(schema, chunk)?;
        }
        let next_rows = self.row_count.checked_add(chunk.len()).ok_or_else(|| {
            ExecutionFailure::from_domain(JoinStoreRows {
                live: self.row_count,
                incoming: chunk.len(),
            })
        })?;
        if let Some(owner) = owner {
            let growth = novarocks_type_contract::owned_resources::vec::push_growth(&self.batches)
                .map_err(|cause| match cause {
                    novarocks_type_contract::ControlResourceError::Control(cause) => {
                        ConstructionFailure::Control(cause)
                    }
                    novarocks_type_contract::ControlResourceError::SourceModel(cause) => {
                        ConstructionFailure::InvalidSource(cause.to_owned())
                    }
                })?;
            if let Some(layout) = growth.requested_backing {
                owner.request(layout)?;
            }
            owner.observe(
                0,
                &[],
                crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units: 1 },
            )?;
            self.batches.push(chunk.batch_projection());
            if growth.requested_backing.is_some() {
                // The original amortized push freed/replaced the old table.
                self.table_storage = owner.storage_lifetime();
            }
        } else {
            self.batches.push(chunk.batch_projection());
        }
        if self.schema.is_none() {
            self.schema = Some(chunk.chunk_schema_ref());
        }
        self.row_count = next_rows;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn row_count(&self) -> usize {
        self.row_count
    }

    #[cfg(test)]
    pub(crate) fn finish(self) -> Result<Option<BuildStore>, ExecutionFailure> {
        self.finish_core(None)
    }
    pub(crate) fn finish_owned(
        self,
        owner: &dyn InputConstructionOwner,
    ) -> crate::runtime::fragment::ExecutionResult<Option<BuildStore>> {
        self.finish_core(Some(owner))
    }
    fn finish_core(
        self,
        owner: Option<&dyn InputConstructionOwner>,
    ) -> crate::runtime::fragment::ExecutionResult<Option<BuildStore>> {
        let Some(chunk_schema) = self.schema else {
            return Ok(None);
        };
        if self.batches.is_empty() {
            return Ok(None);
        }
        let chunk = if self.batches.len() == 1 {
            self.batches
                .into_iter()
                .next()
                .expect("one build batch")
                .into_chunk()
        } else {
            let arrow_schema = chunk_schema.arrow_schema_ref();
            let batch = concat_batches(
                &arrow_schema,
                self.batches.iter().map(ChunkBatchProjection::record_batch),
            )
            .map_err(|cause| {
                ExecutionFailure::from_domain(super::gather::GatherKernelFailure(cause))
            })?;
            if let Some(owner) = owner {
                // Original concat payload and its internal allocations are not
                // relabeled as this wrapper request. Preserve actual source
                // type creator holds without retaining source RecordBatches.
                let mut source_types = owner.storage_lifetime();
                for source in &self.batches {
                    source_types = source_types
                        .retaining_observed(&source.array_type_storage_lifetime(), |layout| {
                            owner.request(layout)
                        })?;
                }
                Chunk::try_new_typed_record_batch_owned_observed::<
                    super::gather::JoinGatherError,
                    _,
                    _,
                >(
                    batch,
                    chunk_schema,
                    source_types,
                    owner.storage_lifetime(),
                    |layout| owner.request(layout).map_err(Into::into),
                    |step| owner.observe(0, &[], step).map_err(Into::into),
                )
                .map_err(super::gather::JoinGatherError::into_execution)?
            } else {
                Chunk::try_new_with_chunk_schema(batch, chunk_schema)?
            }
        };
        Ok(Some(match owner {
            Some(owner) => BuildStore::new_with_header_owner(chunk, owner)?,
            None => BuildStore::new(chunk),
        }))
    }
}

fn validate_chunk_schema(expected: &ChunkSchemaRef, chunk: &Chunk) -> Result<(), ExecutionFailure> {
    let actual = chunk.chunk_schema();
    if expected.slots() == actual.slots() {
        return Ok(());
    }
    if expected.slots().len() != actual.slots().len() {
        return Err(contract(format_args!(
            "join build store schema slot count mismatch: expected={} actual={}",
            expected.slots().len(),
            actual.slots().len()
        )));
    }
    for (idx, (expected_slot, actual_slot)) in expected
        .slots()
        .iter()
        .zip(actual.slots().iter())
        .enumerate()
    {
        if expected_slot.slot_id() != actual_slot.slot_id() {
            return Err(contract(format_args!(
                "join build store schema slot mismatch at index {idx}: expected={} actual={}",
                expected_slot.slot_id(),
                actual_slot.slot_id()
            )));
        }
        if expected_slot.field() != actual_slot.field() {
            return Err(contract(format_args!(
                "join build store schema field mismatch at slot {}: expected={:?} actual={:?}",
                expected_slot.slot_id(),
                expected_slot.field(),
                actual_slot.field()
            )));
        }
    }
    Err(contract("join build store schema mismatch"))
}

#[derive(Debug)]
struct JoinStoreRows {
    live: usize,
    incoming: usize,
}
impl std::fmt::Display for JoinStoreRows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "join build store row capacity exceeded: live={} incoming={}",
            self.live, self.incoming
        )
    }
}
impl std::error::Error for JoinStoreRows {}
impl novarocks_failure_contract::FailureProjection for JoinStoreRows {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, ResourceDimension, ResourceFacts, ResourceScope,
        };
        FailureFact::new(
            FailureClass::Resource,
            crate::runtime::fragment::ExecutionReason::JoinStoreRows.reason(),
        )
        .with_resource(ResourceFacts {
            request_scope: Some(ResourceScope::Operator),
            constraint_scope: Some(ResourceScope::Operator),
            dimension: ResourceDimension::Positions,
            requested: u64::try_from(self.incoming).ok(),
            required_growth: u64::try_from(self.incoming).ok(),
            limit: Some(usize::MAX as u64),
            live: u64::try_from(self.live).ok(),
            committed: None,
        })
    }
}
#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};

    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::runtime::mem_tracker::MemTracker;
    use novarocks_types::SlotId;

    use super::*;

    fn one_column_chunk_with_slot(slot_id: SlotId, name: &str, values: Vec<i32>) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]));
        let array = Arc::new(Int32Array::from(values)) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot_id])
                .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn one_column_chunk(values: Vec<i32>) -> Chunk {
        one_column_chunk_with_slot(SlotId(1), "k", values)
    }

    #[test]
    fn build_store_builder_merges_chunks_into_single_chunk() {
        let mut builder = BuildStoreBuilder::new();

        builder
            .push_chunk(&one_column_chunk(vec![1, 2]))
            .expect("push");
        assert_eq!(builder.row_count(), 2);
        builder
            .push_chunk(&one_column_chunk(vec![3]))
            .expect("push");
        let store = builder.finish().expect("finish").expect("store");

        assert_eq!(store.len(), 3);
        assert!(!store.is_empty());
        assert_eq!(store.chunk().record_batch().num_columns(), 1);
        let chunk = store.chunk();
        let values = chunk
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 values");
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), 2);
        assert_eq!(values.value(2), 3);
    }

    #[test]
    fn build_store_builder_rejects_slot_schema_mismatch() {
        let mut builder = BuildStoreBuilder::new();

        builder
            .push_chunk(&one_column_chunk_with_slot(SlotId(1), "k", vec![1]))
            .expect("push first chunk");
        let err = builder
            .push_chunk(&one_column_chunk_with_slot(SlotId(2), "k", vec![2]))
            .expect_err("slot mismatch");

        assert!(
            err.detail()
                .contains("join build store schema slot mismatch")
        );
    }

    #[test]
    fn build_store_tracks_concatenated_batch_memory() {
        let mut builder = BuildStoreBuilder::new();

        builder
            .push_chunk(&one_column_chunk(vec![1, 2]))
            .expect("push");
        builder
            .push_chunk(&one_column_chunk(vec![3, 4]))
            .expect("push");
        let mut store = builder.finish().expect("finish").expect("store");
        let tracker = MemTracker::new_root("build-store-test");

        store.transfer_to(&tracker);

        assert!(tracker.current() > 0);
        drop(store);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn build_store_tracks_single_batch_memory() {
        let mut builder = BuildStoreBuilder::new();

        builder
            .push_chunk(&one_column_chunk(vec![1, 2]))
            .expect("push");
        let mut store = builder.finish().expect("finish").expect("store");
        let tracker = MemTracker::new_root("build-store-single-batch-test");

        store.transfer_to(&tracker);

        assert!(tracker.current() > 0);
        drop(store);
        assert_eq!(tracker.current(), 0);
    }

    fn actual_header_factory(
        tracker: Arc<MemTracker>,
    ) -> Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory> {
        crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker,
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        )
        .unwrap()
    }
    fn typed_header_source(
        values: Vec<i32>,
        factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
    ) -> Chunk {
        use crate::exec::chunk::ChunkSlotSchema;
        use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
        // The fixed declaration and fixture array are ordinary source authors;
        // the actual runtime wrapper uses the real production creator.
        let ty = FunctionValueType::new(DataType::Int32, false);
        let slot = ChunkSlotSchema::try_new_typed(
            SlotId::new(1),
            Arc::new(Field::new("k", DataType::Int32, false)),
            ty.typed_binding().clone(),
            None,
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        let schema = Arc::new(
            ChunkSchema::try_new_typed(vec![slot], ValueTypeStorageLifetime::pure()).unwrap(),
        );
        let owner = factory.begin_typed_construction(&schema).unwrap();
        Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
            schema,
            vec![Arc::new(Int32Array::from(values.clone())) as ArrayRef],
            values.len(),
            owner.storage_lifetime(),
            |step| owner.observe(0, &[], step),
        )
        .unwrap()
    }
    fn int_values(chunk: &Chunk) -> Vec<i32> {
        let values = chunk
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        (0..values.len()).map(|row| values.value(row)).collect()
    }

    #[test]
    fn build_store_first_actual_table_request_refusal_preserves_rows_and_source() {
        let source_tracker = MemTracker::new_root("original build source wrapper");
        let source_factory = actual_header_factory(source_tracker.clone());
        let source = typed_header_source(vec![1, 2], source_factory.as_ref());
        let source_data = MemTracker::new_root("original source payload accounting");
        let mut source = source;
        source.transfer_to(&source_data);
        let source_bytes = source_data.current();
        let source_wrapper = source_tracker.current();
        let tracker = MemTracker::new_root("first actual build table request");
        let factory = actual_header_factory(tracker.clone());
        let owner = factory
            .begin_typed_construction(source.chunk_schema())
            .unwrap();
        let baseline = tracker.current();
        tracker.install_limit_once(baseline + 1).unwrap();
        let mut builder = BuildStoreBuilder::new();
        let refused = builder
            .push_chunk_owned(&source, owner.as_ref())
            .unwrap_err();
        assert!(matches!(
            refused.cause(),
            crate::runtime::fragment::ExecutionFailureCause::Construction(
                ConstructionFailure::Memory(_)
            )
        ));
        assert_eq!(builder.row_count(), 0);
        assert!(builder.batches.is_empty());
        assert!(builder.schema.is_none());
        assert_eq!(int_values(&source), vec![1, 2]);
        assert_eq!(source_data.current(), source_bytes);
        assert_eq!(source_tracker.current(), source_wrapper);
        assert_eq!(tracker.current(), baseline);
        drop(builder);
        drop(owner);
        drop(factory);
        assert_eq!(tracker.current(), 0);
        drop(source);
        drop(source_factory);
        assert_eq!(source_data.current(), 0);
        assert_eq!(source_tracker.current(), 0);
    }

    #[test]
    fn build_store_first_actual_header_request_refusal_preserves_original_source() {
        let source_tracker = MemTracker::new_root("original build source");
        let source_factory = actual_header_factory(source_tracker.clone());
        let mut source = typed_header_source(vec![1, 2], source_factory.as_ref());
        let source_data = MemTracker::new_root("original build source charge");
        source.transfer_to(&source_data);
        let original = source.batch_projection();
        let original_bytes = source_data.current();
        let original_wrapper = source_tracker.current();
        let tracker = MemTracker::new_root("first actual build header request");
        let factory = actual_header_factory(tracker.clone());
        let owner = factory
            .begin_typed_construction(source.chunk_schema())
            .unwrap();
        let baseline = tracker.current();
        tracker.install_limit_once(baseline + 1).unwrap();
        let refused = BuildStore::new_with_header_owner(source, owner.as_ref()).unwrap_err();
        assert!(matches!(refused, ConstructionFailure::Memory(_)));
        assert_eq!(int_values(&original.clone().into_chunk()), vec![1, 2]);
        assert_eq!(source_data.current(), original_bytes);
        assert_eq!(source_tracker.current(), original_wrapper);
        assert_eq!(tracker.current(), baseline);
        drop(owner);
        drop(factory);
        assert_eq!(tracker.current(), 0);
        drop(original);
        drop(source_factory);
        assert_eq!(source_data.current(), 0);
        assert_eq!(source_tracker.current(), 0);
    }

    #[test]
    fn independent_build_view_projection_keeps_only_actual_header_and_source_holds() {
        use crate::exec::operators::hashjoin::build_artifact::{BuildView, JoinBuildArtifact};
        use crate::exec::operators::hashjoin::build_requirements::{
            BuildComponentRequirements, LookupRequirement, MatchFlagRequirement,
            NullKeyRequirement, RowPayloadRequirement,
        };
        let source_tracker = MemTracker::new_root("independent original source creator");
        let source_factory = actual_header_factory(source_tracker.clone());
        let source_factory_weak = Arc::downgrade(&source_factory);
        let mut source = typed_header_source(vec![4, 5], source_factory.as_ref());
        let data_tracker = MemTracker::new_root("independent original source payload");
        source.transfer_to(&data_tracker);
        let source_bytes = data_tracker.current();
        let tracker = MemTracker::new_root("independent build header");
        let factory = actual_header_factory(tracker.clone());
        let weak_factory = Arc::downgrade(&factory);
        let owner = factory
            .begin_typed_construction(source.chunk_schema())
            .unwrap();
        let store = BuildStore::new_with_header_owner(source, owner.as_ref()).unwrap();
        let required = BuildComponentRequirements {
            lookup: LookupRequirement::NotNeeded,
            row_payload: RowPayloadRequirement::Required,
            match_flags: MatchFlagRequirement::NotNeeded,
            null_keys: NullKeyRequirement::NotNeeded,
        };
        let artifact = Arc::new(JoinBuildArtifact::new_native(
            required,
            Some(store),
            None,
            2,
            false,
            None,
        ));
        let weak_artifact = Arc::downgrade(&artifact);
        let view = BuildView::new(artifact.clone(), required).unwrap();
        let projection = view.build_chunk("independent original rows").unwrap();
        let late = projection.clone();
        drop(view);
        drop(artifact);
        drop(owner);
        drop(factory);
        drop(source_factory);
        assert!(weak_artifact.upgrade().is_none());
        assert!(weak_factory.upgrade().is_none());
        assert!(source_factory_weak.upgrade().is_none());
        assert_eq!(int_values(&projection), vec![4, 5]);
        assert_eq!(data_tracker.current(), source_bytes);
        assert!(source_tracker.current() > 0);
        assert!(tracker.current() > 0);
        drop(projection);
        assert!(tracker.current() > 0);
        drop(late);
        assert_eq!(tracker.current(), 0);
        assert_eq!(source_tracker.current(), 0);
        assert_eq!(data_tracker.current(), 0);
    }

    #[test]
    fn build_store_owned_headers_preserve_original_single_multi_rows_and_accounting() {
        for batches in [vec![vec![1, 2, 3, 4]], vec![vec![1, 2], vec![3, 4]]] {
            let tracker = MemTracker::new_root("actual build wrappers");
            let factory = actual_header_factory(tracker.clone());
            let mut original = BuildStoreBuilder::new();
            let mut owned = BuildStoreBuilder::new();
            for values in batches {
                original
                    .push_chunk(&one_column_chunk(values.clone()))
                    .unwrap();
                let source = typed_header_source(values, factory.as_ref());
                let owner = factory
                    .begin_typed_construction(source.chunk_schema())
                    .unwrap();
                owned.push_chunk_owned(&source, owner.as_ref()).unwrap();
            }
            let mut original = original.finish().unwrap().unwrap();
            let owned_schema = owned.schema.clone().unwrap();
            let owner = factory.begin_typed_construction(&owned_schema).unwrap();
            let mut owned = owned.finish_owned(owner.as_ref()).unwrap().unwrap();
            let old_payload = MemTracker::new_root("original payload policy");
            let new_payload = MemTracker::new_root("same owned payload policy");
            original.transfer_to(&old_payload);
            owned
                .transfer_to_with_header_owner(&new_payload, owner.as_ref())
                .unwrap();
            assert_eq!(int_values(&original.chunk()), vec![1, 2, 3, 4]);
            assert_eq!(int_values(&owned.chunk()), vec![1, 2, 3, 4]);
            assert_eq!(original.len(), owned.len());
            assert_eq!(new_payload.current(), old_payload.current());
            assert!(old_payload.current() > 0);
            drop(original);
            drop(owned);
            drop(owner);
            drop(owned_schema);
            drop(factory);
            assert_eq!(old_payload.current(), 0);
            assert_eq!(new_payload.current(), 0);
            assert_eq!(tracker.current(), 0);
        }
    }
}
