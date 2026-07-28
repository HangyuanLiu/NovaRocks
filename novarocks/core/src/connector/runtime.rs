// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the License
// at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Core-owned runtime adapters for connector-provided batches.
//!
//! Providers own handle codecs and `ConnectorBatchReader`; this module is the
//! sole conversion boundary into core's `Chunk` execution representation.

use std::collections::BTreeSet;
use std::sync::{Arc, RwLock};

use novarocks_spi::connector::{
    ConnectorBatchReader, ConnectorInstance, ConnectorOpenReaderRequest, ConnectorSplit,
};

use crate::common::ids::SlotId;
use crate::connector::host::ConnectorInstanceLease;
use crate::connector::iceberg::equality_delete::EqualityDeleteSet;
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::node::ExecResult;
use crate::exec::node::scan::{
    BoundScanRanges, IncrementalScanRange, RuntimeFilterContext, ScanMorsel,
    ScanMorselPruneDecision, ScanMorsels, ScanOp, ScanSource,
};
use crate::fs::scan_context::FileScanRange;
use crate::runtime::profile::RuntimeProfile;
use crate::runtime_filter::exec::ordered_range_predicate::NativeOrderedRangePredicate;

pub(crate) struct ConnectorBatchReaderIter {
    reader: Option<Box<dyn ConnectorBatchReader>>,
    chunk_schema: ChunkSchemaRef,
    finished: bool,
}

/// Provider-private conversion result for FE ranges that arrive after a scan
/// source has already been scheduled. The generic runtime never decodes the
/// range or split payload.
#[derive(Clone)]
pub(crate) enum ConnectorSplitAppend {
    Plain {
        splits: Vec<ConnectorSplit>,
        has_more: bool,
    },
    Scheduled {
        scheduled: Vec<ConnectorScheduledSplit>,
        has_more: bool,
    },
}

impl ConnectorSplitAppend {
    fn scheduled(&self) -> (Vec<ConnectorScheduledSplit>, bool) {
        match self {
            Self::Plain { splits, has_more } => (plain_scheduled(splits.clone()), *has_more),
            Self::Scheduled {
                scheduled,
                has_more,
            } => (scheduled.clone(), *has_more),
        }
    }

    pub(crate) fn scheduled_file_splits(&self) -> Option<&[ConnectorScheduledSplit]> {
        match self {
            Self::Plain { .. } => None,
            Self::Scheduled { scheduled, .. } => Some(scheduled),
        }
    }
}

/// A queued provider split plus the optional file metadata that remains owned
/// by core execution. Provider payloads never contain the sidecar.
#[derive(Clone)]
pub(crate) struct ConnectorScheduledSplit {
    split: ConnectorSplit,
    file_range: Option<FileScanRange>,
}

impl ConnectorScheduledSplit {
    pub(crate) fn plain(split: ConnectorSplit) -> Self {
        Self {
            split,
            file_range: None,
        }
    }

    pub(crate) fn file(split: ConnectorSplit, file_range: FileScanRange) -> Self {
        Self {
            split,
            file_range: Some(file_range),
        }
    }

    pub(crate) fn split(&self) -> &ConnectorSplit {
        &self.split
    }

    pub(crate) fn file_range(&self) -> Option<&FileScanRange> {
        self.file_range.as_ref()
    }

    fn morsel(&self, index: usize) -> ScanMorsel {
        match &self.file_range {
            Some(range) => ScanMorsel::ConnectorFileSplit {
                index,
                range: range.clone(),
            },
            None => ScanMorsel::ConnectorSplit { index },
        }
    }
}

/// Core-internal adapter used only by compat/native transport adapters that
/// receive incremental ranges. It deliberately is not part of the SPI trait.
pub(crate) trait IncrementalConnectorSplitAdapter: Send + Sync {
    fn prepare_incremental_ranges(
        &self,
        ranges: &[IncrementalScanRange],
    ) -> Result<ConnectorSplitAppend, String>;

    /// Commits provider-private state only after the generic queue has
    /// validated every opaque split and its core sidecar. Implementations must
    /// reject a stale or malformed prepared append without partial mutation.
    fn commit_incremental_ranges(&self, _append: &ConnectorSplitAppend) -> Result<(), String> {
        Ok(())
    }
}

/// Core-private provider hook for opening delete-file data. The core runner
/// retains all filtering semantics; a provider only resolves its storage
/// credentials and decodes its delete files.
pub(crate) trait ConnectorReadAuxiliary: Send + Sync {
    fn load_iceberg_position_deletes(
        &self,
        range: &FileScanRange,
    ) -> Result<Option<roaring::RoaringTreemap>, String>;

    fn load_iceberg_equality_deletes(
        &self,
        range: &FileScanRange,
    ) -> Result<Option<Vec<EqualityDeleteSet>>, String>;
}

pub(crate) trait ConnectorReadCoreFacet: Send + Sync {
    fn flush_morsel_materialization_profile(&self, _profile: &RuntimeProfile) {}

    fn late_prune_morsel_with_ordered_predicate(
        &self,
        _morsel: &ScanMorsel,
        _slot_id: SlotId,
        _predicate: &NativeOrderedRangePredicate,
    ) -> Result<ScanMorselPruneDecision, String> {
        Ok(ScanMorselPruneDecision::Keep)
    }
}

struct ConnectorSplitState {
    scheduled: Vec<ConnectorScheduledSplit>,
    split_ids: BTreeSet<String>,
    total_payload_bytes: usize,
    has_more: bool,
}

impl ConnectorSplitState {
    fn new(scheduled: Vec<ConnectorScheduledSplit>, has_more: bool) -> Self {
        let split_ids = scheduled
            .iter()
            .map(|scheduled| scheduled.split.split_id().to_string())
            .collect();
        let total_payload_bytes = scheduled
            .iter()
            .map(|scheduled| scheduled.split.payload().len())
            .sum();
        Self {
            scheduled,
            split_ids,
            total_payload_bytes,
            has_more,
        }
    }
}

fn plain_scheduled(splits: Vec<ConnectorSplit>) -> Vec<ConnectorScheduledSplit> {
    splits
        .into_iter()
        .map(ConnectorScheduledSplit::plain)
        .collect()
}

impl ConnectorBatchReaderIter {
    pub(crate) fn new(reader: Box<dyn ConnectorBatchReader>, chunk_schema: ChunkSchemaRef) -> Self {
        Self {
            reader: Some(reader),
            chunk_schema,
            finished: false,
        }
    }

    fn close(&mut self) -> Result<(), String> {
        self.reader
            .take()
            .map(|mut reader| reader.close().map_err(|error| error.to_string()))
            .transpose()
            .map(|_| ())
    }

    fn finish_with_primary_error(&mut self, primary: String) -> ExecResult {
        self.finished = true;
        match self.close() {
            Ok(()) => Err(primary),
            Err(cleanup) => Err(format!("{primary} (cleanup: {cleanup})")),
        }
    }
}

impl Iterator for ConnectorBatchReaderIter {
    type Item = ExecResult;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let next_batch = self
            .reader
            .as_mut()
            .expect("connector reader must exist before end of stream")
            .next_batch();
        match next_batch {
            Ok(Some(batch)) => Some(
                Chunk::try_new_with_chunk_schema(batch, self.chunk_schema.clone())
                    .map_err(|error| error.to_string())
                    .or_else(|error| self.finish_with_primary_error(error)),
            ),
            Ok(None) => {
                self.finished = true;
                self.close().err().map(Err)
            }
            Err(error) => Some(self.finish_with_primary_error(error.to_string())),
        }
    }
}

impl Drop for ConnectorBatchReaderIter {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.close();
            self.finished = true;
        }
    }
}

/// A generic physical source for one already-assigned SPI split.
///
/// The source owns no provider-specific type.  Wire decoders resolve the
/// opaque split to its typed host instance, while core owns scheduling and
/// adapts the returned Arrow batches into `Chunk`s.
pub(crate) struct ConnectorReadScanSource {
    instance: Arc<ConnectorInstance>,
    splits: Arc<RwLock<ConnectorSplitState>>,
    request: ConnectorOpenReaderRequest,
    chunk_schema: ChunkSchemaRef,
    lifecycle: Option<Arc<ConnectorInstanceLease>>,
    incremental: Option<Arc<dyn IncrementalConnectorSplitAdapter>>,
    auxiliary: Option<Arc<dyn ConnectorReadAuxiliary>>,
    facet: Option<Arc<dyn ConnectorReadCoreFacet>>,
}

impl ConnectorReadScanSource {
    pub(crate) fn new(
        instance: Arc<ConnectorInstance>,
        splits: Vec<ConnectorSplit>,
        request: ConnectorOpenReaderRequest,
        chunk_schema: ChunkSchemaRef,
    ) -> Self {
        Self {
            instance,
            splits: Arc::new(RwLock::new(ConnectorSplitState::new(
                plain_scheduled(splits),
                false,
            ))),
            request,
            chunk_schema,
            lifecycle: None,
            incremental: None,
            auxiliary: None,
            facet: None,
        }
    }

    pub(crate) fn new_ephemeral(
        instance: Arc<ConnectorInstance>,
        splits: Vec<ConnectorSplit>,
        request: ConnectorOpenReaderRequest,
        chunk_schema: ChunkSchemaRef,
        lifecycle: Arc<ConnectorInstanceLease>,
    ) -> Self {
        Self {
            instance,
            splits: Arc::new(RwLock::new(ConnectorSplitState::new(
                plain_scheduled(splits),
                false,
            ))),
            request,
            chunk_schema,
            lifecycle: Some(lifecycle),
            incremental: None,
            auxiliary: None,
            facet: None,
        }
    }

    pub(crate) fn new_with_incremental(
        instance: Arc<ConnectorInstance>,
        splits: Vec<ConnectorSplit>,
        request: ConnectorOpenReaderRequest,
        chunk_schema: ChunkSchemaRef,
        incremental: Arc<dyn IncrementalConnectorSplitAdapter>,
        has_more: bool,
    ) -> Self {
        Self {
            instance,
            splits: Arc::new(RwLock::new(ConnectorSplitState::new(
                plain_scheduled(splits),
                has_more,
            ))),
            request,
            chunk_schema,
            lifecycle: None,
            incremental: Some(incremental),
            auxiliary: None,
            facet: None,
        }
    }

    pub(crate) fn new_scheduled(
        instance: Arc<ConnectorInstance>,
        scheduled: Vec<ConnectorScheduledSplit>,
        request: ConnectorOpenReaderRequest,
        chunk_schema: ChunkSchemaRef,
    ) -> Self {
        Self {
            instance,
            splits: Arc::new(RwLock::new(ConnectorSplitState::new(scheduled, false))),
            request,
            chunk_schema,
            lifecycle: None,
            incremental: None,
            auxiliary: None,
            facet: None,
        }
    }

    pub(crate) fn new_scheduled_ephemeral(
        instance: Arc<ConnectorInstance>,
        scheduled: Vec<ConnectorScheduledSplit>,
        request: ConnectorOpenReaderRequest,
        chunk_schema: ChunkSchemaRef,
        lifecycle: Arc<ConnectorInstanceLease>,
        auxiliary: Option<Arc<dyn ConnectorReadAuxiliary>>,
    ) -> Self {
        Self::new_scheduled_ephemeral_with_incremental(
            instance,
            scheduled,
            request,
            chunk_schema,
            lifecycle,
            None,
            false,
            auxiliary,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_scheduled_ephemeral_with_incremental(
        instance: Arc<ConnectorInstance>,
        scheduled: Vec<ConnectorScheduledSplit>,
        request: ConnectorOpenReaderRequest,
        chunk_schema: ChunkSchemaRef,
        lifecycle: Arc<ConnectorInstanceLease>,
        incremental: Option<Arc<dyn IncrementalConnectorSplitAdapter>>,
        has_more: bool,
        auxiliary: Option<Arc<dyn ConnectorReadAuxiliary>>,
    ) -> Self {
        Self {
            instance,
            splits: Arc::new(RwLock::new(ConnectorSplitState::new(scheduled, has_more))),
            request,
            chunk_schema,
            lifecycle: Some(lifecycle),
            incremental,
            auxiliary,
            facet: None,
        }
    }

    pub(crate) fn new_scheduled_with_auxiliary(
        instance: Arc<ConnectorInstance>,
        scheduled: Vec<ConnectorScheduledSplit>,
        request: ConnectorOpenReaderRequest,
        chunk_schema: ChunkSchemaRef,
        auxiliary: Arc<dyn ConnectorReadAuxiliary>,
    ) -> Self {
        Self {
            instance,
            splits: Arc::new(RwLock::new(ConnectorSplitState::new(scheduled, false))),
            request,
            chunk_schema,
            lifecycle: None,
            incremental: None,
            auxiliary: Some(auxiliary),
            facet: None,
        }
    }

    pub(crate) fn with_core_facet(mut self, facet: Arc<dyn ConnectorReadCoreFacet>) -> Self {
        self.facet = Some(facet);
        self
    }
}

impl ScanSource for ConnectorReadScanSource {
    fn bind(&self, ranges: BoundScanRanges) -> Result<Arc<dyn ScanOp>, String> {
        if !matches!(ranges, BoundScanRanges::None) {
            return Err("SPI connector scan source requires an empty range binding".to_string());
        }
        Ok(Arc::new(ConnectorReadScanOp {
            instance: Arc::clone(&self.instance),
            splits: Arc::clone(&self.splits),
            request: self.request.clone(),
            chunk_schema: Arc::clone(&self.chunk_schema),
            _lifecycle: self.lifecycle.clone(),
            incremental: self.incremental.clone(),
            auxiliary: self.auxiliary.clone(),
            facet: self.facet.clone(),
        }))
    }
}

struct ConnectorReadScanOp {
    instance: Arc<ConnectorInstance>,
    splits: Arc<RwLock<ConnectorSplitState>>,
    request: ConnectorOpenReaderRequest,
    chunk_schema: ChunkSchemaRef,
    // Keep ephemeral provider credentials registered until every scan op and
    // reader derived from this source has drained.
    _lifecycle: Option<Arc<ConnectorInstanceLease>>,
    incremental: Option<Arc<dyn IncrementalConnectorSplitAdapter>>,
    auxiliary: Option<Arc<dyn ConnectorReadAuxiliary>>,
    facet: Option<Arc<dyn ConnectorReadCoreFacet>>,
}

impl ScanOp for ConnectorReadScanOp {
    fn flush_morsel_materialization_profile(&self, profile: &RuntimeProfile) {
        if let Some(facet) = &self.facet {
            facet.flush_morsel_materialization_profile(profile);
        }
    }

    fn late_prune_morsel_with_ordered_predicate(
        &self,
        morsel: &ScanMorsel,
        slot_id: SlotId,
        predicate: &NativeOrderedRangePredicate,
    ) -> Result<ScanMorselPruneDecision, String> {
        self.facet
            .as_ref()
            .map(|facet| facet.late_prune_morsel_with_ordered_predicate(morsel, slot_id, predicate))
            .unwrap_or(Ok(ScanMorselPruneDecision::Keep))
    }

    fn execute_iter(
        &self,
        morsel: ScanMorsel,
        _profile: Option<RuntimeProfile>,
        _runtime_filters: Option<&RuntimeFilterContext>,
    ) -> Result<crate::exec::node::BoxedExecIter, String> {
        let index = match morsel {
            ScanMorsel::ConnectorSplit { index } | ScanMorsel::ConnectorFileSplit { index, .. } => {
                index
            }
            _ => {
                return Err("SPI connector scan received an unexpected morsel".to_string());
            }
        };
        let split = self
            .splits
            .read()
            .map_err(|_| "SPI connector split state lock poisoned".to_string())?
            .scheduled
            .get(index)
            .map(|scheduled| scheduled.split.clone())
            .ok_or_else(|| format!("SPI connector scan split index {index} is out of bounds"))?;
        let reader = self
            .instance
            .read()
            .open_reader(&split, self.request.clone())
            .map_err(|error| error.to_string())?;
        Ok(Box::new(ConnectorBatchReaderIter::new(
            reader,
            Arc::clone(&self.chunk_schema),
        )))
    }

    fn build_morsels(&self) -> Result<ScanMorsels, String> {
        let state = self
            .splits
            .read()
            .map_err(|_| "SPI connector split state lock poisoned".to_string())?;
        Ok(ScanMorsels::new(
            state
                .scheduled
                .iter()
                .enumerate()
                .map(|(index, scheduled)| scheduled.morsel(index))
                .collect(),
            state.has_more,
        ))
    }

    fn load_iceberg_position_deletes(
        &self,
        morsel: &ScanMorsel,
    ) -> Result<Option<roaring::RoaringTreemap>, String> {
        let Some(range) = morsel.file_range() else {
            return Ok(None);
        };
        self.auxiliary
            .as_ref()
            .map(|auxiliary| auxiliary.load_iceberg_position_deletes(&range))
            .unwrap_or(Ok(None))
    }

    fn load_iceberg_equality_deletes(
        &self,
        morsel: &ScanMorsel,
    ) -> Result<Option<Vec<EqualityDeleteSet>>, String> {
        let Some(range) = morsel.file_range() else {
            return Ok(None);
        };
        self.auxiliary
            .as_ref()
            .map(|auxiliary| auxiliary.load_iceberg_equality_deletes(&range))
            .unwrap_or(Ok(None))
    }

    fn supports_incremental_scan_ranges(&self) -> bool {
        self.incremental.is_some()
    }

    fn build_incremental_morsels(
        &self,
        ranges: &[IncrementalScanRange],
    ) -> Result<ScanMorsels, String> {
        let adapter = self
            .incremental
            .as_ref()
            .ok_or_else(|| "SPI connector scan does not support incremental ranges".to_string())?;
        let mut state = self
            .splits
            .write()
            .map_err(|_| "SPI connector split state lock poisoned".to_string())?;
        if !state.has_more {
            return Err("SPI connector split queue is closed".to_string());
        }
        let append = adapter.prepare_incremental_ranges(ranges)?;
        let (appended, has_more) = append.scheduled();
        let expected_owner = &self.instance.descriptor().instance_id;
        let start = state.scheduled.len();
        let mut appended_ids = BTreeSet::new();
        let append_payload_bytes = appended.iter().try_fold(0usize, |total, scheduled| {
            let split = &scheduled.split;
            if split.owner() != expected_owner {
                return Err(
                    "incremental connector split owner does not match its instance".to_string(),
                );
            }
            if split.payload().len() > self.request.context.max_handle_payload_bytes() {
                return Err(
                    "incremental connector split payload exceeds its handle budget".to_string(),
                );
            }
            if state.split_ids.contains(split.split_id()) {
                return Err(format!(
                    "incremental connector split ID `{}` already exists",
                    split.split_id()
                ));
            }
            if !appended_ids.insert(split.split_id().to_string()) {
                return Err(format!(
                    "incremental connector split ID `{}` is duplicated in one append",
                    split.split_id()
                ));
            }
            total
                .checked_add(split.payload().len())
                .ok_or_else(|| "incremental connector split payload total overflowed".to_string())
        })?;
        let total_payload_bytes = state
            .total_payload_bytes
            .checked_add(append_payload_bytes)
            .ok_or_else(|| "incremental connector split payload total overflowed".to_string())?;
        if total_payload_bytes > self.request.context.max_total_payload_bytes() {
            return Err(
                "incremental connector split payloads exceed their total budget".to_string(),
            );
        }
        adapter.commit_incremental_ranges(&append)?;
        for scheduled in appended {
            state
                .split_ids
                .insert(scheduled.split.split_id().to_string());
            state.scheduled.push(scheduled);
        }
        state.total_payload_bytes = total_payload_bytes;
        state.has_more = has_more;
        let end = state.scheduled.len();
        Ok(ScanMorsels::new(
            (start..end)
                .map(|index| state.scheduled[index].morsel(index))
                .collect(),
            state.has_more,
        ))
    }
}
