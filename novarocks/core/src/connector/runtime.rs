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

use crate::connector::host::ConnectorInstanceLease;
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::node::ExecResult;
use crate::exec::node::scan::{
    BoundScanRanges, IncrementalScanRange, RuntimeFilterContext, ScanMorsel, ScanMorsels, ScanOp,
    ScanSource,
};
use crate::fs::scan_context::FileScanRange;
use crate::runtime::profile::RuntimeProfile;

pub(crate) struct ConnectorBatchReaderIter {
    reader: Option<Box<dyn ConnectorBatchReader>>,
    chunk_schema: ChunkSchemaRef,
    finished: bool,
}

/// Provider-private conversion result for FE ranges that arrive after a scan
/// source has already been scheduled. The generic runtime never decodes the
/// range or split payload.
pub(crate) struct ConnectorSplitAppend {
    pub(crate) splits: Vec<ConnectorSplit>,
    pub(crate) has_more: bool,
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
    fn append_incremental_ranges(
        &self,
        ranges: &[IncrementalScanRange],
    ) -> Result<ConnectorSplitAppend, String>;
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
        }
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
}

impl ScanOp for ConnectorReadScanOp {
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
        let appended = adapter.append_incremental_ranges(ranges)?;
        let expected_owner = &self.instance.descriptor().instance_id;
        let start = state.scheduled.len();
        let mut appended_ids = BTreeSet::new();
        let append_payload_bytes = appended.splits.iter().try_fold(0usize, |total, split| {
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
        for split in appended.splits {
            state.split_ids.insert(split.split_id().to_string());
            state.scheduled.push(ConnectorScheduledSplit::plain(split));
        }
        state.total_payload_bytes = total_payload_bytes;
        state.has_more = appended.has_more;
        let end = state.scheduled.len();
        Ok(ScanMorsels::new(
            (start..end)
                .map(|index| state.scheduled[index].morsel(index))
                .collect(),
            state.has_more,
        ))
    }
}
