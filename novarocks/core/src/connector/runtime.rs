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

use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorBatchReader, ConnectorInstance, ConnectorOpenReaderRequest, ConnectorSplit,
};

use crate::connector::host::ConnectorInstanceLease;
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::node::ExecResult;
use crate::exec::node::scan::{
    BoundScanRanges, RuntimeFilterContext, ScanMorsel, ScanMorsels, ScanOp, ScanSource,
};
use crate::runtime::profile::RuntimeProfile;

pub(crate) struct ConnectorBatchReaderIter {
    reader: Option<Box<dyn ConnectorBatchReader>>,
    chunk_schema: ChunkSchemaRef,
    finished: bool,
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
    splits: Vec<ConnectorSplit>,
    request: ConnectorOpenReaderRequest,
    chunk_schema: ChunkSchemaRef,
    lifecycle: Option<Arc<ConnectorInstanceLease>>,
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
            splits,
            request,
            chunk_schema,
            lifecycle: None,
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
            splits,
            request,
            chunk_schema,
            lifecycle: Some(lifecycle),
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
            splits: self.splits.clone(),
            request: self.request.clone(),
            chunk_schema: Arc::clone(&self.chunk_schema),
            _lifecycle: self.lifecycle.clone(),
        }))
    }
}

struct ConnectorReadScanOp {
    instance: Arc<ConnectorInstance>,
    splits: Vec<ConnectorSplit>,
    request: ConnectorOpenReaderRequest,
    chunk_schema: ChunkSchemaRef,
    // Keep ephemeral provider credentials registered until every scan op and
    // reader derived from this source has drained.
    _lifecycle: Option<Arc<ConnectorInstanceLease>>,
}

impl ScanOp for ConnectorReadScanOp {
    fn execute_iter(
        &self,
        morsel: ScanMorsel,
        _profile: Option<RuntimeProfile>,
        _runtime_filters: Option<&RuntimeFilterContext>,
    ) -> Result<crate::exec::node::BoxedExecIter, String> {
        let ScanMorsel::ConnectorSplit { index } = morsel else {
            return Err("SPI connector scan received an unexpected morsel".to_string());
        };
        let split = self
            .splits
            .get(index)
            .ok_or_else(|| format!("SPI connector scan split index {index} is out of bounds"))?;
        let reader = self
            .instance
            .read()
            .open_reader(split, self.request.clone())
            .map_err(|error| error.to_string())?;
        Ok(Box::new(ConnectorBatchReaderIter::new(
            reader,
            Arc::clone(&self.chunk_schema),
        )))
    }

    fn build_morsels(&self) -> Result<ScanMorsels, String> {
        Ok(ScanMorsels::new(
            (0..self.splits.len())
                .map(|index| ScanMorsel::ConnectorSplit { index })
                .collect(),
            false,
        ))
    }
}
