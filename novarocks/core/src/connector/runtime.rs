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

use novarocks_spi::connector::ConnectorBatchReader;

use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::node::ExecResult;

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
