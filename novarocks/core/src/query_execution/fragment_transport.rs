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

//! Fragment dispatcher port and native submission DTO.

use std::net::SocketAddr;
use std::sync::Arc;

use crate::common::types::UniqueId;
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::query_execution::contract::QueryId;

/// Opaque data-plane batch returned by a fragment dispatcher.
///
/// The execution-layer `Chunk` remains owned by core. Role crates may route
/// this value through the query-execution contract but cannot inspect or
/// manufacture execution batches.
pub struct FetchedQueryBatch {
    chunk: Chunk,
}

impl FetchedQueryBatch {
    pub(crate) fn new(chunk: Chunk) -> Self {
        Self { chunk }
    }

    pub(crate) fn into_chunk(self) -> Chunk {
        self.chunk
    }
}

/// Borrowed opaque view of the root fetch schema.
#[derive(Clone, Copy)]
pub struct ExpectedOutputSchemaView<'a> {
    schema: &'a ChunkSchemaRef,
}

impl<'a> ExpectedOutputSchemaView<'a> {
    pub(crate) const fn new(schema: &'a ChunkSchemaRef) -> Self {
        Self { schema }
    }

    pub(crate) const fn chunk_schema(self) -> &'a ChunkSchemaRef {
        self.schema
    }
}

/// Outcome of a single `fetch_result` call.
pub enum FetchOutcome {
    /// A result batch is available.
    Ready(FetchedQueryBatch),
    /// No chunk available yet; fragment is still running.
    NotReady,
    /// All chunks have been delivered; the root fragment is complete.
    Eof,
    /// Fragment execution failed.
    Err(String),
}

/// Result and cancellation transport for an already-running native query.
///
/// Query startup belongs exclusively to the query lifecycle Stage/Start
/// barrier. This port deliberately exposes only the residual fetch, cancel,
/// and reporting policy needed after that barrier has entered `Running`.
pub trait FragmentDispatcher: Send + Sync + 'static {
    /// Poll for the next result chunk from the root fragment on the given backend.
    fn fetch_result(
        &self,
        backend_idx: usize,
        finst_id: UniqueId,
        max_wait_ms: i64,
        expected_output_schema: Option<ExpectedOutputSchemaView<'_>>,
    ) -> Result<FetchOutcome, String>;

    /// Cancel all listed fragment instances owned by the given query on the backend. Idempotent.
    fn cancel_fragments(&self, backend_idx: usize, query_id: QueryId, finst_ids: &[UniqueId]);

    /// Number of backends this dispatcher can route to.
    fn backend_count(&self) -> usize;

    /// Whether non-write fragments need final status reports back to the coordinator.
    fn needs_fragment_status_report(&self) -> bool {
        false
    }
}

/// Build the production result/cancellation transport from one explicit
/// immutable backend snapshot.
pub fn new_grpc_fragment_dispatcher(
    backends: &[(usize, SocketAddr)],
) -> Result<Arc<dyn FragmentDispatcher>, String> {
    Ok(Arc::new(
        crate::service::grpc_fragment_dispatcher::RemoteDispatcher::new_with_backend_ids(backends)?,
    ))
}
