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
use crate::query_execution::lifecycle::QueryExecutionId;

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

/// Fragment dispatcher trait.
///
/// Implementations choose where and how fragments run. The coordinator calls
/// `submit_fragment` for each fragment, then polls `fetch_result` for the root
/// fragment instance until `Eof` or `Err`.
pub trait FragmentDispatcher: Send + Sync + 'static {
    /// Submit a fragment for asynchronous execution to the given backend.
    fn submit_fragment(
        &self,
        backend_idx: usize,
        submission: NativeFragmentEnvelope,
    ) -> Result<(), String>;

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

/// Build the production gRPC fragment dispatcher from one explicit immutable
/// backend snapshot.
pub fn new_grpc_fragment_dispatcher(
    backends: &[(usize, SocketAddr)],
) -> Result<Arc<dyn FragmentDispatcher>, String> {
    Ok(Arc::new(
        crate::service::grpc_fragment_dispatcher::RemoteDispatcher::new_with_backend_ids(backends)?,
    ))
}

pub struct NativeFragmentEnvelope {
    execution_id: QueryExecutionId,
    plan: crate::proto::plan::PlanFragment,
    instance_params: crate::proto::novarocks::InstanceParams,
}

impl NativeFragmentEnvelope {
    pub(crate) fn new(
        execution_id: QueryExecutionId,
        plan: crate::proto::plan::PlanFragment,
        instance_params: crate::proto::novarocks::InstanceParams,
    ) -> Self {
        Self {
            execution_id,
            plan,
            instance_params,
        }
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    #[cfg(test)]
    pub(crate) fn plan_for_test(&self) -> &crate::proto::plan::PlanFragment {
        &self.plan
    }

    #[cfg(test)]
    pub(crate) fn instance_params_for_test(&self) -> &crate::proto::novarocks::InstanceParams {
        &self.instance_params
    }

    pub fn fragment_id(&self) -> u32 {
        self.plan.fragment_id
    }

    pub fn query_id(&self) -> Result<UniqueId, String> {
        let id = self
            .instance_params
            .query_id
            .as_ref()
            .ok_or_else(|| "fragment submission missing query_id".to_string())?;
        Ok(UniqueId {
            hi: id.hi,
            lo: id.lo,
        })
    }

    pub fn fragment_instance_id(&self) -> Result<UniqueId, String> {
        let id = self
            .instance_params
            .fragment_instance_id
            .as_ref()
            .ok_or_else(|| "fragment submission missing fragment_instance_id".to_string())?;
        Ok(UniqueId {
            hi: id.hi,
            lo: id.lo,
        })
    }

    pub fn has_report_endpoint(&self) -> bool {
        self.instance_params.report_endpoint.is_some()
    }

    pub fn uses_typed_result_sink(&self) -> bool {
        self.instance_params.typed_result_sink
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        crate::proto::plan::PlanFragment,
        crate::proto::novarocks::InstanceParams,
    ) {
        (self.plan, self.instance_params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::common::UniqueId as ProtoUniqueId;
    use crate::query_execution::lifecycle::{AttemptId, QueryExecutionId};

    #[test]
    fn native_fragment_envelope_preserves_native_plan_and_instance_params() {
        let envelope = NativeFragmentEnvelope::new(
            QueryExecutionId::new(
                QueryId::new(7, 9),
                AttemptId::new(3).expect("nonzero attempt"),
            )
            .expect("valid execution id"),
            crate::proto::plan::PlanFragment {
                fragment_id: 9,
                ..Default::default()
            },
            crate::proto::novarocks::InstanceParams {
                query_id: Some(ProtoUniqueId { hi: 7, lo: 9 }),
                fragment_instance_id: Some(ProtoUniqueId { hi: 7, lo: 11 }),
                backend_num: 3,
                ..Default::default()
            },
        );

        assert_eq!(envelope.execution_id().attempt_id().get(), 3);
        assert_eq!(
            envelope.query_id().expect("native query id"),
            UniqueId { hi: 7, lo: 9 }
        );
        assert_eq!(
            envelope.fragment_instance_id().expect("native finst id"),
            UniqueId { hi: 7, lo: 11 }
        );
        let (plan, instance_params) = envelope.into_parts();
        assert_eq!(plan.fragment_id, 9);
        assert_eq!(instance_params.backend_num, 3);
    }
}
