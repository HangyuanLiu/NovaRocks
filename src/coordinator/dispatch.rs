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

use crate::common::types::UniqueId;
use crate::exec::chunk::{Chunk, ChunkSchemaRef};

/// Outcome of a single `fetch_result` call.
pub enum FetchOutcome {
    /// A result chunk is available.
    Ready(Chunk),
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
        submission: FragmentSubmission,
    ) -> Result<(), String>;

    /// Poll for the next result chunk from the root fragment on the given backend.
    fn fetch_result(
        &self,
        backend_idx: usize,
        finst_id: UniqueId,
        max_wait_ms: i64,
        expected_chunk_schema: Option<&ChunkSchemaRef>,
    ) -> Result<FetchOutcome, String>;

    /// Cancel all listed fragment instances on the given backend. Idempotent.
    fn cancel_fragments(&self, backend_idx: usize, finst_ids: &[UniqueId]);

    /// Number of backends this dispatcher can route to.
    fn backend_count(&self) -> usize;

    /// Whether non-write fragments need final status reports back to the coordinator.
    fn needs_fragment_status_report(&self) -> bool {
        false
    }
}

pub(crate) struct FragmentSubmission {
    plan: crate::proto::plan::PlanFragment,
    instance_params: crate::proto::novarocks::InstanceParams,
}

impl FragmentSubmission {
    pub(crate) fn new(
        plan: crate::proto::plan::PlanFragment,
        instance_params: crate::proto::novarocks::InstanceParams,
    ) -> Self {
        Self {
            plan,
            instance_params,
        }
    }

    #[cfg(test)]
    pub(crate) fn plan_for_test(&self) -> &crate::proto::plan::PlanFragment {
        &self.plan
    }

    pub(crate) fn fragment_id(&self) -> u32 {
        self.plan.fragment_id
    }

    pub(crate) fn query_id(&self) -> Result<UniqueId, String> {
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

    pub(crate) fn fragment_instance_id(&self) -> Result<UniqueId, String> {
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

    #[test]
    fn fragment_submission_requires_native_plan_and_instance_params() {
        let submission = FragmentSubmission::new(
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

        assert_eq!(
            submission.query_id().expect("native query id"),
            UniqueId { hi: 7, lo: 9 }
        );
        assert_eq!(
            submission.fragment_instance_id().expect("native finst id"),
            UniqueId { hi: 7, lo: 11 }
        );
        let (plan, instance_params) = submission.into_parts();
        assert_eq!(plan.fragment_id, 9);
        assert_eq!(instance_params.backend_num, 3);
    }
}
