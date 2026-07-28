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

//! Feature-gated cross-crate contract fixtures.
//!
//! This module is absent from default and compat production dependency graphs.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use arrow::array::RecordBatchOptions;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::query_execution::cancellation::QueryCancellationView;
use crate::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, DistributedQueryIntent,
    DistributedQueryOutcome, DistributedQueryRequest, build_distributed_query_request,
};
use crate::query_execution::fragment_transport::FetchedQueryBatch;
use crate::query_execution::preparation::{PreparedFragmentRole, prepared_fragment_set_for_test};
use crate::query_execution::write::NativeExecutionReport;
use crate::runtime::query_options::QueryOptions;
use crate::sql::planner::distributed::{
    DataPartition, FragmentEdge, FragmentEdgeKind, FragmentStreamKind,
};

pub struct ResultContractFixture {
    request: DistributedQueryRequest,
    backends: Vec<(usize, SocketAddr)>,
    result_chunk: Chunk,
    cancellation: Arc<AtomicBool>,
}

impl ResultContractFixture {
    pub fn backends(&self) -> &[(usize, SocketAddr)] {
        &self.backends
    }

    pub fn result_batch(&self) -> FetchedQueryBatch {
        FetchedQueryBatch::new(self.result_chunk.clone())
    }

    pub fn cancellation_flag(&self) -> Arc<AtomicBool> {
        self.cancellation.clone()
    }

    pub fn failed_fragment_report(&self) -> NativeExecutionReport {
        NativeExecutionReport::for_contract_test(
            crate::common::types::UniqueId { hi: 41, lo: 73 },
            crate::common::types::UniqueId {
                hi: 41,
                lo: i64::from(11_u32) << 16,
            },
            0,
            crate::proto::common::Status {
                code: 1,
                message: "contract native failure".to_string(),
            },
            None,
        )
    }

    pub fn into_request(self) -> DistributedQueryRequest {
        self.request
    }
}

pub fn non_empty_result_contract_fixture() -> ResultContractFixture {
    let edge = FragmentEdge {
        source_fragment_id: 11,
        target_fragment_id: 19,
        target_exchange_node_id: 190,
        output_partition: DataPartition::unpartitioned(),
        stream_kind: FragmentStreamKind::Gather,
        edge_kind: FragmentEdgeKind::Stream,
        output_slot_ids: Vec::new(),
    };
    let prepared = prepared_fragment_set_for_test(
        vec![
            (11, PreparedFragmentRole::NonTerminal, Vec::new()),
            (19, PreparedFragmentRole::Result, Vec::new()),
        ],
        vec![11, 19],
        19,
        vec![edge],
    );
    let native_bundle =
        crate::protocol::native::encode::native_fragment_bundle_for_contract_test(vec![
            crate::proto::plan::PlanFragment {
                fragment_id: 11,
                ..Default::default()
            },
            crate::proto::plan::PlanFragment {
                fragment_id: 19,
                ..Default::default()
            },
        ])
        .expect("contract fixture native bundle");
    let cancellation = Arc::new(AtomicBool::new(false));
    let request = build_distributed_query_request(
        prepared,
        native_bundle,
        Some(QueryOptions {
            pipeline_dop: Some(2),
            query_timeout: Some(5),
            ..Default::default()
        }),
        DistributedQueryIntent::Result,
        QueryCancellationView::new(cancellation.clone()),
    )
    .expect("contract fixture request");
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .expect("one-row zero-column contract batch");
    let result_chunk = Chunk::try_new_with_chunk_schema(batch, Arc::new(ChunkSchema::empty()))
        .expect("contract result chunk");
    ResultContractFixture {
        request,
        backends: vec![
            (3, SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19031)),
            (8, SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19032)),
        ],
        result_chunk,
        cancellation,
    }
}

pub fn assert_result_outcome_preserved(
    outcome: DistributedQueryOutcome,
    expected_rows: usize,
) -> Result<(), DistributedQueryError> {
    let result = outcome.into_result()?.into_query_result();
    if result.row_count() != expected_rows {
        return Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            format!(
                "engine consumed {} rows from Result outcome, expected {expected_rows}",
                result.row_count()
            ),
        ));
    }
    Ok(())
}

pub struct WriteContractFixture {
    request: DistributedQueryRequest,
    backends: Vec<(usize, SocketAddr)>,
}

impl WriteContractFixture {
    pub fn backends(&self) -> &[(usize, SocketAddr)] {
        &self.backends
    }

    pub fn successful_writer_report(&self) -> NativeExecutionReport {
        NativeExecutionReport::for_contract_test(
            crate::common::types::UniqueId { hi: 51, lo: 91 },
            crate::common::types::UniqueId {
                hi: 51,
                lo: i64::from(23_u32) << 16,
            },
            0,
            crate::proto::common::Status {
                code: 0,
                message: String::new(),
            },
            None,
        )
    }

    pub fn failed_writer_report(&self) -> NativeExecutionReport {
        NativeExecutionReport::for_contract_test(
            crate::common::types::UniqueId { hi: 51, lo: 91 },
            crate::common::types::UniqueId {
                hi: 51,
                lo: i64::from(23_u32) << 16,
            },
            0,
            crate::proto::common::Status {
                code: 1,
                message: "contract writer failure".to_string(),
            },
            None,
        )
    }

    pub fn into_request(self) -> DistributedQueryRequest {
        self.request
    }
}

pub fn non_empty_write_contract_fixture() -> WriteContractFixture {
    let prepared = prepared_fragment_set_for_test(
        vec![(23, PreparedFragmentRole::TerminalWrite, Vec::new())],
        vec![23],
        23,
        Vec::new(),
    );
    let native_bundle =
        crate::protocol::native::encode::native_fragment_bundle_for_contract_test(vec![
            crate::proto::plan::PlanFragment {
                fragment_id: 23,
                ..Default::default()
            },
        ])
        .expect("write contract native bundle");
    let request = build_distributed_query_request(
        prepared,
        native_bundle,
        Some(QueryOptions {
            pipeline_dop: Some(1),
            query_timeout: Some(5),
            ..Default::default()
        }),
        DistributedQueryIntent::Write,
        QueryCancellationView::never_cancelled(),
    )
    .expect("write contract request");
    WriteContractFixture {
        request,
        backends: vec![(3, SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19031))],
    }
}

pub fn assert_write_outcome_preserved(
    outcome: DistributedQueryOutcome,
) -> Result<(), DistributedQueryError> {
    let (_, commit, abort) = outcome.into_write()?.into_parts();
    match (commit, abort) {
        (Some(commit), None) if commit.writers.len() == 1 => Ok(()),
        (None, Some(abort)) if !abort.reason.is_empty() => Ok(()),
        _ => Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "engine did not receive a non-empty commit or abort payload",
        )),
    }
}

pub struct ProfileContractFixture {
    request: DistributedQueryRequest,
    backends: Vec<(usize, SocketAddr)>,
    result_chunk: Chunk,
}

impl ProfileContractFixture {
    pub fn backends(&self) -> &[(usize, SocketAddr)] {
        &self.backends
    }

    pub fn result_batch(&self) -> FetchedQueryBatch {
        FetchedQueryBatch::new(self.result_chunk.clone())
    }

    pub fn fragment_profile_report(&self) -> NativeExecutionReport {
        NativeExecutionReport::for_contract_test(
            crate::common::types::UniqueId { hi: 61, lo: 101 },
            crate::common::types::UniqueId {
                hi: 61,
                lo: i64::from(11_u32) << 16,
            },
            0,
            crate::proto::common::Status {
                code: 0,
                message: String::new(),
            },
            Some(
                crate::runtime::profile::Profiler::new("contract-fragment-profile")
                    .to_native_tree(),
            ),
        )
    }

    pub fn into_request(self) -> DistributedQueryRequest {
        self.request
    }
}

pub fn non_empty_profile_contract_fixture() -> ProfileContractFixture {
    let edge = FragmentEdge {
        source_fragment_id: 11,
        target_fragment_id: 19,
        target_exchange_node_id: 190,
        output_partition: DataPartition::unpartitioned(),
        stream_kind: FragmentStreamKind::Gather,
        edge_kind: FragmentEdgeKind::Stream,
        output_slot_ids: Vec::new(),
    };
    let prepared = prepared_fragment_set_for_test(
        vec![
            (11, PreparedFragmentRole::NonTerminal, Vec::new()),
            (19, PreparedFragmentRole::Result, Vec::new()),
        ],
        vec![11, 19],
        19,
        vec![edge],
    );
    let native_bundle =
        crate::protocol::native::encode::native_fragment_bundle_for_contract_test(vec![
            crate::proto::plan::PlanFragment {
                fragment_id: 11,
                ..Default::default()
            },
            crate::proto::plan::PlanFragment {
                fragment_id: 19,
                ..Default::default()
            },
        ])
        .expect("profile contract native bundle");
    let request = build_distributed_query_request(
        prepared,
        native_bundle,
        Some(QueryOptions {
            pipeline_dop: Some(2),
            query_timeout: Some(5),
            enable_profile: true,
            ..Default::default()
        }),
        DistributedQueryIntent::Profile,
        QueryCancellationView::never_cancelled(),
    )
    .expect("profile contract request");
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .expect("one-row zero-column profile contract batch");
    let result_chunk = Chunk::try_new_with_chunk_schema(batch, Arc::new(ChunkSchema::empty()))
        .expect("profile contract result chunk");
    ProfileContractFixture {
        request,
        backends: vec![
            (3, SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19031)),
            (8, SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19032)),
        ],
        result_chunk,
    }
}

pub fn assert_profile_outcome_preserved(
    outcome: DistributedQueryOutcome,
    expected_rows: usize,
) -> Result<(), DistributedQueryError> {
    let (result, profiles) = outcome.into_profile()?.into_parts();
    let profiles = profiles.into_profiles();
    if result.row_count() != expected_rows
        || profiles.len() != 1
        || profiles[0].root.name != "contract-fragment-profile"
    {
        return Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "engine did not receive the expected non-empty Profile payload",
        ));
    }
    Ok(())
}
