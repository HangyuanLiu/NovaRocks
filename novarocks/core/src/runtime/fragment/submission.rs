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

use std::sync::Arc;

use crate::exec::fragment::error::{
    FragmentBindingError, FragmentBindingErrorKind, FragmentBindingTarget,
};
use crate::exec::fragment::program::FragmentProgram;
use crate::runtime::fragment::instance::FragmentInstanceSpec;

#[derive(Debug)]
pub(crate) struct FragmentSubmission {
    program: Arc<FragmentProgram>,
    instance: FragmentInstanceSpec,
}

impl FragmentSubmission {
    pub(crate) fn try_new(
        program: Arc<FragmentProgram>,
        instance: FragmentInstanceSpec,
    ) -> Result<Self, FragmentBindingError> {
        let expected_version = program.program_options().contract_version();
        let actual_version = instance.contract_version();
        if expected_version != actual_version {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::Instance,
                FragmentBindingErrorKind::ContractVersionMismatch,
                format!(
                    "expected fragment contract version {}, got {}",
                    expected_version.get(),
                    actual_version.get()
                ),
            ));
        }
        Ok(Self { program, instance })
    }

    pub(crate) fn program(&self) -> &Arc<FragmentProgram> {
        &self.program
    }

    pub(crate) const fn instance(&self) -> &FragmentInstanceSpec {
        &self.instance
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use crate::common::types::UniqueId;
    use crate::exec::chunk::{Chunk, ChunkSchemaRef};
    use crate::exec::expr::ExprArena;
    use crate::exec::fragment::error::{
        FragmentBindingError, FragmentBindingErrorKind, FragmentBindingTarget,
    };
    use crate::exec::fragment::program::{
        ExchangeInputContract, FragmentContractVersion, FragmentNodeId, FragmentProgram,
        FragmentProgramOptions, FragmentSinkKind, FragmentSinkSpec, RuntimeFilterContract,
        RuntimeFilterId, ScanAssignmentKind, ScanSourceContract,
    };
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
    use crate::runtime::endpoint::RuntimeFilterProberDestination;
    use crate::runtime::fragment::instance::{
        BackendNum, ExchangeInputAssignment, ExchangeInputAssignments, FragmentInstanceId,
        FragmentInstanceSpec, FragmentRuntimeOptions, FragmentSinkAssignment, ScanAssignments,
    };
    use crate::runtime::query_context::QueryId;
    use crate::runtime::query_options::QueryOptions;
    use crate::runtime::runtime_filter_params::RuntimeFilterParams;

    use super::*;

    fn uid(hi: i64, lo: i64) -> UniqueId {
        UniqueId { hi, lo }
    }

    fn query_id(hi: i64, lo: i64) -> QueryId {
        QueryId { hi, lo }
    }

    fn values_plan(node_id: i32) -> ExecPlan {
        ExecPlan {
            arena: ExprArena::default(),
            root: ExecNode {
                kind: ExecNodeKind::Values(ValuesNode {
                    chunk: Chunk::default(),
                    node_id,
                }),
            },
        }
    }

    fn result_sink() -> FragmentSinkSpec {
        FragmentSinkSpec::try_for_kind(FragmentSinkKind::Result, None).expect("result sink")
    }

    fn program_with(
        plan: ExecPlan,
        sink: FragmentSinkSpec,
        scans: BTreeMap<FragmentNodeId, ScanAssignmentKind>,
        exchanges: BTreeMap<FragmentNodeId, ChunkSchemaRef>,
        build_filters: BTreeSet<RuntimeFilterId>,
    ) -> Arc<FragmentProgram> {
        let scans = scans
            .into_iter()
            .map(|(id, kind)| (id, ScanSourceContract::new(kind)))
            .collect();
        let exchanges = exchanges
            .into_iter()
            .map(|(id, schema)| (id, ExchangeInputContract::new(schema)))
            .collect();
        Arc::new(FragmentProgram::new(
            plan,
            sink,
            FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
            scans,
            exchanges,
            RuntimeFilterContract::new(build_filters, BTreeSet::new()),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn instance_with(
        version: FragmentContractVersion,
        query: QueryId,
        finst: UniqueId,
        scans: BTreeMap<FragmentNodeId, ScanAssignmentKind>,
        exchanges: BTreeMap<FragmentNodeId, usize>,
        sink: FragmentSinkAssignment,
        prober_params: BTreeMap<i32, Vec<RuntimeFilterProberDestination>>,
        builder_counts: BTreeMap<i32, i32>,
    ) -> FragmentInstanceSpec {
        let scans = ScanAssignments::try_new(
            scans
                .into_iter()
                .map(|(id, kind)| (id, (kind, Vec::new())))
                .collect(),
        )
        .expect("scan assignments");
        let exchanges = ExchangeInputAssignments::new(
            exchanges
                .into_iter()
                .map(|(id, count)| {
                    (
                        id,
                        ExchangeInputAssignment::new(
                            NonZeroUsize::new(count).expect("non-zero sender count"),
                        ),
                    )
                })
                .collect(),
        );
        FragmentInstanceSpec::new(
            version,
            query,
            FragmentInstanceId::new(finst),
            scans,
            exchanges,
            sink,
            RuntimeFilterParams::new(prober_params, builder_counts, None),
            FragmentRuntimeOptions::new(QueryOptions::default(), None, false),
            NonZeroUsize::new(1).expect("pipeline DOP"),
            BackendNum::try_new(0).expect("backend number"),
        )
    }

    fn empty_instance(finst_lo: i64) -> FragmentInstanceSpec {
        instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, finst_lo),
            BTreeMap::new(),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        )
    }

    fn assert_error(
        result: Result<FragmentSubmission, FragmentBindingError>,
        target: FragmentBindingTarget,
        kind: FragmentBindingErrorKind,
    ) -> FragmentBindingError {
        let error = result.expect_err("fragment binding error");
        assert_eq!(error.target(), target);
        assert_eq!(error.kind(), kind);
        error
    }

    #[test]
    fn rejects_contract_version_mismatch_before_composition() {
        let program = program_with(
            values_plan(7),
            result_sink(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::new(9),
            query_id(1, 2),
            uid(1, 11),
            BTreeMap::new(),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        assert_error(
            FragmentSubmission::try_new(program, instance),
            FragmentBindingTarget::Instance,
            FragmentBindingErrorKind::ContractVersionMismatch,
        );
    }

    #[test]
    fn composes_empty_contract_without_runtime_resources() {
        let program = program_with(
            values_plan(7),
            result_sink(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let submission = FragmentSubmission::try_new(Arc::clone(&program), empty_instance(11))
            .expect("empty submission");
        assert!(Arc::ptr_eq(submission.program(), &program));
        assert_eq!(submission.instance().query_id(), query_id(1, 2));
        assert_eq!(
            submission.instance().fragment_instance_id().get(),
            uid(1, 11)
        );
    }

    #[test]
    fn shares_immutable_program_across_independent_instances() {
        let program = program_with(
            values_plan(7),
            result_sink(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let first = FragmentSubmission::try_new(Arc::clone(&program), empty_instance(11))
            .expect("first submission");
        let second = FragmentSubmission::try_new(Arc::clone(&program), empty_instance(12))
            .expect("second submission");
        assert!(Arc::ptr_eq(first.program(), &program));
        assert!(Arc::ptr_eq(second.program(), &program));
        assert_ne!(
            first.instance().fragment_instance_id(),
            second.instance().fragment_instance_id()
        );
        assert_eq!(Arc::strong_count(&program), 3);
    }
}
