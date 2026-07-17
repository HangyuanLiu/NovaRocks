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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::fragment::error::{
    FragmentBindingError, FragmentBindingErrorKind, FragmentBindingTarget,
};
use crate::exec::fragment::program::{
    FragmentNodeId, FragmentProgram, FragmentSinkAssignmentKind, FragmentSinkAssignmentRequirement,
    RuntimeFilterId,
};
use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
use crate::runtime::fragment::instance::{FragmentInstanceSpec, FragmentSinkAssignment};

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
        let inventory = ProgramInventory::try_collect(program.plan())?;
        validate_scan_contracts(&program, &inventory)?;
        validate_exchange_contracts(&program, &inventory)?;
        validate_scan_assignments(&program, &instance)?;
        validate_exchange_assignments(&program, &instance)?;
        validate_sink_assignment(&program, &instance)?;
        validate_runtime_filter_params(&program, &instance)?;

        Ok(Self { program, instance })
    }

    pub(crate) fn program(&self) -> &Arc<FragmentProgram> {
        &self.program
    }

    pub(crate) const fn instance(&self) -> &FragmentInstanceSpec {
        &self.instance
    }
}

struct ProgramInventory {
    scan_nodes: BTreeSet<FragmentNodeId>,
    exchange_nodes: BTreeMap<FragmentNodeId, ChunkSchemaRef>,
}

impl ProgramInventory {
    fn try_collect(plan: &ExecPlan) -> Result<Self, FragmentBindingError> {
        let mut inventory = Self {
            scan_nodes: BTreeSet::new(),
            exchange_nodes: BTreeMap::new(),
        };
        inventory.visit(&plan.root)?;
        Ok(inventory)
    }

    fn visit(&mut self, node: &ExecNode) -> Result<(), FragmentBindingError> {
        match &node.kind {
            ExecNodeKind::AssertNumRows(node) => self.visit(&node.input),
            ExecNodeKind::Values(_) => Ok(()),
            ExecNodeKind::Project(node) => self.visit(&node.input),
            ExecNodeKind::Filter(node) => self.visit(&node.input),
            ExecNodeKind::Repeat(node) => self.visit(&node.input),
            ExecNodeKind::ChangeEventExpand(node) => self.visit(&node.input),
            ExecNodeKind::UnionAll(node) => self.visit_inputs(&node.inputs),
            ExecNodeKind::Limit(node) => self.visit(&node.input),
            ExecNodeKind::ExchangeSource(node) => {
                let id = FragmentNodeId::new(node.key.node_id);
                if self
                    .exchange_nodes
                    .insert(id, Arc::clone(&node.expected_chunk_schema))
                    .is_some()
                {
                    return Err(FragmentBindingError::new(
                        FragmentBindingTarget::ExchangeNode(id.get()),
                        FragmentBindingErrorKind::InvalidAssignment,
                        format!("duplicate exchange node id {}", id.get()),
                    ));
                }
                Ok(())
            }
            ExecNodeKind::Scan(node) => {
                if let Some(raw_id) = node.node_id() {
                    self.insert_scan(FragmentNodeId::new(raw_id))?;
                }
                Ok(())
            }
            ExecNodeKind::IcebergDeltaScan(node) => {
                self.insert_scan(FragmentNodeId::new(node.node_id))
            }
            #[cfg(feature = "compat")]
            ExecNodeKind::Fetch(node) => self.visit(&node.input),
            ExecNodeKind::LookUp(_) => Ok(()),
            ExecNodeKind::Aggregate(node) => self.visit(&node.input),
            ExecNodeKind::Join(node) => {
                self.visit(&node.left)?;
                self.visit(&node.right)
            }
            ExecNodeKind::NestedLoopJoin(node) => {
                self.visit(&node.left)?;
                self.visit(&node.right)
            }
            ExecNodeKind::Sort(node) => self.visit(&node.input),
            ExecNodeKind::TableFunction(node) => self.visit(&node.input),
            ExecNodeKind::Analytic(node) => self.visit(&node.input),
            ExecNodeKind::SetOp(node) => self.visit_inputs(&node.inputs),
        }
    }

    fn visit_inputs(&mut self, inputs: &[ExecNode]) -> Result<(), FragmentBindingError> {
        for input in inputs {
            self.visit(input)?;
        }
        Ok(())
    }

    fn insert_scan(&mut self, id: FragmentNodeId) -> Result<(), FragmentBindingError> {
        if !self.scan_nodes.insert(id) {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::ScanNode(id.get()),
                FragmentBindingErrorKind::InvalidAssignment,
                format!("duplicate scan node id {}", id.get()),
            ));
        }
        Ok(())
    }
}

fn validate_scan_contracts(
    program: &FragmentProgram,
    inventory: &ProgramInventory,
) -> Result<(), FragmentBindingError> {
    for id in program.scan_sources().keys() {
        if !inventory.scan_nodes.contains(id) {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::ScanNode(id.get()),
                FragmentBindingErrorKind::InvalidAssignment,
                format!(
                    "declared scan contract {} does not resolve to a scan-shaped plan node",
                    id.get()
                ),
            ));
        }
    }
    Ok(())
}

fn schema_summary(schema: &ChunkSchemaRef) -> String {
    let slots = schema
        .slots()
        .iter()
        .map(|slot| {
            let unique_id = slot
                .unique_id()
                .map_or_else(|| "none".to_string(), |id| id.to_string());
            format!(
                "slot={},name={},type={:?},nullable={},unique_id={}",
                slot.slot_id(),
                slot.name(),
                slot.data_type(),
                slot.nullable(),
                unique_id
            )
        })
        .collect::<Vec<_>>()
        .join(";");
    format!("[{slots}]")
}

fn validate_exchange_contracts(
    program: &FragmentProgram,
    inventory: &ProgramInventory,
) -> Result<(), FragmentBindingError> {
    let mut mismatches = BTreeSet::new();
    for id in program.exchange_inputs().keys() {
        if !inventory.exchange_nodes.contains_key(id) {
            mismatches.insert(*id);
        }
    }
    for id in inventory.exchange_nodes.keys() {
        if !program.exchange_inputs().contains_key(id) {
            mismatches.insert(*id);
        }
    }
    if let Some(id) = mismatches.first().copied() {
        return Err(FragmentBindingError::new(
            FragmentBindingTarget::ExchangeNode(id.get()),
            FragmentBindingErrorKind::InvalidAssignment,
            format!(
                "static exchange inventory mismatch for node {}: declared={} plan={}",
                id.get(),
                program.exchange_inputs().contains_key(&id),
                inventory.exchange_nodes.contains_key(&id)
            ),
        ));
    }
    for (id, contract) in program.exchange_inputs() {
        let actual = inventory
            .exchange_nodes
            .get(id)
            .expect("exchange key sets were validated");
        if contract.expected_schema().slot_ids() != actual.slot_ids() {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::ExchangeNode(id.get()),
                FragmentBindingErrorKind::LayoutMismatch,
                format!(
                    "exchange node {} expected slots {:?}, got {:?}",
                    id.get(),
                    contract.expected_schema().slot_ids(),
                    actual.slot_ids()
                ),
            ));
        }
        if contract.expected_schema().as_ref() != actual.as_ref() {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::ExchangeNode(id.get()),
                FragmentBindingErrorKind::SchemaMismatch,
                format!(
                    "exchange node {} expected schema {}, got {}",
                    id.get(),
                    schema_summary(contract.expected_schema()),
                    schema_summary(actual)
                ),
            ));
        }
    }
    Ok(())
}

fn validate_scan_assignments(
    program: &FragmentProgram,
    instance: &FragmentInstanceSpec,
) -> Result<(), FragmentBindingError> {
    for (id, contract) in program.scan_sources() {
        let Some(assignment) = instance.scan_assignments().get(id) else {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::ScanNode(id.get()),
                FragmentBindingErrorKind::MissingAssignment,
                format!("missing scan assignment for node {}", id.get()),
            ));
        };
        if assignment.kind() != contract.assignment_kind() {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::ScanNode(id.get()),
                FragmentBindingErrorKind::WrongAssignmentKind,
                format!(
                    "scan node {} expected {:?}, got {:?}",
                    id.get(),
                    contract.assignment_kind(),
                    assignment.kind()
                ),
            ));
        }
    }
    for (id, _) in instance.scan_assignments().iter() {
        if !program.scan_sources().contains_key(id) {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::ScanNode(id.get()),
                FragmentBindingErrorKind::ExtraAssignment,
                format!(
                    "scan assignment for node {} has no static contract",
                    id.get()
                ),
            ));
        }
    }
    Ok(())
}

fn validate_exchange_assignments(
    program: &FragmentProgram,
    instance: &FragmentInstanceSpec,
) -> Result<(), FragmentBindingError> {
    for id in program.exchange_inputs().keys() {
        if instance.exchange_inputs().get(id).is_none() {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::ExchangeNode(id.get()),
                FragmentBindingErrorKind::MissingAssignment,
                format!("missing exchange assignment for node {}", id.get()),
            ));
        }
    }
    for (id, _) in instance.exchange_inputs().iter() {
        if !program.exchange_inputs().contains_key(id) {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::ExchangeNode(id.get()),
                FragmentBindingErrorKind::ExtraAssignment,
                format!(
                    "exchange assignment for node {} has no static contract",
                    id.get()
                ),
            ));
        }
    }
    Ok(())
}

fn validate_sink_assignment(
    program: &FragmentProgram,
    instance: &FragmentInstanceSpec,
) -> Result<(), FragmentBindingError> {
    use FragmentSinkAssignmentKind::{DestinationGroups, StreamDestinations};
    use FragmentSinkAssignmentRequirement as Requirement;
    let requirement = program.sink().assignment_requirement();
    let assignment = instance.sink_assignment();
    match (requirement, assignment) {
        (Requirement::None, FragmentSinkAssignment::None)
        | (
            Requirement::Required(StreamDestinations),
            FragmentSinkAssignment::StreamDestinations { .. },
        ) => Ok(()),
        (
            Requirement::Required(DestinationGroups(expected)),
            FragmentSinkAssignment::DestinationGroups { groups, .. },
        ) if groups.len() == expected.get() => Ok(()),
        (
            Requirement::Required(DestinationGroups(expected)),
            FragmentSinkAssignment::DestinationGroups { groups, .. },
        ) => Err(FragmentBindingError::new(
            FragmentBindingTarget::Sink,
            FragmentBindingErrorKind::InvalidAssignment,
            format!(
                "sink expected {} destination groups, got {}",
                expected.get(),
                groups.len()
            ),
        )),
        (Requirement::Required(_), FragmentSinkAssignment::None) => Err(FragmentBindingError::new(
            FragmentBindingTarget::Sink,
            FragmentBindingErrorKind::MissingAssignment,
            format!(
                "sink expected {}, got none",
                sink_requirement_summary(requirement)
            ),
        )),
        _ => Err(FragmentBindingError::new(
            FragmentBindingTarget::Sink,
            FragmentBindingErrorKind::WrongAssignmentKind,
            format!(
                "sink expected {}, got {}",
                sink_requirement_summary(requirement),
                sink_assignment_summary(assignment)
            ),
        )),
    }
}

fn sink_requirement_summary(requirement: FragmentSinkAssignmentRequirement) -> String {
    use FragmentSinkAssignmentKind::{DestinationGroups, StreamDestinations};
    use FragmentSinkAssignmentRequirement::{None, Required};
    match requirement {
        None => "none".to_string(),
        Required(StreamDestinations) => "stream_destinations".to_string(),
        Required(DestinationGroups(count)) => format!("destination_groups(count={})", count.get()),
    }
}

fn sink_assignment_summary(assignment: &FragmentSinkAssignment) -> String {
    match assignment {
        FragmentSinkAssignment::None => "none".to_string(),
        FragmentSinkAssignment::StreamDestinations { .. } => "stream_destinations".to_string(),
        FragmentSinkAssignment::DestinationGroups { groups, .. } => {
            format!("destination_groups(count={})", groups.len())
        }
    }
}

fn validate_runtime_filter_params(
    program: &FragmentProgram,
    instance: &FragmentInstanceSpec,
) -> Result<(), FragmentBindingError> {
    let params = instance.runtime_filter_params();
    for (raw_id, count) in params.runtime_filter_builder_number() {
        if *count <= 0 {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::RuntimeFilter(*raw_id),
                FragmentBindingErrorKind::InvalidAssignment,
                format!("runtime filter {raw_id} builder count must be positive, got {count}"),
            ));
        }
    }
    for raw_id in params.id_to_prober_params().keys() {
        let id = RuntimeFilterId::new(*raw_id);
        if !program.runtime_filters().build_filters().contains(&id) {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::RuntimeFilter(*raw_id),
                FragmentBindingErrorKind::RuntimeFilterMismatch,
                format!(
                    "runtime filter {raw_id} has remote probers but is not a local build filter"
                ),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "compat")]
    use std::collections::HashMap;
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::common::ids::SlotId;
    use crate::common::types::UniqueId;
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
    use crate::exec::expr::ExprArena;
    use crate::exec::fragment::error::{
        FragmentBindingError, FragmentBindingErrorKind, FragmentBindingTarget,
    };
    use crate::exec::fragment::program::{
        ExchangeInputContract, FragmentContractVersion, FragmentNodeId, FragmentProgram,
        FragmentProgramOptions, FragmentSinkKind, FragmentSinkSpec, RuntimeFilterContract,
        RuntimeFilterId, ScanAssignmentKind, ScanSourceContract,
    };
    use crate::exec::node::BoxedExecIter;
    use crate::exec::node::exchange_source::ExchangeSourceNode;
    #[cfg(feature = "compat")]
    use crate::exec::node::fetch::FetchNode;
    use crate::exec::node::scan::{
        RuntimeFilterContext, ScanMorsel, ScanMorsels, ScanNode, ScanOp,
    };
    use crate::exec::node::union_all::UnionAllNode;
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
    use crate::runtime::endpoint::RuntimeEndpoint;
    use crate::runtime::endpoint::RuntimeFilterProberDestination;
    use crate::runtime::exchange::ExchangeKey;
    use crate::runtime::fragment::instance::{
        BackendNum, ExchangeInputAssignment, ExchangeInputAssignments, FragmentInstanceId,
        FragmentInstanceSpec, FragmentRuntimeOptions, FragmentSinkAssignment, ScanAssignments,
    };
    use crate::runtime::profile::RuntimeProfile;
    use crate::runtime::query_context::QueryId;
    use crate::runtime::query_options::QueryOptions;
    use crate::runtime::runtime_filter_params::RuntimeFilterParams;
    use arrow::datatypes::{DataType, Field, Schema};

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

    struct DummyScanOp;

    impl ScanOp for DummyScanOp {
        fn execute_iter(
            &self,
            _morsel: ScanMorsel,
            _profile: Option<RuntimeProfile>,
            _runtime_filters: Option<&RuntimeFilterContext>,
        ) -> Result<BoxedExecIter, String> {
            Ok(Box::new(std::iter::empty()))
        }

        fn build_morsels(&self) -> Result<ScanMorsels, String> {
            Ok(ScanMorsels::default())
        }
    }

    fn scan_node(node_id: Option<i32>) -> ExecNode {
        let mut scan = ScanNode::new(Arc::new(DummyScanOp));
        if let Some(node_id) = node_id {
            scan = scan.with_node_id(node_id);
        }
        scan = scan.with_output_chunk_schema(Arc::new(ChunkSchema::empty()));
        ExecNode {
            kind: ExecNodeKind::Scan(scan),
        }
    }

    fn scan_plan(node_id: Option<i32>) -> ExecPlan {
        ExecPlan {
            arena: ExprArena::default(),
            root: scan_node(node_id),
        }
    }

    fn schema(slot_id: u32, nullable: bool) -> ChunkSchemaRef {
        let arrow_schema = Schema::new(vec![Field::new("v", DataType::Int32, nullable)]);
        ChunkSchema::try_ref_from_schema_and_slot_ids(&arrow_schema, &[SlotId::new(slot_id)])
            .expect("chunk schema")
    }

    fn exchange_node(node_id: i32, expected_schema: ChunkSchemaRef, finst: UniqueId) -> ExecNode {
        ExecNode {
            kind: ExecNodeKind::ExchangeSource(ExchangeSourceNode::new(
                ExchangeKey {
                    finst_id_hi: finst.hi,
                    finst_id_lo: finst.lo,
                    node_id,
                },
                1,
                Duration::from_secs(1),
                expected_schema,
            )),
        }
    }

    fn union_plan(inputs: Vec<ExecNode>) -> ExecPlan {
        ExecPlan {
            arena: ExprArena::default(),
            root: ExecNode {
                kind: ExecNodeKind::UnionAll(UnionAllNode {
                    inputs,
                    node_id: 99,
                }),
            },
        }
    }

    fn prober_destination() -> RuntimeFilterProberDestination {
        RuntimeFilterProberDestination::new(
            uid(3, 4),
            RuntimeEndpoint::new("be-1", 9060).expect("runtime endpoint"),
        )
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

    #[test]
    fn accepts_uncontracted_scan_without_node_id() {
        let program = program_with(
            scan_plan(None),
            result_sink(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        FragmentSubmission::try_new(program, empty_instance(20)).expect("uncontracted scan");
    }

    #[test]
    fn rejects_declared_scan_bound_to_non_scan_node() {
        let id = FragmentNodeId::new(10);
        let program = program_with(
            values_plan(10),
            result_sink(),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 21),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        assert_error(
            FragmentSubmission::try_new(program, instance),
            FragmentBindingTarget::ScanNode(10),
            FragmentBindingErrorKind::InvalidAssignment,
        );
    }

    #[test]
    fn rejects_duplicate_scan_node_identity() {
        let id = FragmentNodeId::new(10);
        let program = program_with(
            union_plan(vec![scan_node(Some(10)), scan_node(Some(10))]),
            result_sink(),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 22),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        assert_error(
            FragmentSubmission::try_new(program, instance),
            FragmentBindingTarget::ScanNode(10),
            FragmentBindingErrorKind::InvalidAssignment,
        );
    }

    #[test]
    fn rejects_missing_scan_assignment() {
        let id = FragmentNodeId::new(10);
        let program = program_with(
            scan_plan(Some(10)),
            result_sink(),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        assert_error(
            FragmentSubmission::try_new(program, empty_instance(23)),
            FragmentBindingTarget::ScanNode(10),
            FragmentBindingErrorKind::MissingAssignment,
        );
    }

    #[test]
    fn rejects_extra_scan_assignment() {
        let id = FragmentNodeId::new(12);
        let program = program_with(
            values_plan(7),
            result_sink(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 24),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        assert_error(
            FragmentSubmission::try_new(program, instance),
            FragmentBindingTarget::ScanNode(12),
            FragmentBindingErrorKind::ExtraAssignment,
        );
    }

    #[test]
    fn rejects_wrong_scan_assignment_kind() {
        let id = FragmentNodeId::new(10);
        let program = program_with(
            scan_plan(Some(10)),
            result_sink(),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 25),
            BTreeMap::from([(id, ScanAssignmentKind::StarRocksTablet)]),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        assert_error(
            FragmentSubmission::try_new(program, instance),
            FragmentBindingTarget::ScanNode(10),
            FragmentBindingErrorKind::WrongAssignmentKind,
        );
    }

    #[test]
    fn reports_smallest_scan_error_first() {
        let id10 = FragmentNodeId::new(10);
        let id20 = FragmentNodeId::new(20);
        let program = program_with(
            union_plan(vec![scan_node(Some(20)), scan_node(Some(10))]),
            result_sink(),
            BTreeMap::from([
                (id20, ScanAssignmentKind::File),
                (id10, ScanAssignmentKind::File),
            ]),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        assert_error(
            FragmentSubmission::try_new(program, empty_instance(26)),
            FragmentBindingTarget::ScanNode(10),
            FragmentBindingErrorKind::MissingAssignment,
        );
    }

    #[test]
    fn rejects_plan_exchange_without_static_contract() {
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: exchange_node(20, schema(1, true), uid(5, 8)),
            },
            result_sink(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        assert_error(
            FragmentSubmission::try_new(program, empty_instance(30)),
            FragmentBindingTarget::ExchangeNode(20),
            FragmentBindingErrorKind::InvalidAssignment,
        );
    }

    #[test]
    fn rejects_static_exchange_without_plan_node() {
        let id = FragmentNodeId::new(20);
        let expected = schema(1, true);
        let program = program_with(
            values_plan(7),
            result_sink(),
            BTreeMap::new(),
            BTreeMap::from([(id, expected)]),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 31),
            BTreeMap::new(),
            BTreeMap::from([(id, 1)]),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        assert_error(
            FragmentSubmission::try_new(program, instance),
            FragmentBindingTarget::ExchangeNode(20),
            FragmentBindingErrorKind::InvalidAssignment,
        );
    }

    #[test]
    fn rejects_duplicate_exchange_node_identity() {
        let id = FragmentNodeId::new(20);
        let expected = schema(1, true);
        let program = program_with(
            union_plan(vec![
                exchange_node(20, Arc::clone(&expected), uid(5, 8)),
                exchange_node(20, Arc::clone(&expected), uid(5, 9)),
            ]),
            result_sink(),
            BTreeMap::new(),
            BTreeMap::from([(id, expected)]),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 32),
            BTreeMap::new(),
            BTreeMap::from([(id, 1)]),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        assert_error(
            FragmentSubmission::try_new(program, instance),
            FragmentBindingTarget::ExchangeNode(20),
            FragmentBindingErrorKind::InvalidAssignment,
        );
    }

    #[test]
    fn rejects_exchange_layout_mismatch_before_schema() {
        let id = FragmentNodeId::new(20);
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: exchange_node(20, schema(2, true), uid(5, 8)),
            },
            result_sink(),
            BTreeMap::new(),
            BTreeMap::from([(id, schema(1, false))]),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 33),
            BTreeMap::new(),
            BTreeMap::from([(id, 1)]),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        assert_error(
            FragmentSubmission::try_new(program, instance),
            FragmentBindingTarget::ExchangeNode(20),
            FragmentBindingErrorKind::LayoutMismatch,
        );
    }

    #[test]
    fn rejects_exchange_schema_mismatch_after_layout_match() {
        let id = FragmentNodeId::new(20);
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: exchange_node(20, schema(1, true), uid(5, 8)),
            },
            result_sink(),
            BTreeMap::new(),
            BTreeMap::from([(id, schema(1, false))]),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 34),
            BTreeMap::new(),
            BTreeMap::from([(id, 1)]),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        let error = assert_error(
            FragmentSubmission::try_new(program, instance),
            FragmentBindingTarget::ExchangeNode(20),
            FragmentBindingErrorKind::SchemaMismatch,
        );
        assert_eq!(
            error.detail(),
            "exchange node 20 expected schema [slot=1,name=v,type=Int32,nullable=false,unique_id=none], got [slot=1,name=v,type=Int32,nullable=true,unique_id=none]"
        );
    }

    #[test]
    fn rejects_missing_exchange_assignment() {
        let id = FragmentNodeId::new(20);
        let expected = schema(1, true);
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: exchange_node(20, Arc::clone(&expected), uid(5, 8)),
            },
            result_sink(),
            BTreeMap::new(),
            BTreeMap::from([(id, expected)]),
            BTreeSet::new(),
        );
        assert_error(
            FragmentSubmission::try_new(program, empty_instance(35)),
            FragmentBindingTarget::ExchangeNode(20),
            FragmentBindingErrorKind::MissingAssignment,
        );
    }

    #[test]
    fn rejects_extra_exchange_assignment() {
        let id = FragmentNodeId::new(21);
        let program = program_with(
            values_plan(7),
            result_sink(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 36),
            BTreeMap::new(),
            BTreeMap::from([(id, 1)]),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        assert_error(
            FragmentSubmission::try_new(program, instance),
            FragmentBindingTarget::ExchangeNode(21),
            FragmentBindingErrorKind::ExtraAssignment,
        );
    }

    #[test]
    fn accepts_matching_exchange_contract_and_assignment() {
        let id = FragmentNodeId::new(20);
        let expected = schema(1, true);
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: exchange_node(20, Arc::clone(&expected), uid(5, 8)),
            },
            result_sink(),
            BTreeMap::new(),
            BTreeMap::from([(id, expected)]),
            BTreeSet::new(),
        );
        let submission = FragmentSubmission::try_new(
            program,
            instance_with(
                FragmentContractVersion::CURRENT,
                query_id(1, 2),
                uid(1, 37),
                BTreeMap::new(),
                BTreeMap::from([(id, 2)]),
                FragmentSinkAssignment::None,
                BTreeMap::new(),
                BTreeMap::new(),
            ),
        )
        .expect("matching exchange");
        assert_eq!(
            submission
                .instance()
                .exchange_inputs()
                .get(&id)
                .expect("exchange assignment")
                .sender_count()
                .get(),
            2
        );
    }

    #[test]
    fn reports_smallest_static_exchange_inventory_error_first() {
        let plan_id = FragmentNodeId::new(10);
        let contract_id = FragmentNodeId::new(20);
        let expected = schema(1, true);
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: exchange_node(10, Arc::clone(&expected), uid(5, 8)),
            },
            result_sink(),
            BTreeMap::new(),
            BTreeMap::from([(contract_id, expected)]),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 38),
            BTreeMap::new(),
            BTreeMap::from([(contract_id, 1)]),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        assert!(!program.exchange_inputs().contains_key(&plan_id));
        assert_error(
            FragmentSubmission::try_new(program, instance),
            FragmentBindingTarget::ExchangeNode(10),
            FragmentBindingErrorKind::InvalidAssignment,
        );
    }

    #[test]
    fn rejects_sink_assignment_for_result_sink() {
        let program = program_with(
            values_plan(7),
            result_sink(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 40),
            BTreeMap::new(),
            BTreeMap::new(),
            FragmentSinkAssignment::StreamDestinations {
                destinations: Vec::new(),
                sender_id: None,
            },
            BTreeMap::new(),
            BTreeMap::new(),
        );
        let error = assert_error(
            FragmentSubmission::try_new(program, instance),
            FragmentBindingTarget::Sink,
            FragmentBindingErrorKind::WrongAssignmentKind,
        );
        assert_eq!(
            error.detail(),
            "sink expected none, got stream_destinations"
        );
    }

    #[test]
    fn requires_stream_destinations_for_data_stream_sink() {
        let program = program_with(
            values_plan(7),
            FragmentSinkSpec::try_for_kind(FragmentSinkKind::DataStream, None)
                .expect("data stream sink"),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        assert_error(
            FragmentSubmission::try_new(program, empty_instance(41)),
            FragmentBindingTarget::Sink,
            FragmentBindingErrorKind::MissingAssignment,
        );
    }

    #[test]
    fn accepts_stream_destinations_and_preserves_sender_id() {
        let program = program_with(
            values_plan(7),
            FragmentSinkSpec::try_for_kind(FragmentSinkKind::DataStream, None)
                .expect("data stream sink"),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let submission = FragmentSubmission::try_new(
            program,
            instance_with(
                FragmentContractVersion::CURRENT,
                query_id(1, 2),
                uid(1, 42),
                BTreeMap::new(),
                BTreeMap::new(),
                FragmentSinkAssignment::StreamDestinations {
                    destinations: Vec::new(),
                    sender_id: Some(7),
                },
                BTreeMap::new(),
                BTreeMap::new(),
            ),
        )
        .expect("stream sink submission");
        assert!(matches!(
            submission.instance().sink_assignment(),
            FragmentSinkAssignment::StreamDestinations {
                sender_id: Some(7),
                ..
            }
        ));
    }

    #[test]
    fn rejects_wrong_destination_group_count() {
        let program = program_with(
            values_plan(7),
            FragmentSinkSpec::try_for_kind(
                FragmentSinkKind::MultiCastDataStream,
                NonZeroUsize::new(2),
            )
            .expect("grouped sink"),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 43),
            BTreeMap::new(),
            BTreeMap::new(),
            FragmentSinkAssignment::DestinationGroups {
                groups: vec![Vec::new()],
                sender_id: None,
            },
            BTreeMap::new(),
            BTreeMap::new(),
        );
        assert_error(
            FragmentSubmission::try_new(program, instance),
            FragmentBindingTarget::Sink,
            FragmentBindingErrorKind::InvalidAssignment,
        );
    }

    #[test]
    fn accepts_empty_destination_groups_when_group_count_matches() {
        let program = program_with(
            values_plan(7),
            FragmentSinkSpec::try_for_kind(
                FragmentSinkKind::MultiCastDataStream,
                NonZeroUsize::new(2),
            )
            .expect("grouped sink"),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        FragmentSubmission::try_new(
            program,
            instance_with(
                FragmentContractVersion::CURRENT,
                query_id(1, 2),
                uid(1, 44),
                BTreeMap::new(),
                BTreeMap::new(),
                FragmentSinkAssignment::DestinationGroups {
                    groups: vec![Vec::new(), Vec::new()],
                    sender_id: Some(9),
                },
                BTreeMap::new(),
                BTreeMap::new(),
            ),
        )
        .expect("matching grouped sink");
    }

    #[test]
    fn rejects_stream_assignment_for_grouped_sink() {
        let program = program_with(
            values_plan(7),
            FragmentSinkSpec::try_for_kind(FragmentSinkKind::SplitDataStream, NonZeroUsize::new(2))
                .expect("grouped sink"),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 45),
            BTreeMap::new(),
            BTreeMap::new(),
            FragmentSinkAssignment::StreamDestinations {
                destinations: Vec::new(),
                sender_id: None,
            },
            BTreeMap::new(),
            BTreeMap::new(),
        );
        let error = assert_error(
            FragmentSubmission::try_new(program, instance),
            FragmentBindingTarget::Sink,
            FragmentBindingErrorKind::WrongAssignmentKind,
        );
        assert_eq!(
            error.detail(),
            "sink expected destination_groups(count=2), got stream_destinations"
        );
    }

    #[test]
    fn rejects_remote_prober_for_non_build_filter() {
        let program = program_with(
            values_plan(7),
            result_sink(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::from([RuntimeFilterId::new(11)]),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 50),
            BTreeMap::new(),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::from([(13, vec![prober_destination()])]),
            BTreeMap::new(),
        );
        assert_error(
            FragmentSubmission::try_new(program, instance),
            FragmentBindingTarget::RuntimeFilter(13),
            FragmentBindingErrorKind::RuntimeFilterMismatch,
        );
    }

    #[test]
    fn accepts_query_global_builder_count_for_unrelated_filter() {
        let program = program_with(
            values_plan(7),
            result_sink(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::from([RuntimeFilterId::new(11)]),
        );
        FragmentSubmission::try_new(
            program,
            instance_with(
                FragmentContractVersion::CURRENT,
                query_id(1, 2),
                uid(1, 51),
                BTreeMap::new(),
                BTreeMap::new(),
                FragmentSinkAssignment::None,
                BTreeMap::new(),
                BTreeMap::from([(13, 2)]),
            ),
        )
        .expect("query-global builder count");
    }

    #[test]
    fn rejects_zero_and_negative_builder_counts() {
        for (finst_lo, count) in [(52, 0), (53, -1)] {
            let program = program_with(
                values_plan(7),
                result_sink(),
                BTreeMap::new(),
                BTreeMap::new(),
                BTreeSet::from([RuntimeFilterId::new(11)]),
            );
            let instance = instance_with(
                FragmentContractVersion::CURRENT,
                query_id(1, 2),
                uid(1, finst_lo),
                BTreeMap::new(),
                BTreeMap::new(),
                FragmentSinkAssignment::None,
                BTreeMap::new(),
                BTreeMap::from([(11, count)]),
            );
            assert_error(
                FragmentSubmission::try_new(program, instance),
                FragmentBindingTarget::RuntimeFilter(11),
                FragmentBindingErrorKind::InvalidAssignment,
            );
        }
    }

    #[test]
    fn accepts_empty_prober_list_for_local_build_filter() {
        let program = program_with(
            values_plan(7),
            result_sink(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::from([RuntimeFilterId::new(11)]),
        );
        FragmentSubmission::try_new(
            program,
            instance_with(
                FragmentContractVersion::CURRENT,
                query_id(1, 2),
                uid(1, 54),
                BTreeMap::new(),
                BTreeMap::new(),
                FragmentSinkAssignment::None,
                BTreeMap::from([(11, Vec::new())]),
                BTreeMap::new(),
            ),
        )
        .expect("empty prober list");
    }

    #[test]
    fn reports_smallest_builder_count_error_first() {
        let program = program_with(
            values_plan(7),
            result_sink(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 55),
            BTreeMap::new(),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::from([(20, 0), (10, 0)]),
        );
        assert_error(
            FragmentSubmission::try_new(program, instance),
            FragmentBindingTarget::RuntimeFilter(10),
            FragmentBindingErrorKind::InvalidAssignment,
        );
    }

    #[test]
    fn static_exchange_validation_precedes_dynamic_scan_validation() {
        let scan_id = FragmentNodeId::new(10);
        let exchange_id = FragmentNodeId::new(20);
        let program = program_with(
            union_plan(vec![
                scan_node(Some(10)),
                exchange_node(20, schema(2, true), uid(5, 8)),
            ]),
            result_sink(),
            BTreeMap::from([(scan_id, ScanAssignmentKind::File)]),
            BTreeMap::from([(exchange_id, schema(1, true))]),
            BTreeSet::new(),
        );
        assert_error(
            FragmentSubmission::try_new(program, empty_instance(60)),
            FragmentBindingTarget::ExchangeNode(20),
            FragmentBindingErrorKind::LayoutMismatch,
        );
    }

    #[test]
    fn dynamic_scan_validation_precedes_dynamic_exchange_sink_and_rf() {
        let scan_id = FragmentNodeId::new(10);
        let exchange_id = FragmentNodeId::new(20);
        let expected = schema(1, true);
        let program = program_with(
            union_plan(vec![
                scan_node(Some(10)),
                exchange_node(20, Arc::clone(&expected), uid(5, 8)),
            ]),
            result_sink(),
            BTreeMap::from([(scan_id, ScanAssignmentKind::File)]),
            BTreeMap::from([(exchange_id, expected)]),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 61),
            BTreeMap::new(),
            BTreeMap::new(),
            FragmentSinkAssignment::StreamDestinations {
                destinations: Vec::new(),
                sender_id: None,
            },
            BTreeMap::new(),
            BTreeMap::from([(11, 0)]),
        );
        assert_error(
            FragmentSubmission::try_new(program, instance),
            FragmentBindingTarget::ScanNode(10),
            FragmentBindingErrorKind::MissingAssignment,
        );
    }

    #[cfg(feature = "compat")]
    #[test]
    fn compat_fetch_wrapper_is_traversed() {
        let id = FragmentNodeId::new(10);
        let output_schema = Arc::new(ChunkSchema::empty());
        let plan = ExecPlan {
            arena: ExprArena::default(),
            root: ExecNode {
                kind: ExecNodeKind::Fetch(FetchNode {
                    input: Box::new(scan_node(Some(10))),
                    node_id: 30,
                    target_node_id: 10,
                    row_pos_descs: HashMap::new(),
                    nodes_info: None,
                    output_chunk_schema: output_schema,
                }),
            },
        };
        let program = program_with(
            plan,
            result_sink(),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 62),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        FragmentSubmission::try_new(program, instance).expect("fetch child scan");
    }
}
