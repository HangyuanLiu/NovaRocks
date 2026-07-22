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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};

use crate::common::ids::SlotId;
use crate::exec::chunk::{ChunkFieldSchema, ChunkSchemaRef};
use crate::exec::fragment::error::{
    FragmentBindingError, FragmentBindingErrorKind, FragmentBindingTarget,
};
use crate::exec::fragment::program::{
    FragmentNodeId, FragmentProgram, FragmentSinkAssignmentKind, FragmentSinkAssignmentRequirement,
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
        validate_runtime_filter_params(&instance)?;

        Ok(Self { program, instance })
    }

    pub(crate) fn program(&self) -> &Arc<FragmentProgram> {
        &self.program
    }

    pub(crate) const fn instance(&self) -> &FragmentInstanceSpec {
        &self.instance
    }

    #[cfg(feature = "compat")]
    pub(crate) fn incremental_scan_contracts(&self) -> HashMap<i32, Option<SlotId>> {
        let mut contracts = HashMap::new();
        collect_incremental_scan_contracts(&self.program.plan().root, &mut contracts);
        contracts
    }
}

#[cfg(feature = "compat")]
fn collect_incremental_scan_contracts(node: &ExecNode, output: &mut HashMap<i32, Option<SlotId>>) {
    match &node.kind {
        ExecNodeKind::Scan(scan) => {
            if let Some(node_id) = scan.node_id() {
                output.insert(
                    node_id,
                    scan.iceberg_virtual().and_then(|spec| spec.change_op_slot),
                );
            }
        }
        ExecNodeKind::AssertNumRows(value) => {
            collect_incremental_scan_contracts(&value.input, output)
        }
        ExecNodeKind::Project(value) => collect_incremental_scan_contracts(&value.input, output),
        ExecNodeKind::Filter(value) => collect_incremental_scan_contracts(&value.input, output),
        ExecNodeKind::Repeat(value) => collect_incremental_scan_contracts(&value.input, output),
        ExecNodeKind::ChangeEventExpand(value) => {
            collect_incremental_scan_contracts(&value.input, output)
        }
        ExecNodeKind::UnionAll(value) => {
            for input in &value.inputs {
                collect_incremental_scan_contracts(input, output);
            }
        }
        ExecNodeKind::Limit(value) => collect_incremental_scan_contracts(&value.input, output),
        ExecNodeKind::Fetch(value) => collect_incremental_scan_contracts(&value.input, output),
        ExecNodeKind::Aggregate(value) => collect_incremental_scan_contracts(&value.input, output),
        ExecNodeKind::Join(value) => {
            collect_incremental_scan_contracts(&value.left, output);
            collect_incremental_scan_contracts(&value.right, output);
        }
        ExecNodeKind::NestedLoopJoin(value) => {
            collect_incremental_scan_contracts(&value.left, output);
            collect_incremental_scan_contracts(&value.right, output);
        }
        ExecNodeKind::Sort(value) => collect_incremental_scan_contracts(&value.input, output),
        ExecNodeKind::TableFunction(value) => {
            collect_incremental_scan_contracts(&value.input, output)
        }
        ExecNodeKind::Analytic(value) => collect_incremental_scan_contracts(&value.input, output),
        ExecNodeKind::SetOp(value) => {
            for input in &value.inputs {
                collect_incremental_scan_contracts(input, output);
            }
        }
        ExecNodeKind::NativeRuntimeFilterConsumer(value) => {
            collect_incremental_scan_contracts(&value.input, output)
        }
        ExecNodeKind::Values(_)
        | ExecNodeKind::ExchangeSource(_)
        | ExecNodeKind::IcebergDeltaScan(_)
        | ExecNodeKind::LookUp(_) => {}
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
            ExecNodeKind::NativeRuntimeFilterConsumer(node) => self.visit(&node.input),
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
            let metadata = metadata_suffix(slot.field().metadata());
            format!(
                "slot={},name={},type={},nullable={},unique_id={},field_schema={}{}",
                slot.slot_id(),
                slot.name(),
                data_type_summary(slot.data_type()),
                slot.nullable(),
                unique_id,
                field_schema_summary(slot.field_schema()),
                metadata,
            )
        })
        .collect::<Vec<_>>()
        .join(";");
    let schema_metadata = schema.arrow_schema_ref();
    let schema_metadata = metadata_suffix(schema_metadata.metadata());
    format!("[{slots}]{schema_metadata}")
}

fn field_schema_summary(field_schema: &ChunkFieldSchema) -> String {
    format!("{field_schema:?}")
}

fn data_type_summary(data_type: &DataType) -> String {
    match data_type {
        DataType::List(field) => format!("List({})", field_summary(field)),
        DataType::LargeList(field) => format!("LargeList({})", field_summary(field)),
        DataType::ListView(field) => format!("ListView({})", field_summary(field)),
        DataType::LargeListView(field) => format!("LargeListView({})", field_summary(field)),
        DataType::FixedSizeList(field, size) => {
            format!("FixedSizeList(size={size},{})", field_summary(field))
        }
        DataType::Struct(fields) => format!(
            "Struct([{}])",
            fields
                .iter()
                .map(|field| field_summary(field))
                .collect::<Vec<_>>()
                .join(",")
        ),
        DataType::Union(fields, mode) => format!(
            "Union(mode={mode:?},[{}])",
            fields
                .iter()
                .map(|(type_id, field)| format!("type_id={type_id},{}", field_summary(field)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        DataType::Dictionary(key, value) => format!(
            "Dictionary(key={},value={})",
            data_type_summary(key),
            data_type_summary(value)
        ),
        DataType::Map(field, sorted) => {
            format!("Map(sorted={sorted},{})", field_summary(field))
        }
        DataType::RunEndEncoded(run_ends, values) => format!(
            "RunEndEncoded(run_ends={},values={})",
            field_summary(run_ends),
            field_summary(values)
        ),
        _ => data_type.to_string(),
    }
}

fn field_summary(field: &Field) -> String {
    format!(
        "field(name={:?},type={},nullable={}{})",
        field.name(),
        data_type_summary(field.data_type()),
        field.is_nullable(),
        metadata_suffix(field.metadata()),
    )
}

fn metadata_suffix(metadata: &std::collections::HashMap<String, String>) -> String {
    if metadata.is_empty() {
        String::new()
    } else {
        format!(",metadata={}", sorted_metadata(metadata))
    }
}

fn sorted_metadata(metadata: &std::collections::HashMap<String, String>) -> String {
    let entries = metadata
        .iter()
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .map(|(key, value)| format!("{key:?}:{value:?}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{entries}}}")
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
    use FragmentSinkAssignmentKind::{DestinationGroups, StarRocksTable, StreamDestinations};
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
        (Requirement::Required(StarRocksTable), FragmentSinkAssignment::StarRocksTable(_)) => {
            Ok(())
        }
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
    use FragmentSinkAssignmentKind::{DestinationGroups, StarRocksTable, StreamDestinations};
    use FragmentSinkAssignmentRequirement::{None, Required};
    match requirement {
        None => "none".to_string(),
        Required(StreamDestinations) => "stream_destinations".to_string(),
        Required(DestinationGroups(count)) => format!("destination_groups(count={})", count.get()),
        Required(StarRocksTable) => "starrocks_table".to_string(),
    }
}

fn sink_assignment_summary(assignment: &FragmentSinkAssignment) -> String {
    match assignment {
        FragmentSinkAssignment::None => "none".to_string(),
        FragmentSinkAssignment::StreamDestinations { .. } => "stream_destinations".to_string(),
        FragmentSinkAssignment::DestinationGroups { groups, .. } => {
            format!("destination_groups(count={})", groups.len())
        }
        FragmentSinkAssignment::StarRocksTable(_) => "starrocks_table".to_string(),
    }
}

fn validate_runtime_filter_params(
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
    // Do NOT require `id_to_prober_params` keys to be a subset of this fragment's local
    // build filters. `id_to_prober_params` is the query-global runtime-filter route table
    // that the coordinator installs on the top (RF-coordinator) fragment for filters built
    // anywhere in the query (StarRocks FE does this in
    // DefaultCoordinator#setGlobalRuntimeFilterParams, targeting the top fragment). The top
    // fragment is typically a result sink that builds no filter at all yet carries the whole
    // route table, so its keys are intentionally not local build filters. Prober endpoint/ID
    // validity is enforced at decode time (RuntimeFilterParams::from_thrift / native decode)
    // and in the internal-service address checks; the per-fragment invariant here is limited
    // to the builder-count sanity check above.
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::common::ids::SlotId;
    use crate::common::types::UniqueId;
    use crate::exec::chunk::{
        Chunk, ChunkFieldSchema, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema,
    };
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use crate::exec::fragment::error::{
        FragmentBindingError, FragmentBindingErrorKind, FragmentBindingTarget,
    };
    use crate::exec::fragment::program::{
        ExchangeInputContract, FragmentContractVersion, FragmentNodeId, FragmentProgram,
        FragmentProgramOptions, FragmentSinkSpec, RuntimeFilterContract, RuntimeFilterId,
        ScanAssignmentKind, ScanSourceContract,
    };
    use crate::exec::fragment::sink::{
        DataStreamSinkBranchProgram, DataStreamSinkProgram, FragmentSinkProgram,
        MultiCastDataStreamSinkProgram, SplitDataStreamSinkProgram,
    };
    use crate::exec::node::BoxedExecIter;
    use crate::exec::node::exchange_source::ExchangeSourceNode;
    #[cfg(feature = "compat")]
    use crate::exec::node::fetch::FetchNode;
    use crate::exec::node::filter::FilterNode;
    use crate::exec::node::iceberg_delta_scan::{
        ApplyKeySource, BaseTableIdent, IcebergDeltaScanNode, IcebergDeltaTablePayload,
        IcebergRuntimeHandles,
    };
    use crate::exec::node::join::{
        JoinDistributionMode, JoinNode, JoinRuntimeFilterExecution, JoinType,
    };
    use crate::exec::node::runtime_filter::NativeRuntimeFilterConsumerNode;
    use crate::exec::node::scan::{
        RuntimeFilterContext, ScanMorsel, ScanMorsels, ScanNode, ScanOp,
    };
    use crate::exec::node::set_op::{SetOpKind, SetOpNode};
    use crate::exec::node::union_all::UnionAllNode;
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
    use crate::exec::operators::DataStreamPartitionType;
    use crate::runtime::endpoint::RuntimeEndpoint;
    use crate::runtime::endpoint::RuntimeFilterProberDestination;
    use crate::runtime::exchange::ExchangeKey;
    use crate::runtime::fragment::instance::{
        BackendNum, ExchangeInputAssignment, ExchangeInputAssignments, FragmentInstanceId,
        FragmentInstanceSpec, FragmentRuntimeOptions, FragmentSinkAssignment, ScanAssignments,
        StarRocksTableSinkAssignment,
    };
    use crate::runtime::profile::RuntimeProfile;
    use crate::runtime::query_context::QueryId;
    use crate::runtime::query_options::QueryOptions;
    use crate::runtime::runtime_filter_params::RuntimeFilterParams;
    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use novarocks_types::logical::{LogicalType, field_with_logical_type};

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

    fn schema_with_metadata(
        slot_id: u32,
        field: Field,
        schema_metadata: HashMap<String, String>,
    ) -> ChunkSchemaRef {
        let slot = ChunkSlotSchema::from_field(SlotId::new(slot_id), &field, None)
            .expect("chunk slot schema");
        Arc::new(
            ChunkSchema::try_new_with_schema_metadata(vec![slot], schema_metadata)
                .expect("chunk schema with metadata"),
        )
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

    fn native_delta_scan_plan(node_id: i32) -> ExecPlan {
        ExecPlan {
            arena: ExprArena::default(),
            root: ExecNode {
                kind: ExecNodeKind::IcebergDeltaScan(IcebergDeltaScanNode {
                    base_table_ident: BaseTableIdent {
                        catalog: "rest".to_string(),
                        namespace: "db".to_string(),
                        table: "t".to_string(),
                    },
                    table_location: "file:///tmp/novarocks-pbf1c-delta".to_string(),
                    from_snapshot_id: 1,
                    to_snapshot_id: 2,
                    output_chunk_schema: Arc::new(ChunkSchema::empty()),
                    apply_key_source: ApplyKeySource::BaseRowId,
                    change_files: Vec::new(),
                    object_store_config: None,
                    iceberg_runtime: Arc::new(IcebergRuntimeHandles::new(
                        IcebergDeltaTablePayload {
                            table_location: "file:///tmp/novarocks-pbf1c-delta".to_string(),
                            data_columns: Vec::new(),
                        },
                        None,
                    )),
                    node_id,
                    native_runtime_filter_specs: Vec::new(),
                }),
            },
        }
    }

    fn result_sink() -> FragmentSinkSpec {
        FragmentSinkSpec::try_new(FragmentSinkProgram::Result).expect("result sink")
    }

    fn data_stream_sink() -> FragmentSinkSpec {
        FragmentSinkSpec::try_new(FragmentSinkProgram::DataStream(
            DataStreamSinkProgram::try_new(
                9,
                Vec::new(),
                DataStreamPartitionType::Unpartitioned,
                Vec::new(),
                vec![SlotId::new(1)],
                None,
                ExprArena::default(),
            )
            .expect("data stream program"),
        ))
        .expect("data stream sink")
    }

    fn multicast_sink(branch_count: usize) -> FragmentSinkSpec {
        let branches = (0..branch_count)
            .map(|index| {
                DataStreamSinkBranchProgram::try_new(
                    i32::try_from(index).expect("branch index fits i32"),
                    Vec::new(),
                    DataStreamPartitionType::Unpartitioned,
                    Vec::new(),
                    vec![SlotId::new(1)],
                    None,
                )
                .expect("data stream branch")
            })
            .collect();
        FragmentSinkSpec::try_new(FragmentSinkProgram::MultiCastDataStream(
            MultiCastDataStreamSinkProgram::try_new(branches, ExprArena::default())
                .expect("multicast program"),
        ))
        .expect("multicast sink")
    }

    fn split_sink(branch_count: usize) -> FragmentSinkSpec {
        let mut arena = ExprArena::default();
        let split_exprs = (0..branch_count)
            .map(|_| {
                arena.push_typed(
                    ExprNode::Literal(LiteralValue::Bool(true)),
                    DataType::Boolean,
                )
            })
            .collect();
        let branches = (0..branch_count)
            .map(|index| {
                DataStreamSinkBranchProgram::try_new(
                    i32::try_from(index).expect("branch index fits i32"),
                    Vec::new(),
                    DataStreamPartitionType::Unpartitioned,
                    Vec::new(),
                    vec![SlotId::new(1)],
                    None,
                )
                .expect("split stream branch")
            })
            .collect();
        FragmentSinkSpec::try_new(FragmentSinkProgram::SplitDataStream(
            SplitDataStreamSinkProgram::try_new(branches, split_exprs, arena)
                .expect("split stream program"),
        ))
        .expect("split stream sink")
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
            "exchange node 20 expected schema [slot=1,name=v,type=Int32,nullable=false,unique_id=none,field_schema=ChunkFieldSchema { logical_type: None, children: [] }], got [slot=1,name=v,type=Int32,nullable=true,unique_id=none,field_schema=ChunkFieldSchema { logical_type: None, children: [] }]"
        );
    }

    #[test]
    fn reports_field_metadata_only_exchange_schema_mismatch() {
        let id = FragmentNodeId::new(20);
        let expected = schema_with_metadata(
            1,
            Field::new("v", DataType::Int32, false).with_metadata(HashMap::from([(
                "contract".to_string(),
                "expected".to_string(),
            )])),
            HashMap::new(),
        );
        let actual = schema_with_metadata(
            1,
            Field::new("v", DataType::Int32, false).with_metadata(HashMap::from([(
                "contract".to_string(),
                "actual".to_string(),
            )])),
            HashMap::new(),
        );
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: exchange_node(20, actual, uid(5, 8)),
            },
            result_sink(),
            BTreeMap::new(),
            BTreeMap::from([(id, expected)]),
            BTreeSet::new(),
        );
        let error = assert_error(
            FragmentSubmission::try_new(
                program,
                instance_with(
                    FragmentContractVersion::CURRENT,
                    query_id(1, 2),
                    uid(1, 341),
                    BTreeMap::new(),
                    BTreeMap::from([(id, 1)]),
                    FragmentSinkAssignment::None,
                    BTreeMap::new(),
                    BTreeMap::new(),
                ),
            ),
            FragmentBindingTarget::ExchangeNode(20),
            FragmentBindingErrorKind::SchemaMismatch,
        );
        assert_eq!(
            error.detail(),
            "exchange node 20 expected schema [slot=1,name=v,type=Int32,nullable=false,unique_id=none,field_schema=ChunkFieldSchema { logical_type: None, children: [] },metadata={\"contract\":\"expected\"}], got [slot=1,name=v,type=Int32,nullable=false,unique_id=none,field_schema=ChunkFieldSchema { logical_type: None, children: [] },metadata={\"contract\":\"actual\"}]"
        );
    }

    #[test]
    fn reports_field_schema_only_exchange_schema_mismatch() {
        let id = FragmentNodeId::new(20);
        let field = Field::new("v", DataType::Int32, false);
        let expected_field_schema = ChunkFieldSchema::empty();
        let actual_field_schema = ChunkFieldSchema::from_field(&field_with_logical_type(
            Field::new("logical", DataType::Utf8, true),
            LogicalType::Json,
        ))
        .expect("logical field schema");
        let with_field_schema = |field_schema| {
            Arc::new(
                ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                    SlotId::new(1),
                    field.clone(),
                    Some(field_schema),
                    None,
                )])
                .expect("chunk schema with explicit field schema"),
            )
        };
        let expected = with_field_schema(expected_field_schema);
        let actual = with_field_schema(actual_field_schema);
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: exchange_node(20, actual, uid(5, 8)),
            },
            result_sink(),
            BTreeMap::new(),
            BTreeMap::from([(id, expected)]),
            BTreeSet::new(),
        );
        let error = assert_error(
            FragmentSubmission::try_new(
                program,
                instance_with(
                    FragmentContractVersion::CURRENT,
                    query_id(1, 2),
                    uid(1, 344),
                    BTreeMap::new(),
                    BTreeMap::from([(id, 1)]),
                    FragmentSinkAssignment::None,
                    BTreeMap::new(),
                    BTreeMap::new(),
                ),
            ),
            FragmentBindingTarget::ExchangeNode(20),
            FragmentBindingErrorKind::SchemaMismatch,
        );
        assert_eq!(
            error.detail(),
            "exchange node 20 expected schema [slot=1,name=v,type=Int32,nullable=false,unique_id=none,field_schema=ChunkFieldSchema { logical_type: None, children: [] }], got [slot=1,name=v,type=Int32,nullable=false,unique_id=none,field_schema=ChunkFieldSchema { logical_type: Some(Json), children: [] }]"
        );
    }

    #[test]
    fn reports_nested_type_metadata_in_sorted_exact_detail() {
        let id = FragmentNodeId::new(20);
        let nested = |last: &str| {
            schema_with_metadata(
                1,
                Field::new(
                    "v",
                    DataType::Struct(Fields::from(vec![Arc::new(
                        Field::new("item", DataType::Int64, true).with_metadata(HashMap::from([
                            ("zeta".to_string(), last.to_string()),
                            ("alpha".to_string(), "first".to_string()),
                        ])),
                    )])),
                    false,
                ),
                HashMap::new(),
            )
        };
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: exchange_node(20, nested("actual"), uid(5, 8)),
            },
            result_sink(),
            BTreeMap::new(),
            BTreeMap::from([(id, nested("expected"))]),
            BTreeSet::new(),
        );
        let error = assert_error(
            FragmentSubmission::try_new(
                program,
                instance_with(
                    FragmentContractVersion::CURRENT,
                    query_id(1, 2),
                    uid(1, 342),
                    BTreeMap::new(),
                    BTreeMap::from([(id, 1)]),
                    FragmentSinkAssignment::None,
                    BTreeMap::new(),
                    BTreeMap::new(),
                ),
            ),
            FragmentBindingTarget::ExchangeNode(20),
            FragmentBindingErrorKind::SchemaMismatch,
        );
        assert_eq!(
            error.detail(),
            "exchange node 20 expected schema [slot=1,name=v,type=Struct([field(name=\"item\",type=Int64,nullable=true,metadata={\"alpha\":\"first\",\"zeta\":\"expected\"})]),nullable=false,unique_id=none,field_schema=ChunkFieldSchema { logical_type: None, children: [ChunkFieldSchema { logical_type: None, children: [] }] }], got [slot=1,name=v,type=Struct([field(name=\"item\",type=Int64,nullable=true,metadata={\"alpha\":\"first\",\"zeta\":\"actual\"})]),nullable=false,unique_id=none,field_schema=ChunkFieldSchema { logical_type: None, children: [ChunkFieldSchema { logical_type: None, children: [] }] }]"
        );
    }

    #[test]
    fn reports_schema_metadata_only_exchange_schema_mismatch() {
        let id = FragmentNodeId::new(20);
        let expected = schema_with_metadata(
            1,
            Field::new("v", DataType::Int32, false),
            HashMap::from([("owner".to_string(), "expected".to_string())]),
        );
        let actual = schema_with_metadata(
            1,
            Field::new("v", DataType::Int32, false),
            HashMap::from([("owner".to_string(), "actual".to_string())]),
        );
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: exchange_node(20, actual, uid(5, 8)),
            },
            result_sink(),
            BTreeMap::new(),
            BTreeMap::from([(id, expected)]),
            BTreeSet::new(),
        );
        let error = assert_error(
            FragmentSubmission::try_new(
                program,
                instance_with(
                    FragmentContractVersion::CURRENT,
                    query_id(1, 2),
                    uid(1, 343),
                    BTreeMap::new(),
                    BTreeMap::from([(id, 1)]),
                    FragmentSinkAssignment::None,
                    BTreeMap::new(),
                    BTreeMap::new(),
                ),
            ),
            FragmentBindingTarget::ExchangeNode(20),
            FragmentBindingErrorKind::SchemaMismatch,
        );
        assert_eq!(
            error.detail(),
            "exchange node 20 expected schema [slot=1,name=v,type=Int32,nullable=false,unique_id=none,field_schema=ChunkFieldSchema { logical_type: None, children: [] }],metadata={\"owner\":\"expected\"}, got [slot=1,name=v,type=Int32,nullable=false,unique_id=none,field_schema=ChunkFieldSchema { logical_type: None, children: [] }],metadata={\"owner\":\"actual\"}"
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
            data_stream_sink(),
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
            data_stream_sink(),
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
            multicast_sink(2),
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
            multicast_sink(2),
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
            multicast_sink(2),
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
    fn split_sink_validates_destination_groups_only_in_submission() {
        let program = program_with(
            values_plan(7),
            split_sink(2),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeSet::new(),
        );

        assert_error(
            FragmentSubmission::try_new(Arc::clone(&program), empty_instance(46)),
            FragmentBindingTarget::Sink,
            FragmentBindingErrorKind::MissingAssignment,
        );
        assert_error(
            FragmentSubmission::try_new(
                Arc::clone(&program),
                instance_with(
                    FragmentContractVersion::CURRENT,
                    query_id(1, 2),
                    uid(1, 47),
                    BTreeMap::new(),
                    BTreeMap::new(),
                    FragmentSinkAssignment::DestinationGroups {
                        groups: vec![Vec::new()],
                        sender_id: None,
                    },
                    BTreeMap::new(),
                    BTreeMap::new(),
                ),
            ),
            FragmentBindingTarget::Sink,
            FragmentBindingErrorKind::InvalidAssignment,
        );
        assert_error(
            FragmentSubmission::try_new(
                Arc::clone(&program),
                instance_with(
                    FragmentContractVersion::CURRENT,
                    query_id(1, 2),
                    uid(1, 48),
                    BTreeMap::new(),
                    BTreeMap::new(),
                    FragmentSinkAssignment::StreamDestinations {
                        destinations: Vec::new(),
                        sender_id: None,
                    },
                    BTreeMap::new(),
                    BTreeMap::new(),
                ),
            ),
            FragmentBindingTarget::Sink,
            FragmentBindingErrorKind::WrongAssignmentKind,
        );
        FragmentSubmission::try_new(
            program,
            instance_with(
                FragmentContractVersion::CURRENT,
                query_id(1, 2),
                uid(1, 49),
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
        .expect("matching split destination groups");
    }

    #[test]
    fn starrocks_table_sink_requires_explicit_assignment_kind() {
        assert_eq!(
            FragmentSinkAssignmentRequirement::Required(FragmentSinkAssignmentKind::StarRocksTable),
            FragmentSinkAssignmentRequirement::Required(FragmentSinkAssignmentKind::StarRocksTable)
        );
        let assignment = FragmentSinkAssignment::StarRocksTable(StarRocksTableSinkAssignment::new(
            41,
            uid(5, 6),
            Some(RuntimeEndpoint::new("fe", 9020).expect("frontend endpoint")),
        ));
        assert!(matches!(
            assignment,
            FragmentSinkAssignment::StarRocksTable(_)
        ));
    }

    #[test]
    fn accepts_query_global_prober_route_without_local_build_filter() {
        // The top fragment acts as the runtime-filter coordinator: it carries the
        // query-global id_to_prober_params route table (plus builder counts) for filters
        // that are built in *other* fragments, without building any of them locally. This
        // mirrors StarRocks FE `setGlobalRuntimeFilterParams`, which installs the whole
        // route table on the top fragment (DefaultCoordinator#setId_to_prober_params), not
        // on each build-side fragment. Requiring every prober-routed filter to have a local
        // builder is therefore wrong and rejects valid FE plans (e.g. sc07 skew split join).
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
            uid(1, 50),
            BTreeMap::new(),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::from([
                (13, vec![prober_destination()]),
                (17, vec![prober_destination()]),
            ]),
            BTreeMap::from([(13, 2), (17, 3)]),
        );
        FragmentSubmission::try_new(program, instance)
            .expect("query-global prober route on coordinator fragment");
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

    #[test]
    fn regular_unary_wrapper_is_traversed() {
        let id = FragmentNodeId::new(10);
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: ExecNode {
                    kind: ExecNodeKind::Filter(FilterNode {
                        input: Box::new(scan_node(Some(10))),
                        node_id: 30,
                        predicate: crate::exec::expr::ExprId(0),
                    }),
                },
            },
            result_sink(),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 63),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        FragmentSubmission::try_new(program, instance).expect("filter child scan");
    }

    #[test]
    fn native_runtime_filter_consumer_wrapper_is_traversed() {
        let id = FragmentNodeId::new(10);
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: ExecNode {
                    kind: ExecNodeKind::NativeRuntimeFilterConsumer(
                        NativeRuntimeFilterConsumerNode {
                            input: Box::new(scan_node(Some(10))),
                            owner_node_id: 30,
                            bindings: Vec::new(),
                        },
                    ),
                },
            },
            result_sink(),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 67),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );

        FragmentSubmission::try_new(program, instance)
            .expect("native runtime filter consumer child scan");
    }

    #[test]
    fn binary_right_child_is_traversed() {
        let id = FragmentNodeId::new(10);
        let empty_schema = Arc::new(ChunkSchema::empty());
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: ExecNode {
                    kind: ExecNodeKind::Join(JoinNode {
                        left: Box::new(values_plan(7).root),
                        right: Box::new(scan_node(Some(10))),
                        node_id: 30,
                        join_type: JoinType::Inner,
                        distribution_mode: JoinDistributionMode::Partitioned,
                        left_chunk_schema: Arc::clone(&empty_schema),
                        right_chunk_schema: Arc::clone(&empty_schema),
                        join_scope_chunk_schema: empty_schema,
                        probe_keys: Vec::new(),
                        build_keys: Vec::new(),
                        eq_null_safe: Vec::new(),
                        residual_predicate: None,
                        runtime_filter_execution: JoinRuntimeFilterExecution::Native {
                            producers: Vec::new(),
                        },
                    }),
                },
            },
            result_sink(),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 64),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        FragmentSubmission::try_new(program, instance).expect("join right child scan");
    }

    #[test]
    fn set_op_children_are_traversed() {
        let id = FragmentNodeId::new(10);
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: ExecNode {
                    kind: ExecNodeKind::SetOp(SetOpNode {
                        kind: SetOpKind::Intersect,
                        inputs: vec![values_plan(7).root, scan_node(Some(10))],
                        node_id: 30,
                        output_chunk_schema: Arc::new(ChunkSchema::empty()),
                    }),
                },
            },
            result_sink(),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 65),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        FragmentSubmission::try_new(program, instance).expect("set op child scan");
    }

    #[test]
    fn iceberg_delta_scan_required_identity_is_collected() {
        let id = FragmentNodeId::new(44);
        let program = program_with(
            native_delta_scan_plan(44),
            result_sink(),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            BTreeSet::new(),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query_id(1, 2),
            uid(1, 66),
            BTreeMap::from([(id, ScanAssignmentKind::File)]),
            BTreeMap::new(),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        FragmentSubmission::try_new(program, instance).expect("Iceberg delta scan identity");
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

    use std::sync::atomic::{AtomicI64, Ordering};

    use crate::runtime::exchange::snapshot_receiver_state;
    use crate::runtime::query_context::query_context_manager;
    use crate::runtime::query_state::in_flight_table;
    use crate::runtime::result_buffer::{self, FetchErrorKind, TryFetchResult};
    use crate::runtime::runtime_filter_observability::{QueryKey, RuntimeFilterLifecycleRegistry};

    static NEXT_TEST_ID: AtomicI64 = AtomicI64::new(8_500_000_000_000_000_000);

    fn assert_runtime_state_absent(
        query: QueryId,
        finst: UniqueId,
        exchange_key: ExchangeKey,
        rf_key: QueryKey,
    ) {
        let manager = query_context_manager();
        assert!(manager.query_mem_tracker(query).is_none());
        assert!(manager.query_id_by_finst(finst).is_none());
        assert!(in_flight_table().state(query).is_none());
        let TryFetchResult::Error(error) = result_buffer::try_fetch(finst) else {
            panic!("missing result buffer entry must return an error");
        };
        assert!(matches!(error.kind, FetchErrorKind::NotFound));
        assert!(snapshot_receiver_state(exchange_key).is_none());
        assert!(
            RuntimeFilterLifecycleRegistry::global()
                .snapshot(rf_key)
                .is_none()
        );
    }

    #[test]
    fn malformed_submission_does_not_touch_runtime_state_or_retain_program_arc() {
        let unique = NEXT_TEST_ID.fetch_add(10, Ordering::Relaxed);
        let query = query_id(unique, unique + 1);
        let finst = uid(unique, unique + 2);
        let exchange_id = FragmentNodeId::new(41);
        let exchange_key = ExchangeKey {
            finst_id_hi: finst.hi,
            finst_id_lo: finst.lo,
            node_id: 41,
        };
        let rf_key = QueryKey::from_hi_lo(query.hi, query.lo);
        let expected = schema(1, true);
        let program = program_with(
            ExecPlan {
                arena: ExprArena::default(),
                root: exchange_node(41, Arc::clone(&expected), finst),
            },
            result_sink(),
            BTreeMap::new(),
            BTreeMap::from([(exchange_id, expected)]),
            BTreeSet::from([RuntimeFilterId::new(11)]),
        );
        let instance = instance_with(
            FragmentContractVersion::CURRENT,
            query,
            finst,
            BTreeMap::new(),
            BTreeMap::from([(exchange_id, 1)]),
            FragmentSinkAssignment::None,
            BTreeMap::new(),
            BTreeMap::from([(13, 0)]),
        );

        assert_runtime_state_absent(query, finst, exchange_key, rf_key);
        let before = Arc::strong_count(&program);
        assert_error(
            FragmentSubmission::try_new(Arc::clone(&program), instance),
            FragmentBindingTarget::RuntimeFilter(13),
            FragmentBindingErrorKind::InvalidAssignment,
        );
        assert_eq!(Arc::strong_count(&program), before);
        assert_runtime_state_absent(query, finst, exchange_key, rf_key);
    }
}
