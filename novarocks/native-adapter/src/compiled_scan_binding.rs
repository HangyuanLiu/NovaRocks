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

//! Task binding of a compiled program's provider scans.
//!
//! A compiled Scan carries its whole frozen read as one provider-validated
//! recipe: the binding, relation and column payloads, the ordered
//! assignments, the predicate responsibilities and the public facts. This
//! binder turns each recipe into the Task's typed scan source. It reads no
//! generated DTO and seals nothing again: the relation and columns are
//! decoded from the recipe's own payloads by the installed read execution of
//! exactly the recipe's binding, and the source's output is the compiled
//! layout, which is the provider's public schema.
//!
//! Splits stay runtime work. The source polls the Task's split queue of its
//! physical scan node, and that node is registered with the Task's read
//! context so split delivery decodes against the same installed execution.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use novarocks_connector_contract::{
    ConnectorReadDistribution, ConnectorReadProgramRecipe, ConnectorReadRelationKind,
    FrozenConnectorRead, FrozenConnectorScan, ScanColumnId,
};
use novarocks_execution::exec::chunk::ChunkSchema;
use novarocks_execution::exec::node::scan::ScanSource;
use novarocks_execution::runtime::fragment::scan::CompiledScanSources;
use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_execution::runtime_filter::{
    RuntimeFilterConsumerContract, RuntimeFilterContractViolation,
    RuntimeFilterContractViolationKind, RuntimeFilterSessionRef,
};
use novarocks_local_program::{LocalProgram, ProgramNodeKind, StaticLayout};
use novarocks_spi::connector::read_stack::runtime::ConnectorReadAssignment;
use novarocks_spi::connector::read_stack::{
    Assignment, CompleteAllDynamicFilter, ConnectorDataCacheOptions,
    ConnectorPageSourceProviderOptions, ConnectorReadColumnHandle, ConnectorReadDynamicFilter,
    ConnectorReadWorkSource, ConnectorSourceOperations,
};
use novarocks_spi::connector::{
    ConnectorRangeScope, ConnectorRequestContext, ConnectorStopView,
    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};
use novarocks_types::UniqueId;
use novarocks_worker::typed_connector_runtime::TypedConnectorScanSource;
use novarocks_worker::typed_scan_filter::TypedScanLiveDynamicFilterFactory;
use novarocks_worker::{TypedConnectorReadDescriptor, TypedScanRuntime};

/// The Task facts every compiled scan of one task binds with.
pub(crate) struct CompiledScanTask<'a> {
    pub(crate) runtime: &'a TypedScanRuntime,
    pub(crate) fragment_instance_id: UniqueId,
    pub(crate) query_options: &'a QueryOptions,
    pub(crate) stop: ConnectorStopView,
}

/// Why a compiled scan could not be bound to its Task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledScanBindingError {
    scan_node: Option<i32>,
    detail: String,
}

impl CompiledScanBindingError {
    fn program(detail: impl Into<String>) -> Self {
        Self {
            scan_node: None,
            detail: detail.into(),
        }
    }

    fn at(scan_node: i32, detail: impl Into<String>) -> Self {
        Self {
            scan_node: Some(scan_node),
            detail: detail.into(),
        }
    }

    pub const fn scan_node(&self) -> Option<i32> {
        self.scan_node
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for CompiledScanBindingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.scan_node {
            Some(node) => write!(f, "compiled scan node {node}: {}", self.detail),
            None => f.write_str(&self.detail),
        }
    }
}

impl std::error::Error for CompiledScanBindingError {}

/// Bind every compiled Scan of `program` to the Task: one typed scan source
/// per Scan node, keyed by its local node. Every refusal precedes the first
/// read-execution registration of its scan.
pub(crate) fn bind_compiled_scans(
    program: &LocalProgram,
    task: &CompiledScanTask<'_>,
) -> Result<CompiledScanSources, CompiledScanBindingError> {
    let nodes = program.graph().nodes();
    let mut sources = CompiledScanSources::new();
    for (id, input) in program.scan_inputs() {
        let node = nodes.get(id.index()).ok_or_else(|| {
            CompiledScanBindingError::program(format!(
                "compiled scan address names absent local node {}",
                id.index()
            ))
        })?;
        let scan_node = i32::try_from(input.scan_node).map_err(|_| {
            CompiledScanBindingError::program(format!(
                "compiled scan node {} exceeds i32",
                input.scan_node
            ))
        })?;
        let ProgramNodeKind::Scan { source, .. } = node.kind() else {
            return Err(CompiledScanBindingError::at(
                scan_node,
                format!("local node {} is not a Scan", id.index()),
            ));
        };
        let recipe = source.compiled().ok_or_else(|| {
            CompiledScanBindingError::at(scan_node, "the Scan carries no compiled provider read")
        })?;
        let bound = bind_compiled_scan(scan_node, recipe, node.output_layout(), task)?;
        sources.insert(*id, bound);
    }
    Ok(sources)
}

/// Bind one compiled provider read as the typed scan source of `scan_node`.
pub(crate) fn bind_compiled_scan(
    scan_node: i32,
    recipe: &ConnectorReadProgramRecipe,
    layout: &StaticLayout,
    task: &CompiledScanTask<'_>,
) -> Result<Arc<dyn ScanSource>, CompiledScanBindingError> {
    let refused = |detail: String| CompiledScanBindingError::at(scan_node, detail);
    let frozen: &FrozenConnectorRead = recipe.frozen();
    let scan = frozen.scan();
    let draft = scan.recipe();

    // Everything the recipe and this backend must agree on is decided from
    // the recipe alone, before any installed provider is consulted, so a read
    // this slice cannot run is refused on its own terms.
    match draft.relation().kind() {
        ConnectorReadRelationKind::Table
        | ConnectorReadRelationKind::ChangeWindow
        | ConnectorReadRelationKind::SystemTable
        | ConnectorReadRelationKind::TableExecute => {}
        kind @ (ConnectorReadRelationKind::TableFunction
        | ConnectorReadRelationKind::MergeTable) => {
            return Err(refused(format!(
                "provider relation {kind:?} is not read by a compiled scan"
            )));
        }
    }
    match scan.work_source() {
        ConnectorReadWorkSource::RuntimeSplits => {}
        ConnectorReadWorkSource::WholeRelation => {
            return Err(refused(
                "a whole-relation compiled scan is not executable yet".to_string(),
            ));
        }
    }
    if !scan.dynamic_filters().is_empty() {
        return Err(refused(
            "a compiled scan subscribes to no runtime filter yet".to_string(),
        ));
    }
    // `slot_ids[i]` names page channel `i`, and a channel exists for each
    // assignment, so the layout is exactly one slot per assignment.
    if layout.slots().len() != scan.assignments().len() {
        return Err(refused(format!(
            "the compiled layout has {} slots for {} assignments",
            layout.slots().len(),
            scan.assignments().len()
        )));
    }
    let output_schema = ChunkSchema::from_compiled_layout(layout)
        .map_err(|error| refused(format!("compiled scan layout: {error}")))?;

    let execution = task
        .runtime
        .catalog_read_execution(draft.binding().catalog_handle())
        .map_err(|error| refused(format!("no installed read execution: {error}")))?;
    if draft.binding() != execution.binding() {
        return Err(refused(
            "compiled scan binding does not match the installed read execution".to_string(),
        ));
    }
    let decoder = execution.decoder();
    let relation = decoder
        .decode_relation_payload(draft.relation())
        .map_err(|error| refused(format!("provider relation is not decodable: {error}")))?;
    if relation.kind() != draft.relation().kind() {
        return Err(refused(format!(
            "provider decoded relation {:?} from a frozen {:?} relation",
            relation.kind(),
            draft.relation().kind()
        )));
    }
    let columns = draft
        .columns()
        .iter()
        .map(|payload| decoder.decode_column_payload(payload))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| refused(format!("provider column is not decodable: {error}")))?;
    require_first_ordinal_facts(frozen, &columns).map_err(refused)?;
    let assignments = scan
        .assignments()
        .iter()
        .zip(columns)
        .map(|(assignment, column)| {
            Assignment::try_new(assignment.variable(), column, assignment.value_type())
        })
        .collect::<Result<Vec<ConnectorReadAssignment>, _>>()
        .map_err(|error| refused(format!("scan assignment: {error}")))?;
    let dynamic_filter = complete_all_dynamic_filter(scan, &assignments);
    let descriptor =
        TypedConnectorReadDescriptor::new(relation.table().clone(), assignments, dynamic_filter);

    let execution_id = task.runtime.execution_id();
    let range_scope = ConnectorRangeScope::try_new(
        execution_id.query_id().high(),
        execution_id.query_id().low(),
        execution_id.attempt_id().get(),
        task.fragment_instance_id.high(),
        task.fragment_instance_id.low(),
        scan_node,
    )
    .map_err(|error| refused(format!("scan range scope: {error}")))?;
    let (_, query_expire) = novarocks_execution::runtime::query_options::query_expire_durations(
        Some(task.query_options),
    );
    // One execution source per Task and scan node: its requests' I/O is
    // scheduled under this scope and admitted to these operations.
    let request = ConnectorRequestContext::try_new(
        std::time::Instant::now() + query_expire,
        task.stop.clone(),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map(|request| {
        request
            .with_storage_resolver(task.runtime.storage_resolver())
            .with_execution_source(range_scope, ConnectorSourceOperations::new())
    })
    .map_err(|error| refused(format!("connector request: {error}")))?;
    let reader_policy = reader_policy(task.query_options).map_err(refused)?;

    task.runtime
        .register_read_execution(scan_node, execution.clone())
        .map_err(refused)?;
    // The provider is opened only when the source is bound, after the Task's
    // admission installed its fragment tracker.
    let provider_factory = execution.provider_factory();
    let runtime = task.runtime.clone();
    let provider_request = request.clone();
    let page_source_provider = Arc::new(move || {
        let resources =
            runtime.admitted_connector_resources_for_bound_task(runtime.execution_id())?;
        provider_factory
            .create_page_source_provider(&provider_request, resources, reader_policy)
            .map_err(|error| error.to_string())
    });
    Ok(Arc::new(TypedConnectorScanSource::new_deferred(
        descriptor,
        page_source_provider,
        task.runtime.session(),
        request,
        task.runtime.queues(),
        scan_node,
        layout.slots().to_vec(),
        output_schema,
        task.runtime.runtime_filter(),
        Arc::new(NoLiveDynamicFilter),
        crate::debug_environment::debug_emit_connector_reader_marker(),
        task.runtime.stream_host().clone(),
    )))
}

/// A provider column assigned at several ordinals is one column, and the
/// frozen read states each fact about it once, at the first ordinal that
/// assigns it; every later ordinal reads the same values. A predicate domain
/// or a read property addressed to a later ordinal of a decoded column is
/// therefore not that statement: it is refused, never merged onto the column.
fn require_first_ordinal_facts(
    frozen: &FrozenConnectorRead,
    columns: &[ConnectorReadColumnHandle],
) -> Result<(), String> {
    let mut first = BTreeMap::<&ConnectorReadColumnHandle, ScanColumnId>::new();
    for (ordinal, column) in columns.iter().enumerate() {
        first.entry(column).or_insert(ScanColumnId::new(ordinal));
    }
    let check = |fact: &str, ordinal: ScanColumnId| -> Result<(), String> {
        let column = columns
            .get(ordinal.index())
            .ok_or_else(|| format!("{fact} names absent scan ordinal {}", ordinal.index()))?;
        let canonical = first[column];
        if canonical != ordinal {
            return Err(format!(
                "{fact} is stated at ordinal {} of a provider column first assigned at ordinal {}",
                ordinal.index(),
                canonical.index()
            ));
        }
        Ok(())
    };
    let scan = frozen.scan();
    for (fact, domain) in [
        ("enforced predicate", scan.enforced_predicate()),
        ("unenforced predicate", scan.unenforced_predicate()),
    ] {
        if let Some(domains) = domain.domains() {
            for ordinal in domains.keys() {
                check(fact, *ordinal)?;
            }
        }
    }
    let properties = frozen.public_facts().source().properties();
    match properties.distribution() {
        ConnectorReadDistribution::Unconstrained
        | ConnectorReadDistribution::Singleton
        | ConnectorReadDistribution::RoundRobin => {}
        ConnectorReadDistribution::Hash { keys, .. }
        | ConnectorReadDistribution::BucketShuffle { keys, .. } => {
            for key in keys.iter() {
                check("distribution key", *key)?;
            }
        }
    }
    for key in properties.ordering() {
        check("ordering key", *key.column())?;
    }
    Ok(())
}

/// The scan's dynamic filter as its frozen contract states it, for a scan
/// that receives no live feedback: complete from the start, over the columns
/// its dynamic-filter variables assign. No dynamic filter is complete-all.
fn complete_all_dynamic_filter(
    scan: &FrozenConnectorScan,
    assignments: &[ConnectorReadAssignment],
) -> Arc<ConnectorReadDynamicFilter> {
    let filtered = scan
        .dynamic_filters()
        .iter()
        .map(|filter| filter.variable())
        .collect::<BTreeSet<_>>();
    let covered = assignments
        .iter()
        .filter(|assignment| filtered.contains(assignment.variable()))
        .map(|assignment| assignment.column().clone())
        .collect();
    Arc::new(CompleteAllDynamicFilter::new(covered))
}

/// A compiled scan subscribes to no live runtime filter: its source never
/// records a consumer contract, so a build request is a contract violation.
struct NoLiveDynamicFilter;

impl TypedScanLiveDynamicFilterFactory for NoLiveDynamicFilter {
    fn build(
        &self,
        _session: Option<&RuntimeFilterSessionRef>,
        _contracts: &BTreeMap<u32, RuntimeFilterConsumerContract>,
    ) -> Result<Arc<ConnectorReadDynamicFilter>, RuntimeFilterContractViolation> {
        Err(RuntimeFilterContractViolation::new(
            RuntimeFilterContractViolationKind::ContractMismatch,
            "a compiled scan subscribes to no live runtime filter",
        ))
    }
}

/// The reader policy a Task's query options give its connector page sources.
fn reader_policy(options: &QueryOptions) -> Result<ConnectorPageSourceProviderOptions, String> {
    let cache = novarocks_execution::runtime::cache::ExecutionCacheOptions::from_query_options(
        Some(options),
    )?;
    Ok(ConnectorPageSourceProviderOptions {
        enable_parquet_reader_page_index: options.enable_parquet_reader_page_index(),
        data_cache: ConnectorDataCacheOptions {
            enable_scan_datacache: cache.enable_scan_datacache,
            enable_populate_datacache: cache.enable_populate_datacache,
            enable_datacache_async_populate_mode: cache.enable_datacache_async_populate_mode,
            enable_datacache_io_adaptor: cache.enable_datacache_io_adaptor,
            enable_cache_select: cache.enable_cache_select,
            datacache_evict_probability: cache.datacache_evict_probability,
            datacache_priority: cache.datacache_priority,
            datacache_ttl_seconds: cache.datacache_ttl_seconds,
            datacache_sharing_work_period: cache.datacache_sharing_work_period,
        },
    })
}

#[cfg(test)]
#[path = "compiled_scan_binding_tests.rs"]
pub(crate) mod tests;
