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

//! What a completed plan's scans need in order to go on the wire.
//!
//! The plan says which provider relation each scan reads and in what order its
//! columns come back. It deliberately does not say what those columns are
//! called, what the provider agreed to enforce, or how to address the relation
//! in a provider's own encoding - those are private to the freeze that produced
//! the scan, and the freeze kept them.
//!
//! This joins the two. Every scan in the plan must find its frozen read, and
//! every frozen read must belong to a scan; that is already guaranteed by the
//! pairing the plan was published with, so a miss here is a defect rather than
//! a case to handle.

use std::collections::BTreeMap;

use novarocks_functions::EngineFunctionCatalog;
use novarocks_physical_plan::{
    FragmentId, NodeId, NodeKind, PhysicalPlan, ProviderColumnReference, Relation,
};
use novarocks_plan_codec::{
    PhysicalV1PrivateFacts, PhysicalV1ScanColumn, PhysicalV1ScanFact, encode_physical_plan_v1,
    physical_v1_scan_runtime_filters, physical_v1_scan_source_seal_digest,
};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_codec::connector_read::{
    ConnectorReadEncoder, ConnectorTableScanSource, encode_connector_expression,
};
use novarocks_proto_models::{connector_read as dto, plan};
use novarocks_query_application::preparation::{CompletedPlanWithAccess, FinalPlanRuntimeAccess};
use novarocks_spi::connector::read_stack::ConnectorReadWorkSource;

use crate::query_execution::preparation::attempt_access::{
    ConnectorAttemptAccessPlan, attempt_access_for_completed_plan,
};
use crate::query_execution::provider_read_facts::{FrozenProviderRead, FrozenReadEncoding};

/// A completed plan on the wire, and the capabilities its reads will be
/// performed with.
pub(crate) struct EncodedCompletedPlan {
    pub(crate) plan: plan::DistributedPlan,
    pub(crate) access: ConnectorAttemptAccessPlan,
}

/// Put one completed plan on the wire, and place the capabilities its scans
/// were frozen with.
///
/// The two halves of a frozen read separate exactly here, and in this order:
/// the encoding half is read while the capabilities are still accounted for,
/// and the capabilities are then moved - not copied, because a capability
/// cannot be - into the plan for the attempt that will use them.
pub(crate) fn encode_completed_plan(
    paired: CompletedPlanWithAccess<FrozenProviderRead>,
    functions: &EngineFunctionCatalog,
) -> Result<EncodedCompletedPlan, String> {
    let (candidate, reads) = paired.into_parts();
    let plan = candidate.plan();
    let facts = physical_v1_private_facts(plan, &reads)?;
    let encoded = encode_physical_plan_v1(plan, functions, &facts)?;
    Ok(EncodedCompletedPlan {
        plan: encoded,
        access: attempt_access_for_completed_plan(plan, reads)?,
    })
}

/// One plan's wire-private scan facts, addressed the way the encoder asks for
/// them.
pub(crate) struct FrontendPhysicalV1Facts {
    by_node: BTreeMap<(FragmentId, NodeId), PhysicalV1ScanFact>,
}

impl PhysicalV1PrivateFacts for FrontendPhysicalV1Facts {
    fn scan_fact(&self, fragment: FragmentId, node: NodeId) -> Option<&PhysicalV1ScanFact> {
        self.by_node.get(&(fragment, node))
    }
}

/// Build the private facts for every scan of one completed plan.
pub(crate) fn physical_v1_private_facts(
    plan: &PhysicalPlan,
    reads: &FinalPlanRuntimeAccess<FrozenProviderRead>,
) -> Result<FrontendPhysicalV1Facts, String> {
    // The encoder derives the runtime-filter binding identities itself and
    // checks what it is handed against them. Asking it rather than repeating
    // the numbering is what keeps the two from drifting apart.
    let runtime_filters = physical_v1_scan_runtime_filters(plan)?;
    let mut by_node = BTreeMap::new();
    for fragment in plan.fragments().values() {
        for node in fragment.nodes().values() {
            let NodeKind::Scan {
                occurrence,
                relation,
                read_budget,
                provider_outputs,
                ..
            } = &node.kind
            else {
                continue;
            };
            let frozen = reads.get(*occurrence).ok_or_else(|| {
                format!(
                    "completed plan scans provider read occurrence {} with no frozen read",
                    occurrence.get()
                )
            })?;
            let encoding = &frozen.access.encoding;
            let dynamic_filters = runtime_filters
                .get(&(fragment.id(), node.id))
                .map_or(&[][..], Vec::as_slice);
            let source = scan_source(
                encoding,
                read_budget.max_batch_rows,
                read_budget.max_batch_bytes,
                dynamic_filters,
                provider_outputs,
            )?;
            let seal = physical_v1_scan_source_seal_digest(
                *occurrence,
                relation.read(),
                selection_digest(relation),
                source.as_proto(),
            )?;
            by_node.insert(
                (fragment.id(), node.id),
                PhysicalV1ScanFact {
                    occurrence: *occurrence,
                    read: relation.read().clone(),
                    selection_digest: selection_digest(relation),
                    source_seal_digest: seal,
                    database: encoding.identity.namespace.as_str().into(),
                    // A scan addresses its relation by frozen reference, so it
                    // has no alias to carry: an alias is a name a statement
                    // used, and no reader resolves anything by it.
                    alias: None,
                    table: table_def(encoding, source)?,
                    columns: scan_columns(encoding, provider_outputs)?,
                },
            );
        }
    }
    Ok(FrontendPhysicalV1Facts { by_node })
}

const fn selection_digest(relation: &Relation) -> [u8; 32] {
    match relation {
        Relation::Data(relation) => relation.selection_digest,
        Relation::Metadata(relation) => relation.selection_digest,
    }
}

/// The provider-owned half of one scan, as the wire carries it.
fn scan_source(
    encoding: &FrozenReadEncoding,
    max_batch_rows: u64,
    max_batch_bytes: u64,
    dynamic_filters: &[(u32, novarocks_physical_plan::ValueId)],
    provider_outputs: &[(ProviderColumnReference, novarocks_physical_plan::ValueId)],
) -> Result<ConnectorTableScanSource, String> {
    let encoder = encoding.encoder.as_ref();
    let assignments = encoding
        .assignments
        .iter()
        .enumerate()
        .map(|(index, assignment)| {
            encoder.encode_assignment(
                assignment,
                FieldPath::root("connector_table_scan_source")
                    .field("assignments")
                    .index(index),
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let raw = dto::ConnectorTableScanSource {
        table: Some(
            encoder
                .encode_relation(&encoding.relation)
                .map_err(|error| error.to_string())?,
        ),
        assignments,
        enforced_predicate: Some(
            encoder
                .encode_tuple_domain(
                    &encoding.enforced_predicate,
                    FieldPath::root("connector_table_scan_source").field("enforced_predicate"),
                )
                .map_err(|error| error.to_string())?,
        ),
        unenforced_predicate: Some(
            encoder
                .encode_tuple_domain(
                    &encoding.unenforced_predicate,
                    FieldPath::root("connector_table_scan_source").field("unenforced_predicate"),
                )
                .map_err(|error| error.to_string())?,
        ),
        remaining_expression: encoding
            .remaining_expression
            .as_ref()
            .map(encode_connector_expression),
        dynamic_filters: dynamic_filters
            .iter()
            .map(|(filter_id, value)| {
                let ordinal = provider_outputs
                    .iter()
                    .position(|(_, output)| output == value)
                    .ok_or_else(|| {
                        format!(
                            "runtime filter {filter_id} constrains a value this scan does not produce"
                        )
                    })?;
                Ok(dto::DynamicFilterBinding {
                    filter_id: *filter_id,
                    variable: encoding.assignments[ordinal].variable().to_owned(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
        max_batch_rows,
        max_batch_bytes,
        work_source: match encoding.work_source {
            ConnectorReadWorkSource::RuntimeSplits => dto::ScanWorkSource::RuntimeSplits as i32,
            ConnectorReadWorkSource::WholeRelation => dto::ScanWorkSource::WholeRelation as i32,
        },
    };
    ConnectorTableScanSource::parse(raw, FieldPath::root("connector_table_scan_source"))
        .map_err(|error| error.to_string())
}

fn table_def(
    encoding: &FrozenReadEncoding,
    source: ConnectorTableScanSource,
) -> Result<plan::TableDef, String> {
    Ok(plan::TableDef {
        name: encoding.identity.table.clone(),
        columns: encoding
            .columns
            .iter()
            .map(|(_, column)| column_def(column))
            .collect::<Result<Vec<_>, String>>()?,
        // Row-lineage metadata columns are an Iceberg read shape the completion
        // contract states as ordinary projected columns, so there is no second
        // list to fill here.
        iceberg_row_lineage_metadata_columns: Vec::new(),
        source: Some(plan::ScanSource {
            kind: Some(plan::scan_source::Kind::TypedConnectorRead(
                source.as_proto().clone(),
            )),
        }),
    })
}

fn column_def(
    column: &novarocks_sql::compiler::ProviderReadColumnNeed,
) -> Result<plan::ColumnDef, String> {
    Ok(plan::ColumnDef {
        name: column.name().to_string(),
        data_type: Some(
            novarocks_plan_codec::encode_native_type(&column.engine_type().data_type)
                .map_err(|error| error.to_string())?,
        ),
        nullable: column.engine_type().nullable,
        // No decoder consumes this deprecated field, and a write default
        // reaches execution through the provider schema instead.
        write_default_json: None,
        // The completion contract states a column's engine type outright, so
        // there is no Arrow representation left to disambiguate.
        logical_type: None,
    })
}

/// Each column the scan produces, in the order the plan produces them.
fn scan_columns(
    encoding: &FrozenReadEncoding,
    provider_outputs: &[(ProviderColumnReference, novarocks_physical_plan::ValueId)],
) -> Result<Box<[PhysicalV1ScanColumn]>, String> {
    provider_outputs
        .iter()
        .map(|(reference, _)| {
            let (_, column) = encoding
                .columns
                .iter()
                .find(|(frozen, _)| frozen == reference)
                .ok_or_else(|| {
                    "completed plan scan produces a provider column its freeze did not project"
                        .to_string()
                })?;
            Ok(PhysicalV1ScanColumn {
                column: reference.clone(),
                name: column.name().into(),
                ty: column.engine_type().clone(),
                connector_type: column.connector_type(),
                // A scan produces relation columns; `internal` marks a writer
                // relation value, which a read never carries.
                internal: false,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use novarocks_physical_plan::{
        MAX_SCAN_BATCH_BYTES, MAX_SCAN_BATCH_ROWS, PipelineDopDomain, PlanVersionId, ScanReadBudget,
    };
    use novarocks_query_application::preparation::{
        FinalPlanCompletionDriver, ReadAccessSink, SqlCompletionFactSource,
    };
    use novarocks_sql::compiler::{
        DEFAULT_COMPLETION_LIMITS, SessionOptimizerSettings, SqlCompileControl, SqlCompileIntent,
        SqlFactBatch, SqlFinalPlanCompileRequest, SqlNeedBatch, SqlPlanningEnvironment,
        SqlSessionContext, SqlStatementInput, builtin_sql_function_catalog,
        noop_constant_evaluator,
    };
    use novarocks_workload_control::{
        ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };

    use super::*;

    struct NoFacts;

    #[async_trait::async_trait]
    impl SqlCompletionFactSource for NoFacts {
        type Access = FrozenProviderRead;

        async fn resolve(
            &self,
            _: &SqlNeedBatch,
            _: &ReadAccessSink<FrozenProviderRead>,
        ) -> Result<SqlFactBatch, String> {
            panic!("a VALUES statement asks for nothing")
        }
    }

    /// A statement over literal rows completes, encodes, and asks for no
    /// capability - which is the whole chain from SQL text to wire form with
    /// nothing in it that a provider had to answer.
    #[test]
    fn a_statement_over_literal_rows_reaches_the_wire_with_no_capability() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .expect("workload control");
        control.mark_ready().expect("workload control ready");
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("query root");
        let scope = root.owner.scope();
        let completed = runtime
            .block_on(FinalPlanCompletionDriver::new(Arc::new(NoFacts)).complete(request(), &scope))
            .unwrap_or_else(|error| panic!("VALUES completes without facts: {error}"));

        let encoded = encode_completed_plan(
            completed,
            &novarocks_sql::compiler::build_builtin_engine_function_catalog()
                .expect("builtin engine function catalog"),
        )
        .expect("a completed plan encodes");
        // The shape is the distributed one - rows are produced somewhere and
        // gathered at the result - and nothing in it had to be frozen.
        assert!(encoded.plan.fragments.len() >= 2);
        assert!(
            encoded
                .plan
                .fragments
                .iter()
                .map(|fragment| fragment.fragment_id)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == encoded.plan.fragments.len()
        );
        assert_eq!(encoded.access.iter().count(), 0);
    }

    fn request() -> SqlFinalPlanCompileRequest {
        SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([9; 16]).expect("plan version"),
            SqlStatementInput::sql("SELECT 1"),
            SqlCompileIntent::Query,
            SqlSessionContext {
                current_catalog: Some("iceberg".to_string()),
                current_database: "db".to_string(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed,
            builtin_sql_function_catalog().snapshot(),
            noop_constant_evaluator(),
            SqlCompileControl::unbounded(),
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            ScanReadBudget {
                max_batch_rows: MAX_SCAN_BATCH_ROWS,
                max_batch_bytes: MAX_SCAN_BATCH_BYTES,
            },
            DEFAULT_COMPLETION_LIMITS,
        )
    }
}
