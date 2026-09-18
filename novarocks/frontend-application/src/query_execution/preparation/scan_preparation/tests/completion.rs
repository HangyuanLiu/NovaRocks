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

//! One read, negotiated and frozen against a real provider.
//!
//! Everything else about this path is decided in isolation - which owner
//! answers, what a disposition means, what the reader is told. This is the one
//! place the whole sequence runs end to end against a provider that answers,
//! so the parts have to agree with each other rather than each being right on
//! its own.

use std::sync::Arc;

use novarocks_physical_plan::{ProviderReadOccurrenceId, ValueType};
use novarocks_query_application::preparation::{ProviderReadFactPort, ReadAccessSink};
use novarocks_spi::connector::read_stack::{ConnectorSession, ConnectorValueType};
use novarocks_sql::compiler::{
    ProviderReadNeed, ProviderReadRelationNeed, ProviderReadVersionNeed, fixtures,
};
use novarocks_sql::test_support::{
    NativeScanFixture, native_scan_fixture_binding, native_scan_plan,
};
use novarocks_types::naming::TableIdentity;

use super::{data_file, fixture_control_role_host, fixture_query_table_bindings, registry};
use crate::query_execution::provider_read_facts::FrontendProviderReadFacts;

fn session() -> ConnectorSession {
    ConnectorSession::try_new(
        "fixture-query",
        "fixture-user",
        "UTC",
        "en_US",
        std::time::SystemTime::UNIX_EPOCH,
    )
    .expect("fixture connector session")
}

fn need(
    identity: TableIdentity,
    binding: novarocks_sql::binding::SqlTableBindingId,
) -> ProviderReadNeed {
    let column = fixtures::provider_read_column_need(
        0,
        "id",
        ValueType::new(arrow::datatypes::DataType::Int64, false),
        ConnectorValueType::BigInt,
    )
    .expect("column need");
    fixtures::provider_read_need(
        1,
        ProviderReadOccurrenceId::new(1),
        binding,
        ProviderReadRelationNeed::Data {
            relation: identity,
            version: ProviderReadVersionNeed::Current,
        },
        vec![column],
        Vec::new(),
        None,
    )
    .expect("provider read need")
}

/// The freeze answers the request it was asked to commit, and leaves behind
/// exactly one capability for the occurrence that asked.
#[test]
fn one_read_is_negotiated_frozen_and_accounted_for() {
    let connectors = registry(vec![data_file("s3://bucket/current.parquet")]);
    let controls = crate::connector::FixtureControlResolver::new(connectors);
    let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
        .expect("sealed ordinary fixture");
    let admitted = native_scan_fixture_binding(&plan).expect("fixture scan binding");
    let store = Arc::new(fixture_query_table_bindings(&plan, &controls));
    let host = fixture_control_role_host(&plan, &controls);
    let (binding, _) = store
        .captured_bindings()
        .into_iter()
        .next()
        .expect("the fixture admitted one binding");
    let identity = TableIdentity::new(&admitted.catalog, &admitted.namespace, &admitted.table);
    let need = need(identity, binding);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("runtime");
    let facts = FrontendProviderReadFacts::new(
        host,
        store,
        session(),
        crate::connector::test_request_context(),
        crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor::new(
            runtime.handle().clone(),
            novarocks_native_adapter::connector_blocking_io::ConnectorBlockingIoBudget::try_new(
                2, 1,
            )
            .expect("blocking budget"),
        ),
    );
    let taken = ReadAccessSink::new();
    let frozen = runtime
        .block_on(facts.resolve_provider_reads(std::slice::from_ref(&need), &taken))
        .expect("the fixture provider answers one read");

    assert_eq!(frozen.len(), 1);
    let fact = &frozen[0];
    assert_eq!(fact.occurrence(), need.occurrence());
    assert_eq!(fact.binding(), need.binding());
    // The contract echoes the request it answers, which is what lets the
    // compiler resume against the read it actually asked for.
    assert_eq!(fact.contract().schema.len(), 1);
    assert_eq!(fact.contract().schema[0].request_ordinal(), 0);
    assert_eq!(fact.contract().sql_binding, need.binding());

    // One freeze, one capability, and the encoding half kept beside it.
    let access = match taken.try_into_access() {
        Ok(access) => access,
        Err((error, _)) => panic!("one occurrence, one freeze: {error}"),
    };
    assert_eq!(access.len(), 1);
    let kept = access
        .get(need.occurrence())
        .expect("the occurrence that asked");
    assert_eq!(kept.binding, need.binding());
    assert_eq!(kept.access.encoding.assignments.len(), 1);
    assert_eq!(kept.access.encoding.columns.len(), 1);
    assert_eq!(kept.access.encoding.columns[0].1.name(), "id");
}

/// The whole path on a statement that actually reads a provider: the compiler
/// asks, this process answers, the plan completes, and it encodes.
///
/// The catalog, statistics and view answers are stated directly here because
/// what is being proved is the provider half and the shape of the result. The
/// provider half is the real adapter against the real fixture provider.
mod scanning_statement {
    use std::sync::Arc;

    use novarocks_physical_plan::{
        MAX_SCAN_BATCH_BYTES, MAX_SCAN_BATCH_ROWS, PipelineDopDomain, PlanVersionId, ScanReadBudget,
    };
    use novarocks_query_application::api::{NativeScanWork, PlanSeal, QueryExecutionKind};
    use novarocks_query_application::coordination::{ExecutionEffect, RecoveryMode};
    use novarocks_query_application::preparation::{
        ExecutionResourceRequirements, FrozenCostEstimate, FrozenEstimateUnknownReason,
        FrozenExecutionDescription, OutputContract,
    };
    use novarocks_query_application::preparation::{
        FinalPlanCompletionDriver, ProviderReadFactPort, ReadAccessSink, SqlCompletionFactSource,
    };
    use novarocks_sql::compiler::{
        CatalogRelationFact, DEFAULT_COMPLETION_LIMITS, MaterializedViewFact,
        SessionOptimizerSettings, SqlCompileControl, SqlCompileIntent, SqlFactBatch,
        SqlFinalPlanCompileRequest, SqlNeedBatch, SqlPlanningEnvironment, SqlSessionContext,
        SqlStatementInput, StatisticsFact, builtin_sql_function_catalog, noop_constant_evaluator,
    };
    use novarocks_sql::planning::dml::DmlStatisticsEvidence;
    use novarocks_sql::test_support::{NativeScanFixture, native_scan_plan};
    use novarocks_workload_control::{
        ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };

    use super::super::{
        data_file, fixture_control_role_host, fixture_query_table_bindings, registry,
    };
    use crate::query_execution::physical_encoding::encode_completed_plan;
    use crate::query_execution::provider_read_facts::{
        FrontendProviderReadFacts, FrozenProviderRead,
    };

    /// Answers the three lookups from the fixture, and the provider read from
    /// the real adapter.
    struct FixtureFacts {
        resolved: novarocks_sql::planning::catalog::ResolvedAnalyzerTable,
        provider_reads: FrontendProviderReadFacts,
    }

    #[async_trait::async_trait]
    impl SqlCompletionFactSource for FixtureFacts {
        type Access = FrozenProviderRead;

        async fn resolve(
            &self,
            needs: &SqlNeedBatch,
            taken: &ReadAccessSink<FrozenProviderRead>,
        ) -> Result<SqlFactBatch, String> {
            match needs {
                SqlNeedBatch::CatalogRelations(needs) => needs
                    .iter()
                    .map(|need| {
                        CatalogRelationFact::resolved(need, self.resolved.clone())
                            .map_err(|error| error.to_string())
                    })
                    .collect::<Result<Vec<_>, String>>()
                    .map(|facts| SqlFactBatch::CatalogRelations(facts.into_boxed_slice())),
                SqlNeedBatch::Statistics(needs) => needs
                    .iter()
                    .map(|need| {
                        StatisticsFact::try_new(
                            need,
                            need.metrics().to_vec(),
                            DmlStatisticsEvidence::Missing {
                                binding: need.binding(),
                                label: "fixture".to_string(),
                                reason: "the fixture publishes no statistics".to_string(),
                            },
                        )
                        .map_err(|error| error.to_string())
                    })
                    .collect::<Result<Vec<_>, String>>()
                    .map(|facts| SqlFactBatch::Statistics(facts.into_boxed_slice())),
                SqlNeedBatch::MaterializedViews(needs) => needs
                    .iter()
                    .map(|need| {
                        MaterializedViewFact::missing(need, "the fixture has no views")
                            .map_err(|error| error.to_string())
                    })
                    .collect::<Result<Vec<_>, String>>()
                    .map(|facts| SqlFactBatch::MaterializedViews(facts.into_boxed_slice())),
                SqlNeedBatch::ProviderReads(needs) => self
                    .provider_reads
                    .resolve_provider_reads(needs, taken)
                    .await
                    .map(|facts| SqlFactBatch::ProviderReads(facts.into_boxed_slice())),
            }
        }
    }

    /// Everything one statement needs to be completed against the real
    /// fixture provider, held together so more than one statement can use it.
    struct Fixture {
        runtime: tokio::runtime::Runtime,
        facts: Arc<dyn SqlCompletionFactSource<Access = FrozenProviderRead>>,
        _control: WorkloadControl,
        root: novarocks_workload_control::RootWork,
    }

    impl Fixture {
        fn new() -> Self {
            let connectors = registry(vec![data_file("s3://bucket/current.parquet")]);
            let controls = crate::connector::FixtureControlResolver::new(connectors);
            let fixture = native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
                .expect("sealed ordinary fixture");
            let store = Arc::new(fixture_query_table_bindings(&fixture, &controls));
            // The store's own resolved table, so the catalog identity and the scan
            // source name the same relation - which is what the completion
            // contract checks and what production materialization guarantees.
            let resolved = store
                .captured_bindings()
                .first()
                .map(|(_, binding)| binding.resolved.clone())
                .expect("the fixture admitted one binding");
            let host = fixture_control_role_host(&fixture, &controls);

            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .expect("runtime");
            let facts = FixtureFacts {
                resolved,
                provider_reads: FrontendProviderReadFacts::new(
                    host,
                    store,
                    super::session(),
                    crate::connector::test_request_context(),
                    crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor::new(
                        runtime.handle().clone(),
                        novarocks_native_adapter::connector_blocking_io::ConnectorBlockingIoBudget::try_new(2, 1)
                            .expect("blocking budget"),
                    ),
                ),
            };
            let control = WorkloadControl::try_new(
                WorkloadConfig::default(),
                ResourceConfig {
                    total_bytes: 4096,
                    control_bytes: 512,
                    per_scope_bytes: 3584,
                },
            )
            .expect("workload control");
            control.mark_ready().expect("workload control ready");
            let root = control
                .try_begin_root(WorkRequest::new(WorkClass::Query))
                .expect("query root");
            let scope = root.owner.scope();

            Self {
                runtime,
                facts: Arc::new(facts),
                _control: control,
                root,
            }
        }

        fn complete(
            &self,
            sql: &str,
        ) -> novarocks_query_application::preparation::CompletedPlanWithAccess<FrozenProviderRead>
        {
            let scope = self.root.owner.scope();
            self.runtime
                .block_on(
                    FinalPlanCompletionDriver::new(Arc::clone(&self.facts))
                        .complete(request_for(sql), &scope),
                )
                .unwrap_or_else(|error| panic!("a statement completes: {error}"))
        }
    }

    #[test]
    fn a_statement_that_reads_a_provider_completes_and_encodes() {
        let fixture = Fixture::new();
        let completed = fixture.complete("SELECT id FROM test_catalog.test_db.test_table");
        assert_eq!(completed.access().len(), 1, "one scan, one frozen read");
        let plan = Arc::clone(completed.candidate().plan());

        let encoded = encode_completed_plan(
            completed,
            &novarocks_sql::compiler::build_builtin_engine_function_catalog()
                .expect("builtin engine function catalog"),
        )
        .expect("a completed plan that scans encodes");
        assert_eq!(encoded.access.iter().count(), 1);
        assert!(
            encoded
                .plan_facts
                .scheduling()
                .fragments
                .values()
                .any(|fragment| fragment.has_scans()),
            "the scan reaches scheduling"
        );

        // The owner that places tasks reads one shape, whichever
        // representation produced the plan. A completed plan reaches it the
        // same way a sealed one does, and names its read with its own plan
        // version rather than a preparation seal it does not have.
        let version = PlanVersionId::try_new([11; 16]).expect("plan version");
        let attempt = encoded
            .plan_facts
            .scheduling()
            .attempt_scheduling_facts(PlanSeal::Version(version))
            .expect("a completed plan states what scheduling reads");
        assert_eq!(
            attempt.topological_fragment_order, encoded.topology.order,
            "scheduling establishes fragments in the plan's own order"
        );
        assert_eq!(
            attempt.execution_anchor_fragment_id,
            encoded.topology.anchor
        );
        let scans = attempt
            .fragments
            .iter()
            .flat_map(|fragment| fragment.scans.iter())
            .collect::<Vec<_>>();
        assert_eq!(scans.len(), 1, "one scan, one scheduled read");
        assert_eq!(scans[0].scan.plan(), PlanSeal::Version(version));
        assert_eq!(
            scans[0].work,
            NativeScanWork::RuntimeSplits,
            "the fixture provider hands out splits, so the freeze says so"
        );

        // The attempt owner takes one value, whichever representation
        // produced the plan. What it then reads about the plan is the same
        // thing the scheduler was told, because it is the same projection.
        let topology_order = encoded.topology.order.clone();
        let template = encoded.into_attempt_template(version);
        let from_template = template
            .attempt_scheduling_facts()
            .expect("a completed-plan template states what scheduling reads");
        assert_eq!(from_template, attempt);
        assert_eq!(from_template.topological_fragment_order, topology_order);

        // The semantic input every attempt of this statement shares. It
        // re-checks no negotiation, because completion already paired every
        // scan with the read it was frozen with -- and it names the same plan
        // the template does, which is what keeps an attempt of one plan from
        // being prepared against another's description.
        let description = FrozenExecutionDescription::for_completed_plan(
            QueryExecutionKind::Read,
            version,
            from_template
                .fragments
                .iter()
                .flat_map(|fragment| fragment.scans.iter().map(|scan| scan.scan))
                .collect(),
            OutputContract::from_completed_plan(QueryExecutionKind::Read, &plan)
                .expect("a completed read plan states what it delivers"),
            ExecutionEffect::None,
            RecoveryMode::NoRecovery,
            Vec::new(),
            FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
            ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
        )
        .expect("a completed plan freezes into an execution description");
        assert!(description.matches_plan_seal(template.native_manifest_template().plan()));
        assert_eq!(
            description.scan_identities(),
            &[attempt.fragments[1].scans[0].scan][..],
            "the description names the read the scheduler was told about"
        );
        assert_eq!(
            description
                .output()
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect::<Vec<_>>(),
            vec!["id".to_string()],
        );
        assert!(
            description.plan().is_none(),
            "a completed plan is its own description"
        );

        // Opening this read takes the plan's facts and the capability held
        // for it, and the attempt asks the template for the pair the same way
        // it does for a sealed plan.
        let attempt_artifacts = template.instantiate();
        let scan = attempt.fragments[1].scans[0].scan;
        crate::query_execution::split_assignment_round::RoundSplitSourceRecipe::from_artifacts(
            &attempt_artifacts,
            scan.fragment_id(),
            scan.node_id(),
        )
        .expect("a completed plan's scan opens from its own attempt artifacts");
    }

    /// A statement whose CTE is read more than once, which is the shape that
    /// makes the completed plan multicast it.
    ///
    /// Submitting a multicast producer reads what each consumer receives, and
    /// this is the only place that is proved on a real statement rather than
    /// on a constructed plan.
    #[test]
    fn a_statement_whose_cte_has_two_consumers_encodes_its_multicast() {
        let fixture = Fixture::new();
        let completed = fixture.complete(
            "WITH ids AS (SELECT id FROM test_catalog.test_db.test_table) \
             SELECT l.id FROM ids AS l JOIN ids AS r ON l.id = r.id",
        );
        let plan = Arc::clone(completed.candidate().plan());
        let multicast = plan
            .fragments()
            .values()
            .filter(|fragment| {
                matches!(
                    fragment.sink(),
                    novarocks_physical_plan::FragmentSink::Multicast { .. }
                )
            })
            .count();
        assert!(
            multicast > 0,
            "a CTE read twice is multicast rather than compiled twice"
        );

        let encoded = encode_completed_plan(
            completed,
            &novarocks_sql::compiler::build_builtin_engine_function_catalog()
                .expect("builtin engine function catalog"),
        )
        .expect("a completed plan that multicasts a CTE encodes");
        let template =
            encoded.into_attempt_template(PlanVersionId::try_new([11; 16]).expect("plan version"));
        let submission = template
            .native_manifest_template()
            .plan_facts()
            .submission();
        let producers = submission
            .cte_consumers()
            .keys()
            .copied()
            .collect::<Vec<_>>();
        assert_eq!(
            producers.len(),
            multicast,
            "every multicast producer is a CTE with consumers"
        );
        for consumers in submission.cte_consumers().values() {
            assert!(
                consumers.len() >= 2,
                "a multicast CTE is read by more than one consumer"
            );
            for (_, _, _, output_slot_ids, _) in consumers {
                assert!(
                    !output_slot_ids.is_empty(),
                    "a consumer is sent the columns the producer projects"
                );
            }
        }
    }

    fn request_for(sql: &str) -> SqlFinalPlanCompileRequest {
        SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([11; 16]).expect("plan version"),
            SqlStatementInput::sql(sql),
            SqlCompileIntent::Query,
            SqlSessionContext {
                current_catalog: Some("test_catalog".to_string()),
                current_database: "test_db".to_string(),
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
