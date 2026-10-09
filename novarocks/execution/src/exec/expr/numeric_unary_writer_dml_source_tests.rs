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
//! Actual public SQL DML optimization, Writer statistics and final publication.
//! This fixture never constructs a replacement PhysicalPlan or state route.
use arrow::datatypes::DataType;
use novarocks_connector_iceberg_functions::{
    ICEBERG_THETA_AGGREGATE_NAME, iceberg_theta_registration,
};
use novarocks_functions::{
    AggregateBindingSelection, EngineFunctionCatalog, EngineFunctionCatalogBuilder,
    FunctionArgument, FunctionBindingRequest, FunctionBindingSelection, FunctionKind,
    FunctionResultType,
};
use novarocks_physical_plan::{EdgeKind, NodeKind, PipelineDopDomain, PlanVersionId};
use novarocks_spi::connector::write_stack::WriteTargetOrdinal;
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceId, ConnectorProviderId,
    ConnectorWriteFieldToken, StatisticsArtifactIdentity, StatisticsRequiredAggregation,
    StatisticsScanColumn,
};
use novarocks_sql::binding::SqlTableBindingAllocator;
use novarocks_sql::compiler::{
    RootDistributionRequirement, SessionOptimizerSettings, SqlAnalyzeRequest, SqlCompileControl,
    SqlCompileIntent, SqlCompiler, SqlOptimizeRequest, SqlPhysicalEmissionMode,
    SqlPlannerTableSnapshot, SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
};
use novarocks_sql::planning::catalog::PlannerMemoryCatalog;
use novarocks_sql::planning::dml::{
    ConnectorWriteInputBinding, DmlFinalPlanContext, DmlFinalWritePlanContext,
    DmlFinalizedProviderReadSet, DmlFinalizedWriteTarget, DmlFinalizedWriteTargetSet,
    DmlStatisticsSnapshot, DmlWritePlanInput, DmlWriteSinkMode, DmlWriteTarget,
    DmlWriteTargetField, compile_final_connector_write_plan,
};
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use novarocks_types::schema::ColumnDef;

fn source(
    mode: SqlPhysicalEmissionMode,
    with_statistics: bool,
    dtype: DataType,
    sql_type: &str,
) -> (
    novarocks_sql::compiler::SqlAuthoredPhysicalPlan,
    EngineFunctionCatalog,
) {
    let mut functions = EngineFunctionCatalogBuilder::new();
    novarocks_functions::builtin::catalogue::contribute_builtin_functions(&mut functions).unwrap();
    functions
        .register(iceberg_theta_registration().unwrap().definition().clone())
        .unwrap();
    let functions = functions.seal_bound().unwrap();
    let catalog = PlannerMemoryCatalog::default();
    let catalog = SqlPlannerTableSnapshot::new(&catalog);
    let settings = SessionOptimizerSettings {
        enable_materialized_view_rewrite: Some(false),
        ..SessionOptimizerSettings::default()
    };
    let control = SqlCompileControl::unbounded();
    let analyzed = SqlCompiler::analyze(SqlAnalyzeRequest::new(
        SqlStatementInput::sql(format!("SELECT CAST(7 AS {sql_type}) AS order_id")),
        SqlCompileIntent::IcebergWrite {
            root_distribution: RootDistributionRequirement::Any,
        },
        SqlSessionContext {
            sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            current_catalog: None,
            current_database: "fixture".into(),
            optimizer_settings: settings.clone(),
        },
        SqlPlanningEnvironment::Distributed,
        &catalog,
        &functions,
        novarocks_sql::compiler::noop_constant_evaluator(),
        None,
        super::pure_differential::constant_policy(),
        novarocks_sql::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
        control.clone(),
    ))
    .unwrap()
    .into_pending()
    .unwrap();
    let column = ColumnDef {
        name: "order_id".into(),
        data_type: dtype.clone(),
        nullable: false,
        write_default: None,
        logical_type: None,
    };
    let mut allocator = SqlTableBindingAllocator::new_unique().unwrap();
    let sink = DmlWritePlanInput::try_new(
        DmlWriteSinkMode::Data,
        DmlWriteTarget {
            binding: allocator.allocate().unwrap(),
            catalog: "iceberg".into(),
            namespace: "fixture".into(),
            table: "target".into(),
            fields: vec![DmlWriteTargetField {
                token: ConnectorWriteFieldToken::from_bytes([1; 32]),
                column: column.clone(),
                is_hidden: false,
            }],
        },
        vec![column],
        ConnectorWriteInputBinding::RootOutputByOrdinal,
    )
    .unwrap();
    let ordinal = WriteTargetOrdinal::try_new(0).unwrap();
    let requirements = if with_statistics {
        vec![
            StatisticsRequiredAggregation::try_new(
                StatisticsScanColumn::try_new(0, "order_id", FunctionValueType::new(dtype, false))
                    .unwrap(),
                ICEBERG_THETA_AGGREGATE_NAME,
                StatisticsArtifactIdentity::try_new(vec![1], "writer-fixture-v1").unwrap(),
            )
            .unwrap(),
        ]
    } else {
        Vec::new()
    };
    let handle = ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            CatalogHandle::new(
                ConnectorInstanceId::parse("warehouse").unwrap(),
                CatalogVersion::from_bytes([9; 32]),
            ),
            ConnectorCodecCategory::WriteHandle,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        vec![7].into(),
    );
    let statistics = DmlStatisticsSnapshot::empty();
    let source = compile_final_connector_write_plan(
        SqlOptimizeRequest::new(analyzed, &statistics, control),
        sink,
        ordinal,
        &requirements,
        &settings,
        DmlFinalWritePlanContext::new(
            DmlFinalPlanContext::new(
                PlanVersionId::try_new([91; 16]).unwrap(),
                PipelineDopDomain {
                    min: 1,
                    max: 8,
                    requires_power_of_two: true,
                },
                DmlFinalizedProviderReadSet::empty(),
                mode,
            ),
            DmlFinalizedWriteTargetSet::try_new([DmlFinalizedWriteTarget { ordinal, handle }])
                .unwrap(),
        ),
        DecimalOverflowPolicy::OutputNull,
    )
    .unwrap();
    (source, functions)
}

#[test]
fn numeric_unary_writer_dml_actual_statistics_publishes_in_both_modes() {
    for mode in [
        SqlPhysicalEmissionMode::OriginalNativeV1,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    ] {
        let (source, functions) = source(mode, true, DataType::Int64, "BIGINT");
        assert_writer_selections(&source, &functions, mode);
        let mut partials = 0;
        let mut finals = 0;
        for fragment in source.plan().fragments().values() {
            for node in fragment.nodes().values() {
                match &node.kind {
                    NodeKind::TableWriter { target } => {
                        assert_eq!(target.partial_aggregates.len(), 1);
                        assert_eq!(target.target_fields.len(), 1);
                        partials += 1;
                    }
                    NodeKind::TableFinish(finish) => {
                        assert_eq!(finish.final_aggregates.len(), 1);
                        finals += 1;
                    }
                    _ => {}
                }
            }
        }
        assert_eq!((partials, finals), (1, 1));
        assert!(
            source
                .plan()
                .edges()
                .values()
                .any(|edge| matches!(edge.kind, EdgeKind::Stream))
        );
    }
}

#[test]
fn numeric_unary_writer_dml_actual_no_statistics_publishes_in_both_modes() {
    for mode in [
        SqlPhysicalEmissionMode::OriginalNativeV1,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    ] {
        let (source, _) = source(mode, false, DataType::Int64, "BIGINT");
        for fragment in source.plan().fragments().values() {
            for node in fragment.nodes().values() {
                match &node.kind {
                    NodeKind::TableWriter { target } => {
                        assert!(target.partial_aggregates.is_empty())
                    }
                    NodeKind::TableFinish(finish) => assert!(finish.final_aggregates.is_empty()),
                    _ => {}
                }
            }
        }
    }
}

fn assert_writer_selections(
    source: &novarocks_sql::compiler::SqlAuthoredPhysicalPlan,
    functions: &EngineFunctionCatalog,
    mode: SqlPhysicalEmissionMode,
) {
    let control = SqlCompileControl::unbounded();
    let semantics = novarocks_sql::compiler::author_fragment_package_semantics(
        source,
        super::pure_differential::constant_policy(),
        &control,
    )
    .unwrap();
    assert_eq!(semantics.len(), source.plan().fragments().len());
    for fragment in source.plan().fragments().values() {
        for node in fragment.nodes().values() {
            let (partial, calls) = match &node.kind {
                NodeKind::TableWriter { target } => (true, &target.partial_aggregates),
                NodeKind::TableFinish(finish) => (false, &finish.final_aggregates),
                _ => continue,
            };
            for (ordinal, call) in calls.iter().enumerate() {
                let site = if partial {
                    novarocks_physical_plan::PhysicalCallSite::WriterPartial {
                        node: node.id,
                        call: u32::try_from(ordinal).unwrap(),
                    }
                } else {
                    novarocks_physical_plan::PhysicalCallSite::WriterFinal {
                        node: node.id,
                        call: u32::try_from(ordinal).unwrap(),
                    }
                };
                let request = fragment
                    .call_requests()
                    .get(novarocks_physical_plan::PhysicalCallDefinition::Relational(
                        site,
                    ))
                    .unwrap();
                let arguments = request
                    .arguments
                    .iter()
                    .map(|arg| -> FunctionArgument {
                        match arg {
                            novarocks_physical_plan::StaticFunctionArgument::Value {
                                value_type,
                                constant: None,
                            } => FunctionArgument::Value {
                                value_type: value_type.clone(),
                                constant: None,
                            },
                            _ => {
                                panic!("actual Writer statistics request is one nonconstant Value")
                            }
                        }
                    })
                    .collect::<Vec<_>>();
                let materialized_request = || FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: request.logical_argument_count,
                    expected_result_type: request.expected_result_type.as_ref(),
                };
                let binding = &call.binding;
                let actual = FunctionBindingSelection {
                    overload: binding.function.overload.clone(),
                    argument_types: binding.function.argument_types.clone(),
                    result_type: FunctionResultType::Scalar(binding.function.result_type.clone()),
                    aggregate: Some(AggregateBindingSelection {
                        state_argument_contract: binding.state_argument_contract,
                        intermediate_type: binding.intermediate_type.clone(),
                        state_format: binding.state_format.clone(),
                    }),
                };
                let expected = functions
                    .select_exact_overload_observed(
                        &binding.function.function_id,
                        FunctionKind::Aggregate,
                        &binding.function.overload,
                        materialized_request(),
                        &control,
                    )
                    .unwrap();
                assert_eq!(
                    expected.as_ref(),
                    &actual,
                    "actual DML mode={mode:?} fragment={:?} site={site:?} original request={:?}",
                    fragment.id(),
                    materialized_request()
                );
                functions
                    .validate_frozen_selection(
                        &binding.function.function_id,
                        FunctionKind::Aggregate,
                        &actual,
                        materialized_request(),
                        &control,
                    )
                    .unwrap();
            }
        }
    }
}
#[test]
fn numeric_unary_writer_dml_actual_narrow_statistics_preserve_exact_selected_contract() {
    for (dtype, sql_type) in [
        (DataType::Int8, "TINYINT"),
        (DataType::Int16, "SMALLINT"),
        (DataType::Int32, "INT"),
    ] {
        for mode in [
            SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
            SqlPhysicalEmissionMode::OriginalNativeV1,
        ] {
            let (source, functions) = source(mode, true, dtype.clone(), sql_type);
            assert_writer_selections(&source, &functions, mode);
        }
    }
}

#[path = "sql_call_dependency_writer_loan_tests.rs"]
mod sql_call_dependency_writer_loan_tests;
