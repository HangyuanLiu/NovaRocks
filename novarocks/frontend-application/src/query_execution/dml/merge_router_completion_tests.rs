// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This witness keeps the production MERGE producer and completion/encoding
// path. Literal relations replace external provider reads only. It identifies
// this isolated shape's first refusal, not an unexported native error cause.
#[test]
fn mor_merge_completion_reaches_the_original_compiled_router_refusal() {
    use novarocks_failure_contract::{FailureClass, FailureProjection, domain};
    use novarocks_query_application::preparation::{
        CompletedPhysicalPlanCandidate, CompletedPlanWithAccess, ReadAccessSink,
    };
    use novarocks_spi::connector::{
        ConnectorCodecCategory, ConnectorMutationRouteInput, ConnectorRowMutationEffect,
        ConnectorWriteFieldToken, ConnectorWriteRouteId, write_stack::WriteTargetOrdinal,
    };
    use novarocks_sql::compiler::{
        SqlAnalyzeRequest, SqlCompileControl, SqlCompileIntent, SqlCompiler, SqlOptimizeRequest,
        SqlPlannerTableSnapshot, SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
    };
    use novarocks_sql::planning::dml::{
        ConnectorWriteInputBinding, DmlChangeStreamStatisticsTarget, DmlFinalPlanContext,
        DmlFinalWritePlanContext, DmlFinalizedProviderReadSet, DmlFinalizedWriteTarget,
        DmlFinalizedWriteTargetSet, DmlStatisticsSnapshot, DmlWritePlanInput, DmlWritePlanShape,
        DmlWriteTarget, DmlWriteTargetField,
    };

    let columns = vec![col("id"), col("v")];
    // Two source rows exercise the same matched UPDATE and unmatched INSERT
    // branches as the native row-level scenario. The target supplies the exact
    // locator and v3 lineage columns consumed by the production SQL builder.
    let match_sql = build_merge_match_query_sql(
        "(SELECT CAST(2 AS BIGINT) AS id, CAST(2 AS BIGINT) AS v, \
         'fixture.parquet' AS _file, CAST(0 AS BIGINT) AS _pos, \
         CAST(20 AS BIGINT) AS _row_id, \
         CAST(1 AS BIGINT) AS _last_updated_sequence_number) AS t",
        "t",
        "(SELECT CAST(2 AS BIGINT) AS id, CAST(2000 AS BIGINT) AS v \
         UNION ALL SELECT CAST(99 AS BIGINT), CAST(9900 AS BIGINT)) AS s",
        "t.id = s.id",
        None,
        None,
        &columns,
        &[("v", "CAST(s.v AS BIGINT)")],
        &[("id", "CAST(s.id AS BIGINT)"), ("v", "CAST(s.v AS BIGINT)")],
        Some(MERGE_ACTION_MATCHED_UPDATE),
        true,
    );
    assert!(match_sql.contains("LEFT JOIN") && match_sql.contains("ROW_NUMBER() OVER"));
    let query = parse_generated_query(&match_sql, "MOR MERGE router witness").unwrap();
    let control = SqlCompileControl::unbounded();
    let catalog = novarocks_sql::planning::catalog::PlannerMemoryCatalog::default();
    let snapshot = SqlPlannerTableSnapshot::new(&catalog);
    let functions = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let analyzed = SqlCompiler::analyze(SqlAnalyzeRequest::new(
        SqlStatementInput::parsed_query(Box::new(query)),
        SqlCompileIntent::ChangeStreamWrite,
        SqlSessionContext {
            current_catalog: None,
            current_database: "default_database".into(),
            optimizer_settings: dml_change_stream_optimizer_settings(),
            sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        },
        SqlPlanningEnvironment::Distributed,
        &snapshot,
        &functions,
        crate::query_execution::constant_eval::constant_evaluator(),
        None,
        crate::application::test_constant_policy(),
        control.clone(),
    ))
    .expect("real analysis of the production MERGE match query")
    .into_pending()
    .unwrap();

    let mut allocator = novarocks_sql::binding::SqlTableBindingAllocator::new_unique().unwrap();
    let mut routes = Vec::new();
    for ordinal in 0_u32..2 {
        let route_columns = if ordinal == 0 {
            vec![
                col("id"),
                col("v"),
                col("_row_id"),
                col("_last_updated_sequence_number"),
            ]
        } else {
            vec![
                ColumnDef {
                    data_type: DataType::Utf8,
                    ..col("_file")
                },
                col("_pos"),
            ]
        };
        let input_ordinals: &[u32] = if ordinal == 0 { &[0, 1, 2, 3] } else { &[4, 5] };
        let fields = route_columns
            .iter()
            .enumerate()
            .map(|(index, column)| DmlWriteTargetField {
                token: ConnectorWriteFieldToken::from_bytes(
                    [1 + ordinal as u8 * 8 + index as u8; 32],
                ),
                column: column.clone(),
                is_hidden: column.name.starts_with('_'),
            })
            .collect::<Vec<_>>();
        let inputs = fields
            .iter()
            .zip(input_ordinals)
            .map(|(field, input)| ConnectorMutationRouteInput::new(field.token, *input))
            .collect();
        routes.push(DmlChangeStreamRoute {
            requirements: novarocks_spi::connector::write_stack::ConnectorWriteRouteRequirements {
                colocation: novarocks_spi::connector::write_stack::ConnectorWriteColocation::None,
                layout: novarocks_spi::connector::write_stack::ConnectorWriteLayoutPreference::None,
            },
            route_id: ConnectorWriteRouteId::from_bytes([ordinal as u8 + 1; 32]),
            write_target_ordinal: WriteTargetOrdinal::try_new(ordinal).unwrap(),
            accepted_effects: if ordinal == 0 {
                vec![
                    ConnectorRowMutationEffect::Replace,
                    ConnectorRowMutationEffect::Insert,
                ]
            } else {
                vec![ConnectorRowMutationEffect::Replace]
            },
            input_ordinals: inputs,
            partition_input_tokens: Vec::new(),
            sink: DmlWritePlanInput::try_new(
                if ordinal == 0 {
                    DmlWriteSinkMode::RowLineageData
                } else {
                    DmlWriteSinkMode::DeletionVectors
                },
                DmlWriteTarget {
                    binding: allocator.allocate().unwrap(),
                    catalog: "iceberg".into(),
                    namespace: "db".into(),
                    table: "target".into(),
                    fields,
                },
                route_columns,
                ConnectorWriteInputBinding::RootOutputByOrdinal,
            )
            .unwrap(),
        });
    }
    let statistics = DmlStatisticsSnapshot::empty();
    let (completion, needs) = novarocks_sql::planning::dml::begin_final_dml_change_stream(
        DmlChangeStreamCompileRequest {
            optimize_request: SqlOptimizeRequest::new(analyzed, &statistics, control.clone()),
            kind: DmlChangeStreamKind::Merge {
                target_columns: columns,
                matched_update: true,
                matched_delete: false,
                not_matched_insert: true,
            },
            routes,
            statistics_targets: (0..2)
                .map(|ordinal| DmlChangeStreamStatisticsTarget {
                    write_target_ordinal: WriteTargetOrdinal::try_new(ordinal).unwrap(),
                    requirements: Vec::new(),
                })
                .collect(),
            pre_expand_keyed_assert: Some(DmlPreExpandKeyedAssert {
                applicability_output_ordinal: None,
                key_column_name: "__nr_merge_assert_key".into(),
                key_label: "_row_id".into(),
                message_prefix: "MOR MERGE matched target row".into(),
            }),
            shape: DmlWritePlanShape::Dataflow,
        },
        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
    )
    .expect("real optimizer and MERGE change-stream preparation");
    assert!(
        needs.is_empty(),
        "literal relations acquire no provider capabilities"
    );
    let finalized = completion
        .finish(
            DmlFinalWritePlanContext::new(
                DmlFinalPlanContext::new(
                    novarocks_physical_plan::PlanVersionId::try_new([61; 16]).unwrap(),
                    novarocks_physical_plan::PipelineDopDomain {
                        min: 1,
                        max: 2,
                        requires_power_of_two: true,
                    },
                    DmlFinalizedProviderReadSet::try_new([]).unwrap(),
                ),
                DmlFinalizedWriteTargetSet::try_new((0..2).map(|ordinal| {
                    DmlFinalizedWriteTarget {
                        ordinal: WriteTargetOrdinal::try_new(ordinal).unwrap(),
                        handle: row_mutation_payload(
                            ConnectorCodecCategory::WriteHandle,
                            bytes::Bytes::from_static(b"merge-router-witness"),
                        ),
                    }
                }))
                .unwrap(),
            ),
            &control,
        )
        .expect("actual production DML completion must precede encoding");
    assert_eq!(
        finalized
            .physical_plan()
            .fragments()
            .values()
            .filter(|fragment| {
                matches!(
                    fragment.sink(),
                    novarocks_physical_plan::FragmentSink::Router { .. }
                )
            })
            .count(),
        1
    );
    assert_eq!(
        finalized
            .physical_plan()
            .fragments()
            .values()
            .flat_map(|fragment| { fragment.nodes().values() })
            .filter(|node| matches!(
                &node.kind,
                novarocks_physical_plan::NodeKind::TableWriter { .. }
            ))
            .count(),
        2
    );
    let (plan, writer_routes) = finalized.into_parts();
    assert_eq!(writer_routes.len(), 2);
    let candidate = CompletedPhysicalPlanCandidate::for_sql_program(plan, &control)
        .unwrap()
        .freeze_root_output(novarocks_result_contract::FrozenRootOutput::InternalFacts(
            novarocks_result_contract::InternalResultDomain::PreparedWriteCommitV1,
        ))
        .unwrap();
    let sink =
        ReadAccessSink::<crate::query_execution::provider_read_facts::FrozenProviderRead>::new();
    let access = sink
        .try_into_access()
        .unwrap_or_else(|(error, _)| panic!("empty access: {error}"));
    let paired = CompletedPlanWithAccess::try_pair(candidate, access)
        .unwrap_or_else(|(error, _)| panic!("pair original completed plan: {error}"));
    let freezes = crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread();
    let error = match crate::query_execution::physical_encoding::encode_completed_plan(
        paired,
        &crate::query_execution::package_freeze::CompiledPackageCarrier::new(
            novarocks_plan_codec::physical_package_v2::FROZEN_PACKAGE_ADMISSION,
            novarocks_plan_codec::physical_package_v2::PackageEncodeLimits::FROZEN,
        ),
        crate::application::test_constant_policy(),
        None,
        &control,
    ) {
        Err(error) => error,
        Ok(_) => panic!("the compiled carrier cannot yet encode the completed Router"),
    };
    assert!(
        matches!(&error, novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(reason)
        if reason == "the compiled package carrier does not express change-stream router sinks yet")
    );
    assert_eq!(
        crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread(),
        freezes
    );
    let fact = error.failure_fact();
    assert_eq!(fact.class(), FailureClass::Unsupported);
    assert_eq!(fact.reason().domain(), domain::PLAN_CODEC.get());
    assert_eq!(fact.reason().code(), 40);
    let dml = crate::dml::error::DmlExecutionError::from_encode(error).into_dml_error(None);
    assert_eq!(dml.execution_failure().unwrap().fact(), fact);
}
