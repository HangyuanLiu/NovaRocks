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

use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::sync::Arc;

use novarocks_connector_contract::{
    ConnectorReadRelationRecipeDraft, ConnectorValueType, FrozenConnectorScan,
    StaticScanAssignment, TupleDomain,
};
use novarocks_type_contract::{SemanticParameterId, SemanticParameterValue, SemanticParameters};

use super::*;

fn package_input(fragment: Fragment) -> FragmentPackageInput {
    FragmentPackageInput {
        version: version(),
        required: RequiredContracts::default(),
        fragment,
        cuts: FragmentCuts::default(),
        result: None,
        parameters: SemanticParameters::default(),
        scans: BTreeMap::new(),
        writes: BTreeMap::new(),
        annotations: Box::default(),
    }
}

fn frozen_scan(fragment: &Fragment) -> FrozenConnectorScan {
    let NodeKind::Scan {
        relation,
        read_budget,
        ..
    } = &fragment.nodes()[&fragment.root()].kind
    else {
        unreachable!()
    };
    let read = relation.read();
    let draft = ConnectorReadRelationRecipeDraft::try_new(
        read.binding.clone(),
        read.relation.clone(),
        relation
            .schema()
            .iter()
            .map(|field| field.column.column_payload.clone())
            .collect(),
    )
    .unwrap();
    FrozenConnectorScan::try_new(
        draft,
        vec![StaticScanAssignment::new(
            Arc::from("v"),
            ConnectorValueType::BigInt,
        )],
        TupleDomain::all(),
        TupleDomain::all(),
        None,
        vec![],
        NonZeroU64::new(read_budget.max_batch_rows).unwrap(),
        NonZeroU64::new(read_budget.max_batch_bytes).unwrap(),
        relation.work_source(),
    )
    .unwrap()
}

#[test]
fn package_extracts_metadata_without_losing_public_scan_facts() {
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 7),
    };
    let fragment = finish_scan_relation(metadata_relation(&binding, column)).unwrap();
    let scan = frozen_scan(&fragment);
    let fragment_id = fragment.id();
    let node_id = fragment.root();
    let mut builder = PlanBuilder::new(version());
    builder.add_fragment(fragment.clone()).unwrap();
    builder.add_annotation(PlanAnnotation {
        subject: AnnotationSubject::Node(fragment_id, node_id),
        key: "statistics.row_count".into(),
        value: "17".into(),
    });
    let plan = builder.finish().unwrap();
    let parameters = SemanticParameters::try_new([(
        SemanticParameterId::new(u32::MAX),
        SemanticParameterValue::StatementStartUtc(-17),
    )])
    .unwrap();
    let scans = BTreeMap::from([(ProviderReadOccurrenceId::new(0), scan.clone())]);
    let packages = extract_fragment_packages(&plan, &scans, &parameters, &BTreeMap::new()).unwrap();
    let package = &packages[&fragment_id];
    assert_eq!(package.fragment(), &fragment);
    assert!(package.parameters().entries().is_empty());
    assert_eq!(package.scans()[&node_id], scan);
    assert_eq!(package.annotations(), plan.annotations());
    assert!(
        matches!(&package.fragment().nodes()[&node_id].kind, NodeKind::Scan { relation, .. } if matches!(relation.as_ref(), Relation::Metadata(_)))
    );
    assert!(matches!(
        extract_fragment_packages(&plan, &BTreeMap::new(), &parameters, &BTreeMap::new()),
        Err(FragmentPackageExtractionError::MissingScan(_))
    ));
    let mut extra = scans;
    extra.insert(ProviderReadOccurrenceId::new(1), scan);
    assert!(matches!(
        extract_fragment_packages(&plan, &extra, &parameters, &BTreeMap::new()),
        Err(FragmentPackageExtractionError::UnusedScan)
    ));
}

#[test]
fn package_refuses_missing_or_wrong_scan_node_facts() {
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 7),
    };
    let fragment = finish_scan_relation(metadata_relation(&binding, column)).unwrap();
    let mut input = package_input(fragment.clone());
    assert!(
        FragmentPackage::try_new(input.clone())
            .unwrap_err()
            .to_string()
            .contains("no complete frozen public facts")
    );
    input.scans.insert(fragment.root(), frozen_scan(&fragment));
    FragmentPackage::try_new(input.clone()).unwrap();
    input
        .scans
        .insert(NodeId::new(u32::MAX), frozen_scan(&fragment));
    assert!(
        FragmentPackage::try_new(input)
            .unwrap_err()
            .to_string()
            .contains("missing or non-scan node")
    );
}

#[test]
fn package_refuses_exact_relation_and_batch_contract_drift() {
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 7),
    };
    let fragment = finish_scan_relation(metadata_relation(&binding, column)).unwrap();
    let scan = frozen_scan(&fragment);
    let recipe = scan.recipe();
    for mutation in 0..4 {
        let mut columns = recipe.columns().to_vec();
        if mutation == 0 {
            columns[0] = encoded(&binding, ConnectorCodecCategory::ReadColumn, 8);
        }
        let draft = ConnectorReadRelationRecipeDraft::try_new(
            recipe.binding().clone(),
            recipe.relation().clone(),
            columns,
        )
        .unwrap();
        let malformed = FrozenConnectorScan::try_new(
            draft,
            if mutation == 3 {
                vec![StaticScanAssignment::new(
                    Arc::from("v"),
                    ConnectorValueType::Varchar,
                )]
            } else {
                scan.assignments().to_vec()
            },
            TupleDomain::all(),
            TupleDomain::all(),
            None,
            vec![],
            if mutation == 1 {
                NonZeroU64::new(scan.max_batch_rows().get() + 1).unwrap()
            } else {
                scan.max_batch_rows()
            },
            scan.max_batch_bytes(),
            if mutation == 2 {
                ConnectorReadWorkSource::WholeRelation
            } else {
                scan.work_source()
            },
        )
        .unwrap();
        let mut input = package_input(fragment.clone());
        input.scans.insert(fragment.root(), malformed);
        assert!(
            FragmentPackage::try_new(input).is_err(),
            "mutation {mutation}"
        );
    }
}

#[test]
fn result_and_annotation_facts_are_checked_at_the_local_boundary() {
    let id = FragmentId::new(81);
    let (fragment, value) = literal_fragment(id, FragmentSink::Result, false);
    let field = ResultField {
        name: "v".into(),
        alias: None,
        value,
        ty: fragment.values()[&value].ty.clone(),
    };
    let mut input = package_input(fragment.clone());
    assert!(FragmentPackage::try_new(input.clone()).is_err());
    input.result = Some(ResultPort {
        fragment: id,
        output: fragment.nodes()[&fragment.root()].output.clone(),
        fields: Box::from([field]),
    });
    FragmentPackage::try_new(input.clone()).unwrap();
    let mut wrong_result = input.clone();
    wrong_result.result.as_mut().unwrap().fields[0].ty.nullable = true;
    assert!(
        FragmentPackage::try_new(wrong_result)
            .unwrap_err()
            .to_string()
            .contains("result type differs")
    );
    input.annotations = Box::from([PlanAnnotation {
        subject: AnnotationSubject::Node(FragmentId::new(82), fragment.root()),
        key: "statistics.row_count".into(),
        value: "17".into(),
    }]);
    assert!(
        FragmentPackage::try_new(input)
            .unwrap_err()
            .to_string()
            .contains("subject this plan does not have")
    );
}

#[test]
fn remote_plan_statistics_do_not_grow_a_fragment_package() {
    let (fragment, _) = literal_fragment(FragmentId::new(81), FragmentSink::Noop, false);
    let mut builder = PlanBuilder::new(version());
    builder.add_fragment(fragment.clone()).unwrap();
    let before = builder.finish().unwrap();
    let mut builder = PlanBuilder::new(version());
    builder.add_fragment(fragment).unwrap();
    builder.add_annotation(PlanAnnotation {
        subject: AnnotationSubject::Plan,
        key: "optimizer.table_statistics".into(),
        value: "remote table statistics".repeat(100).into(),
    });
    let after = builder.finish().unwrap();
    let parameters = SemanticParameters::default();
    assert_eq!(
        extract_fragment_packages(&before, &BTreeMap::new(), &parameters, &BTreeMap::new())
            .unwrap(),
        extract_fragment_packages(&after, &BTreeMap::new(), &parameters, &BTreeMap::new()).unwrap()
    );
    assert_eq!(after.annotations().len(), 1);
}

#[test]
fn duplicate_scan_payloads_keep_runtime_filter_assignment_occurrences() {
    let (fragment, filter, second) =
        super::contract_regressions::scan_lineage_filter(false, false, false, true);
    let scan_id = filter.consumers[0].endpoint.node;
    let second = second.unwrap();
    let mut nodes = fragment.nodes().clone();
    let NodeKind::Scan {
        relation,
        provider_outputs,
        ..
    } = &mut nodes.get_mut(&scan_id).unwrap().kind
    else {
        unreachable!()
    };
    let first_field = provider_outputs[0].0.clone();
    provider_outputs[1].0 = first_field.clone();
    let schema = Box::from([relation.schema()[0].clone(), relation.schema()[0].clone()]);
    match relation.as_mut() {
        Relation::Data(relation) => relation.schema = schema,
        Relation::Metadata(relation) => relation.schema = schema,
    }
    let mut values = fragment.values().clone();
    values.get_mut(&second).unwrap().origin = ValueOrigin::ProviderField {
        scan_node: scan_id,
        field: first_field,
    };
    let fragment = Fragment::from(crate::plan::FragmentParts {
        id: fragment.id(),
        root: fragment.root(),
        nodes,
        values,
        expressions: fragment.expressions().clone(),
        sink: fragment.sink().clone(),
        dop_domain: fragment.dop_domain(),
        runtime_filters: fragment.runtime_filters().into(),
    });
    let NodeKind::Scan {
        relation,
        read_budget,
        ..
    } = &fragment.nodes()[&scan_id].kind
    else {
        unreachable!()
    };
    let read = relation.read();
    let recipe = ConnectorReadRelationRecipeDraft::try_new(
        read.binding.clone(),
        read.relation.clone(),
        relation
            .schema()
            .iter()
            .map(|field| field.column.column_payload.clone())
            .collect(),
    )
    .unwrap();
    let mut input = package_input(fragment.clone());
    input.cuts = FragmentCuts {
        runtime_filters: Box::from([filter]),
        ..FragmentCuts::default()
    };
    // The consumer targets the first ValueId. The second assignment has an
    // identical private payload but is a different output occurrence.
    for (variable, accepted) in [("v0", true), ("v1", false)] {
        let scan = FrozenConnectorScan::try_new(
            recipe.clone(),
            vec![
                StaticScanAssignment::new(Arc::from("v0"), ConnectorValueType::BigInt),
                StaticScanAssignment::new(Arc::from("v1"), ConnectorValueType::BigInt),
            ],
            TupleDomain::all(),
            TupleDomain::all(),
            None,
            vec![novarocks_connector_contract::StaticScanDynamicFilter::new(
                101,
                Arc::from(variable),
            )],
            NonZeroU64::new(read_budget.max_batch_rows).unwrap(),
            NonZeroU64::new(read_budget.max_batch_bytes).unwrap(),
            relation.work_source(),
        )
        .unwrap();
        input.scans.insert(scan_id, scan);
        let result = FragmentPackage::try_new(input.clone());
        assert_eq!(result.is_ok(), accepted, "{variable}: {result:?}");
    }
}

fn parameter_fragment(reference: novarocks_type_contract::SemanticParameterRef) -> Fragment {
    let mut builder = FragmentBuilder::new(FragmentId::new(81));
    let node = builder.reserve_node_id().unwrap();
    let value_type = ty(DataType::Int64, false);
    let expr = builder
        .add_expression(
            node,
            value_type.clone(),
            ExprKind::FunctionCall {
                function: BoundFunction {
                    function_id: FunctionId::try_new("test.parameter").unwrap(),
                    overload: FunctionOverloadId::try_new("test.parameter.zero").unwrap(),
                    kind: FunctionKind::Scalar,
                    argument_types: Box::default(),
                    result_type: value_type.clone(),
                    volatility: FunctionVolatility::Stable,
                    argument_evaluation: FunctionArgumentEvaluation::Eager,
                    failure_behavior: FunctionFailureBehavior::Propagate,
                    intrinsic_row_error: FunctionIntrinsicRowError::NoRowError,
                    semantic_parameters: Box::from([reference]),
                },
                args: Box::default(),
            },
        )
        .unwrap();
    let value = builder
        .add_value(
            value_type,
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton(),
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([expr])]),
            },
        })
        .unwrap();
    builder
        .finish_definition(node, FragmentSink::Noop, dop())
        .unwrap()
}

#[test]
fn package_parameters_are_the_exact_call_dependency_closure() {
    use novarocks_type_contract::{SemanticParameterKey, SemanticParameterRef};
    let reference = SemanticParameterRef {
        id: SemanticParameterId::new(u32::MAX),
        expected_key: SemanticParameterKey::TimeZone,
    };
    let fragment = parameter_fragment(reference);
    let mut builder = PlanBuilder::new(version());
    builder.add_fragment(fragment).unwrap();
    let plan = builder.finish().unwrap();
    let parameters = SemanticParameters::try_new([
        (
            reference.id,
            SemanticParameterValue::TimeZone("Asia/Shanghai".into()),
        ),
        (
            SemanticParameterId::new(1),
            SemanticParameterValue::TimeZone("UTC".into()),
        ),
    ])
    .unwrap();
    let packages =
        extract_fragment_packages(&plan, &BTreeMap::new(), &parameters, &BTreeMap::new()).unwrap();
    let mut input = packages[&FragmentId::new(81)].clone().into_input();
    assert_eq!(input.parameters.entries().len(), 1);
    assert_eq!(
        input.parameters.require(reference).unwrap(),
        &SemanticParameterValue::TimeZone("Asia/Shanghai".into())
    );
    input.parameters = parameters;
    assert!(
        FragmentPackage::try_new(input.clone())
            .unwrap_err()
            .to_string()
            .contains("unused definitions")
    );
    input.parameters = SemanticParameters::default();
    assert!(
        FragmentPackage::try_new(input.clone())
            .unwrap_err()
            .to_string()
            .contains("missing semantic parameter ID")
    );
    input.parameters = SemanticParameters::try_new([(
        reference.id,
        SemanticParameterValue::AllowThrowException(true),
    )])
    .unwrap();
    assert!(
        FragmentPackage::try_new(input)
            .unwrap_err()
            .to_string()
            .contains("expected key")
    );
}

#[test]
fn package_cannot_carry_whole_plan_display_annotations() {
    let (fragment, _) = literal_fragment(FragmentId::new(81), FragmentSink::Noop, false);
    let mut input = package_input(fragment);
    input.annotations = Box::from([PlanAnnotation {
        subject: AnnotationSubject::Plan,
        key: "optimizer.table_statistics".into(),
        value: "peer facts".into(),
    }]);
    assert!(FragmentPackage::try_new(input).is_err());
}

#[test]
fn shared_expression_definitions_keep_each_use_demand_and_occurrence() {
    use novarocks_type_contract::EvaluationDemand::{TruthOnly, Value};
    let expr = ExprId::new(u32::MAX);
    let value = ValueId::new(2);
    let filter = NodeKind::Filter {
        predicates: Box::from([expr, expr]),
    };
    assert_eq!(
        filter.evaluation_roots(),
        vec![
            ExprUse {
                expr,
                demand: TruthOnly
            };
            2
        ]
    );
    let project = NodeKind::Project {
        expressions: Box::from([(expr, value), (expr, value)]),
    };
    assert_eq!(
        project.evaluation_roots(),
        vec![
            ExprUse {
                expr,
                demand: Value
            };
            2
        ]
    );
    for (kind, demand) in [
        (JoinKind::Inner, TruthOnly),
        (JoinKind::NullAwareLeftAnti, Value),
    ] {
        let join = NodeKind::HashJoin {
            kind,
            build_side: JoinSide::Right,
            distribution: JoinDistribution::BroadcastBuild,
            keys: Box::from([JoinKey {
                left: expr,
                right: expr,
                null_safe: false,
            }]),
            residual: Some(expr),
            null_extended: Box::default(),
        };
        assert_eq!(
            join.evaluation_roots(),
            vec![
                ExprUse {
                    expr,
                    demand: Value
                },
                ExprUse {
                    expr,
                    demand: Value
                },
                ExprUse { expr, demand }
            ]
        );
    }
}

#[test]
fn writer_package_requires_the_exact_public_input_recipe() {
    use novarocks_connector_contract::{
        ConnectorWriteBinding, ConnectorWriteFieldBinding, ConnectorWriteFieldToken,
        ConnectorWriteInputShape, ConnectorWriteRecipeDraft,
    };
    let plan = super::sink_contract::finish_router_writer_plan(
        super::sink_contract::RouterWriterShape::Valid,
    )
    .unwrap();
    let fragment = plan.fragments()[&FragmentId::new(722)].clone();
    let writer_id = fragment.root();
    let NodeKind::TableWriter { target } = &fragment.nodes()[&writer_id].kind else {
        unreachable!()
    };
    let read_binding = connector_binding();
    let binding = ConnectorWriteBinding::new(
        read_binding.descriptor().clone(),
        read_binding.catalog_handle().clone(),
    );
    let fields = target
        .target_fields
        .iter()
        .map(|field| {
            ConnectorWriteFieldBinding::new(
                field.token,
                arrow_schema::Field::new(
                    field.provider_name.as_ref(),
                    field.ty.data_type.clone(),
                    field.ty.nullable,
                ),
            )
        })
        .collect::<Vec<_>>();
    let write = ConnectorWriteRecipeDraft::try_new(
        binding.clone(),
        target.handle.clone(),
        ConnectorWriteInputShape::Data {
            fields: fields.clone(),
        },
    )
    .unwrap();
    let mut input = package_input(fragment.clone());
    input.cuts = fragment_cuts(&plan, fragment.id()).unwrap();
    assert!(
        FragmentPackage::try_new(input.clone())
            .unwrap_err()
            .to_string()
            .contains("no complete frozen public facts")
    );
    input.writes.insert(writer_id, write.clone());
    FragmentPackage::try_new(input.clone()).unwrap();
    let mut wrong_node = input.clone();
    wrong_node.writes.insert(NodeId::new(u32::MAX), write);
    assert!(
        FragmentPackage::try_new(wrong_node)
            .unwrap_err()
            .to_string()
            .contains("missing or non-writer node")
    );
    for mutation in 0..3 {
        let field = &fields[0];
        let malformed = match mutation {
            0 => ConnectorWriteFieldBinding::new(
                ConnectorWriteFieldToken::from_bytes([8; 32]),
                field.field().clone(),
            ),
            1 => ConnectorWriteFieldBinding::new(
                field.token(),
                arrow_schema::Field::new("different", DataType::Int64, false),
            ),
            _ => ConnectorWriteFieldBinding::new(
                field.token(),
                arrow_schema::Field::new("v", DataType::Utf8, false),
            ),
        };
        let write = ConnectorWriteRecipeDraft::try_new(
            binding.clone(),
            target.handle.clone(),
            ConnectorWriteInputShape::Data {
                fields: vec![malformed],
            },
        )
        .unwrap();
        input.writes.insert(writer_id, write);
        assert!(
            FragmentPackage::try_new(input.clone())
                .unwrap_err()
                .to_string()
                .contains("input field occurrence")
        );
    }
}

#[test]
fn writer_package_preserves_nested_dictionary_field_identity() {
    use novarocks_connector_contract::{
        ConnectorWriteBinding, ConnectorWriteFieldBinding, ConnectorWriteInputShape,
        ConnectorWriteRecipeDraft,
    };
    #[allow(deprecated)]
    let nested_type = |dictionary_id| {
        DataType::Struct(
            vec![arrow_schema::Field::new_dict(
                "dictionary",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                false,
                dictionary_id,
                false,
            )]
            .into(),
        )
    };
    let plan = super::sink_contract::finish_router_writer_plan(
        super::sink_contract::RouterWriterShape::Valid,
    )
    .unwrap();
    let original = &plan.fragments()[&FragmentId::new(722)];
    let mut nodes = original.nodes().clone();
    let NodeKind::TableWriter { target } = &mut nodes.get_mut(&original.root()).unwrap().kind
    else {
        unreachable!()
    };
    let input_value = target.target_fields[0].input;
    target.target_fields[0].ty.data_type = nested_type(1);
    let target = target.clone();
    let mut values = original.values().clone();
    values.get_mut(&input_value).unwrap().ty.data_type = nested_type(1);
    let fragment = Fragment::from(crate::plan::FragmentParts {
        id: original.id(),
        root: original.root(),
        values,
        expressions: original.expressions().clone(),
        nodes,
        sink: original.sink().clone(),
        dop_domain: original.dop_domain(),
        runtime_filters: original.runtime_filters().into(),
    });
    let mut input = package_input(fragment);
    input.cuts = fragment_cuts(&plan, original.id()).unwrap();
    for cut in &mut input.cuts.inbound {
        for import in &mut cut.imports {
            if import.destination == input_value {
                import.source.ty.data_type = nested_type(1);
            }
        }
    }
    let read_binding = connector_binding();
    let binding = ConnectorWriteBinding::new(
        read_binding.descriptor().clone(),
        read_binding.catalog_handle().clone(),
    );
    for id in [1, 2] {
        let fields = target
            .target_fields
            .iter()
            .map(|field| {
                ConnectorWriteFieldBinding::new(
                    field.token,
                    arrow_schema::Field::new(
                        field.provider_name.as_ref(),
                        nested_type(id),
                        field.ty.nullable,
                    ),
                )
            })
            .collect();
        let recipe = ConnectorWriteRecipeDraft::try_new(
            binding.clone(),
            target.handle.clone(),
            ConnectorWriteInputShape::Data { fields },
        )
        .unwrap();
        input.writes.insert(original.root(), recipe);
        let result = FragmentPackage::try_new(input.clone());
        if id == 1 {
            result.unwrap();
        } else {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("input field occurrence")
            );
        }
    }
}
