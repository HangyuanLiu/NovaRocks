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

//! Final-owner correspondence tests, not complete compiler/native acceptance.
use super::*;
use crate::*;
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use novarocks_connector_contract::*;
use novarocks_functions::FunctionValueType;
use novarocks_type_contract::{
    CompilePhase, EvaluationDomainId, ExpressionEffectContext, ExpressionUseId,
    FunctionArgumentType,
};
use novarocks_types::SlotId;
use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Control {
    stop: Option<(usize, CompileControlError)>,
    trace: Mutex<Vec<u32>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let index = trace.len();
        trace.push(units);
        match self.stop {
            Some((at, cause)) if at == index => Err(cause),
            _ => Ok(()),
        }
    }
}
fn checked(writer: bool, sink: bool, legacy: bool) -> ProgramLexicalBindings {
    checked_with(writer, sink, legacy, false)
}
/// The same graph whose one value channel is nullable when `nullable` holds.
fn checked_with(writer: bool, sink: bool, legacy: bool, nullable: bool) -> ProgramLexicalBindings {
    let control = Control::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "v",
        DataType::Int64,
        nullable,
    )]));
    let layout = StaticLayout::try_new(schema.clone(), Arc::from([SlotId::new(1)])).unwrap();
    let values = StaticValues::try_new(
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7]))]).unwrap(),
        layout.clone(),
    )
    .unwrap();
    let node = |index, kind| {
        if legacy {
            ProgramNode::new(index as i32, kind, layout.clone())
        } else {
            ProgramNode::new_local(
                ProgramNodeId::new(index),
                vec![DiagnosticSourceNodeId::new(u32::MAX)],
                kind,
                layout.clone(),
            )
        }
    };
    let mut nodes = vec![node(0, ProgramNodeKind::Values { values })];
    if writer {
        let arena = Arc::new(
            ImmutableExpressions::try_new(
                vec![StaticExprNode::new(
                    StaticExprKind::Literal(StaticLiteral::Int64(7)),
                    DataType::Int64,
                    None,
                )],
                false,
                HashMap::new(),
                None,
            )
            .unwrap(),
        );
        nodes.push(node(
            1,
            ProgramNodeKind::TableWriter {
                input: ProgramNodeId::new(0),
                target: WriteTargetOrdinal::try_new(0).unwrap(),
                expected_layout: layout.clone(),
                projection: StaticWriterProjection {
                    arena,
                    expressions: vec![ProgramExprId::new(0)],
                    layout: layout.clone(),
                },
                writer_multiplex_layout: layout.clone(),
                partial_aggregates: vec![],
            },
        ));
    }
    let root = ProgramNodeId::new(nodes.len() - 1);
    let mut entries = if writer {
        vec![BindingRequirement::TableWriter {
            node: root,
            layout: layout.clone(),
        }]
    } else {
        vec![]
    };
    if sink {
        entries.push(BindingRequirement::ResultSink {
            layout: layout.clone(),
        });
    }
    let requirements = BindingRequirements::try_new(entries).unwrap();
    let graph = LocalProgramGraph::try_new_with_sink(
        nodes,
        root,
        Arc::new(ImmutableExpressions::try_new(vec![], false, HashMap::new(), None).unwrap()),
        CompileProfile::new(
            NonZeroUsize::new(1).unwrap(),
            None,
            layout.identity().unwrap(),
            KernelAbiVersion::CURRENT,
        ),
        requirements,
        sink.then_some(StaticSinkProgram::Result),
    )
    .unwrap();
    let roots = ProgramExpressionRoots::collect(&graph, &control).unwrap();
    let mut bindings = Vec::new();
    let mut flows = BTreeMap::new();
    let mut types = BTreeMap::new();
    for (scope, arena) in roots.arenas() {
        assert!(arena.nodes().iter().all(|expr| matches!(
            expr.kind(),
            StaticExprKind::Literal(StaticLiteral::Int64(7))
        )));
        let uses = roots
            .sites()
            .iter()
            .filter(|(site, _)| site.arena() == *scope)
            .enumerate()
            .map(|(index, (site, root))| {
                let id = ExpressionUseId::new(index as u32);
                bindings.push(ProgramRootUseBinding {
                    site: *site,
                    use_id: id,
                });
                ProgramExpressionUse {
                    context: ExpressionEffectContext {
                        use_id: id,
                        domain: EvaluationDomainId::new(0),
                        demand: root.demand,
                    },
                    definition: root.definition,
                    control: ControlShape::Eager,
                    arguments: Box::default(),
                }
            })
            .collect();
        flows.insert(
            *scope,
            ProgramControlFlow::try_new(
                vec![ProgramEvaluationDomain {
                    id: EvaluationDomainId::new(0),
                    parent: None,
                    guard: None,
                }],
                uses,
                arena.nodes().len(),
                &control,
            )
            .unwrap(),
        );
        types.insert(
            *scope,
            vec![
                FunctionArgumentType::Value(FunctionValueType::new(DataType::Int64, nullable));
                arena.nodes().len()
            ],
        );
    }
    let snapshot = ProgramRootControlBindings::try_new(graph, flows, bindings, &control).unwrap();
    let calls = ProgramResolvedCalls::try_new(snapshot, vec![], &control).unwrap();
    let expressions = ProgramTypedExpressions::try_new(calls, types, &control).unwrap();
    let mut channels = vec![(
        ProgramChannelSite::Layout {
            node: ProgramNodeId::new(0),
            role: ProgramChannelLayoutRole::NodeOutput,
            ordinal: 0,
        },
        FunctionValueType::new(DataType::Int64, nullable),
    )];
    if writer {
        for role in [
            ProgramChannelLayoutRole::NodeOutput,
            ProgramChannelLayoutRole::WriterProjection,
            ProgramChannelLayoutRole::WriterMultiplex,
        ] {
            channels.push((
                ProgramChannelSite::Layout {
                    node: root,
                    role,
                    ordinal: 0,
                },
                FunctionValueType::new(DataType::Int64, nullable),
            ));
        }
    }
    let channels = ProgramTypedChannels::try_new(expressions, channels, &control).unwrap();
    ProgramLexicalBindings::try_new(channels, vec![], vec![], &control).unwrap()
}
fn operators(writer: bool, source: u32) -> Vec<LocalOperatorProvenance> {
    (0..if writer { 2 } else { 1 })
        .map(|index| LocalOperatorProvenance {
            id: LocalOperatorId::new(index),
            lowered_nodes: Box::from([ProgramNodeId::new(index as usize)]),
            sources: Box::from([DiagnosticSourceNodeId::new(source)]),
            origin: if index == 0 {
                LocalOperatorOrigin::Direct
            } else {
                LocalOperatorOrigin::Split { piece: 1 }
            },
            cost_owner: LocalOperatorId::new(index),
            metrics: OperatorMetricAggregation {
                cpu_time: MetricAggregation::Sum,
                wall_time: MetricAggregation::Maximum,
                peak_retained_bytes: MetricAggregation::Maximum,
            },
        })
        .collect()
}
fn allowed() -> BTreeSet<DiagnosticSourceNodeId> {
    BTreeSet::from([
        DiagnosticSourceNodeId::new(u32::MAX),
        DiagnosticSourceNodeId::new(0),
    ])
}
struct IdentityProvider;
impl ConnectorWriteRecipeCompiler for IdentityProvider {
    type Error = ConnectorError;
    fn compile_private(
        &self,
        draft: &ConnectorWriteRecipeDraft,
        _: &dyn PureCompileControl,
    ) -> Result<ConnectorWriteRecipeDraft, PureProviderCompileError<ConnectorError>> {
        Ok(draft.clone())
    }
}
fn recipe(field: Field) -> ConnectorWriteRecipe {
    // A pure contract fixture, not the installed Iceberg private compiler.
    let instance = ConnectorInstanceId::try_from_canonical("lake").unwrap();
    let catalog = CatalogHandle::new(instance.clone(), CatalogVersion::from_bytes([7; 32]));
    let provider = ConnectorProviderId::parse("iceberg").unwrap();
    let binding = ConnectorWriteBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: provider.clone(),
            instance_id: instance,
        },
        catalog.clone(),
    );
    let payload = ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            provider,
            catalog,
            ConnectorCodecCategory::WriteHandle,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        bytes::Bytes::from_static(b"fixture"),
    );
    let draft = ConnectorWriteRecipeDraft::try_new(
        binding,
        payload,
        ConnectorWriteInputShape::Data {
            fields: vec![ConnectorWriteFieldBinding::new(
                ConnectorWriteFieldToken::from_bytes([1; 32]),
                field,
            )],
        },
    )
    .unwrap();
    ConnectorWriteRecipe::try_compile_with_provider(&draft, &IdentityProvider, &Control::default())
        .unwrap()
}
#[test]
fn final_owner_has_one_complete_chain_and_sparse_actual_graph_origins() {
    let checked = checked(false, true, false);
    let graph_ptr = checked
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot()
        .program()
        .nodes()
        .as_ptr();
    let program = LocalProgram::try_new(
        checked,
        operators(false, u32::MAX),
        &allowed(),
        CompiledProgramFacts {
            writes: BTreeMap::new(),
            exchange_inputs: BTreeMap::new(),
            scan_inputs: BTreeMap::new(),
            aggregates: BTreeMap::new(),
        },
        &Control::default(),
    )
    .unwrap();
    assert_eq!(program.graph().nodes().as_ptr(), graph_ptr);
    assert!(program.graph().nodes()[0].legacy_native_node_id().is_none());
    assert_eq!(
        program.graph().nodes()[0].local_id(),
        Some(ProgramNodeId::new(0))
    );
    assert_eq!(
        program
            .provenance()
            .get(LocalOperatorId::new(0))
            .unwrap()
            .sources
            .as_ref(),
        &[DiagnosticSourceNodeId::new(u32::MAX)]
    );
    assert!(program.write_recipes().is_empty());
}
#[test]
fn final_owner_rejects_foreign_origins_legacy_identity_and_missing_sink() {
    for (legacy, sink, source, expected) in [
        (
            false,
            true,
            0,
            LocalProgramCompileError::Origins(CompiledOriginsError::SourceMismatch),
        ),
        (
            true,
            true,
            u32::MAX,
            LocalProgramCompileError::Origins(CompiledOriginsError::InvalidLocalIdentity),
        ),
        (
            false,
            false,
            u32::MAX,
            LocalProgramCompileError::MissingSink,
        ),
    ] {
        assert_eq!(
            LocalProgram::try_new(
                checked(false, sink, legacy),
                operators(false, source),
                &allowed(),
                CompiledProgramFacts {
                    writes: BTreeMap::new(),
                    exchange_inputs: BTreeMap::new(),
                    scan_inputs: BTreeMap::new(),
                    aggregates: BTreeMap::new(),
                },
                &Control::default()
            )
            .unwrap_err(),
            expected
        );
    }
}
#[test]
fn actual_writer_recipe_coverage_and_exact_projection_fields_are_mandatory() {
    let id = ProgramNodeId::new(1);
    assert_eq!(
        LocalProgram::try_new(
            checked(true, true, false),
            operators(true, u32::MAX),
            &allowed(),
            CompiledProgramFacts {
                writes: BTreeMap::new(),
                exchange_inputs: BTreeMap::new(),
                scan_inputs: BTreeMap::new(),
                aggregates: BTreeMap::new(),
            },
            &Control::default()
        )
        .unwrap_err(),
        LocalProgramCompileError::Provider(ProviderLinkError::MissingWriterRecipe(id))
    );
    let exact = recipe(Field::new("v", DataType::Int64, false));
    let program = LocalProgram::try_new(
        checked(true, true, false),
        operators(true, u32::MAX),
        &allowed(),
        CompiledProgramFacts {
            writes: BTreeMap::from([(id, exact.clone())]),
            exchange_inputs: BTreeMap::new(),
            scan_inputs: BTreeMap::new(),
            aggregates: BTreeMap::new(),
        },
        &Control::default(),
    )
    .unwrap();
    assert_eq!(program.write_recipes()[&id], exact);
    assert_eq!(
        LocalProgram::try_new(
            checked(false, true, false),
            operators(false, u32::MAX),
            &allowed(),
            CompiledProgramFacts {
                writes: BTreeMap::from([(ProgramNodeId::new(0), exact)]),
                exchange_inputs: BTreeMap::new(),
                scan_inputs: BTreeMap::new(),
                aggregates: BTreeMap::new(),
            },
            &Control::default()
        )
        .unwrap_err(),
        LocalProgramCompileError::Provider(ProviderLinkError::UnexpectedWriterRecipe(
            ProgramNodeId::new(0)
        ))
    );
    let renamed = recipe(Field::new("foreign", DataType::Int64, false));
    assert_eq!(
        LocalProgram::try_new(
            checked(true, true, false),
            operators(true, u32::MAX),
            &allowed(),
            CompiledProgramFacts {
                writes: BTreeMap::from([(id, renamed)]),
                exchange_inputs: BTreeMap::new(),
                scan_inputs: BTreeMap::new(),
                aggregates: BTreeMap::new(),
            },
            &Control::default()
        )
        .unwrap_err(),
        LocalProgramCompileError::Provider(ProviderLinkError::FieldMismatch {
            node: id,
            ordinal: 0
        })
    );
}
#[test]
fn writer_projection_admits_either_nullability_direction_but_nothing_else() {
    let id = ProgramNodeId::new(1);
    let facts = |field| CompiledProgramFacts {
        writes: BTreeMap::from([(id, recipe(field))]),
        exchange_inputs: BTreeMap::new(),
        scan_inputs: BTreeMap::new(),
        aggregates: BTreeMap::new(),
    };
    // A nullable value feeding a NOT NULL target field is the writer's row
    // obligation, and a non-null value feeding a nullable one needs none.
    for (nullable_value, nullable_target) in [(true, false), (false, true), (true, true)] {
        let program = LocalProgram::try_new(
            checked_with(true, true, false, nullable_value),
            operators(true, u32::MAX),
            &allowed(),
            facts(Field::new("v", DataType::Int64, nullable_target)),
            &Control::default(),
        )
        .unwrap();
        assert_eq!(
            program.write_recipes()[&id]
                .draft()
                .input()
                .fields_iter()
                .next()
                .unwrap()
                .field()
                .is_nullable(),
            nullable_target
        );
    }
    // Only top-level nullability is relaxed: a name or carrier still differs.
    for foreign in [
        Field::new("foreign", DataType::Int64, false),
        Field::new("v", DataType::Int32, false),
    ] {
        let error = LocalProgram::try_new(
            checked_with(true, true, false, true),
            operators(true, u32::MAX),
            &allowed(),
            facts(foreign),
            &Control::default(),
        )
        .unwrap_err();
        assert!(
            matches!(
                error,
                LocalProgramCompileError::Provider(
                    ProviderLinkError::FieldMismatch { .. } | ProviderLinkError::TypeMismatch(_)
                )
            ),
            "{error:?}"
        );
    }
}
#[test]
fn every_final_author_callback_propagates_original_control_without_rechecking() {
    let baseline = Control::default();
    LocalProgram::try_new(
        checked(true, true, false),
        operators(true, u32::MAX),
        &allowed(),
        CompiledProgramFacts {
            writes: BTreeMap::from([(
                ProgramNodeId::new(1),
                recipe(Field::new("v", DataType::Int64, false)),
            )]),
            exchange_inputs: BTreeMap::new(),
            scan_inputs: BTreeMap::new(),
            aggregates: BTreeMap::new(),
        },
        &baseline,
    )
    .unwrap();
    let trace = baseline.trace.into_inner().unwrap();
    assert!(trace.iter().any(|units| *units > 0));
    for index in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                stop: Some((index, cause)),
                ..Control::default()
            };
            let error = LocalProgram::try_new(
                checked(true, true, false),
                operators(true, u32::MAX),
                &allowed(),
                CompiledProgramFacts {
                    writes: BTreeMap::from([(
                        ProgramNodeId::new(1),
                        recipe(Field::new("v", DataType::Int64, false)),
                    )]),
                    exchange_inputs: BTreeMap::new(),
                    scan_inputs: BTreeMap::new(),
                    aggregates: BTreeMap::new(),
                },
                &control,
            )
            .unwrap_err();
            assert_eq!(error, LocalProgramCompileError::Control(cause));
            assert_eq!(*control.trace.lock().unwrap(), trace[..=index]);
            assert_eq!(
                std::error::Error::source(&error)
                    .unwrap()
                    .downcast_ref::<CompileControlError>(),
                Some(&cause)
            );
        }
    }
}
#[test]
fn exchange_input_addresses_cover_exactly_the_actual_exchange_sources() {
    let address = CompiledExchangeInput {
        receiver_node: 3,
        edge: 5,
        source_fragment: 1,
        hash_partition_slots: Box::default(),
    };
    // A non-exchange node and a node outside the graph both refuse an address.
    for node in [ProgramNodeId::new(0), ProgramNodeId::new(9)] {
        assert_eq!(
            LocalProgram::try_new(
                checked(false, true, false),
                operators(false, u32::MAX),
                &allowed(),
                CompiledProgramFacts {
                    writes: BTreeMap::new(),
                    exchange_inputs: BTreeMap::from([(node, address.clone())]),
                    scan_inputs: BTreeMap::new(),
                    aggregates: BTreeMap::new(),
                },
                &Control::default()
            )
            .unwrap_err(),
            LocalProgramCompileError::ExchangeInputMismatch(node)
        );
    }
    let program = LocalProgram::try_new(
        checked(false, true, false),
        operators(false, u32::MAX),
        &allowed(),
        CompiledProgramFacts {
            writes: BTreeMap::new(),
            exchange_inputs: BTreeMap::new(),
            scan_inputs: BTreeMap::new(),
            aggregates: BTreeMap::new(),
        },
        &Control::default(),
    )
    .unwrap();
    assert!(program.exchange_inputs().is_empty());
}
#[test]
fn scan_input_address_on_a_non_scan_node_is_refused() {
    // The Values root is neither a scan nor a declared scan requirement.
    assert_eq!(
        LocalProgram::try_new(
            checked(false, true, false),
            operators(false, u32::MAX),
            &allowed(),
            CompiledProgramFacts {
                writes: BTreeMap::new(),
                exchange_inputs: BTreeMap::new(),
                scan_inputs: BTreeMap::from([(
                    ProgramNodeId::new(0),
                    CompiledScanInput { scan_node: 4 }
                )]),
                aggregates: BTreeMap::new(),
            },
            &Control::default()
        )
        .unwrap_err(),
        LocalProgramCompileError::ScanInputMismatch(ProgramNodeId::new(0))
    );
}
#[test]
fn aggregate_facts_cover_exactly_the_actual_aggregate_nodes() {
    // The Values root is not an Aggregate, and a node outside the graph is
    // not one either: neither may carry a grouping guarantee.
    for node in [ProgramNodeId::new(0), ProgramNodeId::new(9)] {
        assert_eq!(
            LocalProgram::try_new(
                checked(false, true, false),
                operators(false, u32::MAX),
                &allowed(),
                CompiledProgramFacts {
                    aggregates: BTreeMap::from([(
                        node,
                        CompiledAggregate {
                            grouping: CompiledAggregateGrouping::Complete,
                        },
                    )]),
                    ..CompiledProgramFacts::default()
                },
                &Control::default(),
            )
            .unwrap_err(),
            LocalProgramCompileError::AggregateMismatch(node)
        );
    }
    let program = LocalProgram::try_new(
        checked(false, true, false),
        operators(false, u32::MAX),
        &allowed(),
        CompiledProgramFacts::default(),
        &Control::default(),
    )
    .unwrap();
    assert!(program.aggregates().is_empty());
}
