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

//! Actual final-owner scan links through the complete read contract seal.
//! The identity private compiler is a pure contract fixture, not an installed
//! provider implementation, native plan lowering or provider capability proof.

use super::*;
use crate::*;
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use novarocks_connector_contract::*;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, EvaluationDomainId, FunctionArgumentType, FunctionValueType,
    NR_LOGICAL_TYPE_KEY, ValueLogicalType,
};
use novarocks_types::SlotId;
use std::{
    collections::HashMap,
    num::{NonZeroU64, NonZeroUsize},
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let index = trace.len();
        trace.push(units);
        if let Some((at, cause)) = self.stop
            && index == at
        {
            return Err(cause);
        }
        Ok(())
    }
}
struct IdentityFixture;
impl ConnectorReadProgramCompiler for IdentityFixture {
    type Error = ConnectorError;
    fn compile_private(
        &self,
        frozen: &FrozenConnectorRead,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorReadRelationRecipeDraft, PureProviderCompileError<Self::Error>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)
            .map_err(PureProviderCompileError::Control)?;
        // The actual port receives full public source/root facts. No legacy
        // payload-only validation is used to authorize the full seal.
        let public = frozen.public_facts();
        assert_eq!(public.source().input_version().as_bytes(), &[9]);
        assert_eq!(public.logical_types(), &[ValueLogicalType::Json]);
        assert!(
            !public
                .schema()
                .field(0)
                .metadata()
                .contains_key(NR_LOGICAL_TYPE_KEY)
        );
        work.step().map_err(PureProviderCompileError::Control)?;
        let canonical = canonical(frozen.scan().recipe());
        work.finish().map_err(PureProviderCompileError::Control)?;
        Ok(canonical)
    }
}
// This independent legacy fixture proves rejection of its payload-only seal;
// it never transforms a legacy scan into a full compiled read.
impl ConnectorReadRelationRecipeCompiler for IdentityFixture {
    type Error = ConnectorError;
    fn compile_private(
        &self,
        draft: &ConnectorReadRelationRecipeDraft,
    ) -> Result<ConnectorReadRelationRecipeDraft, Self::Error> {
        Ok(draft.clone())
    }
    fn compile_split_private(
        &self,
        _: &ConnectorReadBinding,
        draft: &ConnectorReadRecipeSplitDraft,
    ) -> Result<ConnectorReadRecipeSplitDraft, Self::Error> {
        Ok(draft.clone())
    }
}
fn canonical(draft: &ConnectorReadRelationRecipeDraft) -> ConnectorReadRelationRecipeDraft {
    let payload = |source: &ConnectorEncodedPayload| {
        ConnectorEncodedPayload::new(
            source.header().clone(),
            Bytes::from_static(b"canonical-private"),
        )
    };
    ConnectorReadRelationRecipeDraft::try_new(
        draft.binding().clone(),
        ConnectorReadRelationPayload::new(
            draft.relation().kind(),
            payload(draft.relation().table()),
            payload(draft.relation().view()),
        ),
        draft.columns().iter().map(payload).collect(),
    )
    .unwrap()
}
fn draft() -> ConnectorReadRelationRecipeDraft {
    let provider = ConnectorProviderId::parse("fixture").unwrap();
    let instance = ConnectorInstanceId::parse("fixture").unwrap();
    let catalog = CatalogHandle::new(instance.clone(), CatalogVersion::from_bytes([1; 32]));
    let binding = ConnectorReadBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: provider.clone(),
            instance_id: instance,
        },
        catalog.clone(),
    );
    let payload = |category| {
        ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                provider.clone(),
                catalog.clone(),
                category,
                ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            Bytes::from_static(b"original-private"),
        )
    };
    ConnectorReadRelationRecipeDraft::try_new(
        binding,
        ConnectorReadRelationPayload::new(
            ConnectorReadRelationKind::Table,
            payload(ConnectorCodecCategory::ReadTable),
            payload(ConnectorCodecCategory::ReadView),
        ),
        vec![payload(ConnectorCodecCategory::ReadColumn)],
    )
    .unwrap()
}
fn schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("json_source", DataType::Utf8, true).with_metadata(HashMap::from([
                ("provider.field-id".into(), "17".into()),
                ("provider.note".into(), "retained".into()),
            ])),
        ],
        HashMap::from([("provider.schema".into(), "source-generation".into())]),
    )
}
fn frozen() -> FrozenConnectorRead {
    let scan = FrozenConnectorScan::try_new(
        draft(),
        vec![StaticScanAssignment::new(
            Arc::from("json_source"),
            ConnectorValueType::Varchar,
        )],
        TupleDomain::all(),
        TupleDomain::all(),
        None,
        vec![],
        NonZeroU64::new(100).unwrap(),
        NonZeroU64::new(4096).unwrap(),
        ConnectorReadWorkSource::RuntimeSplits,
    )
    .unwrap();
    let source = ConnectorReadStaticFacts::try_new(
        ConnectorReadInputVersion::try_new([9]).unwrap(),
        [7; 32],
        ConnectorReadProperties::try_new(ConnectorReadDistribution::Unconstrained, vec![]).unwrap(),
        ConnectorReadArtifactCoverage::NoArtifactInputs,
        vec![],
    )
    .unwrap();
    let public =
        ConnectorReadPublicFacts::try_new(source, None, schema(), vec![ValueLogicalType::Json])
            .unwrap();
    FrozenConnectorRead::try_new(scan, public).unwrap()
}
pub(super) fn recipe() -> ConnectorReadProgramRecipe {
    ConnectorReadProgramRecipe::try_compile_with_provider(
        &frozen(),
        &IdentityFixture,
        &Control::default(),
    )
    .unwrap()
}
fn legacy() -> StaticConnectorScan {
    let recipe =
        ConnectorReadRelationRecipe::try_compile_with_provider(&draft(), &IdentityFixture).unwrap();
    StaticConnectorScan::try_new(
        recipe,
        vec![StaticScanAssignment::new(
            Arc::from("json_source"),
            ConnectorValueType::Varchar,
        )],
        TupleDomain::all(),
        TupleDomain::all(),
        None,
        vec![],
        NonZeroU64::new(100).unwrap(),
        NonZeroU64::new(4096).unwrap(),
        ConnectorReadWorkSource::RuntimeSplits,
    )
    .unwrap()
}
fn value_type(logical: ValueLogicalType, field: &Field) -> FunctionValueType {
    FunctionValueType::try_with_logical_type(
        field.data_type().clone(),
        field.is_nullable(),
        logical,
    )
    .unwrap()
}
pub(super) fn checked(
    source: ProgramScanSource,
    schema: Schema,
    logical: ValueLogicalType,
) -> ProgramLexicalBindings {
    let control = Control::default();
    let ty = value_type(logical, schema.field(0));
    let layout = StaticLayout::try_new(Arc::new(schema), Arc::from([SlotId::new(1)])).unwrap();
    let header = source.relation_header().clone();
    let graph = LocalProgramGraph::try_new_with_sink(
        vec![ProgramNode::new_local(
            ProgramNodeId::new(0),
            vec![DiagnosticSourceNodeId::new(u32::MAX)],
            ProgramNodeKind::Scan {
                source,
                runtime_filters: vec![],
                conjunct_predicate: None,
                limit: None,
            },
            layout.clone(),
        )],
        ProgramNodeId::new(0),
        Arc::new(ImmutableExpressions::try_new(vec![], false, HashMap::new(), None).unwrap()),
        CompileProfile::new(
            NonZeroUsize::new(1).unwrap(),
            None,
            layout.identity().unwrap(),
            KernelAbiVersion::CURRENT,
        ),
        BindingRequirements::try_new(vec![
            BindingRequirement::Scan {
                node: ProgramNodeId::new(0),
                kind: ScanSourceKind::TypedConnector { relation: header },
                layout: layout.clone(),
            },
            BindingRequirement::ResultSink { layout },
        ])
        .unwrap(),
        Some(StaticSinkProgram::Result),
    )
    .unwrap();
    let roots = ProgramExpressionRoots::collect(&graph, &control).unwrap();
    assert!(roots.sites().is_empty());
    let mut flows = BTreeMap::new();
    let mut types = BTreeMap::new();
    for (scope, arena) in roots.arenas() {
        assert!(arena.nodes().is_empty());
        flows.insert(
            *scope,
            ProgramControlFlow::try_new(
                vec![ProgramEvaluationDomain {
                    id: EvaluationDomainId::new(0),
                    parent: None,
                    guard: None,
                }],
                vec![],
                0,
                &control,
            )
            .unwrap(),
        );
        types.insert(*scope, Vec::<FunctionArgumentType>::new());
    }
    let snapshot = ProgramRootControlBindings::try_new(graph, flows, vec![], &control).unwrap();
    let calls = ProgramResolvedCalls::try_new(snapshot, vec![], &control).unwrap();
    let expressions = ProgramTypedExpressions::try_new(calls, types, &control).unwrap();
    let channels = ProgramTypedChannels::try_new(
        expressions,
        vec![(
            ProgramChannelSite::Layout {
                node: ProgramNodeId::new(0),
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal: 0,
            },
            ty,
        )],
        &control,
    )
    .unwrap();
    ProgramLexicalBindings::try_new(channels, vec![], vec![], &control).unwrap()
}
fn finish(
    checked: ProgramLexicalBindings,
    control: &dyn PureCompileControl,
) -> Result<LocalProgram, LocalProgramCompileError> {
    LocalProgram::try_new(
        checked,
        vec![LocalOperatorProvenance {
            id: LocalOperatorId::new(0),
            lowered_nodes: Box::from([ProgramNodeId::new(0)]),
            sources: Box::from([DiagnosticSourceNodeId::new(u32::MAX)]),
            origin: LocalOperatorOrigin::Direct,
            cost_owner: LocalOperatorId::new(0),
            metrics: OperatorMetricAggregation {
                cpu_time: MetricAggregation::Sum,
                wall_time: MetricAggregation::Maximum,
                peak_retained_bytes: MetricAggregation::Maximum,
            },
        }],
        &BTreeSet::from([DiagnosticSourceNodeId::new(u32::MAX)]),
        BTreeMap::new(),
        BTreeMap::new(),
        control,
    )
}

#[test]
fn complete_read_seal_preserves_explicit_json_without_duplicating_root_field_tag() {
    let recipe = recipe();
    let facts = recipe.frozen().public_facts().clone();
    let shared: ProgramScanSource = recipe.clone().into();
    let ProgramScanSource::Compiled(expected_arc) = &shared else {
        panic!("complete seal");
    };
    let checked = checked(shared.clone(), schema(), ValueLogicalType::Json);
    let graph_ptr = checked
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot()
        .program()
        .nodes()
        .as_ptr();
    let program = finish(checked, &Control::default()).unwrap();
    assert_eq!(program.graph().nodes().as_ptr(), graph_ptr);
    let ProgramNodeKind::Scan { source, .. } = program.graph().nodes()[0].kind() else {
        panic!("scan");
    };
    let ProgramScanSource::Compiled(actual_arc) = source else {
        panic!("retained complete seal");
    };
    assert!(Arc::ptr_eq(expected_arc, actual_arc));
    let retained = source.compiled().unwrap();
    assert_eq!(retained, &recipe);
    assert_eq!(retained.frozen().public_facts(), &facts);
    assert_eq!(
        retained.frozen().public_facts().logical_types(),
        &[ValueLogicalType::Json]
    );
    assert!(
        !retained
            .frozen()
            .public_facts()
            .schema()
            .field(0)
            .metadata()
            .contains_key(NR_LOGICAL_TYPE_KEY)
    );
    assert_eq!(
        retained
            .frozen()
            .scan()
            .recipe()
            .relation()
            .table()
            .payload()
            .as_ref(),
        b"canonical-private"
    );
    assert_eq!(
        retained.frozen().scan().recipe().columns()[0]
            .payload()
            .as_ref(),
        b"canonical-private"
    );
    assert_eq!(
        retained.frozen().scan().work_source(),
        ConnectorReadWorkSource::RuntimeSplits
    );
    assert_eq!(retained.frozen().scan().max_batch_rows().get(), 100);
    assert_eq!(
        retained.frozen().public_facts().source().selection_digest(),
        [7; 32]
    );
}

#[test]
fn final_read_links_reject_source_schema_and_same_carrier_logical_retagging() {
    let site = ProgramChannelSite::Layout {
        node: ProgramNodeId::new(0),
        role: ProgramChannelLayoutRole::NodeOutput,
        ordinal: 0,
    };
    let exact = recipe();
    assert_eq!(
        finish(
            checked(exact.clone().into(), schema(), ValueLogicalType::Physical),
            &Control::default()
        )
        .unwrap_err(),
        LocalProgramCompileError::Provider(ProviderLinkError::TypeMismatch(site))
    );
    let mut meta = schema().metadata().clone();
    meta.insert("provider.schema".into(), "foreign-generation".into());
    let foreign_meta = Schema::new_with_metadata(schema().fields().clone(), meta);
    assert_eq!(
        finish(
            checked(exact.clone().into(), foreign_meta, ValueLogicalType::Json),
            &Control::default()
        )
        .unwrap_err(),
        LocalProgramCompileError::Provider(ProviderLinkError::SchemaMetadata(ProgramNodeId::new(
            0
        )))
    );
    for field in [
        schema().field(0).clone().with_name("foreign_alias"),
        schema().field(0).clone().with_nullable(false),
        Field::new("json_source", DataType::Binary, true),
    ] {
        let changed = Schema::new_with_metadata(vec![field], schema().metadata().clone());
        let logical = if changed.field(0).data_type() == &DataType::Binary {
            ValueLogicalType::Physical
        } else {
            ValueLogicalType::Json
        };
        assert_eq!(
            finish(
                checked(exact.clone().into(), changed, logical),
                &Control::default()
            )
            .unwrap_err(),
            LocalProgramCompileError::Provider(ProviderLinkError::FieldMismatch {
                node: ProgramNodeId::new(0),
                ordinal: 0
            })
        );
    }
}

#[test]
fn final_owner_rejects_real_payload_only_legacy_scan_without_promoting_it() {
    let source: ProgramScanSource = legacy().into();
    assert!(source.compiled().is_none());
    assert_eq!(
        finish(
            checked(source, schema(), ValueLogicalType::Json),
            &Control::default()
        )
        .unwrap_err(),
        LocalProgramCompileError::Provider(ProviderLinkError::LegacyScan(ProgramNodeId::new(0)))
    );
}

#[test]
fn actual_final_scan_owner_refusals_keep_original_control_at_every_callback() {
    let source = checked(recipe().into(), schema(), ValueLogicalType::Json);
    let baseline = Control::default();
    finish(source.clone(), &baseline).unwrap();
    let trace = baseline.trace.into_inner().unwrap();
    assert!(trace.iter().any(|units| *units > 0));
    for at in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                trace: Mutex::default(),
                stop: Some((at, cause)),
            };
            let error = finish(source.clone(), &control).unwrap_err();
            assert_eq!(error, LocalProgramCompileError::Control(cause));
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            assert_eq!(
                std::error::Error::source(&error)
                    .unwrap()
                    .downcast_ref::<CompileControlError>(),
                Some(&cause)
            );
        }
    }
}
