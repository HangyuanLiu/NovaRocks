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
//! Permanent real SQL package and LocalCompiler complete Filter consumer contract.
use super::ndv_filter_actual_sql_source_tests::ndv_sql_source;
use super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue;
use arrow::{
    array::{Array, ArrayRef, Int32Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_connector_contract::*;
use novarocks_functions::*;
use novarocks_local_compiler::{
    LocalCompileOptions, compile_fragment, validate_fragment_providers,
};
use novarocks_local_program::{
    KernelAbiVersion, LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole,
    ProgramNodeKind, ProgramUseRef, StaticExprKind,
};
use novarocks_physical_plan::*;
use novarocks_sql::compiler::{
    SqlAuthoredPhysicalPlan, SqlPhysicalEmissionMode, author_fragment_package_semantics,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, PureCompileControl, ValueLogicalType,
};
use std::{
    collections::BTreeMap,
    num::{NonZeroU64, NonZeroUsize},
    sync::{Arc, Mutex},
    time::Duration,
};
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("numeric transaction never waits")
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 64,
        max_array_nodes: 1024,
        max_logical_elements: 4096,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 20,
        max_library_validation_bytes: 1 << 20,
    }
}
// The same canonicalizing provider seam used by existing LocalCompiler tests.
// This closes the exact private/public recipe contract, not an Iceberg service.
struct Port;
impl ConnectorReadProgramCompiler for Port {
    type Error = ConnectorError;
    fn compile_private(
        &self,
        input: &FrozenConnectorRead,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorReadRelationRecipeDraft, PureProviderCompileError<ConnectorError>> {
        control.checkpoint(CompilePhase::ProviderValidation, 0)?;
        let recipe = input.scan().recipe();
        let result = ConnectorReadRelationRecipeDraft::try_new(
            recipe.binding().clone(),
            recipe.relation().clone(),
            recipe.columns().to_vec(),
        )
        .map_err(|error| {
            PureProviderCompileError::Provider(ConnectorError::new(
                novarocks_connector_contract::ConnectorErrorKind::InvalidRequest,
                error.to_string(),
            ))
        });
        control.checkpoint(CompilePhase::ProviderValidation, 0)?;
        result
    }
}
fn providers() -> PureProviderProgramCatalog<ConnectorError> {
    let id = ConnectorProviderId::parse("iceberg").unwrap();
    PureProviderProgramCatalog::try_new(
        &[PureProviderManifestEntry::new(id.clone(), true, false)],
        vec![PureProviderProgramDefinition::new(
            id,
            Some(Arc::new(Port)
                as Arc<
                    dyn ConnectorReadProgramCompiler<Error = ConnectorError>,
                >),
            None,
        )],
        &Control,
    )
    .unwrap()
}
fn frozen_reads(
    source: &SqlAuthoredPhysicalPlan,
) -> BTreeMap<ProviderReadOccurrenceId, FrozenConnectorRead> {
    let mut reads = BTreeMap::new();
    for fragment in source.plan().fragments().values() {
        for node in fragment.nodes().values() {
            if let NodeKind::Scan {
                occurrence,
                relation,
                read_budget,
                provider_outputs,
                residuals,
                derived_values,
            } = &node.kind
            {
                assert!(residuals.is_empty() && derived_values.is_empty());
                assert!(relation.predicate_guarantees().is_empty());
                assert_eq!(relation.schema().len(), provider_outputs.len());
                let read = relation.read();
                let mut assignments = Vec::new();
                let mut fields = Vec::new();
                let mut logical_types = Vec::new();
                for (ordinal, field) in relation.schema().iter().enumerate() {
                    assert!(field.ty.nullable, "original k/v/s all nullable");
                    let carrier = connector_type_for_value_type(&field.ty)
                        .expect("sole original connector projection");
                    let name = format!("fixture_column_{ordinal}");
                    assignments.push(StaticScanAssignment::new(Arc::from(name.as_str()), carrier));
                    fields.push(Field::new(
                        name,
                        field.ty.data_type.clone(),
                        field.ty.nullable,
                    ));
                    logical_types.push(field.ty.logical_type);
                }
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
                let scan = FrozenConnectorScan::try_new(
                    recipe,
                    assignments,
                    TupleDomain::all(),
                    TupleDomain::all(),
                    None,
                    Vec::new(),
                    NonZeroU64::new(read_budget.max_batch_rows).unwrap(),
                    NonZeroU64::new(read_budget.max_batch_bytes).unwrap(),
                    relation.work_source(),
                )
                .unwrap();
                let facts = ConnectorReadStaticFacts::try_new(
                    read.input_version.clone(),
                    relation.selection_digest(),
                    ConnectorReadProperties::try_new(
                        ConnectorReadDistribution::Unconstrained,
                        Vec::new(),
                    )
                    .unwrap(),
                    ConnectorReadArtifactCoverage::NoArtifactInputs,
                    Vec::new(),
                )
                .unwrap();
                let public = ConnectorReadPublicFacts::try_new(
                    facts,
                    None,
                    Schema::new(fields),
                    logical_types,
                )
                .unwrap();
                assert!(
                    reads
                        .insert(
                            *occurrence,
                            FrozenConnectorRead::try_new(scan, public).unwrap()
                        )
                        .is_none()
                );
            }
        }
    }
    reads
}
pub(super) fn source_packages(
    source: &SqlAuthoredPhysicalPlan,
) -> BTreeMap<FragmentId, Arc<FragmentPackage>> {
    let semantics = author_fragment_package_semantics(source, policy(), &Control).unwrap();
    let uses = semantics
        .iter()
        .map(|(&id, s)| (id, s.expression_uses.clone()))
        .collect();
    let calls = semantics
        .iter()
        .map(|(&id, s)| (id, s.calls.clone()))
        .collect();
    let pruning = semantics
        .iter()
        .map(|(&id, s)| (id, s.pruning.clone()))
        .collect();
    let admissions = source
        .plan()
        .fragments()
        .keys()
        .map(|&id| {
            (
                id,
                FragmentPackageAdmission {
                    plan_limits: PlanLimits::FROZEN,
                    source_retained_bytes: 64 << 20,
                    property_projection_limits: PropertyProofProjectionLimits {
                        max_request_bytes: 16 << 20,
                        max_coexisting_bytes: 256 << 20,
                        max_projection_work: 16 << 20,
                    },
                },
            )
        })
        .collect();
    let packages = extract_fragment_packages(
        source.plan(),
        &frozen_reads(source),
        &BTreeMap::new(),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &Control,
    )
    .unwrap();
    packages
        .into_iter()
        .map(|(id, package)| (id, Arc::new(package)))
        .collect()
}
pub(super) fn compile_options() -> LocalCompileOptions {
    LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(1).unwrap(),
        root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        exchange_wait: Duration::from_secs(120),
        constants: policy(),
    }
}
pub(super) fn compiler_results(
    source: &SqlAuthoredPhysicalPlan,
    functions: &PureEngineFunctionCatalog,
) -> BTreeMap<FragmentId, Result<LocalProgram, novarocks_local_compiler::FragmentCompileError>> {
    source_packages(source)
        .into_iter()
        .map(|(id, package)| {
            (
                id,
                compile_fragment(
                    validate_fragment_providers(package, &providers(), &Control).unwrap(),
                    functions,
                    compile_options(),
                    &Control,
                ),
            )
        })
        .collect()
}
pub(super) fn validate_package(
    package: Arc<FragmentPackage>,
) -> novarocks_local_compiler::ProviderValidatedFragment {
    validate_fragment_providers(package, &providers(), &Control).unwrap()
}

const HAVING: &str = "SELECT k FROM fixture.ndv_null_contract GROUP BY k\nHAVING ndv(v) = 0 AND approx_count_distinct(v) = 0 ORDER BY k;";
fn permanent(mode: SqlPhysicalEmissionMode) {
    let source = ndv_sql_source(HAVING, mode);
    let mut filter_fragment = None;
    for (&id, fragment) in source.plan().fragments() {
        for node in fragment.nodes().values() {
            if let NodeKind::Filter { predicates } = &node.kind {
                if predicates.len() > 1 {
                    assert_eq!(node.inputs.len(), 1);
                    assert_eq!(predicates.len(), 2);
                    assert!(filter_fragment.replace((id, node.id)).is_none());
                }
            }
        }
    }
    let (id, original_node) = filter_fragment.unwrap();
    let mut actual = compiler_results(&source, &installed_builtin_owner_catalogue());
    let program = actual
        .remove(&id)
        .unwrap()
        .expect("the full original SQL Filter must lower successfully");
    assert_eq!(
        program
            .graph()
            .nodes()
            .iter()
            .filter(|node| matches!(node.kind(), ProgramNodeKind::Filter { .. }))
            .count(),
        1
    );
    eprintln!("actual full Filter lowered from original node {original_node:?} in mode {mode:?}");
}
#[test]
fn filter_conjunction_original_sql_compiler_complete() {
    permanent(SqlPhysicalEmissionMode::OriginalNativeV1);
}
#[test]
fn filter_conjunction_candidate_sql_compiler_complete() {
    permanent(SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration);
}
