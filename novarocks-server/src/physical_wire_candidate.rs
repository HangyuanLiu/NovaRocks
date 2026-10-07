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

//! Inputs of the debug-only physical wire v2 candidate island.
//!
//! None of these is production sizing or a production catalogue. They exist so
//! a real binary can run the compiled-package interpreter on its own
//! compatibility island before the one-shot flip, and they are deleted with
//! it. Production limits need their config owner, and a production pure
//! catalogue needs an execution-side producer of the installed-kernel
//! manifest; neither exists yet.

use std::sync::Arc;

use novarocks_arrow_ipc_frame::VerifierOptions;
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_execution::exec::expr::agg::SealedExecutionFunctionSet;
use novarocks_functions::{
    ConstantPolicy, EngineFunctionCatalog, EngineFunctionCatalogBuilder, FunctionDefinition,
    InstalledPureKernel, PureEngineFunctionCatalog,
};
use novarocks_native_adapter::backend_application::{
    BackendStaticPlanInterpreter, CompiledStaticPlan,
};
use novarocks_physical_plan as p;
use novarocks_plan_codec::ipc_flat_batch_v2::FlatBatchProjectionLimits;
use novarocks_plan_codec::ipc_flat_pool_v2::FlatPoolWriteLimits;
use novarocks_plan_codec::ipc_flat_stream_v2::{
    FlatReaderProjectionLimits, FlatStreamProjectionLimits,
};
use novarocks_plan_codec::ipc_recursive_pool_v2::RecursivePoolWriteLimits;
use novarocks_plan_codec::ipc_recursive_stream_v2::{
    RecursiveBatchProjectionLimits, RecursiveReaderProjectionLimits,
    RecursiveStreamProjectionLimits,
};
use novarocks_plan_codec::ipc_schema_v2::IpcSchemaProjectionLimits;
use novarocks_plan_codec::physical_binding_v2::BindingProjectionLimits;
use novarocks_plan_codec::physical_call_requests_v2::CallRequestProjectionLimits;
use novarocks_plan_codec::physical_connector_payload_v2::ConnectorPayloadProjectionLimits;
use novarocks_plan_codec::physical_constant_v2::{
    ConstantDecodeProjectionLimits, ConstantNamespaceProjectionLimits,
    ConstantWriteProjectionLimits,
};
use novarocks_plan_codec::physical_control_v2::ControlProjectionLimits;
use novarocks_plan_codec::physical_cuts_v2::CutsProjectionLimits;
use novarocks_plan_codec::physical_expression_v2::ExpressionProjectionLimits;
use novarocks_plan_codec::physical_node_v2::NodeProjectionLimits;
use novarocks_plan_codec::physical_package_v2::{
    BindingSourceLimits, DefinitionSourceLimits, PackageDecodeLimits, PackageEncodeLimits,
    ProviderSourceLimits, TypeViewLimits,
};
use novarocks_plan_codec::physical_properties_v2::PhysicalPropertyProjectionLimits;
use novarocks_plan_codec::physical_provider_binding_v2::ProviderBindingProjectionLimits;
use novarocks_plan_codec::physical_provider_read_v2::ProviderReadProjectionLimits;
use novarocks_plan_codec::physical_read_scan_v2::ReadScanProjectionLimits;
use novarocks_plan_codec::physical_relation_v2::RelationProjectionLimits;
use novarocks_plan_codec::physical_semantics_v2::ParameterProjectionLimits;
use novarocks_plan_codec::physical_type_v2::PackageTypeProjectionLimits;
use novarocks_plan_codec::physical_value_origin_v2::ValueOriginProjectionLimits;
use novarocks_plan_codec::physical_value_v2::ValueProjectionLimits;
use novarocks_plan_codec::physical_writer_recipe_v2::WriterRecipeProjectionLimits;
use novarocks_plan_codec::physical_writer_schema_v2::WriterSchemaProjectionLimits;
use novarocks_plan_codec::resource_preflight_v2::DecodeProjectionLimits;
use novarocks_spi::connector::ConnectorCodecError;

use crate::provider_manifest::ServerProviderManifest;
use crate::static_plan::CompositionControl;

/// Everything the candidate backend interpreter is composed from.
pub(crate) struct CandidateCompiledPackage {
    decode_limits: PackageDecodeLimits,
    constants: ConstantPolicy,
    functions: Arc<PureEngineFunctionCatalog>,
    providers: Arc<PureProviderProgramCatalog<ConnectorCodecError>>,
}

impl CandidateCompiledPackage {
    pub(crate) fn compose(
        function_set: &SealedExecutionFunctionSet,
        provider_manifest: &ServerProviderManifest,
    ) -> anyhow::Result<Self> {
        let seal = seal_candidate_pure_catalog(function_set.catalog())?;
        tracing::warn!(
            installed_definitions = seal.installed_definitions,
            excluded_definitions = seal.excluded.len(),
            pure_function_catalog_digest = %hex::encode(seal.catalog.digest()),
            "physical-wire-v2-candidate composes a compiled-package interpreter over a sealed pure \
             subset; a call to an excluded function is refused when its package compiles"
        );
        tracing::debug!(excluded = ?seal.excluded, "candidate pure catalogue exclusions");
        Ok(Self {
            decode_limits: candidate_decode_limits(),
            constants: candidate_constant_policy(),
            functions: Arc::new(seal.catalog),
            providers: Arc::new(provider_manifest.compose_pure_program_catalog()?),
        })
    }

    pub(crate) fn pure_function_catalog_digest(&self) -> [u8; 32] {
        self.functions.digest()
    }

    /// The frontend freezes packages under the receiver's own admission.
    pub(crate) fn frontend_carrier(&self) -> crate::static_plan::FrontendStaticPlanCarrier {
        crate::static_plan::FrontendStaticPlanCarrier::CompiledPackage {
            admission: self.decode_limits.admission,
            limits: candidate_encode_limits(),
        }
    }

    pub(crate) fn backend_interpreter(&self) -> anyhow::Result<BackendStaticPlanInterpreter> {
        CompiledStaticPlan::try_new(
            self.decode_limits.clone(),
            Arc::clone(&self.functions),
            Arc::clone(&self.providers),
            self.constants,
        )
        .map(BackendStaticPlanInterpreter::CompiledPackage)
        .map_err(|error| {
            anyhow::anyhow!("compose the candidate compiled-package interpreter: {error}")
        })
    }
}

/// The candidate's sealed pure catalogue and what it left out.
pub(crate) struct CandidatePureSeal {
    pub(crate) catalog: PureEngineFunctionCatalog,
    pub(crate) installed_definitions: usize,
    /// `name/kind: reason` for every process definition outside the seal.
    pub(crate) excluded: Vec<String>,
}

/// Seals exactly the process definitions whose every overload has an
/// installed pure owner with complete effects.
///
/// Each record is read from the owner the metadata catalogue itself carries;
/// none is authored here, and a definition with any uncovered overload is
/// left out whole rather than sealed partially. Because the records come from
/// the same owners `seal_pure` compares them with, that comparison proves only
/// those owners' consistency. It is not an independent installation manifest:
/// no execution-side producer of one exists yet. The resulting digest differs
/// from the process metadata catalogue's digest whenever anything is left out,
/// so it enters the candidate's compatibility identity through the interpreter
/// component.
pub(crate) fn seal_candidate_pure_catalog(
    metadata: &EngineFunctionCatalog,
) -> anyhow::Result<CandidatePureSeal> {
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut installed = Vec::new();
    let mut installed_definitions = 0;
    let mut excluded = Vec::new();
    for definition in metadata.definitions() {
        match installed_records(metadata, definition) {
            Ok(records) => {
                builder.register(definition.clone()).map_err(|error| {
                    anyhow::anyhow!(
                        "register candidate pure definition {}: {error}",
                        definition.canonical_name()
                    )
                })?;
                installed.extend(records);
                installed_definitions += 1;
            }
            Err(reason) => excluded.push(format!(
                "{}/{:?}: {reason}",
                definition.canonical_name(),
                definition.kind()
            )),
        }
    }
    anyhow::ensure!(
        installed_definitions > 0,
        "no process function definition has an installed pure owner"
    );
    let catalog = builder
        .seal_pure(installed)
        .map_err(|error| anyhow::anyhow!("seal the candidate pure function catalogue: {error}"))?;
    Ok(CandidatePureSeal {
        catalog,
        installed_definitions,
        excluded,
    })
}

fn installed_records(
    metadata: &EngineFunctionCatalog,
    definition: &FunctionDefinition,
) -> Result<Vec<InstalledPureKernel>, String> {
    let declaration = definition
        .binding_declaration()
        .ok_or_else(|| "no binding declaration".to_owned())?;
    declaration
        .validate_complete_effects()
        .map_err(|error| error.to_string())?;
    declaration
        .overloads()
        .iter()
        .map(|overload| {
            let installed = metadata
                .pure_overload_declaration_observed(
                    declaration.function_id(),
                    declaration.kind(),
                    &overload.identity,
                    &CompositionControl,
                )
                .map_err(|error| format!("{}: {error}", overload.identity.as_str()))?;
            Ok(InstalledPureKernel {
                function: declaration.function_id().clone(),
                kind: declaration.kind(),
                implementation: installed.implementation().clone(),
                aggregate_state_format: overload
                    .aggregate
                    .as_ref()
                    .map(|aggregate| aggregate.state_format.clone()),
            })
        })
        .collect()
}

/// Candidate constant policy for decode and compile. A candidate input, not
/// production sizing.
pub(crate) fn candidate_constant_policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 4096,
        max_array_nodes: 4096,
        max_logical_elements: 65536,
        max_retained_buffer_bytes: 8 * MIB as u64,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 8,
        max_metadata_bytes: 65536,
        max_library_validation_work: 32 * MIB as u64,
        max_library_validation_bytes: 32 * MIB as u64,
    }
}

const MIB: usize = 1024 * 1024;
const GIB: usize = 1024 * MIB;
/// Bytes a decoded source may retain.
const SOURCE: usize = 128 * MIB;
/// Bytes one namespace may request.
const REQUEST: usize = 512 * MIB;
const COEXIST: usize = SOURCE + REQUEST;
/// Work bounds: some namespace work grows with records times declared source
/// bytes, and one constant record's with the declared source bytes alone, so
/// a fixed cap would refuse ordinary packages.
const WORK: usize = usize::MAX / 4;

fn properties() -> PhysicalPropertyProjectionLimits {
    PhysicalPropertyProjectionLimits {
        max_value_references: 100_000,
        max_allocation_requests: 1_000_000,
        max_allocation_request_bytes: REQUEST,
        max_coexisting_source_and_request_bytes: COEXIST,
        max_work: WORK,
    }
}

fn node() -> NodeProjectionLimits {
    NodeProjectionLimits {
        max_input_nodes: 100_000,
        max_value_references: 100_000,
        max_list_items: 1_000_000,
        max_allocation_requests: 1_000_000,
        max_allocation_request_bytes: REQUEST,
        max_coexisting_source_and_request_bytes: COEXIST,
        max_work: WORK,
        properties: properties(),
    }
}

fn bindings() -> BindingProjectionLimits {
    BindingProjectionLimits {
        max_definitions: 100_000,
        max_type_references: 1_000_000,
        max_request_bytes: REQUEST,
        max_allocation_requests: 1_000_000,
        max_coexisting_source_and_request_bytes: COEXIST,
        max_work: WORK,
    }
}

fn ipc_schema() -> IpcSchemaProjectionLimits {
    IpcSchemaProjectionLimits {
        max_field_occurrences: 4096,
        max_type_occurrences: 4096,
        max_string_bytes: 65536,
        max_flatbuffer_bytes: 4 * MIB,
    }
}

fn ipc_batch() -> FlatBatchProjectionLimits {
    FlatBatchProjectionLimits {
        max_metadata_bytes: MIB,
        max_body_bytes: 8 * MIB,
        max_rows: 4096,
        max_buffer_descriptors: 16384,
        max_view_validation_bytes: 8 * MIB,
    }
}

/// The one admission every candidate package is constructed under, by the
/// frontend's checked-package author and by the backend's receiver alike.
///
/// The package's retained source floor travels in the sender's cumulative
/// source invoice, so it is exactly the encode limits' `source_retained_bytes`,
/// and its property projection coexists within the same source-plus-request
/// envelope every component limit uses.
pub(crate) fn candidate_package_admission() -> p::FragmentPackageAdmission {
    p::FragmentPackageAdmission {
        plan_limits: p::PlanLimits::FROZEN,
        source_retained_bytes: SOURCE,
        property_projection_limits: p::PropertyProofProjectionLimits {
            max_request_bytes: REQUEST,
            max_coexisting_bytes: COEXIST,
            max_projection_work: WORK,
        },
    }
}

fn flat_pool() -> FlatPoolWriteLimits {
    FlatPoolWriteLimits {
        max_rows: 4096,
        max_buffer_descriptors: 16384,
        max_body_bytes: 8 * MIB,
        max_encoded_stream_bytes: 16 * MIB,
        schema: ipc_schema(),
        max_new_allocation_request_bytes: REQUEST,
        max_coexisting_source_and_request_bytes: COEXIST,
        // A record writer's work bound scans whole-source metadata, so it
        // grows with the declared source size from the package's retained
        // floor upward; a fixed cap would refuse ordinary packages.
        max_cumulative_library_work: WORK,
    }
}

/// Candidate sender limits for one whole package. They prove composition on
/// the candidate island and are not production sizing; their magnitudes match
/// the plan codec's own composition fixtures.
pub(crate) fn candidate_encode_limits() -> PackageEncodeLimits {
    PackageEncodeLimits {
        source_retained_bytes: SOURCE,
        max_wire_bytes: 64 * MIB,
        views: TypeViewLimits {
            max_occurrences: 1_000_000,
            max_value_roots: 1_000_000,
            max_field_roots: 1_000_000,
            max_writer_recipes: 10_000,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
        binding_sources: BindingSourceLimits {
            max_functions: 100_000,
            max_aggregates: 100_000,
            max_arguments: 100_000,
            max_lambda_parameters: 100_000,
            max_relation_results: 100_000,
        },
        provider_sources: ProviderSourceLimits {
            max_provider_occurrences: 100_000,
            max_payload_occurrences: 100_000,
            max_read_occurrences: 100_000,
            max_relation_occurrences: 100_000,
            max_schema_occurrences: 100_000,
            max_scan_occurrences: 100_000,
            max_writer_occurrences: 100_000,
            max_relation_fields: 100_000,
            max_schema_fields: 100_000,
            max_connector_expression_occurrences: 100_000,
        },
        definition_sources: DefinitionSourceLimits {
            max_constants: 100_000,
            max_values: 100_000,
            max_expressions: 100_000,
            max_expression_lambda_parameters: 100_000,
            max_requests: 100_000,
            max_request_arguments: 100_000,
            max_request_lambda_parameters: 100_000,
            max_cut_type_occurrences: 100_000,
            max_result_fields: 100_000,
            max_writer_nodes: 100_000,
            max_writer_node_fields: 100_000,
        },
        node: node(),
        types: PackageTypeProjectionLimits {
            max_definitions: 1_000_000,
            max_expanded_nodes: 1_000_000,
            max_string_bytes: 64 * MIB,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
        bindings: bindings(),
        provider_bindings: ProviderBindingProjectionLimits {
            max_definitions: 100_000,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
        payloads: ConnectorPayloadProjectionLimits {
            max_definitions: 100_000,
            max_payload_bytes: 64 * MIB,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
        reads: ProviderReadProjectionLimits {
            max_definitions: 100_000,
            max_input_version_bytes: 1024,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
        relations: RelationProjectionLimits {
            max_definitions: 100_000,
            max_schema_fields: 100_000,
            max_predicate_guarantees: 100_000,
            max_metadata_kind_bytes: 1024,
            max_coverage_bytes: 1024,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
            properties: properties(),
        },
        constant_records: ConstantWriteProjectionLimits {
            flat: flat_pool(),
            recursive: RecursivePoolWriteLimits {
                flat: flat_pool(),
                max_field_nodes: 4096,
                max_total_rows: 65536,
            },
        },
        constants: ConstantNamespaceProjectionLimits {
            max_records: 100_000,
            max_preparation_request_bytes: REQUEST,
            max_new_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_cumulative_library_work: WORK,
        },
        parameters: ParameterProjectionLimits {
            max_parameters: 100_000,
            max_timezone_request_bytes: MIB,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
        values: ValueProjectionLimits {
            max_definitions: 1_000_000,
            max_origin_references: 1_000_000,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
            origins: ValueOriginProjectionLimits {
                max_allocation_requests: 1_000_000,
                max_allocation_request_bytes: REQUEST,
                max_coexisting_source_and_request_bytes: COEXIST,
                max_work: WORK,
            },
        },
        expressions: ExpressionProjectionLimits {
            max_definitions: 1_000_000,
            max_type_references: 1_000_000,
            max_expression_references: 1_000_000,
            max_new_allocation_requests: 1_000_000,
            max_new_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_cumulative_work: WORK,
        },
        requests: CallRequestProjectionLimits {
            max_definitions: 100_000,
            max_type_references: 1_000_000,
            max_request_bytes: REQUEST,
            max_allocation_requests: 1_000_000,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
        writer_schema: WriterSchemaProjectionLimits {
            max_fields: 100_000,
            max_name_bytes: MIB,
            max_type_references: 1_000_000,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
        control: ControlProjectionLimits {
            max_domains: 100_000,
            max_use_references: 1_000_000,
            max_root_bindings: 1_000_000,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
        cuts: CutsProjectionLimits {
            node: node(),
            binding: bindings(),
        },
        scans: ReadScanProjectionLimits {
            max_scans: 100_000,
            max_items: 1_000_000,
            max_scalar_bytes: 64 * MIB,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
        writers: WriterRecipeProjectionLimits {
            max_recipes: 100_000,
            max_input_fields: 100_000,
            max_token_bytes: 32_000,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
    }
}

/// Candidate receiver limits for one whole package: the sender's component
/// limits where one limit type serves both directions, the receiver's own
/// wire, constant-record and verifier admission, and the shared package
/// admission. Not production sizing.
pub(crate) fn candidate_decode_limits() -> PackageDecodeLimits {
    let encode = candidate_encode_limits();
    PackageDecodeLimits {
        wire: DecodeProjectionLimits {
            max_input_bytes: 64 * MIB,
            max_requested_heap_bytes: 512 * MIB,
            max_message_occurrences: 1_000_000,
            max_scalar_elements: 1_000_000,
            max_field_occurrences: 1_000_000,
            max_copied_bytes: 512 * MIB,
            max_initialization_bytes: 512 * MIB,
            max_wire_depth: 100,
        },
        types: encode.types,
        node: encode.node,
        constant_policy: candidate_constant_policy(),
        constant_records: ConstantDecodeProjectionLimits {
            flat_stream: FlatStreamProjectionLimits {
                max_input_bytes: 16 * MIB,
                schema: ipc_schema(),
                batch: ipc_batch(),
            },
            flat_reader: FlatReaderProjectionLimits {
                max_new_allocation_request_bytes: 128 * MIB,
                max_coexisting_source_and_request_bytes: 4 * GIB,
                max_cumulative_library_work: 512 * MIB,
            },
            recursive_stream: RecursiveStreamProjectionLimits {
                max_input_bytes: 16 * MIB,
                schema: ipc_schema(),
                batch: RecursiveBatchProjectionLimits {
                    flat: ipc_batch(),
                    max_field_nodes: 4096,
                    max_total_rows: 65536,
                    max_geometry_request_bytes: MIB,
                },
            },
            recursive_reader: RecursiveReaderProjectionLimits {
                max_new_allocation_request_bytes: 128 * MIB,
                max_coexisting_source_and_request_bytes: 4 * GIB,
                max_cumulative_library_work: GIB,
            },
        },
        constants: encode.constants,
        verifier: VerifierOptions {
            max_depth: 67,
            max_tables: 65536,
            max_apparent_size: 16 * MIB,
            ignore_missing_null_terminator: false,
        },
        bindings: encode.bindings,
        provider_bindings: encode.provider_bindings,
        payloads: encode.payloads,
        reads: encode.reads,
        relations: encode.relations,
        values: encode.values,
        expressions: encode.expressions,
        requests: encode.requests,
        parameters: encode.parameters,
        cuts: encode.cuts,
        scans: encode.scans,
        writers: encode.writers,
        writer_schema: encode.writer_schema,
        control: encode.control,
        admission: candidate_package_admission(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composition::compose_process_function_set;
    use crate::static_plan::{FrontendStaticPlanCarrier, ServerStaticPlan};
    use novarocks_functions::FunctionKind;
    use novarocks_version::StaticPlanInterpreter;

    /// The candidate seal holds exactly the definitions with installed pure
    /// owners: RAND is one, so it is sealed with its owner's own records, and
    /// the seal is a strict subset whose digest is not the metadata digest.
    #[test]
    fn the_candidate_seal_is_exactly_the_installed_pure_owners() {
        let function_set = compose_process_function_set().expect("process function set");
        let metadata = function_set.catalog();
        let seal = seal_candidate_pure_catalog(metadata).expect("candidate seal");

        assert!(seal.installed_definitions > 0);
        assert_eq!(
            seal.installed_definitions + seal.excluded.len(),
            metadata.definitions().len(),
            "every process definition is either sealed or reported excluded"
        );
        let rand = seal
            .catalog
            .metadata()
            .definition("rand", FunctionKind::Scalar)
            .expect("RAND has an installed pure owner")
            .binding_declaration()
            .expect("RAND binding");
        for overload in rand.overloads() {
            seal.catalog
                .metadata()
                .pure_overload_declaration_observed(
                    rand.function_id(),
                    rand.kind(),
                    &overload.identity,
                    &CompositionControl,
                )
                .expect("every RAND overload is sealed with its installed implementation");
        }
        for definition in seal.catalog.metadata().definitions() {
            assert!(
                installed_records(metadata, definition).is_ok(),
                "{} was sealed without complete installed coverage",
                definition.canonical_name()
            );
        }
        if !seal.excluded.is_empty() {
            assert_ne!(seal.catalog.digest(), metadata.digest());
        }
        // Deterministic: the same process sealing again is the same island.
        assert_eq!(
            seal_candidate_pure_catalog(metadata)
                .expect("second seal")
                .catalog
                .digest(),
            seal.catalog.digest()
        );
    }

    /// The frontend author and the backend receiver share one admission,
    /// whose source floor is exactly the sender's invoiced source and whose
    /// property projection fits the shared coexistence envelope. Namespace
    /// library work is not capped below records times declared source size,
    /// and no record writer's work is capped below its whole-source scans.
    #[test]
    fn candidate_sender_and_receiver_limits_are_mutually_consistent() {
        let admission = candidate_package_admission();
        let encode = candidate_encode_limits();
        let decode = candidate_decode_limits();

        assert_eq!(
            admission.source_retained_bytes,
            encode.source_retained_bytes
        );
        assert_eq!(
            decode.admission.source_retained_bytes,
            admission.source_retained_bytes
        );
        assert_eq!(decode.admission.plan_limits, admission.plan_limits);
        let property = admission.property_projection_limits;
        assert!(
            admission.source_retained_bytes + property.max_request_bytes
                <= property.max_coexisting_bytes
        );
        assert!(
            encode.source_retained_bytes + encode.types.max_allocation_request_bytes
                <= encode.types.max_coexisting_source_and_request_bytes
        );
        assert!(decode.wire.max_input_bytes <= encode.max_wire_bytes);
        assert_eq!(decode.constant_policy, candidate_constant_policy());
        assert_eq!(encode.constants.max_cumulative_library_work, usize::MAX / 4);
        assert_eq!(decode.constants.max_cumulative_library_work, usize::MAX / 4);
        for record in [
            encode.constant_records.flat,
            encode.constant_records.recursive.flat,
        ] {
            assert_eq!(record.max_cumulative_library_work, usize::MAX / 4);
        }
        let FrontendStaticPlanCarrier::CompiledPackage {
            admission: frontend,
            limits,
        } = CandidateCompiledPackage {
            decode_limits: decode.clone(),
            constants: candidate_constant_policy(),
            functions: Arc::new(
                seal_candidate_pure_catalog(
                    compose_process_function_set()
                        .expect("process function set")
                        .catalog(),
                )
                .expect("candidate seal")
                .catalog,
            ),
            providers: Arc::new(
                ServerProviderManifest::seal()
                    .expect("server provider manifest")
                    .compose_pure_program_catalog()
                    .expect("pure provider catalogue"),
            ),
        }
        .frontend_carrier()
        else {
            panic!("the candidate frontend freezes compiled packages");
        };
        assert_eq!(
            frontend.source_retained_bytes,
            decode.admission.source_retained_bytes
        );
        assert_eq!(limits.source_retained_bytes, encode.source_retained_bytes);
    }

    /// Both directions share one type ceiling, and the sender charges each
    /// strict type root's validator by that root's actual node count, the
    /// receiver's own per-root summary bound. A wide fragment's scalar roots
    /// therefore fit the candidate ceiling under the candidate source invoice,
    /// where a fixed 4096-node charge per root refused from 245 roots.
    #[test]
    fn candidate_type_ceiling_admits_wide_scalar_type_tables() {
        use arrow_schema::DataType;
        use novarocks_plan_codec::physical_type_v2::encode_type_table_writer_sources_observed;
        use novarocks_type_contract::{
            CompileCheckpoints, CompilePhase, FunctionValueType, MAX_VALUE_TYPE_NODES,
        };
        let encode = candidate_encode_limits();
        let decode = candidate_decode_limits();
        assert_eq!(
            decode.types.max_allocation_requests,
            encode.types.max_allocation_requests
        );
        assert_eq!(
            decode.types.max_allocation_request_bytes,
            encode.types.max_allocation_request_bytes
        );
        const ROOTS: u32 = 2000;
        assert!(ROOTS as usize * MAX_VALUE_TYPE_NODES > encode.types.max_allocation_requests);
        let roots = (0..ROOTS)
            .map(|id| (id, FunctionValueType::new(DataType::Int64, true)))
            .collect::<Vec<_>>();
        let mut work =
            CompileCheckpoints::try_new(&CompositionControl, CompilePhase::Encode).unwrap();
        let mut last = None;
        let table = encode_type_table_writer_sources_observed(
            &roots,
            &[],
            &[],
            encode.source_retained_bytes,
            encode.types,
            &mut |facts| {
                last = Some(*facts);
                Ok(())
            },
            &mut work,
        )
        .expect("a wide scalar type table fits the candidate type ceiling");
        work.finish().unwrap();
        assert_eq!(table.as_wire().value_types.len(), ROOTS as usize);
        let facts = last.expect("the sender admitted its type projection");
        assert!(facts.allocation_requests_upper_bound <= encode.types.max_allocation_requests);
        assert!(
            facts.allocation_requests_upper_bound < 4 * ROOTS as usize + 1024,
            "{facts:?}"
        );
    }

    /// The candidate binary composes the compiled-package interpreter in
    /// every role, with the sealed catalogue's digest as its island
    /// component, and its backend interpreter builds from the candidate
    /// inputs.
    #[test]
    fn a_candidate_binary_composes_the_compiled_package_interpreter() {
        let function_set = compose_process_function_set().expect("process function set");
        let manifest = ServerProviderManifest::seal().expect("server provider manifest");
        let plan = ServerStaticPlan::compose(&function_set, &manifest).expect("static plan");
        let seal = seal_candidate_pure_catalog(function_set.catalog()).expect("candidate seal");

        assert_eq!(
            plan.compatibility_component(),
            StaticPlanInterpreter::CompiledPackage {
                pure_function_catalog_digest: seal.catalog.digest(),
            }
        );
        assert!(matches!(
            plan.frontend_carrier(),
            FrontendStaticPlanCarrier::CompiledPackage { .. }
        ));
        assert!(matches!(
            plan.backend_interpreter().expect("backend interpreter"),
            BackendStaticPlanInterpreter::CompiledPackage(_)
        ));
    }
}
