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

//! Frozen package structure and constant-admission limits.
//!
//! Sender and receiver share these ceilings. They bound one package's shape
//! and codec work; deployment-specific runtime capacity remains separately owned.

use crate::ipc_flat_batch_v2::FlatBatchProjectionLimits;
use crate::ipc_flat_pool_v2::FlatPoolWriteLimits;
use crate::ipc_flat_stream_v2::{FlatReaderProjectionLimits, FlatStreamProjectionLimits};
use crate::ipc_recursive_pool_v2::RecursivePoolWriteLimits;
use crate::ipc_recursive_stream_v2::{
    RecursiveBatchProjectionLimits, RecursiveReaderProjectionLimits,
    RecursiveStreamProjectionLimits,
};
use crate::ipc_schema_v2::IpcSchemaProjectionLimits;
use crate::physical_binding_v2::BindingProjectionLimits;
use crate::physical_call_requests_v2::CallRequestProjectionLimits;
use crate::physical_connector_payload_v2::ConnectorPayloadProjectionLimits;
use crate::physical_constant_v2::{
    ConstantDecodeProjectionLimits, ConstantNamespaceProjectionLimits,
    ConstantWriteProjectionLimits,
};
use crate::physical_control_v2::ControlProjectionLimits;
use crate::physical_cuts_v2::CutsProjectionLimits;
use crate::physical_expression_v2::ExpressionProjectionLimits;
use crate::physical_node_v2::NodeProjectionLimits;
use crate::physical_package_v2::{
    BindingSourceLimits, DefinitionSourceLimits, PackageDecodeLimits, PackageEncodeLimits,
    ProviderSourceLimits, TypeViewLimits,
};
use crate::physical_properties_v2::PhysicalPropertyProjectionLimits;
use crate::physical_provider_binding_v2::ProviderBindingProjectionLimits;
use crate::physical_provider_read_v2::ProviderReadProjectionLimits;
use crate::physical_read_scan_v2::ReadScanProjectionLimits;
use crate::physical_relation_v2::RelationProjectionLimits;
use crate::physical_semantics_v2::ParameterProjectionLimits;
use crate::physical_type_v2::PackageTypeProjectionLimits;
use crate::physical_value_origin_v2::ValueOriginProjectionLimits;
use crate::physical_value_v2::ValueProjectionLimits;
use crate::physical_writer_recipe_v2::WriterRecipeProjectionLimits;
use crate::physical_writer_schema_v2::WriterSchemaProjectionLimits;
use crate::resource_preflight_v2::DecodeProjectionLimits;
use novarocks_arrow_ipc_frame::VerifierOptions;
use novarocks_functions::ConstantPolicy;
use novarocks_physical_plan as p;

/// Frozen constant policy shared by package decoding and compilation.
pub const FROZEN_CONSTANT_POLICY: ConstantPolicy = frozen_constant_policy();

/// The sender and receiver admit the same checked package structure.
pub const FROZEN_PACKAGE_ADMISSION: p::FragmentPackageAdmission = frozen_package_admission();

impl PackageEncodeLimits {
    /// Production structure ceilings. These are not deployment resource quotas.
    pub const FROZEN: Self = frozen_encode_limits();
}

impl PackageDecodeLimits {
    /// Production structure ceilings, including pre-Prost wire admission.
    pub const FROZEN: Self = frozen_decode_limits();
}

/// Frozen constant admission for package decoding and local compilation.
const fn frozen_constant_policy() -> ConstantPolicy {
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

const fn properties() -> PhysicalPropertyProjectionLimits {
    PhysicalPropertyProjectionLimits {
        max_value_references: 100_000,
        max_allocation_requests: 1_000_000,
        max_allocation_request_bytes: REQUEST,
        max_coexisting_source_and_request_bytes: COEXIST,
        max_work: WORK,
    }
}

const fn node() -> NodeProjectionLimits {
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

const fn bindings() -> BindingProjectionLimits {
    BindingProjectionLimits {
        max_definitions: 100_000,
        max_type_references: 1_000_000,
        max_request_bytes: REQUEST,
        max_allocation_requests: 1_000_000,
        max_coexisting_source_and_request_bytes: COEXIST,
        max_work: WORK,
    }
}

const fn ipc_schema() -> IpcSchemaProjectionLimits {
    IpcSchemaProjectionLimits {
        max_field_occurrences: 4096,
        max_type_occurrences: 4096,
        max_string_bytes: 65536,
        max_flatbuffer_bytes: 4 * MIB,
    }
}

const fn ipc_batch() -> FlatBatchProjectionLimits {
    FlatBatchProjectionLimits {
        max_metadata_bytes: MIB,
        max_body_bytes: 8 * MIB,
        max_rows: 4096,
        max_buffer_descriptors: 16384,
        max_view_validation_bytes: 8 * MIB,
    }
}

/// The one admission every production package is constructed under, by the
/// frontend's checked-package author and by the backend's receiver alike.
///
/// The package's retained source floor travels in the sender's cumulative
/// source invoice, so it is exactly the encode limits' `source_retained_bytes`,
/// and its property projection coexists within the same source-plus-request
/// envelope every component limit uses.
const fn frozen_package_admission() -> p::FragmentPackageAdmission {
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

const fn flat_pool() -> FlatPoolWriteLimits {
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

/// Frozen sender structure limits for one whole package.
const fn frozen_encode_limits() -> PackageEncodeLimits {
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

/// Frozen receiver limits for one whole package: the sender's component
/// limits where one limit type serves both directions, the receiver's own
/// wire, constant-record and verifier admission, and the shared package
/// admission. These are structural refusal bounds, not runtime quotas.
const fn frozen_decode_limits() -> PackageDecodeLimits {
    let encode = frozen_encode_limits();
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
        constant_policy: frozen_constant_policy(),
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
        admission: frozen_package_admission(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};

    struct Control;
    impl PureCompileControl for Control {
        fn checkpoint(&self, _phase: CompilePhase, _units: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }

    /// The frontend author and the backend receiver share one admission,
    /// whose source floor is exactly the sender's invoiced source and whose
    /// property projection fits the shared coexistence envelope. Namespace
    /// library work is not capped below records times declared source size,
    /// and no record writer's work is capped below its whole-source scans.
    #[test]
    fn frozen_sender_and_receiver_limits_are_mutually_consistent() {
        let admission = FROZEN_PACKAGE_ADMISSION;
        let encode = PackageEncodeLimits::FROZEN;
        let decode = PackageDecodeLimits::FROZEN;

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
        assert_eq!(decode.constant_policy, FROZEN_CONSTANT_POLICY);
        assert_eq!(encode.constants.max_cumulative_library_work, usize::MAX / 4);
        assert_eq!(decode.constants.max_cumulative_library_work, usize::MAX / 4);
        for record in [
            encode.constant_records.flat,
            encode.constant_records.recursive.flat,
        ] {
            assert_eq!(record.max_cumulative_library_work, usize::MAX / 4);
        }
    }

    /// Both directions share one type ceiling, and the sender charges each
    /// strict type root's validator by that root's actual node count, the
    /// receiver's own per-root summary bound. A wide fragment's scalar roots
    /// therefore fit the frozen ceiling under the frozen source invoice,
    /// where a fixed 4096-node charge per root refused from 245 roots.
    #[test]
    fn frozen_type_ceiling_admits_wide_scalar_type_tables() {
        use crate::physical_type_v2::encode_type_table_writer_sources_observed;
        use arrow::datatypes::DataType;
        use novarocks_type_contract::{
            CompileCheckpoints, CompilePhase, FunctionValueType, MAX_VALUE_TYPE_NODES,
        };
        let encode = PackageEncodeLimits::FROZEN;
        let decode = PackageDecodeLimits::FROZEN;
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
        let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
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
        .expect("a wide scalar type table fits the frozen type ceiling");
        work.finish().unwrap();
        assert_eq!(table.as_wire().value_types.len(), ROOTS as usize);
        let facts = last.expect("the sender admitted its type projection");
        assert!(facts.allocation_requests_upper_bound <= encode.types.max_allocation_requests);
        assert!(
            facts.allocation_requests_upper_bound < 4 * ROOTS as usize + 1024,
            "{facts:?}"
        );
    }
}
