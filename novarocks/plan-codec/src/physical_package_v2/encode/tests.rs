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

use super::*;
use crate::ipc_flat_pool_v2::FlatPoolWriteLimits;
use crate::ipc_recursive_pool_v2::RecursivePoolWriteLimits;
use crate::ipc_schema_v2::IpcSchemaProjectionLimits;
use crate::physical_package_v2::definition_sources::tests::{
    cv_package, rich_package, writer_constant_package, writer_package,
};
use crate::physical_package_v2::prepare_package_wire_in;
use crate::physical_package_v2::provider_sources::tests::checked_read;
use crate::physical_properties_v2::PhysicalPropertyProjectionLimits;
use crate::physical_value_origin_v2::ValueOriginProjectionLimits;
use crate::resource_preflight_v2::{DecodeProjectionLimits, FragmentDecodeResourceModel};
use std::sync::Mutex;

const SOURCE: usize = 128 * 1024 * 1024;
const REQUEST: usize = 512 * 1024 * 1024;
const COEXIST: usize = SOURCE + REQUEST;
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
fn flat() -> FlatPoolWriteLimits {
    FlatPoolWriteLimits {
        max_rows: 4096,
        max_buffer_descriptors: 16384,
        max_body_bytes: 8 * 1024 * 1024,
        max_encoded_stream_bytes: 16 * 1024 * 1024,
        schema: IpcSchemaProjectionLimits {
            max_field_occurrences: 4096,
            max_type_occurrences: 4096,
            max_string_bytes: 65536,
            max_flatbuffer_bytes: 4 * 1024 * 1024,
        },
        max_new_allocation_request_bytes: REQUEST,
        max_coexisting_source_and_request_bytes: COEXIST,
        max_cumulative_library_work: 1024 * 1024 * 1024,
    }
}

/// Generous test limits. They prove composition, not production sizing.
pub(in crate::physical_package_v2) fn encode_limits() -> PackageEncodeLimits {
    PackageEncodeLimits {
        source_retained_bytes: SOURCE,
        max_wire_bytes: 64 * 1024 * 1024,
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
            max_string_bytes: 64 * 1024 * 1024,
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
            max_payload_bytes: 64 * 1024 * 1024,
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
            flat: flat(),
            recursive: RecursivePoolWriteLimits {
                flat: flat(),
                max_field_nodes: 4096,
                max_total_rows: 65536,
            },
        },
        constants: ConstantNamespaceProjectionLimits {
            max_records: 100_000,
            max_preparation_request_bytes: REQUEST,
            max_new_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_cumulative_library_work: 4 * 1024 * 1024 * 1024,
        },
        parameters: ParameterProjectionLimits {
            max_parameters: 100_000,
            max_timezone_request_bytes: 1024 * 1024,
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
            max_name_bytes: 1024 * 1024,
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
            max_scalar_bytes: 64 * 1024 * 1024,
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

#[derive(Default)]
struct Control {
    stop: Option<(usize, CompileControlError)>,
    trace: Mutex<Vec<u32>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        trace.push(units);
        match self.stop {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}

fn fixtures() -> Vec<(&'static str, p::FragmentPackage)> {
    vec![
        ("rich", rich_package()),
        ("cv", cv_package()),
        ("writer", writer_constant_package()),
        ("data-read", checked_read(false)),
        ("metadata-read", checked_read(true)),
    ]
}

fn decode_limits() -> DecodeProjectionLimits {
    DecodeProjectionLimits {
        max_input_bytes: 64 << 20,
        max_requested_heap_bytes: 512 << 20,
        max_message_occurrences: 1_000_000,
        max_scalar_elements: 1_000_000,
        max_field_occurrences: 1_000_000,
        max_copied_bytes: 512 << 20,
        max_initialization_bytes: 512 << 20,
        max_wire_depth: 100,
    }
}

#[test]
fn whole_sender_emits_every_component_of_actual_checked_packages() {
    for (name, package) in fixtures() {
        let out = encode_fragment_package(&package, &encode_limits(), &Control::default())
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let fragment = out.fragment.as_ref().expect("fragment");
        let original = package.fragment();
        assert_eq!(out.plan_version.len(), 16, "{name}");
        assert!(out.required.is_some(), "{name}");
        assert!(out.types.is_some(), "{name}");
        assert!(out.expression_control.is_some(), "{name}");
        assert!(out.calls.is_some(), "{name}");
        assert!(out.pruning.is_some(), "{name}");
        assert!(out.parameters.is_some(), "{name}");
        assert!(out.cuts.is_some(), "{name}");
        assert!(
            fragment.call_requests.is_some(),
            "{name}: present even when empty"
        );
        assert_eq!(out.result.is_some(), package.result().is_some(), "{name}");
        assert_eq!(fragment.id, original.id().get(), "{name}");
        assert_eq!(fragment.nodes.len(), original.nodes().len(), "{name}");
        assert_eq!(fragment.values.len(), original.values().len(), "{name}");
        assert_eq!(
            fragment.expressions.len(),
            original.expressions().len(),
            "{name}"
        );
        assert_eq!(
            fragment.call_requests.as_ref().unwrap().entries.len(),
            original.call_requests().entries().len(),
            "{name}"
        );
        assert_eq!(
            out.constants.len(),
            package.constants().entries().len(),
            "{name}"
        );
        assert_eq!(out.scans.len(), package.scans().len(), "{name}");
        assert_eq!(out.writes.len(), package.writes().len(), "{name}");
        assert_eq!(out.annotations.len(), package.annotations().len(), "{name}");
    }
}

#[test]
fn whole_sender_output_passes_generated_preflight_and_is_deterministic() {
    let model = FragmentDecodeResourceModel::try_new(&Control::default()).unwrap();
    for (name, package) in fixtures() {
        let first = encode_fragment_package(&package, &encode_limits(), &Control::default())
            .unwrap()
            .encode_to_vec();
        let second = encode_fragment_package(&package, &encode_limits(), &Control::default())
            .unwrap()
            .encode_to_vec();
        assert_eq!(first, second, "{name}: same package, same bytes");
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let decoded =
            prepare_package_wire_in(&first, &model, decode_limits(), &mut |_| Ok(()), &mut work)
                .and_then(|prepared| prepared.materialize_in(&mut |_| Ok(()), &mut work))
                .unwrap_or_else(|e| panic!("{name}: {e}"));
        work.finish().unwrap();
        assert_eq!(decoded.encode_to_vec(), first, "{name}");
    }
}

#[test]
fn whole_sender_refuses_over_coarse_bound_component_limit_and_control() {
    let package = rich_package();
    let size = encode_fragment_package(&package, &encode_limits(), &Control::default())
        .unwrap()
        .encoded_len();
    let exact = PackageEncodeLimits {
        max_wire_bytes: size,
        ..encode_limits()
    };
    encode_fragment_package(&package, &exact, &Control::default()).unwrap();
    let under = PackageEncodeLimits {
        max_wire_bytes: size - 1,
        ..encode_limits()
    };
    assert!(matches!(
        encode_fragment_package(&package, &under, &Control::default()),
        Err(PackageEncodeError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    let mut tight = encode_limits();
    tight.definition_sources.max_values = package.fragment().values().len() - 1;
    assert!(matches!(
        encode_fragment_package(&package, &tight, &Control::default()),
        Err(PackageEncodeError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    // Representative control: the caller's cause stays primary at the entry,
    // in the middle and at the final footer of the one Encode scope.
    let control = Control::default();
    encode_fragment_package(&package, &encode_limits(), &control).unwrap();
    let callbacks = control.trace.lock().unwrap().len();
    for at in [0, callbacks / 2, callbacks - 1] {
        let stop = Control {
            stop: Some((at, CompileControlError::Cancelled)),
            ..Default::default()
        };
        assert!(matches!(
            encode_fragment_package(&package, &encode_limits(), &stop),
            Err(PackageEncodeError::Control(CompileControlError::Cancelled))
        ));
        assert_eq!(stop.trace.lock().unwrap().len(), at + 1);
    }
}

// The checked writer fixture still carries a legacy Literal in its Values
// row. The v2 sender refuses it as an ordinary expression error: literals
// must be authored as constant pool references before publication.
#[test]
fn legacy_literal_package_is_an_ordinary_refusal_not_a_silent_conversion() {
    let package = writer_package();
    assert!(matches!(
        encode_fragment_package(&package, &encode_limits(), &Control::default()),
        Err(PackageEncodeError::Expression(_))
    ));
}
