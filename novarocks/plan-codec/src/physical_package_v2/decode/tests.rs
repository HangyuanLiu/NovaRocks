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
use crate::ipc_flat_batch_v2::FlatBatchProjectionLimits;
use crate::ipc_flat_stream_v2::{FlatReaderProjectionLimits, FlatStreamProjectionLimits};
use crate::ipc_recursive_stream_v2::{
    RecursiveBatchProjectionLimits, RecursiveReaderProjectionLimits,
    RecursiveStreamProjectionLimits,
};
use crate::ipc_schema_v2::IpcSchemaProjectionLimits;
use crate::physical_package_v2::definition_sources::tests::{
    cv_package, rich_package, writer_constant_package,
};
use crate::physical_package_v2::encode::tests::encode_limits;
use crate::physical_package_v2::encode::{PackageEncodeError, encode_fragment_package};
use crate::physical_package_v2::provider_sources::tests::checked_read;
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use prost::Message;
use std::sync::Mutex;

const GIB: usize = 1024 * 1024 * 1024;

fn schema() -> IpcSchemaProjectionLimits {
    IpcSchemaProjectionLimits {
        max_field_occurrences: 4096,
        max_type_occurrences: 4096,
        max_string_bytes: 65536,
        max_flatbuffer_bytes: 4 * 1024 * 1024,
    }
}
fn batch() -> FlatBatchProjectionLimits {
    FlatBatchProjectionLimits {
        max_metadata_bytes: 1024 * 1024,
        max_body_bytes: 8 * 1024 * 1024,
        max_rows: 4096,
        max_buffer_descriptors: 16384,
        max_view_validation_bytes: 8 * 1024 * 1024,
    }
}

/// Generous receiver limits built from the sender's test limits where the
/// same component limit type serves both directions.
fn decode_limits() -> PackageDecodeLimits {
    let e = encode_limits();
    PackageDecodeLimits {
        wire: DecodeProjectionLimits {
            max_input_bytes: 64 << 20,
            max_requested_heap_bytes: 512 << 20,
            max_message_occurrences: 1_000_000,
            max_scalar_elements: 1_000_000,
            max_field_occurrences: 1_000_000,
            max_copied_bytes: 512 << 20,
            max_initialization_bytes: 512 << 20,
            max_wire_depth: 100,
        },
        types: e.types,
        node: e.node,
        constant_policy: ConstantPolicy {
            max_rows: 4096,
            max_array_nodes: 4096,
            max_logical_elements: 65536,
            max_retained_buffer_bytes: 8 * 1024 * 1024,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 8,
            max_metadata_bytes: 65536,
            max_library_validation_work: 32 * 1024 * 1024,
            max_library_validation_bytes: 32 * 1024 * 1024,
        },
        constant_records: ConstantDecodeProjectionLimits {
            flat_stream: FlatStreamProjectionLimits {
                max_input_bytes: 16 * 1024 * 1024,
                schema: schema(),
                batch: batch(),
            },
            flat_reader: FlatReaderProjectionLimits {
                max_new_allocation_request_bytes: 128 * 1024 * 1024,
                max_coexisting_source_and_request_bytes: 4 * GIB,
                max_cumulative_library_work: 512 * 1024 * 1024,
            },
            recursive_stream: RecursiveStreamProjectionLimits {
                max_input_bytes: 16 * 1024 * 1024,
                schema: schema(),
                batch: RecursiveBatchProjectionLimits {
                    flat: batch(),
                    max_field_nodes: 4096,
                    max_total_rows: 65536,
                    max_geometry_request_bytes: 1024 * 1024,
                },
            },
            recursive_reader: RecursiveReaderProjectionLimits {
                max_new_allocation_request_bytes: 128 * 1024 * 1024,
                max_coexisting_source_and_request_bytes: 4 * GIB,
                max_cumulative_library_work: GIB,
            },
        },
        constants: e.constants,
        verifier: VerifierOptions {
            max_depth: 67,
            max_tables: 65536,
            max_apparent_size: 16 * 1024 * 1024,
            ignore_missing_null_terminator: false,
        },
        bindings: e.bindings,
        provider_bindings: e.provider_bindings,
        payloads: e.payloads,
        reads: e.reads,
        relations: e.relations,
        values: e.values,
        expressions: e.expressions,
        requests: e.requests,
        parameters: e.parameters,
        cuts: e.cuts,
        scans: e.scans,
        writers: e.writers,
        writer_schema: e.writer_schema,
        control: e.control,
        admission: p::FragmentPackageAdmission {
            plan_limits: p::PlanLimits::FROZEN,
            source_retained_bytes: 2 * GIB,
            property_projection_limits: p::PropertyProofProjectionLimits {
                max_request_bytes: 512 * 1024 * 1024,
                max_coexisting_bytes: 4 * GIB,
                max_projection_work: usize::MAX / 4,
            },
        },
    }
}

#[derive(Default)]
struct Control {
    stop: Option<(usize, CompileControlError)>,
    trace: Mutex<Vec<(CompilePhase, u32)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        trace.push((phase, units));
        match self.stop {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}

fn model() -> FragmentDecodeResourceModel {
    FragmentDecodeResourceModel::try_new(&Control::default()).unwrap()
}
fn encode(package: &p::FragmentPackage) -> Result<Vec<u8>, PackageEncodeError> {
    Ok(encode_fragment_package(package, &encode_limits(), &Control::default())?.encode_to_vec())
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

// Lossless and canonical: sender bytes decode through the generated preflight
// and the original constructors into a checked package whose own encoding is
// byte-identical. No component is defaulted, re-derived or dropped.
#[test]
fn whole_package_roundtrip_is_byte_identical_through_original_constructors() {
    let model = model();
    for (name, package) in fixtures() {
        let bytes = encode(&package).unwrap_or_else(|e| panic!("{name}: {e}"));
        let decoded =
            decode_fragment_package(&bytes, &model, &decode_limits(), &Control::default())
                .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(decoded.fragment().id(), package.fragment().id(), "{name}");
        assert_eq!(
            decoded.fragment().nodes().len(),
            package.fragment().nodes().len(),
            "{name}"
        );
        assert_eq!(decoded.scans().len(), package.scans().len(), "{name}");
        assert_eq!(decoded.writes().len(), package.writes().len(), "{name}");
        assert_eq!(
            decoded.result().is_some(),
            package.result().is_some(),
            "{name}"
        );
        let again = encode(&decoded).unwrap_or_else(|e| panic!("{name} re-encode: {e}"));
        assert_eq!(
            again, bytes,
            "{name}: decode∘encode is the identity on sender bytes"
        );
    }
}

fn mutated(
    package: &p::FragmentPackage,
    change: impl FnOnce(&mut wire::FragmentPackage),
) -> Vec<u8> {
    let mut dto = encode_fragment_package(package, &encode_limits(), &Control::default()).unwrap();
    change(&mut dto);
    dto.encode_to_vec()
}
fn refused(bytes: &[u8]) -> PackageDecodeError {
    decode_fragment_package(bytes, &model(), &decode_limits(), &Control::default())
        .expect_err("receiver must refuse")
}

#[test]
fn absent_required_components_are_refused_without_defaults() {
    let package = rich_package();
    let cases: [(&str, fn(&mut wire::FragmentPackage)); 7] = [
        ("fragment", |d| d.fragment = None),
        ("expression_control", |d| d.expression_control = None),
        ("calls", |d| d.calls = None),
        ("pruning", |d| d.pruning = None),
        ("parameters", |d| d.parameters = None),
        ("cuts", |d| d.cuts = None),
        ("call_requests", |d| {
            d.fragment.as_mut().unwrap().call_requests = None;
        }),
    ];
    for (name, change) in cases {
        let error = refused(&mutated(&package, change));
        assert!(
            !matches!(error, PackageDecodeError::Control(_)),
            "{name}: absence is an ordinary refusal, got {error}"
        );
    }
}

#[test]
fn duplicate_node_ids_and_malformed_bytes_are_refused() {
    let package = rich_package();
    let duplicate = mutated(&package, |d| {
        let fragment = d.fragment.as_mut().unwrap();
        let first = fragment.nodes[0].clone();
        fragment.nodes.push(first);
    });
    assert!(matches!(
        refused(&duplicate),
        PackageDecodeError::Invalid(_)
    ));
    let mut truncated = encode(&package).unwrap();
    truncated.truncate(truncated.len() / 2);
    assert!(!matches!(
        refused(&truncated),
        PackageDecodeError::Control(_)
    ));
    // An unused constant pool is not silently accepted by the original
    // closed-pool publication law.
    let cv = cv_package();
    let extra = mutated(&cv, |d| {
        let mut pool = d.constants[0].clone();
        pool.id = pool.id.wrapping_sub(1);
        d.constants.push(pool);
    });
    assert!(!matches!(refused(&extra), PackageDecodeError::Control(_)));
}

#[test]
fn oversized_input_is_refused_by_byte_admission_before_any_dto() {
    let bytes = encode(&rich_package()).unwrap();
    let mut limits = decode_limits();
    limits.wire.max_input_bytes = bytes.len() - 1;
    let error = decode_fragment_package(&bytes, &model(), &limits, &Control::default())
        .expect_err("over byte limit");
    assert!(matches!(
        error,
        PackageDecodeError::Control(CompileControlError::ResourceExhausted)
            | PackageDecodeError::Wire(_)
    ));
}

// Representative positions on the one Decode scope: the caller's cause is
// primary at entry, in the middle and at the footer.
#[test]
fn caller_control_cause_stays_primary_across_the_receiver() {
    let package = checked_read(true);
    let bytes = encode(&package).unwrap();
    let control = Control::default();
    decode_fragment_package(&bytes, &model(), &decode_limits(), &control).unwrap();
    let callbacks = control.trace.lock().unwrap().len();
    assert!(callbacks > 2);
    assert!(
        control
            .trace
            .lock()
            .unwrap()
            .iter()
            .all(|(phase, _)| *phase == CompilePhase::Decode)
    );
    for at in [0, callbacks / 3, callbacks / 2, callbacks - 1] {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let stop = Control {
                stop: Some((at, cause)),
                ..Default::default()
            };
            assert!(
                matches!(
                    decode_fragment_package(&bytes, &model(), &decode_limits(), &stop),
                    Err(PackageDecodeError::Control(actual)) if actual == cause
                ),
                "position {at}"
            );
            assert_eq!(stop.trace.lock().unwrap().len(), at + 1);
        }
    }
}
