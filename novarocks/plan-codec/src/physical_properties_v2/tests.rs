// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use std::sync::Mutex;
const SOURCE: usize = 128 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after refusal");
        }
        trace.push((phase, units));
        match self.stop {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn limits() -> PhysicalPropertyProjectionLimits {
    PhysicalPropertyProjectionLimits {
        max_value_references: 2048,
        max_allocation_requests: 8,
        max_allocation_request_bytes: 128 * 1024,
        max_coexisting_source_and_request_bytes: 512 * 1024,
        max_work: 4 * 1024 * 1024,
    }
}
fn properties(distribution: physical::Distribution) -> physical::PhysicalProperties {
    physical::PhysicalProperties {
        distribution,
        row_multiplicity: physical::RowMultiplicity::Replicated,
        ordering: vec![
            physical::OrderingKey {
                value: physical::ValueId::new(0),
                direction: physical::SortDirection::Ascending,
                null_ordering: physical::NullOrdering::First,
            },
            physical::OrderingKey {
                value: physical::ValueId::new(u32::MAX),
                direction: physical::SortDirection::Descending,
                null_ordering: physical::NullOrdering::Last,
            },
            physical::OrderingKey {
                value: physical::ValueId::new(0),
                direction: physical::SortDirection::Descending,
                null_ordering: physical::NullOrdering::First,
            },
        ]
        .into_boxed_slice(),
    }
}
fn hash() -> physical::PhysicalProperties {
    properties(physical::Distribution::Hash {
        keys: vec![
            physical::ValueId::new(u32::MAX),
            physical::ValueId::new(0),
            physical::ValueId::new(u32::MAX),
        ]
        .into_boxed_slice(),
        scheme: physical::HashPartitionScheme {
            space: PartitionSpaceId::try_new([7; 32]).unwrap(),
            count: physical::PartitionCountParameter {
                id: PartitionCountParameterId::try_new([8; 32]).unwrap(),
                admissible: physical::PartitionCountDomain {
                    min: 2,
                    max: 8,
                    requires_power_of_two: true,
                },
            },
            definition: physical::HashDefinition {
                algorithm: PartitionHashAlgorithm::NativeExchangeV1,
            },
        },
    })
}
fn bucket() -> physical::PhysicalProperties {
    properties(physical::Distribution::BucketShuffle {
        keys: vec![physical::ValueId::new(0), physical::ValueId::new(u32::MAX)].into_boxed_slice(),
        scheme: physical::BucketPartitionScheme {
            space: PartitionSpaceId::try_new([9; 32]).unwrap(),
            bucket_count: 3,
            hash: PartitionHashAlgorithm::NativeBucketCrc32V1,
            layout: BucketLayoutAlgorithm::DenseZeroBasedV1,
            ordinal_domain: physical::BucketOrdinalDomainProof {
                first_ordinal: 0,
                ordinal_count: 3,
                evidence_digest: [10; 32],
            },
        },
    })
}
fn expected(kind: wire::distribution::Kind) -> wire::PhysicalProperties {
    // Independent published wire values, including repeated output keys.
    wire::PhysicalProperties {
        distribution: Some(wire::Distribution { kind: Some(kind) }),
        row_multiplicity: 2,
        ordering: vec![
            wire::OrderingKey {
                value_id: Some(0),
                direction: 1,
                null_ordering: 1,
            },
            wire::OrderingKey {
                value_id: Some(u32::MAX),
                direction: 2,
                null_ordering: 2,
            },
            wire::OrderingKey {
                value_id: Some(0),
                direction: 2,
                null_ordering: 1,
            },
        ],
    }
}
fn expected_hash() -> wire::PhysicalProperties {
    expected(wire::distribution::Kind::Hash(wire::HashDistribution {
        key_value_ids: vec![u32::MAX, 0, u32::MAX],
        scheme: Some(wire::HashPartitionScheme {
            partition_space: vec![7; 32],
            count: Some(wire::PartitionCountParameter {
                id: vec![8; 32],
                admissible: Some(wire::PartitionCountDomain {
                    min: 2,
                    max: 8,
                    requires_power_of_two: true,
                }),
            }),
            algorithm: 1,
        }),
    }))
}
fn expected_bucket() -> wire::PhysicalProperties {
    expected(wire::distribution::Kind::BucketShuffle(
        wire::BucketDistribution {
            key_value_ids: vec![0, u32::MAX],
            scheme: Some(wire::BucketPartitionScheme {
                partition_space: vec![9; 32],
                bucket_count: 3,
                hash: 2,
                layout: 1,
                ordinal_domain: Some(wire::BucketOrdinalDomainProof {
                    first_ordinal: 0,
                    ordinal_count: 3,
                    evidence_digest: vec![10; 32],
                }),
            }),
        },
    ))
}
#[test]
fn complete_properties_all_distributions_have_independent_expected_wire_and_exact_receiving_values()
{
    let cases = [
        (
            properties(physical::Distribution::Unconstrained),
            expected(wire::distribution::Kind::Unconstrained(Empty {})),
        ),
        (
            properties(physical::Distribution::Singleton),
            expected(wire::distribution::Kind::Singleton(Empty {})),
        ),
        (
            properties(physical::Distribution::RoundRobin),
            expected(wire::distribution::Kind::RoundRobin(Empty {})),
        ),
        (
            properties(physical::Distribution::Broadcast),
            expected(wire::distribution::Kind::Broadcast(Empty {})),
        ),
        (hash(), expected_hash()),
        (bucket(), expected_bucket()),
    ];
    for (source, expected) in cases {
        let (encoded, _) =
            encode_physical_properties(&source, SOURCE, limits(), &Control::default()).unwrap();
        assert_eq!(encoded, expected);
        let (decoded, _) =
            decode_physical_properties(&expected, SOURCE, limits(), &Control::default()).unwrap();
        assert_eq!(decoded, source);
    }
    let source = physical::PhysicalProperties {
        distribution: physical::Distribution::Singleton,
        row_multiplicity: physical::RowMultiplicity::SingleCopy,
        ordering: Box::new([]),
    };
    let expected = wire::PhysicalProperties {
        distribution: Some(wire::Distribution {
            kind: Some(wire::distribution::Kind::Singleton(Empty {})),
        }),
        row_multiplicity: 1,
        ordering: vec![],
    };
    assert_eq!(
        encode_physical_properties(&source, SOURCE, limits(), &Control::default())
            .unwrap()
            .0,
        expected
    );
    assert_eq!(
        decode_physical_properties(&expected, SOURCE, limits(), &Control::default())
            .unwrap()
            .0,
        source
    );
}
fn hash_wire(value: &mut wire::PhysicalProperties) -> &mut wire::HashPartitionScheme {
    match value.distribution.as_mut().unwrap().kind.as_mut().unwrap() {
        wire::distribution::Kind::Hash(hash) => hash.scheme.as_mut().unwrap(),
        _ => panic!("hash fixture"),
    }
}
fn bucket_wire(value: &mut wire::PhysicalProperties) -> &mut wire::BucketPartitionScheme {
    match value.distribution.as_mut().unwrap().kind.as_mut().unwrap() {
        wire::distribution::Kind::BucketShuffle(bucket) => bucket.scheme.as_mut().unwrap(),
        _ => panic!("bucket fixture"),
    }
}
#[test]
fn complete_properties_receiving_rejects_every_required_closed_shape_and_exact_identity_failure() {
    for bad in 0..18 {
        let mut value = if bad >= 14 {
            expected_bucket()
        } else {
            expected_hash()
        };
        match bad {
            0 => value.distribution = None,
            1 => value.distribution.as_mut().unwrap().kind = None,
            2 => value.row_multiplicity = 0,
            3 => value.row_multiplicity = 99,
            4 => value.ordering[0].value_id = None,
            5 => value.ordering[0].direction = 0,
            6 => value.ordering[0].direction = 99,
            7 => value.ordering[0].null_ordering = 0,
            8 => value.ordering[0].null_ordering = 99,
            9 => hash_wire(&mut value).count = None,
            10 => hash_wire(&mut value).count.as_mut().unwrap().admissible = None,
            11 => hash_wire(&mut value).partition_space = vec![7; 31],
            12 => hash_wire(&mut value).count.as_mut().unwrap().id = vec![0; 32],
            13 => hash_wire(&mut value).algorithm = 99,
            14 => bucket_wire(&mut value).layout = 0,
            15 => bucket_wire(&mut value).ordinal_domain = None,
            16 => {
                bucket_wire(&mut value)
                    .ordinal_domain
                    .as_mut()
                    .unwrap()
                    .evidence_digest = vec![1; 33]
            }
            17 => bucket_wire(&mut value).partition_space = vec![0; 32],
            _ => unreachable!(),
        }
        assert!(
            matches!(
                decode_physical_properties(&value, SOURCE, limits(), &Control::default()),
                Err(Error::InvalidShape(_))
            ),
            "bad case {bad}"
        );
    }
    for algorithm in [0, 99] {
        let mut value = expected_bucket();
        bucket_wire(&mut value).hash = algorithm;
        assert!(decode_physical_properties(&value, SOURCE, limits(), &Control::default()).is_err());
    }
    let mut value = expected_hash();
    hash_wire(&mut value).count.as_mut().unwrap().id = vec![1; 33];
    assert!(decode_physical_properties(&value, SOURCE, limits(), &Control::default()).is_err());
}
fn exact_limits(facts: PhysicalPropertyProjectionFacts) -> PhysicalPropertyProjectionLimits {
    PhysicalPropertyProjectionLimits {
        max_value_references: facts.value_reference_count,
        max_allocation_requests: facts.allocation_requests_upper_bound,
        max_allocation_request_bytes: facts.allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: facts
            .coexisting_source_and_request_bytes_upper_bound,
        max_work: facts.cumulative_work_upper_bound,
    }
}
fn reduce(limits: &mut PhysicalPropertyProjectionLimits, which: usize) {
    match which {
        0 => limits.max_value_references -= 1,
        1 => limits.max_allocation_requests -= 1,
        2 => limits.max_allocation_request_bytes -= 1,
        3 => limits.max_coexisting_source_and_request_bytes -= 1,
        4 => limits.max_work -= 1,
        _ => unreachable!(),
    }
}
#[test]
fn complete_properties_exact_limits_and_source_capacities_are_checked_before_output_requests() {
    let source = hash();
    let wire = expected_hash();
    let ef = encode_physical_properties(&source, SOURCE, limits(), &Control::default())
        .unwrap()
        .1;
    let df = decode_physical_properties(&wire, SOURCE, limits(), &Control::default())
        .unwrap()
        .1;
    assert!(
        encode_physical_properties(&source, SOURCE, exact_limits(ef), &Control::default()).is_ok()
    );
    assert!(
        decode_physical_properties(&wire, SOURCE, exact_limits(df), &Control::default()).is_ok()
    );
    for which in 0..5 {
        let mut e = exact_limits(ef);
        reduce(&mut e, which);
        assert!(encode_physical_properties(&source, SOURCE, e, &Control::default()).is_err());
        let mut d = exact_limits(df);
        reduce(&mut d, which);
        assert!(decode_physical_properties(&wire, SOURCE, d, &Control::default()).is_err());
    }
    let mut spare = expected_hash();
    hash_wire(&mut spare).partition_space.reserve_exact(4096);
    let visible = size_of::<wire::PhysicalProperties>()
        + 3 * size_of::<u32>()
        + 3 * size_of::<wire::OrderingKey>()
        + 64;
    assert!(decode_physical_properties(&spare, visible, limits(), &Control::default()).is_err());
    assert!(decode_physical_properties(&spare, SOURCE, limits(), &Control::default()).is_ok());
    assert!(encode_physical_properties(&source, 0, limits(), &Control::default()).is_err());
    assert!(add(usize::MAX, 1).is_err());
    assert!(mul(usize::MAX, 2).is_err());
    assert!(bytes::<wire::OrderingKey>(usize::MAX).is_err());
}
fn prefixes(call: impl Fn(&Control) -> Result<(), Error>, succeeds: bool) {
    let accepted = Control::default();
    let result = call(&accepted);
    assert_eq!(result.is_ok(), succeeds);
    let baseline = accepted.trace.lock().unwrap().clone();
    assert!(baseline.len() >= 2);
    for at in 0..baseline.len() {
        for cause in CAUSES {
            let refusing = Control {
                stop: Some((at, cause)),
                ..Control::default()
            };
            assert_eq!(call(&refusing), Err(Error::Control(cause)));
            assert_eq!(*refusing.trace.lock().unwrap(), baseline[..=at]);
        }
    }
}
#[test]
fn complete_properties_both_directions_and_ordinary_tails_keep_every_original_control_cause() {
    let source = hash();
    let wire = expected_hash();
    prefixes(
        |control| encode_physical_properties(&source, SOURCE, limits(), control).map(|_| ()),
        true,
    );
    prefixes(
        |control| decode_physical_properties(&wire, SOURCE, limits(), control).map(|_| ()),
        true,
    );
    let mut bad = wire.clone();
    bad.ordering[1].direction = 0;
    prefixes(
        |control| decode_physical_properties(&bad, SOURCE, limits(), control).map(|_| ()),
        false,
    );
    prefixes(
        |control| encode_physical_properties(&source, 0, limits(), control).map(|_| ()),
        false,
    );
    prefixes(
        |control| decode_physical_properties(&wire, 0, limits(), control).map(|_| ()),
        false,
    );
}
#[test]
fn complete_properties_real_wide_ordered_occurrences_observe_quantum_and_preserve_sparse_ids() {
    let mut source = hash();
    source.ordering = (0..320)
        .map(|at| physical::OrderingKey {
            value: physical::ValueId::new(if at % 2 == 0 { 0 } else { u32::MAX }),
            direction: physical::SortDirection::Ascending,
            null_ordering: physical::NullOrdering::Last,
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let encode_control = Control::default();
    let (wire, ef) =
        encode_physical_properties(&source, SOURCE, limits(), &encode_control).unwrap();
    let decode_control = Control::default();
    let (actual, df) =
        decode_physical_properties(&wire, SOURCE, limits(), &decode_control).unwrap();
    assert_eq!(actual, source);
    for (baseline, phase, facts) in [
        (
            encode_control.trace.lock().unwrap().clone(),
            CompilePhase::Encode,
            ef,
        ),
        (
            decode_control.trace.lock().unwrap().clone(),
            CompilePhase::Decode,
            df,
        ),
    ] {
        assert!(baseline.iter().any(|(_, units)| *units == 256));
        assert!(baseline.iter().all(|(p, _)| *p == phase));
        assert!(
            baseline.iter().map(|(_, n)| *n as usize).sum::<usize>()
                <= facts.cumulative_work_upper_bound
        );
        for at in [
            0,
            baseline.iter().position(|(_, n)| *n == 256).unwrap(),
            baseline.len() - 1,
        ] {
            for cause in CAUSES {
                let refusing = Control {
                    stop: Some((at, cause)),
                    ..Control::default()
                };
                let result = if phase == CompilePhase::Encode {
                    encode_physical_properties(&source, SOURCE, limits(), &refusing).map(|_| ())
                } else {
                    decode_physical_properties(&wire, SOURCE, limits(), &refusing).map(|_| ())
                };
                assert_eq!(result, Err(Error::Control(cause)));
                assert_eq!(*refusing.trace.lock().unwrap(), baseline[..=at]);
            }
        }
    }
}
#[test]
fn complete_properties_projection_preserves_raw_domains_without_claiming_fragment_legality() {
    // Existing Fragment validation, rather than this codec, rejects a hash
    // domain with no admissible member and mismatched bucket evidence.
    let mut wire = expected_hash();
    let scheme = hash_wire(&mut wire);
    scheme.algorithm = 2;
    scheme.count.as_mut().unwrap().admissible = Some(wire::PartitionCountDomain {
        min: 0,
        max: u32::MAX,
        requires_power_of_two: true,
    });
    let (typed, _) =
        decode_physical_properties(&wire, SOURCE, limits(), &Control::default()).unwrap();
    assert_eq!(
        encode_physical_properties(&typed, SOURCE, limits(), &Control::default())
            .unwrap()
            .0,
        wire
    );
    let mut wire = expected_bucket();
    let scheme = bucket_wire(&mut wire);
    scheme.ordinal_domain.as_mut().unwrap().first_ordinal = 7;
    scheme.ordinal_domain.as_mut().unwrap().ordinal_count = u32::MAX;
    let (typed, _) =
        decode_physical_properties(&wire, SOURCE, limits(), &Control::default()).unwrap();
    assert_eq!(
        encode_physical_properties(&typed, SOURCE, limits(), &Control::default())
            .unwrap()
            .0,
        wire
    );
}

#[test]
fn composing_property_preflight_has_same_numerical_author_before_emission() {
    for original in [
        properties(physical::Distribution::Unconstrained),
        properties(physical::Distribution::Singleton),
        properties(physical::Distribution::RoundRobin),
        properties(physical::Distribution::Broadcast),
        hash(),
        bucket(),
    ] {
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let admitted = preflight_encode_observed(&original, SOURCE, limits(), &mut work).unwrap();
        work.finish().unwrap();
        let (wire, actual) =
            encode_physical_properties(&original, SOURCE, limits(), &Control::default()).unwrap();
        assert_eq!(admitted, actual);
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let admitted = preflight_decode_observed(&wire, SOURCE, limits(), &mut work).unwrap();
        work.finish().unwrap();
        let (decoded, actual) =
            decode_physical_properties(&wire, SOURCE, limits(), &Control::default()).unwrap();
        assert_eq!(admitted, actual);
        assert_eq!(decoded, original);
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let cap = PhysicalPropertyProjectionLimits {
            max_allocation_request_bytes: admitted.allocation_request_bytes_upper_bound - 1,
            ..limits()
        };
        assert!(matches!(
            preflight_decode_observed(&wire, SOURCE, cap, &mut work),
            Err(Error::InvalidShape(_))
        ));
    }
}

#[test]
fn composing_property_preflight_preserves_each_original_callback_cause() {
    let original = hash();
    let (wire, _) =
        encode_physical_properties(&original, SOURCE, limits(), &Control::default()).unwrap();
    for receiving in [false, true] {
        let run = |control: &Control| -> Result<PhysicalPropertyProjectionFacts, Error> {
            let mut work = CompileCheckpoints::try_new(
                control,
                if receiving {
                    CompilePhase::Decode
                } else {
                    CompilePhase::Encode
                },
            )?;
            let result = if receiving {
                preflight_decode_observed(&wire, SOURCE, limits(), &mut work)
            } else {
                preflight_encode_observed(&original, SOURCE, limits(), &mut work)
            };
            finish(work, result)
        };
        let control = Control::default();
        run(&control).unwrap();
        let expected = control.trace.lock().unwrap().clone();
        for cause in CAUSES {
            for stop in 0..expected.len() {
                let control = Control {
                    trace: Mutex::default(),
                    stop: Some((stop, cause)),
                };
                assert!(matches!(run(&control),Err(Error::Control(got)) if got==cause));
                assert_eq!(*control.trace.lock().unwrap(), expected[..=stop]);
            }
        }
    }
}
