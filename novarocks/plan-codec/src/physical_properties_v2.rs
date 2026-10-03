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

//! Complete physical-property vocabulary projection. Receiving validates wire
//! shape and sole partition identity constructors. Fragment/package validation
//! remains mandatory for key references/types, count/bucket legality and
//! cross-occurrence partition definitions. These explicit request bounds are
//! not a host MEM grant or an internal allocator cooperation claim.

use crate::allocation_exit_v2::reserve_exit;
use novarocks_physical_plan as physical;
use novarocks_proto_models::{physical_control_v2::Empty, physical_package_v2 as wire};
use novarocks_type_contract::{
    BucketLayoutAlgorithm, CompileCheckpoints, CompileControlError, CompilePhase,
    PartitionCountParameterId, PartitionHashAlgorithm, PartitionSpaceId, PureCompileControl,
};
use std::{alloc::Layout, fmt, mem::size_of};

#[derive(Clone, Copy, Debug)]
pub struct PhysicalPropertyProjectionLimits {
    pub max_value_references: usize,
    pub max_allocation_requests: usize,
    pub max_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PhysicalPropertyProjectionFacts {
    pub value_reference_count: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhysicalPropertyCodecError {
    Control(CompileControlError),
    InvalidShape(&'static str),
}
impl From<CompileControlError> for PhysicalPropertyCodecError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl fmt::Display for PhysicalPropertyCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(cause) => cause.fmt(f),
            Self::InvalidShape(text) => f.write_str(text),
        }
    }
}
impl std::error::Error for PhysicalPropertyCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(cause) => Some(cause),
            Self::InvalidShape(_) => None,
        }
    }
}
type Error = PhysicalPropertyCodecError;
fn invalid(text: &'static str) -> Error {
    Error::InvalidShape(text)
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b)
        .ok_or_else(|| invalid("physical property resource sum overflow"))
}
fn mul(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_mul(b)
        .ok_or_else(|| invalid("physical property resource product overflow"))
}
fn bytes<T>(count: usize) -> Result<usize, Error> {
    Layout::array::<T>(count)
        .map(|layout| layout.size())
        .map_err(|_| invalid("physical property allocation layout is unrepresentable"))
}
fn floor(source: usize, known: usize) -> Result<(), Error> {
    if source < known {
        Err(invalid(
            "physical property source invoice omits original backing",
        ))
    } else {
        Ok(())
    }
}
fn cap(value: usize, limit: usize, work: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let admitted = value <= limit;
    work.step()?;
    if admitted {
        Ok(())
    } else {
        Err(invalid("physical property projection envelope exceeded"))
    }
}
fn facts(
    references: usize,
    requests: usize,
    requested_bytes: usize,
    source: usize,
    limits: PhysicalPropertyProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PhysicalPropertyProjectionFacts, Error> {
    // All own passes inspect only fixed-size keys/ordering/identity values.
    // This includes numerical gates, constructors, copies and conversion
    // loops. Requested-byte work conservatively covers buffer writes/moves;
    // std allocation and Vec-to-Box internals are bracketed opaque work.
    let result = PhysicalPropertyProjectionFacts {
        value_reference_count: references,
        allocation_requests_upper_bound: requests,
        allocation_request_bytes_upper_bound: requested_bytes,
        coexisting_source_and_request_bytes_upper_bound: add(source, requested_bytes)?,
        cumulative_work_upper_bound: add(
            128,
            add(mul(references, 32)?, mul(requested_bytes, 4)?)?,
        )?,
    };
    cap(references, limits.max_value_references, work)?;
    cap(requests, limits.max_allocation_requests, work)?;
    cap(requested_bytes, limits.max_allocation_request_bytes, work)?;
    cap(
        result.coexisting_source_and_request_bytes_upper_bound,
        limits.max_coexisting_source_and_request_bytes,
        work,
    )?;
    cap(result.cumulative_work_upper_bound, limits.max_work, work)?;
    Ok(result)
}
fn reserve<T>(count: usize, work: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, Error> {
    bytes::<T>(count)?;
    work.flush()?;
    let mut values = Vec::new();
    let result = values.try_reserve_exact(count);
    reserve_exit::<Error>(result, work)?;
    Ok(values)
}
fn boxed<T>(values: Vec<T>, work: &mut CompileCheckpoints<'_>) -> Result<Box<[T]>, Error> {
    // A shrink request is conservatively admitted even when the standard
    // library reuses the exact Vec allocation. No allocation failure retry.
    work.flush()?;
    let values = values.into_boxed_slice();
    work.flush()?;
    Ok(values)
}
fn copy_bytes(input: &[u8; 32], work: &mut CompileCheckpoints<'_>) -> Result<Vec<u8>, Error> {
    let mut output = reserve(32, work)?;
    for byte in input {
        output.push(*byte);
        work.step()?;
    }
    Ok(output)
}
fn encode_hash(value: PartitionHashAlgorithm) -> i32 {
    match value {
        PartitionHashAlgorithm::NativeExchangeV1 => {
            wire::PartitionHashAlgorithm::NativeExchangeV1 as i32
        }
        PartitionHashAlgorithm::NativeBucketCrc32V1 => {
            wire::PartitionHashAlgorithm::NativeBucketCrc32V1 as i32
        }
    }
}
fn decode_hash(value: i32) -> Result<PartitionHashAlgorithm, Error> {
    match wire::PartitionHashAlgorithm::try_from(value) {
        Ok(wire::PartitionHashAlgorithm::NativeExchangeV1) => {
            Ok(PartitionHashAlgorithm::NativeExchangeV1)
        }
        Ok(wire::PartitionHashAlgorithm::NativeBucketCrc32V1) => {
            Ok(PartitionHashAlgorithm::NativeBucketCrc32V1)
        }
        Ok(wire::PartitionHashAlgorithm::Unspecified) | Err(_) => Err(invalid(
            "physical property hash algorithm is unknown or unspecified",
        )),
    }
}
fn encode_direction(value: physical::SortDirection) -> i32 {
    match value {
        physical::SortDirection::Ascending => wire::SortDirection::Ascending as i32,
        physical::SortDirection::Descending => wire::SortDirection::Descending as i32,
    }
}
fn decode_direction(value: i32) -> Result<physical::SortDirection, Error> {
    match wire::SortDirection::try_from(value) {
        Ok(wire::SortDirection::Ascending) => Ok(physical::SortDirection::Ascending),
        Ok(wire::SortDirection::Descending) => Ok(physical::SortDirection::Descending),
        Ok(wire::SortDirection::Unspecified) | Err(_) => Err(invalid(
            "physical property sort direction is unknown or unspecified",
        )),
    }
}
fn encode_nulls(value: physical::NullOrdering) -> i32 {
    match value {
        physical::NullOrdering::First => wire::NullOrdering::First as i32,
        physical::NullOrdering::Last => wire::NullOrdering::Last as i32,
    }
}
fn decode_nulls(value: i32) -> Result<physical::NullOrdering, Error> {
    match wire::NullOrdering::try_from(value) {
        Ok(wire::NullOrdering::First) => Ok(physical::NullOrdering::First),
        Ok(wire::NullOrdering::Last) => Ok(physical::NullOrdering::Last),
        Ok(wire::NullOrdering::Unspecified) | Err(_) => Err(invalid(
            "physical property null ordering is unknown or unspecified",
        )),
    }
}
fn encode_multiplicity(value: physical::RowMultiplicity) -> i32 {
    match value {
        physical::RowMultiplicity::SingleCopy => wire::RowMultiplicity::SingleCopy as i32,
        physical::RowMultiplicity::Replicated => wire::RowMultiplicity::Replicated as i32,
    }
}
fn decode_multiplicity(value: i32) -> Result<physical::RowMultiplicity, Error> {
    match wire::RowMultiplicity::try_from(value) {
        Ok(wire::RowMultiplicity::SingleCopy) => Ok(physical::RowMultiplicity::SingleCopy),
        Ok(wire::RowMultiplicity::Replicated) => Ok(physical::RowMultiplicity::Replicated),
        Ok(wire::RowMultiplicity::Unspecified) | Err(_) => Err(invalid(
            "physical property row multiplicity is unknown or unspecified",
        )),
    }
}
fn exact32(input: &[u8]) -> Result<[u8; 32], Error> {
    input
        .try_into()
        .map_err(|_| invalid("physical property identity or digest is not exactly 32 bytes"))
}

pub(crate) fn encode_observed(
    input: &physical::PhysicalProperties,
    source: usize,
    limits: PhysicalPropertyProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(wire::PhysicalProperties, PhysicalPropertyProjectionFacts), Error> {
    let keys = match &input.distribution {
        physical::Distribution::Hash { keys, .. }
        | physical::Distribution::BucketShuffle { keys, .. } => &**keys,
        physical::Distribution::Unconstrained
        | physical::Distribution::Singleton
        | physical::Distribution::RoundRobin
        | physical::Distribution::Broadcast => &[],
    };
    let keyed = matches!(
        input.distribution,
        physical::Distribution::Hash { .. } | physical::Distribution::BucketShuffle { .. }
    );
    let references = add(keys.len(), input.ordering.len())?;
    floor(
        source,
        add(
            size_of::<physical::PhysicalProperties>(),
            add(
                bytes::<physical::ValueId>(keys.len())?,
                bytes::<physical::OrderingKey>(input.ordering.len())?,
            )?,
        )?,
    )?;
    let requested = add(
        add(
            bytes::<u32>(keys.len())?,
            bytes::<wire::OrderingKey>(input.ordering.len())?,
        )?,
        if keyed { 64 } else { 0 },
    )?;
    let requests = add(
        usize::from(!keys.is_empty()),
        add(
            usize::from(!input.ordering.is_empty()),
            if keyed { 2 } else { 0 },
        )?,
    )?;
    let facts = facts(references, requests, requested, source, limits, work)?;
    let mut output_keys = reserve(keys.len(), work)?;
    for key in keys {
        output_keys.push(key.get());
        work.step()?;
    }
    let kind = match &input.distribution {
        physical::Distribution::Unconstrained => wire::distribution::Kind::Unconstrained(Empty {}),
        physical::Distribution::Singleton => wire::distribution::Kind::Singleton(Empty {}),
        physical::Distribution::RoundRobin => wire::distribution::Kind::RoundRobin(Empty {}),
        physical::Distribution::Broadcast => wire::distribution::Kind::Broadcast(Empty {}),
        physical::Distribution::Hash { scheme, .. } => {
            wire::distribution::Kind::Hash(wire::HashDistribution {
                key_value_ids: output_keys,
                scheme: Some(wire::HashPartitionScheme {
                    partition_space: copy_bytes(&scheme.space.as_bytes(), work)?,
                    count: Some(wire::PartitionCountParameter {
                        id: copy_bytes(&scheme.count.id.as_bytes(), work)?,
                        admissible: Some(wire::PartitionCountDomain {
                            min: scheme.count.admissible.min,
                            max: scheme.count.admissible.max,
                            requires_power_of_two: scheme.count.admissible.requires_power_of_two,
                        }),
                    }),
                    algorithm: encode_hash(scheme.definition.algorithm),
                }),
            })
        }
        physical::Distribution::BucketShuffle { scheme, .. } => {
            wire::distribution::Kind::BucketShuffle(wire::BucketDistribution {
                key_value_ids: output_keys,
                scheme: Some(wire::BucketPartitionScheme {
                    partition_space: copy_bytes(&scheme.space.as_bytes(), work)?,
                    bucket_count: scheme.bucket_count,
                    hash: encode_hash(scheme.hash),
                    layout: match scheme.layout {
                        BucketLayoutAlgorithm::DenseZeroBasedV1 => {
                            wire::BucketLayoutAlgorithm::DenseZeroBasedV1 as i32
                        }
                    },
                    ordinal_domain: Some(wire::BucketOrdinalDomainProof {
                        first_ordinal: scheme.ordinal_domain.first_ordinal,
                        ordinal_count: scheme.ordinal_domain.ordinal_count,
                        evidence_digest: copy_bytes(&scheme.ordinal_domain.evidence_digest, work)?,
                    }),
                }),
            })
        }
    };
    work.step()?;
    let mut ordering = reserve(input.ordering.len(), work)?;
    for key in &input.ordering {
        ordering.push(wire::OrderingKey {
            value_id: Some(key.value.get()),
            direction: encode_direction(key.direction),
            null_ordering: encode_nulls(key.null_ordering),
        });
        work.step()?;
    }
    Ok((
        wire::PhysicalProperties {
            distribution: Some(wire::Distribution { kind: Some(kind) }),
            row_multiplicity: encode_multiplicity(input.row_multiplicity),
            ordering,
        },
        facts,
    ))
}

// A no-heap validated header. It is never a public receiving semantic owner;
// the complete output typed value is constructed in this same call.
enum DistributionHeader {
    Unconstrained,
    Singleton,
    RoundRobin,
    Broadcast,
    Hash(physical::HashPartitionScheme),
    Bucket(physical::BucketPartitionScheme),
}
fn decode_header(
    input: &wire::distribution::Kind,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DistributionHeader, Error> {
    let header = match input {
        wire::distribution::Kind::Unconstrained(_) => DistributionHeader::Unconstrained,
        wire::distribution::Kind::Singleton(_) => DistributionHeader::Singleton,
        wire::distribution::Kind::RoundRobin(_) => DistributionHeader::RoundRobin,
        wire::distribution::Kind::Broadcast(_) => DistributionHeader::Broadcast,
        wire::distribution::Kind::Hash(hash) => {
            let scheme = hash
                .scheme
                .as_ref()
                .ok_or_else(|| invalid("physical property hash scheme is absent"))?;
            let count = scheme
                .count
                .as_ref()
                .ok_or_else(|| invalid("physical property count parameter is absent"))?;
            let domain = count
                .admissible
                .as_ref()
                .ok_or_else(|| invalid("physical property count domain is absent"))?;
            DistributionHeader::Hash(physical::HashPartitionScheme {
                space: PartitionSpaceId::try_new(exact32(&scheme.partition_space)?)
                    .map_err(|_| invalid("physical property partition space is zero"))?,
                count: physical::PartitionCountParameter {
                    id: PartitionCountParameterId::try_new(exact32(&count.id)?).map_err(|_| {
                        invalid("physical property count parameter identity is zero")
                    })?,
                    admissible: physical::PartitionCountDomain {
                        min: domain.min,
                        max: domain.max,
                        requires_power_of_two: domain.requires_power_of_two,
                    },
                },
                definition: physical::HashDefinition {
                    algorithm: decode_hash(scheme.algorithm)?,
                },
            })
        }
        wire::distribution::Kind::BucketShuffle(bucket) => {
            let scheme = bucket
                .scheme
                .as_ref()
                .ok_or_else(|| invalid("physical property bucket scheme is absent"))?;
            let proof = scheme
                .ordinal_domain
                .as_ref()
                .ok_or_else(|| invalid("physical property bucket ordinal proof is absent"))?;
            let layout = match wire::BucketLayoutAlgorithm::try_from(scheme.layout) {
                Ok(wire::BucketLayoutAlgorithm::DenseZeroBasedV1) => {
                    BucketLayoutAlgorithm::DenseZeroBasedV1
                }
                Ok(wire::BucketLayoutAlgorithm::Unspecified) | Err(_) => {
                    return Err(invalid(
                        "physical property bucket layout is unknown or unspecified",
                    ));
                }
            };
            DistributionHeader::Bucket(physical::BucketPartitionScheme {
                space: PartitionSpaceId::try_new(exact32(&scheme.partition_space)?)
                    .map_err(|_| invalid("physical property partition space is zero"))?,
                bucket_count: scheme.bucket_count,
                hash: decode_hash(scheme.hash)?,
                layout,
                ordinal_domain: physical::BucketOrdinalDomainProof {
                    first_ordinal: proof.first_ordinal,
                    ordinal_count: proof.ordinal_count,
                    evidence_digest: exact32(&proof.evidence_digest)?,
                },
            })
        }
    };
    work.step()?;
    Ok(header)
}
pub(crate) fn decode_observed(
    input: &wire::PhysicalProperties,
    source: usize,
    limits: PhysicalPropertyProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    (
        physical::PhysicalProperties,
        PhysicalPropertyProjectionFacts,
    ),
    Error,
> {
    let kind = input
        .distribution
        .as_ref()
        .ok_or_else(|| invalid("physical property distribution is absent"))?
        .kind
        .as_ref()
        .ok_or_else(|| invalid("physical property distribution kind is absent"))?;
    let (keys, extra_source) = match kind {
        wire::distribution::Kind::Hash(hash) => {
            let scheme = hash
                .scheme
                .as_ref()
                .ok_or_else(|| invalid("physical property hash scheme is absent"))?;
            let count = scheme
                .count
                .as_ref()
                .ok_or_else(|| invalid("physical property count parameter is absent"))?;
            (
                &hash.key_value_ids[..],
                add(
                    bytes::<u32>(hash.key_value_ids.capacity())?,
                    add(scheme.partition_space.capacity(), count.id.capacity())?,
                )?,
            )
        }
        wire::distribution::Kind::BucketShuffle(bucket) => {
            let scheme = bucket
                .scheme
                .as_ref()
                .ok_or_else(|| invalid("physical property bucket scheme is absent"))?;
            let proof = scheme
                .ordinal_domain
                .as_ref()
                .ok_or_else(|| invalid("physical property bucket ordinal proof is absent"))?;
            (
                &bucket.key_value_ids[..],
                add(
                    bytes::<u32>(bucket.key_value_ids.capacity())?,
                    add(
                        scheme.partition_space.capacity(),
                        proof.evidence_digest.capacity(),
                    )?,
                )?,
            )
        }
        wire::distribution::Kind::Unconstrained(_)
        | wire::distribution::Kind::Singleton(_)
        | wire::distribution::Kind::RoundRobin(_)
        | wire::distribution::Kind::Broadcast(_) => (&[][..], 0),
    };
    floor(
        source,
        add(
            size_of::<wire::PhysicalProperties>(),
            add(
                extra_source,
                bytes::<wire::OrderingKey>(input.ordering.capacity())?,
            )?,
        )?,
    )?;
    // Vec-to-Box may shrink to the exact length; admit both requested layouts
    // and their coexistence even when the library actually reuses backing.
    let requested = mul(
        add(
            bytes::<physical::ValueId>(keys.len())?,
            bytes::<physical::OrderingKey>(input.ordering.len())?,
        )?,
        2,
    )?;
    let requests = mul(
        add(
            usize::from(!keys.is_empty()),
            usize::from(!input.ordering.is_empty()),
        )?,
        2,
    )?;
    let facts = facts(
        add(keys.len(), input.ordering.len())?,
        requests,
        requested,
        source,
        limits,
        work,
    )?;
    let row_multiplicity = decode_multiplicity(input.row_multiplicity)?;
    let header = decode_header(kind, work)?;
    // Validate the complete ordering grammar before the first reserve; the
    // later conversion only consumes this immutable borrowed snapshot.
    for key in &input.ordering {
        key.value_id
            .ok_or_else(|| invalid("physical property ordering value is absent"))?;
        decode_direction(key.direction)?;
        decode_nulls(key.null_ordering)?;
        work.step()?;
    }
    let mut output_keys = reserve(keys.len(), work)?;
    for key in keys {
        output_keys.push(physical::ValueId::new(*key));
        work.step()?;
    }
    let output_keys = boxed(output_keys, work)?;
    let distribution = match header {
        DistributionHeader::Unconstrained => physical::Distribution::Unconstrained,
        DistributionHeader::Singleton => physical::Distribution::Singleton,
        DistributionHeader::RoundRobin => physical::Distribution::RoundRobin,
        DistributionHeader::Broadcast => physical::Distribution::Broadcast,
        DistributionHeader::Hash(scheme) => physical::Distribution::Hash {
            keys: output_keys,
            scheme,
        },
        DistributionHeader::Bucket(scheme) => physical::Distribution::BucketShuffle {
            keys: output_keys,
            scheme,
        },
    };
    work.step()?;
    let mut ordering = reserve(input.ordering.len(), work)?;
    for key in &input.ordering {
        ordering.push(physical::OrderingKey {
            value: physical::ValueId::new(
                key.value_id
                    .ok_or_else(|| invalid("prepared property ordering value is absent"))?,
            ),
            direction: decode_direction(key.direction)?,
            null_ordering: decode_nulls(key.null_ordering)?,
        });
        work.step()?;
    }
    let ordering = boxed(ordering, work)?;
    Ok((
        physical::PhysicalProperties {
            distribution,
            row_multiplicity,
            ordering,
        },
        facts,
    ))
}
fn finish<T>(work: CompileCheckpoints<'_>, result: Result<T, Error>) -> Result<T, Error> {
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
/// Encode all property variants without changing occurrence order or identity.
pub fn encode_physical_properties(
    input: &physical::PhysicalProperties,
    source_retained_bytes: usize,
    limits: PhysicalPropertyProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<(wire::PhysicalProperties, PhysicalPropertyProjectionFacts), Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = encode_observed(input, source_retained_bytes, limits, &mut work);
    finish(work, result)
}
/// Decode the complete closed vocabulary. Actual Fragment/FragmentPackage
/// validation must subsequently check property semantics and references.
pub fn decode_physical_properties(
    input: &wire::PhysicalProperties,
    source_retained_bytes: usize,
    limits: PhysicalPropertyProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<
    (
        physical::PhysicalProperties,
        PhysicalPropertyProjectionFacts,
    ),
    Error,
> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = decode_observed(input, source_retained_bytes, limits, &mut work);
    finish(work, result)
}

#[cfg(test)]
mod tests;
