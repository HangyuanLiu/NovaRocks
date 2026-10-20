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
//! Mann-Whitney owns its explicit value outcomes and unmaskable operations.
//! Whole-value receipts retain their domain reason and full host diagnostic.
//! Required invocations retain a bounded projection, never an argument graph.
use crate::KernelFailure;
use novarocks_failure_contract::{
    BoundedDetail, FailureFact, FailureProjection, OuterFailureProjection,
};
use std::{error::Error, fmt};
#[derive(Debug)]
pub enum MannFailure {
    Value(MannValueFailure),
    Kernel(KernelFailure),
}
impl From<KernelFailure> for MannFailure {
    fn from(source: KernelFailure) -> Self {
        Self::Kernel(source)
    }
}
impl From<MannValueFailure> for MannFailure {
    fn from(source: MannValueFailure) -> Self {
        Self::Value(source)
    }
}
impl fmt::Display for MannFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(source) => source.fmt(f),
            Self::Kernel(source) => source.fmt(f),
        }
    }
}
impl Error for MannFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(match self {
            Self::Value(source) => source,
            Self::Kernel(source) => source,
        })
    }
}
impl MannFailure {
    pub fn into_kernel_failure(self) -> KernelFailure {
        match self {
            Self::Value(source) => source.required_projection(),
            Self::Kernel(source) => source,
        }
    }
}
#[derive(Debug)]
pub enum MannValueFailure {
    InvalidAlternative(String),
    NegativeContinuity,
    StateParametersMismatch,
    TruncatedState,
}
impl fmt::Display for MannValueFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidAlternative(value) => {
                write!(f, "Logical Error: invalid alternative `{value}`.")
            }
            Self::NegativeContinuity => {
                f.write_str("Logical Error: continuity_correction must be non-negative.")
            }
            Self::StateParametersMismatch => {
                f.write_str("mann_whitney_u_test state parameters mismatch")
            }
            Self::TruncatedState => {
                f.write_str("mann_whitney_u_test deserialize: buffer too short")
            }
        }
    }
}
impl Error for MannValueFailure {}
impl MannValueFailure {
    pub(crate) fn reason(&self) -> crate::DataFailureReason {
        match self {
            Self::TruncatedState => crate::DataFailureReason::InvalidEncoding,
            _ => crate::DataFailureReason::InvalidValue,
        }
    }
    pub(crate) fn required_projection(&self) -> KernelFailure {
        KernelFailure::from_domain(RequiredMannValue {
            detail: BoundedDetail::truncating_display(self),
            reason: self.reason(),
        })
    }
}
#[derive(Debug)]
struct RequiredMannValue {
    detail: BoundedDetail,
    reason: crate::DataFailureReason,
}
impl fmt::Display for RequiredMannValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.detail.fmt(f)
    }
}
impl Error for RequiredMannValue {}
impl FailureProjection for RequiredMannValue {
    fn failure_fact(&self) -> FailureFact {
        self.reason.failure_fact()
    }
}
impl OuterFailureProjection for RequiredMannValue {}
#[derive(Debug)]
struct MannContract(BoundedDetail);
impl fmt::Display for MannContract {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl Error for MannContract {}
impl FailureProjection for MannContract {
    fn failure_fact(&self) -> FailureFact {
        use novarocks_failure_contract::*;
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(391).unwrap()),
        )
    }
}
impl OuterFailureProjection for MannContract {}
pub(super) fn contract(detail: impl fmt::Display) -> KernelFailure {
    KernelFailure::from_domain(MannContract(BoundedDetail::truncating_display(detail)))
}

// Counts and byte layouts are checked at the producer before allocating or
// narrowing. These are format facts, never allocator grants or quota facts.
#[derive(Debug)]
enum MannExtentFailure {
    WireCount {
        required: usize,
        source: std::num::TryFromIntError,
    },
    Storage {
        required: u128,
        source: Option<std::alloc::LayoutError>,
    },
}
impl fmt::Display for MannExtentFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WireCount { required, .. } => write!(
                f,
                "Mann-Whitney sample count {required} exceeds the u32 state format"
            ),
            Self::Storage { required, .. } => write!(
                f,
                "Mann-Whitney storage requires {required} bytes beyond the addressable layout"
            ),
        }
    }
}
impl Error for MannExtentFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::WireCount { source, .. } => Some(source),
            Self::Storage { source, .. } => source
                .as_ref()
                .map(|source| source as &(dyn Error + 'static)),
        }
    }
}
impl FailureProjection for MannExtentFailure {
    fn failure_fact(&self) -> FailureFact {
        use novarocks_failure_contract::*;
        let (code, required, limit) = match self {
            Self::WireCount { required, .. } => (393, *required as u128, u32::MAX as u64),
            Self::Storage { required, .. } => (392, *required, isize::MAX as u64),
        };
        let code = std::num::NonZeroU16::new(code).unwrap();
        FailureFact::new(
            FailureClass::Representation,
            FailureReason::new(domain::KERNEL, code),
        )
        .with_representation(RepresentationFacts {
            boundary: RepresentationBoundary::MinimumUnit,
            layout: RepresentationLayout {
                domain: domain::KERNEL,
                code,
            },
            required: u64::try_from(required).ok(),
            extent_limit: Some(limit),
            representable_prefix: None,
        })
    }
}
impl OuterFailureProjection for MannExtentFailure {}
pub(super) fn storage_count<T>(count: u128) -> Result<usize, KernelFailure> {
    // All callers derive count from at most two usize extents; multiplying
    // by these concrete, small element widths fits the u128 calculation.
    let required = count * std::mem::size_of::<T>() as u128;
    let count = usize::try_from(count).map_err(|_| {
        KernelFailure::from_domain(MannExtentFailure::Storage {
            required,
            source: None,
        })
    })?;
    std::alloc::Layout::array::<T>(count).map_err(|source| {
        KernelFailure::from_domain(MannExtentFailure::Storage {
            required,
            source: Some(source),
        })
    })?;
    Ok(count)
}
pub(super) fn wire_geometry(lengths: [usize; 2]) -> Result<([u32; 2], usize), KernelFailure> {
    let mut counts = [0; 2];
    for (out, required) in counts.iter_mut().zip(lengths) {
        *out = u32::try_from(required).map_err(|source| {
            KernelFailure::from_domain(MannExtentFailure::WireCount { required, source })
        })?;
    }
    let bytes = 17 + 8 * (lengths[0] as u128 + lengths[1] as u128);
    Ok((counts, storage_count::<u8>(bytes)?))
}
