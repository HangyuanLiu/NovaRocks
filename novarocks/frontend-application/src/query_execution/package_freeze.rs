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

//! Freeze one completed SQL plan into v2 FragmentPackage wire bytes.
//!
//! The SQL owner authors each fragment's uses, calls and pruning; each scan's
//! frozen read is authored from the plan and the freeze that produced it; each
//! written target's frozen recipe is authored from the write session that
//! sealed it; the original extraction law publishes the checked packages; the
//! v2 sender encodes them.
//!
//! The host provides [`CompiledPackageCarrier`] admission and encode limits
//! once, paired with the compiled-package backend.

use std::collections::BTreeMap;
use std::fmt;

use novarocks_connector_contract::ConnectorWriteRecipeDraft;
use novarocks_physical_plan::{
    ConstantPolicy, FragmentId, FragmentPackage, FragmentPackageAdmission,
    ProviderReadOccurrenceId, WriteTargetOrdinal, extract_fragment_packages,
};
use novarocks_plan_codec::physical_package_v2::{
    PackageEncodeError, PackageEncodeLimits, encode_fragment_package,
};
use novarocks_query_application::preparation::CompletedPhysicalPlanCandidate;
use novarocks_type_contract::{CompileControlError, PureCompileControl};
use prost::Message;

use crate::query_execution::package_reads::author_frozen_reads;
use crate::query_execution::provider_read_facts::FrozenReadEncoding;

/// Host-owned configuration of the compiled package carrier.
///
/// Both values are deployment sizing the composition chooses; neither has a
/// default here. The statement constant policy is not part of it: each plan
/// is packaged with the policy its own statement was compiled with.
#[derive(Clone, Copy, Debug)]
pub struct CompiledPackageCarrier {
    admission: FragmentPackageAdmission,
    limits: PackageEncodeLimits,
}

impl CompiledPackageCarrier {
    pub const fn new(admission: FragmentPackageAdmission, limits: PackageEncodeLimits) -> Self {
        Self { admission, limits }
    }

    /// The admission every fragment's checked package is constructed under.
    pub const fn admission(&self) -> &FragmentPackageAdmission {
        &self.admission
    }

    /// The limits every package is encoded under.
    pub const fn limits(&self) -> &PackageEncodeLimits {
        &self.limits
    }
}

#[derive(Debug)]
pub(crate) enum PackageFreezeError {
    Control(CompileControlError),
    Semantics(novarocks_sql::compiler::PackageSemanticsError),
    Extraction(String),
    Encode(PackageEncodeError),
    /// A carrier fact the plan states inconsistently.
    Facts(String),
    /// A frozen provider read the package cannot carry as frozen.
    Read(String),
    /// A frozen writer recipe the package cannot carry as frozen.
    Write(String),
}
impl fmt::Display for PackageFreezeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(cause) => cause.fmt(f),
            Self::Semantics(error) => error.fmt(f),
            Self::Extraction(detail) => write!(f, "fragment package extraction: {detail}"),
            Self::Encode(error) => error.fmt(f),
            Self::Facts(detail) => write!(f, "package carrier facts: {detail}"),
            Self::Read(detail) => write!(f, "frozen provider read: {detail}"),
            Self::Write(detail) => write!(f, "frozen writer recipe: {detail}"),
        }
    }
}
impl std::error::Error for PackageFreezeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(cause) => Some(cause),
            Self::Semantics(source) => Some(source),
            Self::Encode(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for PackageFreezeError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureProjection, FailureReason, domain,
        };
        let code = match self {
            Self::Control(cause) => return cause.failure_fact(),
            Self::Semantics(source) => return source.failure_fact(),
            Self::Encode(source) => return source.failure_fact(),
            Self::Extraction(_) => 200,
            Self::Facts(_) => 201,
            Self::Read(_) => 202,
            Self::Write(_) => 203,
        };
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}

/// A completed plan the compiled carrier cannot freeze is refused through the
/// encoder's own error, keeping a caller's control cause primary.
impl From<PackageFreezeError> for novarocks_plan_codec::PhysicalEncodeError {
    fn from(error: PackageFreezeError) -> Self {
        match error {
            PackageFreezeError::Control(cause) => Self::Control(cause),
            other => Self::from_domain(other),
        }
    }
}

/// Every fragment's v2 package bytes. The host admission and encode limits
/// are caller-owned configuration; none is defaulted here. `encodings` are
/// what the freeze of each of the plan's provider reads kept; `writes` is the
/// frozen recipe of exactly each target the plan writes.
pub(crate) fn freeze_fragment_packages(
    candidate: &CompletedPhysicalPlanCandidate,
    encodings: &BTreeMap<ProviderReadOccurrenceId, FrozenReadEncoding>,
    writes: &BTreeMap<WriteTargetOrdinal, ConnectorWriteRecipeDraft>,
    statement_constant_policy: ConstantPolicy,
    admission: &FragmentPackageAdmission,
    limits: &PackageEncodeLimits,
    control: &dyn PureCompileControl,
) -> Result<BTreeMap<FragmentId, Vec<u8>>, PackageFreezeError> {
    let packages = extract_checked_packages(
        candidate,
        encodings,
        writes,
        statement_constant_policy,
        admission,
        control,
    )?;
    let mut output = BTreeMap::new();
    for (id, package) in packages {
        output.insert(id, encode_checked_package(&package, limits, control)?);
    }
    Ok(output)
}

/// Every fragment's checked v2 package, before encoding. The extraction law
/// routes each writer's recipe by its target ordinal and refuses a missing or
/// unused one.
pub(crate) fn extract_checked_packages(
    candidate: &CompletedPhysicalPlanCandidate,
    encodings: &BTreeMap<ProviderReadOccurrenceId, FrozenReadEncoding>,
    writes: &BTreeMap<WriteTargetOrdinal, ConnectorWriteRecipeDraft>,
    statement_constant_policy: ConstantPolicy,
    admission: &FragmentPackageAdmission,
    control: &dyn PureCompileControl,
) -> Result<BTreeMap<FragmentId, FragmentPackage>, PackageFreezeError> {
    let plan = candidate.plan();
    let scans = author_frozen_reads(plan, encodings, control)?;
    let semantics = candidate
        .author_package_semantics(statement_constant_policy, control)
        .map_err(|error| match error {
            novarocks_sql::compiler::PackageSemanticsError::Control(cause) => {
                PackageFreezeError::Control(cause)
            }
            other => PackageFreezeError::Semantics(other),
        })?;
    let mut uses = BTreeMap::new();
    let mut calls = BTreeMap::new();
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for (id, authored) in semantics {
        uses.insert(id, authored.expression_uses);
        calls.insert(id, authored.calls);
        pruning.insert(id, authored.pruning);
        admissions.insert(id, *admission);
    }
    extract_fragment_packages(
        plan,
        &scans,
        writes,
        &uses,
        &calls,
        &pruning,
        &admissions,
        control,
    )
    .map_err(|error| match error {
        novarocks_physical_plan::FragmentPackageExtractionError::Control(cause) => {
            PackageFreezeError::Control(cause)
        }
        other => PackageFreezeError::Extraction(format!("{other:?}")),
    })
}

/// The exact wire bytes of one checked package.
pub(crate) fn encode_checked_package(
    package: &FragmentPackage,
    limits: &PackageEncodeLimits,
    control: &dyn PureCompileControl,
) -> Result<Vec<u8>, PackageFreezeError> {
    let wire = encode_fragment_package(package, limits, control).map_err(|error| match error {
        PackageEncodeError::Control(cause) => PackageFreezeError::Control(cause),
        other => PackageFreezeError::Encode(other),
    })?;
    Ok(wire.encode_to_vec())
}

#[cfg(test)]
mod error_projection_tests {
    use super::*;
    use novarocks_plan_codec::PhysicalEncodeError;
    use novarocks_sql::compiler::PackageSemanticsError;

    #[test]
    fn package_freeze_preserves_only_typed_semantic_capability_refusals() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        for original in [
            PackageSemanticsError::UnsupportedCapability("cast List<Int8> to List<Int64>".into()),
            PackageSemanticsError::Effects("UnsupportedCast diagnostic".into()),
        ] {
            let expected = original.failure_fact();
            let error = PhysicalEncodeError::from(PackageFreezeError::Semantics(original));
            assert_eq!(error.failure_fact(), expected);
            let PhysicalEncodeError::Source(source) = error else {
                panic!("retain the actual freeze author")
            };
            let Some(PackageFreezeError::Semantics(original)) =
                source.downcast_ref::<PackageFreezeError>()
            else {
                panic!("retain the nested SQL semantics author")
            };
            assert_eq!(original.failure_fact(), expected);
            assert_eq!(
                matches!(original, PackageSemanticsError::UnsupportedCapability(_)),
                expected.class() == FailureClass::Unsupported
            );
        }
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            assert_eq!(
                PhysicalEncodeError::from(PackageFreezeError::Control(cause.clone())),
                PhysicalEncodeError::Control(cause)
            );
        }
    }
    #[test]
    fn package_freeze_keeps_comparison_source_identity_fact_and_detail() {
        use novarocks_failure_contract::{FailureClass, FailureProjection, SharedFailureCause};
        use novarocks_functions::ComparisonPrepareError;
        use std::error::Error;
        for (original, class) in [
            (
                ComparisonPrepareError::Unsupported,
                FailureClass::Unsupported,
            ),
            (
                ComparisonPrepareError::TypeMismatch,
                FailureClass::Invariant,
            ),
        ] {
            let source = SharedFailureCause::new(original.clone());
            let expected = original.failure_fact();
            let error = PhysicalEncodeError::from(PackageFreezeError::Semantics(
                PackageSemanticsError::PrimitiveSource {
                    source: source.clone(),
                    cast: None,
                },
            ));
            assert_eq!(error.failure_fact(), expected);
            let PhysicalEncodeError::Source(outer) = &error else {
                panic!("freeze must retain its actual author");
            };
            let freeze = outer.downcast_ref::<PackageFreezeError>().unwrap();
            let PackageFreezeError::Semantics(PackageSemanticsError::PrimitiveSource {
                source: actual,
                cast: None,
            }) = freeze
            else {
                panic!("comparison source identity was not retained");
            };
            assert_eq!(actual, &source);
            assert_eq!(actual.detail(), source.detail());
            assert_eq!(
                actual.downcast_ref::<ComparisonPrepareError>(),
                Some(&original)
            );
            assert_eq!(
                actual
                    .source()
                    .unwrap()
                    .downcast_ref::<ComparisonPrepareError>(),
                Some(&original)
            );
            assert_eq!(error.to_string(), source.detail());
            assert_eq!(error.failure_fact().class(), class);
        }
    }
}
