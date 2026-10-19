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
//! Which carrier a Frontend freezes its plans into is one composition choice,
//! [`StaticPlanCarrier`]. Production composes the plan tree; the compiled
//! package carrier is selected only by a composition that also runs a
//! compiled-package backend.

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

/// The static carrier one Frontend process freezes every completed plan into.
///
/// A process interprets exactly one carrier: the composition chooses it once,
/// before any statement is admitted, and every owner that encodes a completed
/// plan receives the same choice beside its function catalog. A plan the
/// chosen carrier cannot express is refused; it never falls back to the other
/// carrier, because a backend interprets only the carrier its own composition
/// chose.
#[derive(Clone, Copy, Debug)]
pub enum StaticPlanCarrier {
    /// The production native plan tree (`FrozenFragment` fields 1-5).
    PlanTree,
    /// One v2 `FragmentPackage` per fragment, carried as
    /// `FrozenFragment.package` with no plan-tree field beside it.
    CompiledPackage(CompiledPackageCarrier),
}

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
impl std::error::Error for PackageFreezeError {}

/// A completed plan the compiled carrier cannot freeze is refused through the
/// encoder's own error, keeping a caller's control cause primary.
impl From<PackageFreezeError> for novarocks_plan_codec::PhysicalEncodeError {
    fn from(error: PackageFreezeError) -> Self {
        match error {
            PackageFreezeError::Control(cause) => Self::Control(cause),
            other => Self::Invalid(format!("compiled package carrier: {other}")),
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
        admissions.insert(id, admission.clone());
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
