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
//! The SQL owner authors each fragment's uses, calls and pruning; the
//! original extraction law publishes the checked packages; the v2 sender
//! encodes them. Provider read facts and writer recipes have no production
//! author on this path yet, so a plan with scans or writers is refused rather
//! than given guessed facts. This is not yet the production carrier.

use std::collections::BTreeMap;
use std::fmt;

use novarocks_physical_plan::{
    ConstantPolicy, FragmentId, FragmentPackageAdmission, NodeKind, extract_fragment_packages,
};
use novarocks_plan_codec::physical_package_v2::{
    PackageEncodeError, PackageEncodeLimits, encode_fragment_package,
};
use novarocks_query_application::preparation::CompletedPhysicalPlanCandidate;
use novarocks_type_contract::{CompileControlError, PureCompileControl};
use prost::Message;

#[derive(Debug)]
pub(crate) enum PackageFreezeError {
    Control(CompileControlError),
    Semantics(novarocks_sql::compiler::PackageSemanticsError),
    Extraction(String),
    Encode(PackageEncodeError),
    /// A fact the package needs has no production author on this path.
    Unsupported(&'static str),
}
impl fmt::Display for PackageFreezeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(cause) => cause.fmt(f),
            Self::Semantics(error) => error.fmt(f),
            Self::Extraction(detail) => write!(f, "fragment package extraction: {detail}"),
            Self::Encode(error) => error.fmt(f),
            Self::Unsupported(detail) => f.write_str(detail),
        }
    }
}
impl std::error::Error for PackageFreezeError {}

/// Every fragment's v2 package bytes. The host admission and encode limits
/// are caller-owned configuration; none is defaulted here.
pub(crate) fn freeze_fragment_packages(
    candidate: &CompletedPhysicalPlanCandidate,
    statement_constant_policy: ConstantPolicy,
    admission: &FragmentPackageAdmission,
    limits: &PackageEncodeLimits,
    control: &dyn PureCompileControl,
) -> Result<BTreeMap<FragmentId, Vec<u8>>, PackageFreezeError> {
    let plan = candidate.plan();
    for fragment in plan.fragments().values() {
        for node in fragment.nodes().values() {
            match node.kind {
                NodeKind::Scan { .. } => {
                    return Err(PackageFreezeError::Unsupported(
                        "provider read facts have no production package author yet",
                    ));
                }
                NodeKind::TableWriter { .. } | NodeKind::TableFinish(_) => {
                    return Err(PackageFreezeError::Unsupported(
                        "writer recipes have no production package author yet",
                    ));
                }
                _ => {}
            }
        }
    }
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
    let packages = extract_fragment_packages(
        plan,
        &BTreeMap::new(),
        &BTreeMap::new(),
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
    })?;
    let mut output = BTreeMap::new();
    for (id, package) in packages {
        let wire =
            encode_fragment_package(&package, limits, control).map_err(|error| match error {
                PackageEncodeError::Control(cause) => PackageFreezeError::Control(cause),
                other => PackageFreezeError::Encode(other),
            })?;
        output.insert(id, wire.encode_to_vec());
    }
    Ok(output)
}
