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

//! The frozen writer recipes a compiled package carries.
//!
//! A package states each written target's writer as one frozen recipe draft:
//! the exact write binding, the provider's handle payload and the input shape
//! the provider signed. The write session that sealed the statement's targets
//! holds all three -- each target's plan and the handle encoder the plan's own
//! handle came from -- so this joins the plan's written targets with that
//! session and defaults nothing. A written target the session did not seal is
//! a refusal; a sealed target the plan does not write belongs to another of
//! the statement's queries and is skipped, exactly as for the plan tree.
//!
//! What it authors is checked again by owners that do not trust it: the draft
//! constructor law, the package validator (the payload is the plan's handle,
//! field by field against the plan's target fields) and the provider's pure
//! compiler.

use std::collections::BTreeMap;

use novarocks_connector_contract::ConnectorWriteRecipeDraft;
use novarocks_physical_plan::{NodeKind, PhysicalPlan, WriteTargetOrdinal};
use novarocks_spi::connector::ConnectorEncodedPayload;
use novarocks_spi::connector::write_stack::{ConnectorWriteTargetPlan, ConnectorWriterHandle};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

use crate::query_execution::package_freeze::PackageFreezeError;
use crate::query_execution::write_session::ConnectorWriteSession;

/// The write session a completed plan's targets were sealed by, as the
/// compiled carrier reads it: each sealed target's plan, and the handle
/// encoder the plan's writer handle payloads came from.
pub(crate) trait WriteRecipeSession {
    fn write_targets(&self) -> &[ConnectorWriteTargetPlan];

    fn writer_handle_payload(
        &self,
        handle: &ConnectorWriterHandle,
    ) -> Result<ConnectorEncodedPayload, String>;
}

impl WriteRecipeSession for ConnectorWriteSession {
    fn write_targets(&self) -> &[ConnectorWriteTargetPlan] {
        self.targets()
    }

    fn writer_handle_payload(
        &self,
        handle: &ConnectorWriterHandle,
    ) -> Result<ConnectorEncodedPayload, String> {
        self.encode_writer_handle_payload(handle)
            .map_err(|error| error.to_string())
    }
}

/// Every written target's frozen recipe draft, keyed by the target ordinal
/// the plan's writers name.
pub(crate) fn author_frozen_writes(
    plan: &PhysicalPlan,
    session: &dyn WriteRecipeSession,
    control: &dyn PureCompileControl,
) -> Result<BTreeMap<WriteTargetOrdinal, ConnectorWriteRecipeDraft>, PackageFreezeError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)
        .map_err(PackageFreezeError::Control)?;
    let result = author_all(plan, session, &mut work);
    // A primary interruption is final; an ordinary refusal still observes its
    // completed tail before it is reported.
    if matches!(&result, Err(PackageFreezeError::Control(_))) {
        return result;
    }
    work.finish().map_err(PackageFreezeError::Control)?;
    result
}

fn author_all(
    plan: &PhysicalPlan,
    session: &dyn WriteRecipeSession,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BTreeMap<WriteTargetOrdinal, ConnectorWriteRecipeDraft>, PackageFreezeError> {
    let mut writes = BTreeMap::new();
    for fragment in plan.fragments().values() {
        for node in fragment.nodes().values() {
            work.step().map_err(PackageFreezeError::Control)?;
            let NodeKind::TableWriter { target } = &node.kind else {
                continue;
            };
            let ordinal = target.write_target_ordinal;
            // One target may be written from more than one node; every one of
            // them writes the same sealed handle, so it is authored once.
            if writes.contains_key(&ordinal) {
                continue;
            }
            let sealed = session
                .write_targets()
                .iter()
                .find(|sealed| sealed.ordinal() == ordinal)
                .ok_or_else(|| {
                    PackageFreezeError::Facts(format!(
                        "table writer node {} writes target {} the write session did not seal",
                        node.id.get(),
                        ordinal.get()
                    ))
                })?;
            let refused = |detail: String| {
                PackageFreezeError::Write(format!("write target {}: {detail}", ordinal.get()))
            };
            work.flush().map_err(PackageFreezeError::Control)?;
            let payload = session
                .writer_handle_payload(sealed.handle())
                .map_err(|error| refused(format!("writer handle payload: {error}")))?;
            work.flush().map_err(PackageFreezeError::Control)?;
            let draft = ConnectorWriteRecipeDraft::try_new(
                sealed.handle().binding().clone(),
                payload,
                sealed.input().clone(),
            )
            .map_err(|error| refused(format!("writer recipe: {error}")))?;
            work.flush().map_err(PackageFreezeError::Control)?;
            writes.insert(ordinal, draft);
        }
    }
    Ok(writes)
}

#[cfg(test)]
pub(crate) mod tests;
