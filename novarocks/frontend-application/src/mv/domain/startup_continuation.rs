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

//! Reopening this deployment's own targets after a restart.
//!
//! A restarted process closes management on every target its deployment owns
//! but a previous incarnation wrote. Retiring those barriers one operator
//! statement at a time does not scale, so the deployment may instead state
//! once, in a file, that the previous writer is isolated. This pass is what
//! turns that statement into readmissions.
//!
//! It is a pass rather than a startup step because neither input is ready at
//! any single moment. The statement can be written after this process is
//! already serving -- an orchestration that reads this incarnation out of
//! `novarocks_mv_management_status` first has to -- and the remote-effect
//! window measured from the declared isolation usually has not elapsed yet
//! when it arrives. So the pass runs on the maintenance tick, does what it
//! can, and leaves the rest closed.
//!
//! Nothing here decides what the old writer's effects did. A permit only
//! allows the exact re-observation that finds out.

use novarocks_mv_application::management::{
    ManagementClock, ManagementContinuationService, ManagementEntrance, StartupContinuation,
    StartupIsolationEvidence, SystemManagementClock,
};
use novarocks_spi::connector::{ConnectorControlResolver, ConnectorInstanceId};

use crate::mv::startup_isolation_file::StartupIsolationSource;

/// Everything one pass needs, owned by the caller.
pub(crate) struct StartupContinuationPass<'a> {
    pub(crate) entrance: &'a ManagementEntrance,
    pub(crate) continuation: &'a ManagementContinuationService,
    pub(crate) readiness: &'a crate::mv::domain::readiness::MvReadinessPort,
    pub(crate) connector_control: &'a dyn ConnectorControlResolver,
    pub(crate) source: &'a StartupIsolationSource,
}

/// What one pass did, as an operator needs to see it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct StartupContinuationReport {
    pub(crate) readmitted: usize,
    pub(crate) waiting: usize,
    pub(crate) operator_only: usize,
    pub(crate) not_covered: usize,
    pub(crate) failed: usize,
}

impl StartupContinuationReport {
    /// Whether this pass found anything the statement still has to act on. A
    /// pass that found nothing is the steady state and says nothing.
    pub(crate) const fn is_silent(&self) -> bool {
        self.readmitted == 0
            && self.waiting == 0
            && self.operator_only == 0
            && self.not_covered == 0
            && self.failed == 0
    }
}

/// Run one pass. An absent statement is the default and does nothing.
pub(crate) fn run_startup_continuation_pass(
    pass: &StartupContinuationPass<'_>,
) -> StartupContinuationReport {
    let clock = SystemManagementClock;
    let Some(evidence) = accept_current_statement(pass, &clock) else {
        return StartupContinuationReport::default();
    };
    let projections = match pass
        .readiness
        .candidate_reader()
        .list_candidate_definitions()
    {
        Ok(projections) => projections,
        Err(error) => {
            tracing::warn!(
                %error,
                "skipping MV startup continuation because the Accelerator is unavailable"
            );
            return StartupContinuationReport::default();
        }
    };
    let mut report = StartupContinuationReport::default();
    for projection in projections {
        let source = projection.facts.source_revision();
        match pass.continuation.resume_target_on_startup_isolation(
            &source.target,
            &evidence,
            &clock,
        ) {
            StartupContinuation::Nothing => {}
            StartupContinuation::NotCovered => report.not_covered += 1,
            StartupContinuation::OperatorOnly => report.operator_only += 1,
            StartupContinuation::Waiting { deadline } => {
                report.waiting += 1;
                tracing::debug!(
                    mv_target = %format!(
                        "{}.{}.{}",
                        source.target.instance_id.as_str(),
                        source.target.namespace,
                        source.target.table
                    ),
                    deadline = deadline.as_unix_millis(),
                    "MV startup continuation is waiting for the guaranteed remote window"
                );
            }
            StartupContinuation::Ready {
                previous_incarnation,
                permits,
            } => match readmit(pass, &source.target, previous_incarnation, permits) {
                Ok(()) => report.readmitted += 1,
                Err(error) => {
                    report.failed += 1;
                    tracing::warn!(
                        mv_target = %format!(
                            "{}.{}.{}",
                            source.target.instance_id.as_str(),
                            source.target.namespace,
                            source.target.table
                        ),
                        %error,
                        "MV startup continuation could not readmit a target its statement covers"
                    );
                }
            },
            StartupContinuation::Refused(error) => {
                report.failed += 1;
                tracing::warn!(
                    mv_target = %format!(
                        "{}.{}.{}",
                        source.target.instance_id.as_str(),
                        source.target.namespace,
                        source.target.table
                    ),
                    %error,
                    "MV startup continuation refused a target its statement named"
                );
            }
        }
    }
    report
}

/// Read and check the statement as it stands right now.
///
/// It is re-read every pass on purpose: the file may appear, be corrected, or
/// be replaced for a later launch, and a statement cached from the first pass
/// would outlive all three.
fn accept_current_statement(
    pass: &StartupContinuationPass<'_>,
    clock: &dyn ManagementClock,
) -> Option<StartupIsolationEvidence> {
    let declaration = match pass.source.read() {
        Ok(Some(declaration)) => declaration,
        Ok(None) => return None,
        Err(error) => {
            tracing::warn!(%error, "MV startup isolation evidence could not be read");
            return None;
        }
    };
    let now = match clock.now() {
        Ok(now) => now,
        Err(error) => {
            tracing::warn!(%error, "MV startup continuation could not read the clock");
            return None;
        }
    };
    match StartupIsolationEvidence::try_accept(
        declaration,
        pass.continuation.local_owner(),
        pass.continuation.local_incarnation(),
        pass.source.launch_nonce(),
        now,
    ) {
        Ok(evidence) => Some(evidence),
        Err(error) => {
            tracing::warn!(
                %error,
                path = %pass.source.path().display(),
                "MV startup isolation evidence does not apply to this process; management stays closed"
            );
            None
        }
    }
}

fn readmit(
    pass: &StartupContinuationPass<'_>,
    table: &novarocks_spi::connector::ConnectorTableIdentity,
    previous_incarnation: novarocks_mv_application::management::ProcessIncarnation,
    permits: Vec<novarocks_mv_application::management::ReadmissionPermit>,
) -> Result<(), String> {
    let instance_id = ConnectorInstanceId::parse(table.instance_id.as_str())
        .map_err(|error| format!("parse the continued MV catalog identity: {error}"))?;
    let lease = pass
        .connector_control
        .acquire_current(&instance_id)
        .map_err(|error| format!("acquire the continued MV catalog generation: {error}"))?;
    let catalog = lease
        .binding()
        .catalog_handle()
        .map_err(|error| format!("bind the continued MV catalog generation: {error}"))?
        .clone();
    crate::mv::domain::staged_create::readmit_declared_target(
        pass.entrance,
        pass.readiness,
        pass.connector_control,
        catalog,
        novarocks_mv_application::product::MvTarget::from_parts(
            Some(table.instance_id.as_str()),
            &table.namespace,
            &table.table,
        ),
        uuid::Uuid::now_v7(),
        previous_incarnation,
        permits,
        // The old writer's effects are settled by the deployment's statement,
        // not by this process; observe under a scope that cannot be mistaken
        // for part of one of them.
        crate::connector::connector_request_context(
            None,
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
        )?
        .after_external_effect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pass_that_found_nothing_says_nothing() {
        assert!(StartupContinuationReport::default().is_silent());
        assert!(
            !StartupContinuationReport {
                waiting: 1,
                ..StartupContinuationReport::default()
            }
            .is_silent()
        );
    }
}
