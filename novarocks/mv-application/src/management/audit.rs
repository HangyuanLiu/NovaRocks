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

//! The record an operator's management declaration leaves behind.
//!
//! A declaration is a human statement that something outside this process is
//! true, and it is the only thing that can reopen a management entrance whose
//! effect outcome was lost. Nobody can check such a statement afterwards
//! unless it was written down, so it is written down first: a declaration that
//! could not be recorded does not take effect.
//!
//! This is deliberately not a correctness ledger. Nothing reads it back to
//! decide anything, no recovery replays it, and losing it cannot corrupt
//! state -- it can only cost a deployment the ability to explain what someone
//! did.

use std::fmt;

use novarocks_spi::connector::ConnectorTableIdentity;

use super::{DeploymentOwner, ProcessIncarnation, ReadmissionChallenge};

/// The largest a single recorded field may be, so one statement cannot fill
/// the record.
pub const MAX_MANAGEMENT_AUDIT_FIELD_BYTES: usize = 4096;

/// What an operator asked this process to do.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagementAuditAction {
    ResumeManagement,
    SetOwner,
}

impl ManagementAuditAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ResumeManagement => "resume_management",
            Self::SetOwner => "set_owner",
        }
    }
}

/// How it ended.
///
/// An attempt is recorded before it runs and its outcome after, because a
/// declaration that was accepted and then failed is exactly the case an
/// operator later needs to see.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ManagementAuditOutcome {
    Attempted,
    Applied,
    Refused(String),
}

impl fmt::Display for ManagementAuditOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Attempted => formatter.write_str("attempted"),
            Self::Applied => formatter.write_str("applied"),
            Self::Refused(reason) => write!(formatter, "refused: {reason}"),
        }
    }
}

/// One complete management declaration, as it happened.
///
/// The session principal is what the server authenticated; the operator
/// reference is what the caller said about themselves. They are separate
/// fields because a statement about who is acting is a claim, and a claim must
/// not be able to impersonate the identity the server established.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagementAuditRecord {
    pub action: ManagementAuditAction,
    pub session_principal: String,
    pub operator_reference: String,
    pub table: ConnectorTableIdentity,
    pub local_owner: DeploymentOwner,
    pub local_incarnation: ProcessIncarnation,
    pub declared_old_incarnation: Option<ProcessIncarnation>,
    pub declared_new_owner: Option<DeploymentOwner>,
    pub challenge: ReadmissionChallenge,
    pub evidence_reference: String,
    pub outcome: ManagementAuditOutcome,
}

impl ManagementAuditRecord {
    /// Refuse a record whose operator-supplied text exceeds its budget, before
    /// it can reach a sink.
    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("session principal", self.session_principal.as_str()),
            ("operator reference", self.operator_reference.as_str()),
            ("evidence reference", self.evidence_reference.as_str()),
        ] {
            if value.is_empty() {
                return Err(format!("management audit {name} must not be empty"));
            }
            if value.len() > MAX_MANAGEMENT_AUDIT_FIELD_BYTES {
                return Err(format!(
                    "management audit {name} exceeds the {MAX_MANAGEMENT_AUDIT_FIELD_BYTES}-byte limit"
                ));
            }
        }
        Ok(())
    }
}

/// Where management declarations are written.
///
/// An implementation must have durably accepted the record before it returns
/// success: the caller treats a successful write as permission to act, and a
/// buffered record that a crash discards would leave an effect nobody can
/// explain.
pub trait ManagementAuditSink: Send + Sync {
    fn record(&self, record: &ManagementAuditRecord) -> Result<(), String>;
}
