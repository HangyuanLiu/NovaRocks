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

//! Read negotiation: what a provider will take on, before anything is frozen.
//!
//! Negotiation and freezing are separate because they answer separate
//! questions. Negotiation asks what a provider *would* do with a read and is
//! pure, repeatable and free of commitment; freezing states what it *will* do
//! and happens once. Collapsing them into one call would make the answer
//! unavailable to anything that runs before the decision is final - a cost
//! model cannot ask a question it can only ask after the plan is fixed.
//!
//! A negotiation carries an ordered list of operations rather than one
//! operation per call. Order is significant: a limit means something different
//! above a filter than below it. The list is open by construction, so taking on
//! a new kind of pushdown later adds a variant rather than a call shape or a
//! new point in the pipeline where providers are consulted.

use crate::connector::ConnectorError;

use super::runtime::{ConnectorReadAssignment, ConnectorReadConstraint, ConnectorReadTableHandle};

/// One pushdown a caller offers to a provider.
///
/// Offering is not requiring: a provider may decline any operation, and
/// declining is a normal answer that leaves the work with the engine.
#[derive(Clone, Debug)]
pub enum ReadPushdownOp {
    /// Read only these columns, in this order. Repeating a column is legal:
    /// an output port may name one value more than once.
    Projection {
        assignments: Vec<ConnectorReadAssignment>,
    },
    /// Restrict rows to those satisfying the constraint.
    Filter { constraint: ConnectorReadConstraint },
    /// Return at most this many rows.
    Limit { rows: u64 },
}

impl ReadPushdownOp {
    /// Stable name for diagnostics and conformance messages.
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Projection { .. } => "projection",
            Self::Filter { .. } => "filter",
            Self::Limit { .. } => "limit",
        }
    }
}

/// What a provider took on for one offered operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadPushdownDisposition {
    /// The provider guarantees the operation. The engine must not repeat it,
    /// and repeating it would be observable for a limit.
    Exact,
    /// The provider uses the operation to skip work but does not guarantee it.
    /// The engine keeps its own evaluation; this is a performance answer, not a
    /// semantic one.
    PruningOnly,
    /// The provider declined. Responsibility stays entirely with the engine.
    /// This is a legitimate answer and never an error.
    Unsupported,
}

impl ReadPushdownDisposition {
    /// Whether the engine may stop doing this work itself.
    pub const fn relieves_engine(self) -> bool {
        matches!(self, Self::Exact)
    }
}

/// Outcome of one offered operation, in the order it was offered.
#[derive(Clone, Debug)]
pub struct ReadPushdownOutcome {
    pub disposition: ReadPushdownDisposition,
    /// What the engine must still evaluate itself.
    ///
    /// Present whenever the provider answered a filter at all, including when
    /// nothing remains: the disposition says whether the engine is relieved,
    /// and this says exactly of what. `None` means the provider declined, or
    /// the operation has no residual form - a limit is taken on or it is not.
    pub residual: Option<ConnectorReadConstraint>,
}

impl ReadPushdownOutcome {
    pub const fn declined() -> Self {
        Self {
            disposition: ReadPushdownDisposition::Unsupported,
            residual: None,
        }
    }

    pub const fn exact() -> Self {
        Self {
            disposition: ReadPushdownDisposition::Exact,
            residual: None,
        }
    }
}

/// One negotiation offer.
///
/// The handle is where the read stands now; the operations are what the caller
/// would like applied on top of it. Offering the same operations against the
/// same handle again must produce the same answer, which is what makes it safe
/// to ask before a decision is final.
#[derive(Clone, Debug)]
pub struct ReadNegotiation {
    pub handle: ConnectorReadTableHandle,
    pub ops: Vec<ReadPushdownOp>,
}

/// What the provider answered.
///
/// `handle` is the read as it now stands. When nothing was taken on it is the
/// handle that was offered, and `changed` is false - which a bounded
/// negotiation loop needs in order to stop, and which a provider must report
/// honestly rather than by returning a fresh but equivalent handle.
#[derive(Clone, Debug)]
pub struct ReadNegotiated {
    pub handle: ConnectorReadTableHandle,
    /// One outcome per offered operation, in the offered order.
    pub outcomes: Vec<ReadPushdownOutcome>,
    pub changed: bool,
}

impl ReadNegotiated {
    /// Nothing was taken on.
    pub fn unchanged(handle: ConnectorReadTableHandle, ops: usize) -> Self {
        Self {
            handle,
            outcomes: vec![ReadPushdownOutcome::declined(); ops],
            changed: false,
        }
    }

    /// Checks the shape a caller is entitled to assume before reading any
    /// outcome: one answer per offered operation.
    pub fn verify_shape(&self, offered: usize) -> Result<(), ConnectorError> {
        if self.outcomes.len() == offered {
            return Ok(());
        }
        Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::InvalidRequest,
            format!(
                "read negotiation answered {} of {offered} offered operations",
                self.outcomes.len()
            ),
        ))
    }
}
