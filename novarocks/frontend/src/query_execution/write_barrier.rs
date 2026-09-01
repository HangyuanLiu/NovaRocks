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

//! The gate an external write commit has to pass.
//!
//! Two facts must hold, and neither implies the other:
//!
//! * **The prepared write set is complete.** Proved by reading the root
//!   result to `Eof`. It says the write data plane closed -- every writer
//!   finished, every sender reached EOS, the finish node emitted, and the
//!   frontend received all of it. It says nothing about whether some other
//!   participant failed.
//! * **Execution succeeded.** Proved by the lifecycle terminal set. It says
//!   every participant of this exact attempt terminated successfully. Since
//!   the lifecycle no longer carries staged artifacts, it says nothing about
//!   whether the frontend actually received the write data.
//!
//! Before this split, one signal stood for both, and a query could reach a
//! commit on the strength of half the evidence. Keeping them separate is the
//! point of this type: the commit call site cannot compile without checking
//! both, and neither can be quietly substituted for the other.
//!
//! Cancellation and deadline are a third, independent veto. They do not prove
//! anything happened; they only forbid starting an external effect.

use crate::query_execution::write_result::DecodedPreparedWriteSet;

/// Why a write may not commit yet, or at all.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WriteCommitBlocked {
    /// The root result never reached end of stream, so no complete set exists.
    PreparedWriteSetIncomplete,
    /// At least one participant of this attempt did not succeed.
    ExecutionDidNotSucceed,
    /// The statement was cancelled or its deadline expired before commit.
    Cancelled,
}

impl WriteCommitBlocked {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::PreparedWriteSetIncomplete => {
                "connector write did not receive a complete prepared write set"
            }
            Self::ExecutionDidNotSucceed => {
                "connector write execution did not succeed on every participant"
            }
            Self::Cancelled => "connector write was cancelled before its external commit",
        }
    }
}

/// Accumulates the independent facts and answers one question.
#[derive(Debug, Default)]
pub(crate) struct WriteCommitBarrier {
    prepared: Option<DecodedPreparedWriteSet>,
    execution_succeeded: bool,
    cancelled: bool,
}

impl WriteCommitBarrier {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Record the complete set.
    ///
    /// The caller must have observed `Eof`; that is why this takes an already
    /// finished set rather than accumulating rows itself. A prefix cannot
    /// reach this method.
    pub(crate) fn observe_prepared_write_set(&mut self, prepared: DecodedPreparedWriteSet) {
        self.prepared = Some(prepared);
    }

    /// Record whether every participant of the exact attempt succeeded.
    pub(crate) const fn observe_execution_terminals(&mut self, succeeded: bool) {
        self.execution_succeeded = succeeded;
    }

    pub(crate) const fn observe_cancelled(&mut self) {
        self.cancelled = true;
    }

    /// Consume the barrier, yielding the set only when every fact holds.
    ///
    /// The order the facts arrived in does not matter; what matters is that
    /// all of them did.
    pub(crate) fn into_committable(self) -> Result<DecodedPreparedWriteSet, WriteCommitBlocked> {
        if self.cancelled {
            return Err(WriteCommitBlocked::Cancelled);
        }
        if !self.execution_succeeded {
            return Err(WriteCommitBlocked::ExecutionDidNotSucceed);
        }
        self.prepared
            .ok_or(WriteCommitBlocked::PreparedWriteSetIncomplete)
    }
}

#[cfg(test)]
mod tests {
    use novarocks_spi::connector::write_stack::WriteTargetOrdinal;

    use super::*;

    fn complete_set() -> DecodedPreparedWriteSet {
        DecodedPreparedWriteSet::for_test(
            7,
            vec![(
                WriteTargetOrdinal::try_new(0).expect("bounded ordinal"),
                vec![1, 2, 3],
            )],
        )
    }

    #[test]
    fn both_facts_together_open_the_gate_in_either_arrival_order() {
        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_prepared_write_set(complete_set());
        barrier.observe_execution_terminals(true);
        assert_eq!(
            barrier.into_committable().expect("committable").row_count(),
            7
        );

        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_execution_terminals(true);
        barrier.observe_prepared_write_set(complete_set());
        assert_eq!(
            barrier.into_committable().expect("committable").row_count(),
            7
        );
    }

    #[test]
    fn a_complete_set_does_not_stand_in_for_a_successful_execution() {
        // The data plane closed, but some other participant failed. Committing
        // here would publish a snapshot for a query that did not succeed.
        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_prepared_write_set(complete_set());
        barrier.observe_execution_terminals(false);
        assert_eq!(
            barrier.into_committable().expect_err("must not commit"),
            WriteCommitBlocked::ExecutionDidNotSucceed
        );
    }

    #[test]
    fn a_successful_execution_does_not_stand_in_for_a_complete_set() {
        // Every participant terminated successfully, but the frontend never
        // read the root result to end of stream. The lifecycle no longer
        // carries the artifacts, so success alone proves nothing about them.
        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_execution_terminals(true);
        assert_eq!(
            barrier.into_committable().expect_err("must not commit"),
            WriteCommitBlocked::PreparedWriteSetIncomplete
        );
    }

    #[test]
    fn cancellation_vetoes_a_write_that_otherwise_had_both_facts() {
        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_prepared_write_set(complete_set());
        barrier.observe_execution_terminals(true);
        barrier.observe_cancelled();
        assert_eq!(
            barrier.into_committable().expect_err("must not commit"),
            WriteCommitBlocked::Cancelled
        );
    }

    #[test]
    fn a_barrier_that_learned_nothing_refuses() {
        assert_eq!(
            WriteCommitBarrier::new()
                .into_committable()
                .expect_err("must not commit"),
            WriteCommitBlocked::ExecutionDidNotSucceed
        );
    }
}
