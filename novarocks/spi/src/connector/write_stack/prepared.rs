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

//! The bounded, complete result of one distributed write's data plane.
//!
//! A prepared write set is complete or it does not exist. There is no partial
//! set, no truncation, no spill, and no "commit what arrived": the frontend may
//! call the provider's finish only with a set that the execution graph closed.
//!
//! Two forms of the same facts exist, and both use the ledger in this module:
//!
//! * while aggregating, the root backend holds canonical encoded bytes it never
//!   interprets;
//! * after the frontend decodes those bytes on its exact control binding, it
//!   holds [`ConnectorPreparedWriteSet`], whose fragments are provider values.

use crate::connector::write_stack::limits::{
    MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES, MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES,
    MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES,
};
use crate::connector::write_stack::runtime::ConnectorCommitFragment;
use crate::connector::write_stack::target::WriteTargetOrdinal;
use crate::connector::{ConnectorError, ConnectorErrorKind};

/// Running budget for one prepared write set.
///
/// Both a byte budget and an entry budget are enforced, because the byte budget
/// alone cannot see the fixed per-entry bookkeeping a large number of tiny
/// fragments would cost.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PreparedWriteSetLedger {
    bytes: usize,
    entries: usize,
}

impl PreparedWriteSetLedger {
    pub const fn new() -> Self {
        Self {
            bytes: 0,
            entries: 0,
        }
    }

    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    pub const fn entries(&self) -> usize {
        self.entries
    }

    /// Charge one canonical commit fragment. Reaching a bound is legal;
    /// exceeding it is `ResourceExhausted` and never a truncation.
    pub fn reserve_fragment(&mut self, fragment_bytes: usize) -> Result<(), ConnectorError> {
        if fragment_bytes > MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector commit fragment exceeds the frozen single-fragment budget",
            ));
        }
        let entries = self.entries.checked_add(1).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector prepared write set entry count overflowed",
            )
        })?;
        if entries > MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector prepared write set exceeds the frozen entry budget",
            ));
        }
        let bytes = self.bytes.checked_add(fragment_bytes).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector prepared write set byte total overflowed",
            )
        })?;
        if bytes > MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector prepared write set exceeds the frozen byte budget",
            ));
        }
        self.bytes = bytes;
        self.entries = entries;
        Ok(())
    }
}

/// Checked accumulation of the rows a distributed write actually accepted.
///
/// Overflow is a contract error, never a saturating counter: a wrong affected
/// row count would be reported to the SQL client as truth.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriteRowCountAccumulator(u64);

impl WriteRowCountAccumulator {
    pub const fn new() -> Self {
        Self(0)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn add(&mut self, rows: u64) -> Result<(), ConnectorError> {
        self.0 = self.0.checked_add(rows).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector write row count overflowed",
            )
        })?;
        Ok(())
    }
}

/// The complete, immutable set the frontend hands to a provider's finish.
#[derive(Debug)]
pub struct ConnectorPreparedWriteSet {
    row_count: u64,
    fragments: Vec<(WriteTargetOrdinal, ConnectorCommitFragment)>,
}

impl ConnectorPreparedWriteSet {
    /// Build a prepared set from an already-complete aggregation.
    ///
    /// `expected_targets` is the sealed logical target set from the begin
    /// session. A fragment naming a target outside it means the plan and the
    /// session disagree, so it fails closed rather than committing a partial or
    /// foreign artifact.
    pub fn try_new(
        row_count: u64,
        fragments: Vec<(WriteTargetOrdinal, ConnectorCommitFragment)>,
        expected_targets: &[WriteTargetOrdinal],
    ) -> Result<Self, ConnectorError> {
        crate::connector::write_stack::target::validate_dense_target_ordinals(expected_targets)?;
        if fragments.len() > MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector prepared write set exceeds the frozen entry budget",
            ));
        }
        let highest = expected_targets
            .iter()
            .map(|target| target.get())
            .max()
            .unwrap_or_default();
        for (target, _) in &fragments {
            if target.get() > highest {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector commit fragment names a write target outside the sealed set",
                ));
            }
        }
        Ok(Self {
            row_count,
            fragments,
        })
    }

    /// The checked total of rows every writer accepted. It becomes the SQL
    /// statement's affected row count only after external commit is known to
    /// have succeeded.
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    pub fn fragments(&self) -> &[(WriteTargetOrdinal, ConnectorCommitFragment)] {
        &self.fragments
    }

    pub fn into_fragments(self) -> Vec<(WriteTargetOrdinal, ConnectorCommitFragment)> {
        self.fragments
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ledger_accepts_the_exact_bounds_and_rejects_one_more() {
        let mut ledger = PreparedWriteSetLedger::new();
        assert!(
            ledger
                .reserve_fragment(MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES)
                .is_ok()
        );
        assert_eq!(ledger.bytes(), MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES);
        assert_eq!(ledger.entries(), 1);

        let mut over = PreparedWriteSetLedger::new();
        assert_eq!(
            over.reserve_fragment(MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES + 1)
                .expect_err("single fragment budget")
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(over, PreparedWriteSetLedger::new());
    }

    #[test]
    fn ledger_rejects_the_set_byte_budget() {
        let mut ledger = PreparedWriteSetLedger::new();
        let full = MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES / MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES;
        for _ in 0..full {
            ledger
                .reserve_fragment(MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES)
                .expect("within the set byte budget");
        }
        assert_eq!(ledger.bytes(), MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES);
        assert_eq!(
            ledger
                .reserve_fragment(1)
                .expect_err("set byte budget")
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn ledger_rejects_the_entry_budget_before_the_byte_budget() {
        let mut ledger = PreparedWriteSetLedger::new();
        for _ in 0..MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES {
            ledger.reserve_fragment(0).expect("within the entry budget");
        }
        assert_eq!(ledger.entries(), MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES);
        assert_eq!(ledger.bytes(), 0);
        assert_eq!(
            ledger.reserve_fragment(0).expect_err("entry budget").kind(),
            ConnectorErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn row_count_overflow_is_a_contract_error_not_a_saturating_counter() {
        let mut rows = WriteRowCountAccumulator::new();
        rows.add(u64::MAX).expect("first add");
        assert_eq!(
            rows.add(1).expect_err("overflow").kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(rows.get(), u64::MAX);
    }

    #[test]
    fn prepared_set_rejects_a_fragment_outside_the_sealed_target_set() {
        let targets = [WriteTargetOrdinal::try_new(0).expect("bounded")];
        assert!(ConnectorPreparedWriteSet::try_new(0, Vec::new(), &targets).is_ok());
        assert!(ConnectorPreparedWriteSet::try_new(0, Vec::new(), &[]).is_err());
    }
}
