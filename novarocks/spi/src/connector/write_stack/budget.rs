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

//! The frontend-owned writer-handle budget.
//!
//! A logical writer handle is charged once, when the sealed plan first uses its
//! target ordinal. Copying that same canonical handle into more physical writer
//! placements does not charge again, because the copies describe the same
//! logical write and cause no additional provider planning.
//!
//! Only the frontend can own this budget: a backend decodes one carrier at a
//! time and can never reconstruct the query-wide unique set, so it re-verifies
//! the single-handle cap and nothing more.

use std::collections::BTreeSet;

use crate::connector::write_stack::limits::{
    MAX_CONNECTOR_UNIQUE_WRITER_HANDLE_BYTES, MAX_CONNECTOR_WRITER_HANDLE_BYTES,
};
use crate::connector::write_stack::target::WriteTargetOrdinal;
use crate::connector::{ConnectorError, ConnectorErrorKind};

/// Verify one canonical writer-handle encoding against the single-handle cap.
///
/// Both the frontend at codec egress and the backend at carrier ingress call
/// this; the backend deliberately calls only this and never the unique-set
/// total.
pub fn validate_writer_handle_bytes(encoded_len: usize) -> Result<(), ConnectorError> {
    if encoded_len > MAX_CONNECTOR_WRITER_HANDLE_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector writer handle exceeds the frozen single-handle budget",
        ));
    }
    Ok(())
}

/// Frontend-only accounting of every unique logical writer handle in one query.
#[derive(Clone, Debug, Default)]
pub struct UniqueWriterHandleLedger {
    charged: BTreeSet<WriteTargetOrdinal>,
    bytes: usize,
}

impl UniqueWriterHandleLedger {
    pub fn new() -> Self {
        Self::default()
    }

    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn unique_handles(&self) -> usize {
        self.charged.len()
    }

    /// Charge `target`'s canonical handle. A repeat of an already-charged
    /// target is a physical copy: it is accepted and costs nothing.
    pub fn charge(
        &mut self,
        target: WriteTargetOrdinal,
        encoded_len: usize,
    ) -> Result<(), ConnectorError> {
        validate_writer_handle_bytes(encoded_len)?;
        if self.charged.contains(&target) {
            return Ok(());
        }
        let bytes = self.bytes.checked_add(encoded_len).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector unique writer handle byte total overflowed",
            )
        })?;
        if bytes > MAX_CONNECTOR_UNIQUE_WRITER_HANDLE_BYTES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector query exceeds the frozen unique writer handle budget",
            ));
        }
        self.bytes = bytes;
        self.charged.insert(target);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(value: u32) -> WriteTargetOrdinal {
        WriteTargetOrdinal::try_new(value).expect("bounded ordinal")
    }

    #[test]
    fn single_handle_cap_is_inclusive() {
        assert!(validate_writer_handle_bytes(MAX_CONNECTOR_WRITER_HANDLE_BYTES).is_ok());
        assert_eq!(
            validate_writer_handle_bytes(MAX_CONNECTOR_WRITER_HANDLE_BYTES + 1)
                .expect_err("single handle cap")
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn physical_copies_of_one_target_are_not_charged_twice() {
        let mut ledger = UniqueWriterHandleLedger::new();
        ledger.charge(target(0), 1024).expect("first copy");
        ledger.charge(target(0), 1024).expect("second copy");
        ledger.charge(target(0), 1024).expect("third copy");
        assert_eq!(ledger.bytes(), 1024);
        assert_eq!(ledger.unique_handles(), 1);
    }

    #[test]
    fn distinct_targets_accumulate_and_the_unique_total_is_enforced() {
        let mut ledger = UniqueWriterHandleLedger::new();
        let per_target = MAX_CONNECTOR_WRITER_HANDLE_BYTES;
        let fits = MAX_CONNECTOR_UNIQUE_WRITER_HANDLE_BYTES / per_target;
        for index in 0..fits {
            let ordinal = u32::try_from(index).expect("bounded");
            ledger
                .charge(target(ordinal), per_target)
                .expect("within the unique budget");
        }
        assert_eq!(ledger.bytes(), MAX_CONNECTOR_UNIQUE_WRITER_HANDLE_BYTES);
        let over = u32::try_from(fits).expect("bounded");
        assert_eq!(
            ledger
                .charge(target(over), 1)
                .expect_err("unique budget")
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(ledger.unique_handles(), fits);
    }
}
