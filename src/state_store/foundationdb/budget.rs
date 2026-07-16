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

use crate::state_store::{Precondition, StateStoreError, StateStoreErrorKind, StateStoreLimits};

const RECORD_TAG_BYTES: usize = 1;
const RECORD_ENVELOPE_BYTES: usize = 1 + 16;
const CHANGE_TAG_BYTES: usize = 1;
const REVISION_BYTES: usize = 10;
const SEQUENCE_BYTES: usize = 4;
const VERSIONSTAMP_TRAILER_BYTES: usize = 4;
const COMMIT_TAG_BYTES: usize = 1;
const TRANSACTION_ID_BYTES: usize = 16;
const PENDING_VALUE_BYTES: usize = 1 + 16;
const COMMITTED_VALUE_OPERAND_BYTES: usize = 1 + REVISION_BYTES + VERSIONSTAMP_TRAILER_BYTES;
const HIGH_WATERMARK_FIELD_BYTES: usize = 2;
const HIGH_WATERMARK_OPERAND_BYTES: usize = REVISION_BYTES + VERSIONSTAMP_TRAILER_BYTES;

#[derive(Clone, Debug)]
pub(super) struct TransactionBudget {
    operations: usize,
    bytes: usize,
    limits: StateStoreLimits,
    root_len: usize,
}

impl TransactionBudget {
    pub(super) fn new(limits: StateStoreLimits, root_len: usize) -> Result<Self, StateStoreError> {
        let bytes = fixed_envelope_bytes(root_len)?;
        if bytes > limits.max_transaction_bytes {
            return Err(limit_error("transaction byte limit exceeded"));
        }
        Ok(Self {
            operations: 0,
            bytes,
            limits,
            root_len,
        })
    }

    pub(super) fn stage_put(
        &mut self,
        key: &[u8],
        value: &[u8],
        precondition: &Precondition,
    ) -> Result<u64, StateStoreError> {
        self.stage(accounted_put_bytes(
            self.root_len,
            key.len(),
            value.len(),
            precondition,
        )?)
    }

    pub(super) fn stage_delete(
        &mut self,
        key: &[u8],
        precondition: &Precondition,
    ) -> Result<u64, StateStoreError> {
        self.stage(accounted_delete_bytes(
            self.root_len,
            key.len(),
            precondition,
        )?)
    }

    pub(super) fn charge_get_conflict(&mut self, key_len: usize) -> Result<(), StateStoreError> {
        let physical_key = checked_add(checked_add(self.root_len, RECORD_TAG_BYTES)?, key_len)?;
        self.charge_bytes(checked_mul(physical_key, 2)?)
    }

    pub(super) fn charge_range_conflict(
        &mut self,
        start_len: usize,
        end_len: usize,
    ) -> Result<(), StateStoreError> {
        let start = checked_add(checked_add(self.root_len, RECORD_TAG_BYTES)?, start_len)?;
        let end = checked_add(checked_add(self.root_len, RECORD_TAG_BYTES)?, end_len)?;
        self.charge_bytes(checked_add(start, end)?)
    }

    fn stage(&mut self, increment: usize) -> Result<u64, StateStoreError> {
        let operations = self
            .operations
            .checked_add(1)
            .ok_or_else(|| limit_error("transaction operation limit exceeded"))?;
        if operations > self.limits.max_transaction_operations {
            return Err(limit_error("transaction operation limit exceeded"));
        }
        let bytes = checked_add(self.bytes, increment)?;
        if bytes > self.limits.max_transaction_bytes {
            return Err(limit_error("transaction byte limit exceeded"));
        }
        self.operations = operations;
        self.bytes = bytes;
        u64::try_from(operations).map_err(|_| limit_error("transaction operation limit exceeded"))
    }

    fn charge_bytes(&mut self, increment: usize) -> Result<(), StateStoreError> {
        let bytes = checked_add(self.bytes, increment)?;
        if bytes > self.limits.max_transaction_bytes {
            return Err(limit_error("transaction byte limit exceeded"));
        }
        self.bytes = bytes;
        Ok(())
    }

    #[cfg(test)]
    fn accounted_bytes(&self) -> usize {
        self.bytes
    }
}

fn fixed_envelope_bytes(root_len: usize) -> Result<usize, StateStoreError> {
    let commit_key = checked_add(root_len, COMMIT_TAG_BYTES + TRANSACTION_ID_BYTES)?;
    let high_watermark_key = checked_add(root_len, HIGH_WATERMARK_FIELD_BYTES)?;
    let mut bytes = 0;
    // Reservation read/write plus exact read/write conflict endpoints.
    bytes = checked_add(bytes, commit_key)?;
    bytes = checked_add(bytes, PENDING_VALUE_BYTES)?;
    bytes = checked_add(bytes, checked_mul(commit_key, 4)?)?;
    // Data transaction terminal state and versionstamped high-watermark mutations.
    bytes = checked_add(bytes, commit_key)?;
    bytes = checked_add(bytes, COMMITTED_VALUE_OPERAND_BYTES)?;
    bytes = checked_add(bytes, checked_mul(commit_key, 2)?)?;
    bytes = checked_add(bytes, high_watermark_key)?;
    bytes = checked_add(bytes, HIGH_WATERMARK_OPERAND_BYTES)?;
    bytes = checked_add(bytes, checked_mul(high_watermark_key, 2)?)?;
    Ok(bytes)
}

fn accounted_put_bytes(
    root_len: usize,
    key_len: usize,
    value_len: usize,
    precondition: &Precondition,
) -> Result<usize, StateStoreError> {
    let record_key = checked_add(checked_add(root_len, RECORD_TAG_BYTES)?, key_len)?;
    let record_value = checked_add(RECORD_ENVELOPE_BYTES, value_len)?;
    let change_key_operand = checked_add(
        root_len,
        CHANGE_TAG_BYTES + REVISION_BYTES + SEQUENCE_BYTES + VERSIONSTAMP_TRAILER_BYTES,
    )?;
    let mut bytes = logical_request_bytes(key_len, value_len, precondition)?;
    bytes = checked_add(bytes, record_key)?;
    bytes = checked_add(bytes, record_value)?;
    bytes = checked_add(bytes, change_key_operand)?;
    bytes = checked_add(bytes, key_len)?;
    // Non-snapshot precondition read and ordinary set exact conflicts.
    bytes = checked_add(bytes, checked_mul(record_key, 4)?)?;
    Ok(bytes)
}

fn accounted_delete_bytes(
    root_len: usize,
    key_len: usize,
    precondition: &Precondition,
) -> Result<usize, StateStoreError> {
    let record_key = checked_add(checked_add(root_len, RECORD_TAG_BYTES)?, key_len)?;
    let change_key_operand = checked_add(
        root_len,
        CHANGE_TAG_BYTES + REVISION_BYTES + SEQUENCE_BYTES + VERSIONSTAMP_TRAILER_BYTES,
    )?;
    let mut bytes = logical_request_bytes(key_len, 0, precondition)?;
    bytes = checked_add(bytes, record_key)?;
    bytes = checked_add(bytes, change_key_operand)?;
    bytes = checked_add(bytes, key_len)?;
    bytes = checked_add(bytes, checked_mul(record_key, 4)?)?;
    Ok(bytes)
}

fn logical_request_bytes(
    key_len: usize,
    value_len: usize,
    precondition: &Precondition,
) -> Result<usize, StateStoreError> {
    let mut bytes = checked_add(1, key_len)?;
    bytes = checked_add(bytes, value_len)?;
    bytes = checked_add(bytes, 1)?;
    if let Precondition::Version(version) = precondition {
        bytes = checked_add(bytes, version.as_bytes().len())?;
    }
    Ok(bytes)
}

fn checked_add(left: usize, right: usize) -> Result<usize, StateStoreError> {
    left.checked_add(right)
        .ok_or_else(|| limit_error("transaction byte limit exceeded"))
}

fn checked_mul(left: usize, right: usize) -> Result<usize, StateStoreError> {
    left.checked_mul(right)
        .ok_or_else(|| limit_error("transaction byte limit exceeded"))
}

fn limit_error(message: &'static str) -> StateStoreError {
    StateStoreError::new(StateStoreErrorKind::LimitExceeded, message)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::state_store::{StateStoreLimitOverrides, VersionToken};

    fn limits(bytes: usize, operations: usize) -> StateStoreLimits {
        StateStoreLimits::from_overrides(&StateStoreLimitOverrides {
            max_transaction_bytes: Some(bytes),
            max_transaction_operations: Some(operations),
            ..Default::default()
        })
        .expect("test limits")
    }

    #[test]
    fn foundationdb_budget_includes_physical_envelopes() {
        let precondition = Precondition::Version(
            VersionToken::try_from(Bytes::from_static(b"persisted-version")).expect("version"),
        );
        let bytes = accounted_put_bytes(22, 3, 5, &precondition).expect("put accounting");
        assert!(bytes > 3 + 5 + b"persisted-version".len());
        assert!(fixed_envelope_bytes(22).expect("fixed accounting") > 0);
    }

    #[test]
    fn budget_is_charged_per_request_without_same_key_refunds() {
        let mut budget = TransactionBudget::new(limits(16 * 1024, 2), 22).expect("budget");
        let initial = budget.accounted_bytes();
        budget
            .stage_put(b"same", b"v1", &Precondition::Any)
            .expect("first put");
        let first = budget.accounted_bytes();
        budget
            .stage_put(b"same", b"v2", &Precondition::Any)
            .expect("second put");
        assert!(budget.accounted_bytes() > first);
        assert!(first > initial);
        assert_eq!(
            budget
                .stage_delete(b"same", &Precondition::Any)
                .expect_err("operation limit")
                .kind(),
            StateStoreErrorKind::LimitExceeded
        );
    }

    #[test]
    fn budget_rejects_logical_maximum_when_physical_envelope_does_not_fit() {
        let mut budget = TransactionBudget::new(limits(16 * 1024, 10), 22).expect("budget");
        let value = vec![0; 16 * 1024];
        assert_eq!(
            budget
                .stage_put(b"key", &value, &Precondition::Any)
                .expect_err("physical overhead exceeds limit")
                .kind(),
            StateStoreErrorKind::LimitExceeded
        );
    }
}
