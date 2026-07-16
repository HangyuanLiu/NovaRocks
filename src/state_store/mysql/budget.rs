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

pub(super) struct TransactionBudget {
    limits: StateStoreLimits,
    operations: usize,
    bytes: usize,
}

impl TransactionBudget {
    pub(super) fn new(limits: StateStoreLimits) -> Self {
        Self {
            limits,
            operations: 0,
            bytes: 0,
        }
    }

    pub(super) fn stage_put(
        &mut self,
        key: &[u8],
        value: &[u8],
        precondition: &Precondition,
    ) -> Result<u64, StateStoreError> {
        self.stage(accounted_bytes(key.len(), value.len(), precondition)?)
    }

    pub(super) fn stage_delete(
        &mut self,
        key: &[u8],
        precondition: &Precondition,
    ) -> Result<u64, StateStoreError> {
        self.stage(accounted_bytes(key.len(), 0, precondition)?)
    }

    fn stage(&mut self, increment: usize) -> Result<u64, StateStoreError> {
        let operations = self.operations.checked_add(1).ok_or_else(operation_limit)?;
        if operations > self.limits.max_transaction_operations {
            return Err(operation_limit());
        }
        let bytes = self.bytes.checked_add(increment).ok_or_else(byte_limit)?;
        if bytes > self.limits.max_transaction_bytes {
            return Err(byte_limit());
        }
        self.operations = operations;
        self.bytes = bytes;
        u64::try_from(operations).map_err(|_| operation_limit())
    }
}

fn accounted_bytes(
    key_len: usize,
    value_len: usize,
    precondition: &Precondition,
) -> Result<usize, StateStoreError> {
    let mut bytes = 2usize
        .checked_add(key_len)
        .and_then(|bytes| bytes.checked_add(value_len))
        .ok_or_else(byte_limit)?;
    if let Precondition::Version(version) = precondition {
        bytes = bytes
            .checked_add(version.as_bytes().len())
            .ok_or_else(byte_limit)?;
    }
    Ok(bytes)
}

const fn operation_limit() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::LimitExceeded,
        "transaction operation limit exceeded",
    )
}

const fn byte_limit() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::LimitExceeded,
        "transaction byte limit exceeded",
    )
}
