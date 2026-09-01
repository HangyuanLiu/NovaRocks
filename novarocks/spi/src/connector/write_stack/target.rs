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

//! Query-local association between a logical write target and its writer
//! output.
//!
//! A [`WriteTargetOrdinal`] is not an operation id, a cohort id, a writer uuid,
//! a recovery token, or a catalog authority. It exists only inside one sealed
//! query plan, and it says exactly one thing: which logical writer handle a
//! commit fragment belongs to.

use std::collections::BTreeSet;

use crate::connector::write_stack::limits::MAX_CONNECTOR_WRITE_TARGETS;
use crate::connector::{ConnectorError, ConnectorErrorKind};

/// A dense, query-local logical write target index.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WriteTargetOrdinal(u32);

impl WriteTargetOrdinal {
    /// The ordinal is dense within one sealed plan, so a value at or beyond the
    /// frozen target bound can never be legal.
    pub fn try_new(value: u32) -> Result<Self, ConnectorError> {
        if usize::try_from(value).is_ok_and(|value| value < MAX_CONNECTOR_WRITE_TARGETS) {
            return Ok(Self(value));
        }
        Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector write target ordinal exceeds the sealed target bound",
        ))
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Validate that `ordinals` form the dense set `0..ordinals.len()` with no gap
/// and no duplicate. A sparse or duplicated set means the begin session and the
/// sealed plan disagree about which logical writers exist, so it fails closed
/// before any fragment submission.
pub fn validate_dense_target_ordinals(
    ordinals: &[WriteTargetOrdinal],
) -> Result<(), ConnectorError> {
    if ordinals.is_empty() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector write session requires at least one logical write target",
        ));
    }
    if ordinals.len() > MAX_CONNECTOR_WRITE_TARGETS {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector write session exceeds the frozen logical write target bound",
        ));
    }
    let unique = ordinals.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != ordinals.len() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector write session repeats a logical write target ordinal",
        ));
    }
    let dense = unique
        .iter()
        .enumerate()
        .all(|(index, ordinal)| u32::try_from(index).is_ok_and(|index| index == ordinal.get()));
    if !dense {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector write session target ordinals are not dense from zero",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ordinals(values: &[u32]) -> Vec<WriteTargetOrdinal> {
        values
            .iter()
            .map(|value| WriteTargetOrdinal::try_new(*value).expect("bounded ordinal"))
            .collect()
    }

    #[test]
    fn ordinal_is_bounded_by_the_frozen_target_limit() {
        assert!(WriteTargetOrdinal::try_new(0).is_ok());
        let last = u32::try_from(MAX_CONNECTOR_WRITE_TARGETS - 1).expect("bounded");
        assert!(WriteTargetOrdinal::try_new(last).is_ok());
        let over = u32::try_from(MAX_CONNECTOR_WRITE_TARGETS).expect("bounded");
        assert!(WriteTargetOrdinal::try_new(over).is_err());
    }

    #[test]
    fn dense_target_ordinals_are_accepted() {
        assert!(validate_dense_target_ordinals(&ordinals(&[0, 1, 2])).is_ok());
        assert!(validate_dense_target_ordinals(&ordinals(&[2, 0, 1])).is_ok());
    }

    #[test]
    fn empty_sparse_and_duplicate_target_ordinals_fail_closed() {
        assert!(validate_dense_target_ordinals(&[]).is_err());
        assert!(validate_dense_target_ordinals(&ordinals(&[0, 2])).is_err());
        assert!(validate_dense_target_ordinals(&ordinals(&[0, 0])).is_err());
        assert!(validate_dense_target_ordinals(&ordinals(&[1, 2])).is_err());
    }
}
