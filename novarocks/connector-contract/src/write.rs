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

use crate::identity::ConnectorIdentityError;

/// A provider-issued, preparation-local field identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorWriteFieldToken([u8; 32]);

/// Maximum number of logical write targets one query plan may address.
pub const MAX_CONNECTOR_WRITE_TARGETS: usize = 4_096;

/// A dense, query-local logical write target index.
///
/// This is an association inside one sealed plan. It is not an operation ID,
/// writer instance ID, recovery token, or catalog authority.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WriteTargetOrdinal(u32);

impl WriteTargetOrdinal {
    pub fn try_new(value: u32) -> Result<Self, ConnectorIdentityError> {
        if usize::try_from(value).is_ok_and(|value| value < MAX_CONNECTOR_WRITE_TARGETS) {
            return Ok(Self(value));
        }
        Err(ConnectorIdentityError::InvalidWriteTargetOrdinal)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

impl ConnectorWriteFieldToken {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_target_ordinal_is_bounded_by_the_contract_limit() {
        assert!(WriteTargetOrdinal::try_new(0).is_ok());
        let last = u32::try_from(MAX_CONNECTOR_WRITE_TARGETS - 1).expect("bounded");
        assert!(WriteTargetOrdinal::try_new(last).is_ok());
        let over = u32::try_from(MAX_CONNECTOR_WRITE_TARGETS).expect("bounded");
        assert_eq!(
            WriteTargetOrdinal::try_new(over),
            Err(ConnectorIdentityError::InvalidWriteTargetOrdinal)
        );
    }
}
