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

//! Execution-binding wire facts are owned by `novarocks-protocol`; SPI only
//! exposes the validated value at its frontend-to-backend port.

pub use novarocks_protocol::provider::{
    ConnectorExecutionBindingDeclaration as ConnectorExecutionDeclaration,
    ConnectorExecutionProviderKind,
};
use uuid::Uuid;

/// A monotonically generated identity for one logical connector instance
/// generation. UUID v7 is used so a newer catalog generation cannot be
/// overwritten by a delayed install request.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorInstanceIncarnation(Uuid);

impl ConnectorInstanceIncarnation {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    pub fn to_bytes(self) -> [u8; 16] {
        *self.0.as_bytes()
    }
}

impl Default for ConnectorInstanceIncarnation {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::ConnectorInstanceIncarnation;

    #[test]
    fn incarnation_round_trips_exact_bytes() {
        let incarnation = ConnectorInstanceIncarnation::from_bytes([9; 16]);
        assert_eq!(incarnation.to_bytes(), [9; 16]);
    }
}
