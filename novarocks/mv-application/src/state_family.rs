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

//! Durable state-family identity for the MV accelerator.

use novarocks_state_store_runtime::PersistentStateFamily;

/// Frozen StateStore identity owned by the MV application.
///
/// The prefix retains its historic `frontend` namespace because deployed
/// records use these bytes. The namespace is not authority: the MV product
/// owns the record version and lifecycle, and composition validates this
/// descriptor with every other durable product family.
pub const MV_ACCELERATOR_STATE_FAMILY: PersistentStateFamily = PersistentStateFamily::new(
    "mv-application/accelerator",
    "novarocks/frontend/mv/accelerator/v1",
    1,
);

#[cfg(test)]
mod tests {
    use super::MV_ACCELERATOR_STATE_FAMILY;

    #[test]
    fn accelerator_family_keeps_its_deployed_identity() {
        assert_eq!(
            MV_ACCELERATOR_STATE_FAMILY.prefix(),
            "novarocks/frontend/mv/accelerator/v1"
        );
        assert_eq!(MV_ACCELERATOR_STATE_FAMILY.record_version(), 1);
    }
}
