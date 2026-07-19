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

//! StarRocks storage-RPC protobuf encoding boundary.
//!
//! Sink programs, assignments, factories, and operators retain domain values.
//! A storage RPC must call this adapter immediately before building its wire
//! request instead of storing generated protobuf messages in connector state.

use crate::common::types::UniqueId;
use crate::service::grpc_client::proto::starrocks::PUniqueId;

pub(crate) fn encode_unique_id(value: UniqueId) -> PUniqueId {
    PUniqueId {
        hi: value.hi,
        lo: value.lo,
    }
}

#[cfg(test)]
mod tests {
    use super::encode_unique_id;
    use crate::common::types::UniqueId;

    #[test]
    fn unique_id_is_encoded_only_at_storage_rpc_boundary() {
        let encoded = encode_unique_id(UniqueId { hi: 7, lo: 11 });
        assert_eq!((encoded.hi, encoded.lo), (7, 11));
    }
}
