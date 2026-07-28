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

use crate::common::types::UniqueId;
use crate::thrift::internal_service;

#[derive(Clone, Debug)]
pub struct SyncExecPlanResult {
    fragment_instance_id: UniqueId,
}

impl SyncExecPlanResult {
    pub const fn new(fragment_instance_id: UniqueId) -> Self {
        Self {
            fragment_instance_id,
        }
    }

    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }
}

pub trait StarRocksFragmentSyncIngress: Send + Sync + 'static {
    fn execute(
        &self,
        request: internal_service::TExecPlanFragmentParams,
    ) -> Result<SyncExecPlanResult, String>;
}
