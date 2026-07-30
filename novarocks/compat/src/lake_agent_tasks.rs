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

//! Compat composition for the temporary BackendService lake-agent callback.

use std::sync::Arc;

use novarocks::runtime::starlet_shard_registry::StarletShardInfo;
use novarocks::service::backend_service::{self, LakeAgentTaskAdapter};
use novarocks::thrift::agent_service::{
    TAlterTabletReqV2, TCreateTabletReq, TUpdateTabletMetaInfoReq,
};

struct CompatLakeAgentTaskAdapter {
    storage_metadata_provider:
        Arc<dyn novarocks::connector::starrocks::ports::StorageMetadataProvider>,
}

impl LakeAgentTaskAdapter for CompatLakeAgentTaskAdapter {
    fn create_tablet(
        &self,
        request: &TCreateTabletReq,
        shard_info: &StarletShardInfo,
    ) -> Result<(), String> {
        backend_service::execute_lake_create_tablet(
            request,
            shard_info,
            Arc::clone(&self.storage_metadata_provider),
        )
    }

    fn alter_tablet(&self, request: &TAlterTabletReqV2) -> Result<(), String> {
        backend_service::execute_lake_alter_tablet(
            request,
            Arc::clone(&self.storage_metadata_provider),
        )
    }

    fn update_tablet_meta_info(&self, request: &TUpdateTabletMetaInfoReq) -> Result<(), String> {
        backend_service::execute_lake_update_tablet_meta_info(
            request,
            Arc::clone(&self.storage_metadata_provider),
        )
    }
}

pub(crate) fn lake_agent_task_adapter(
    storage_metadata_provider: Arc<
        dyn novarocks::connector::starrocks::ports::StorageMetadataProvider,
    >,
) -> Arc<dyn LakeAgentTaskAdapter> {
    Arc::new(CompatLakeAgentTaskAdapter {
        storage_metadata_provider,
    })
}
