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

// Lake transaction modules are scaffolded for publish/abort/compaction/vacuum
// operations that will be wired when FE lake RPCs are connected. Suppress
// dead-code warnings at the module level rather than per-item.
#[allow(dead_code)]
pub(crate) mod abort_executor;
#[allow(dead_code)]
pub(crate) mod abort_policy;
#[allow(dead_code)]
pub(crate) mod applier;
#[allow(dead_code)]
pub(crate) mod compaction;
pub mod context;
pub(crate) mod delete_payload_codec;
#[allow(dead_code)]
pub(crate) mod pk_applier;
#[allow(dead_code)]
pub(crate) mod replay_policy;
pub(crate) mod schema;
pub mod schema_change;
pub mod service_domain;
pub mod storage_domain;
#[allow(dead_code)]
pub(crate) mod transactions;
#[allow(dead_code)]
pub(crate) mod txn_loader;
pub(crate) mod txn_log;

pub use compaction::{execute_abort_compaction, execute_compact};
pub(crate) use context::TabletWriteContext;
pub use schema::{LakeCreateTabletTask, execute_lake_create_tablet_task};
pub use schema_change::{
    LakeTabletMetadataUpdate, LakeUpdateTabletMetaTask, execute_lake_update_tablet_meta_task,
};
pub use transactions::{
    execute_abort_txn, execute_delete_data, execute_delete_tablet, execute_drop_table,
    execute_get_tablet_stats, execute_publish_log_version, execute_publish_log_version_batch,
    execute_publish_version, execute_vacuum,
};
pub(crate) use txn_log::append_lake_txn_log_with_chunk_rowset;
