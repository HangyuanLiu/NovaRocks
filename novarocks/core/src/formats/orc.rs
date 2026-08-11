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

//! Connector-neutral ORC scan configuration.

use novarocks_execution::exec::chunk::ChunkSchemaRef;
use novarocks_fs::DataCacheContext;

#[derive(Clone, Debug)]
pub struct OrcScanConfig {
    pub columns: Vec<String>,
    pub chunk_schema: ChunkSchemaRef,
    pub case_sensitive: bool,
    pub orc_use_column_names: bool,
    pub hive_column_names: Option<Vec<String>>,
    pub batch_size: Option<usize>,
    pub datacache: DataCacheContext,
}
