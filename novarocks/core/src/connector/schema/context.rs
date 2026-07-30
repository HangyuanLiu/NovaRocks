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
#[cfg(feature = "compat")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaUserRoles {
    pub role_id_list: Option<Vec<i64>>,
}

#[cfg(feature = "compat")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaUserIdentity {
    pub username: Option<String>,
    pub host: Option<String>,
    pub is_domain: Option<bool>,
    pub is_ephemeral: Option<bool>,
    pub current_role_ids: Option<SchemaUserRoles>,
}

#[cfg(feature = "compat")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaFrontend {
    pub id: Option<String>,
    pub ip: Option<String>,
    pub http_port: Option<i32>,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct SchemaScanContext {
    pub table_name: String,
    pub db: Option<String>,
    pub table: Option<String>,
    pub wild: Option<String>,
    pub user: Option<String>,
    pub ip: Option<String>,
    pub port: Option<i32>,
    pub thread_id: Option<i64>,
    pub user_ip: Option<String>,
    #[cfg(feature = "compat")]
    pub current_user_ident: Option<SchemaUserIdentity>,
    pub catalog_name: Option<String>,
    pub table_id: Option<i64>,
    pub partition_id: Option<i64>,
    pub tablet_id: Option<i64>,
    pub txn_id: Option<i64>,
    pub job_id: Option<i64>,
    pub label: Option<String>,
    pub type_: Option<String>,
    pub state: Option<String>,
    pub limit: Option<i64>,
    pub log_start_ts: Option<i64>,
    pub log_end_ts: Option<i64>,
    pub log_level: Option<String>,
    pub log_pattern: Option<String>,
    pub log_limit: Option<i64>,
    #[cfg(feature = "compat")]
    pub frontends: Vec<SchemaFrontend>,
}

impl SchemaScanContext {
    pub fn limit_as_usize(&self) -> Option<usize> {
        self.limit
            .and_then(|value| usize::try_from(value).ok())
            .filter(|value| *value > 0)
    }
}
