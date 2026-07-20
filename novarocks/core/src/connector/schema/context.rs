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
pub(crate) struct SchemaUserRoles {
    pub(crate) role_id_list: Option<Vec<i64>>,
}

#[cfg(feature = "compat")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SchemaUserIdentity {
    pub(crate) username: Option<String>,
    pub(crate) host: Option<String>,
    pub(crate) is_domain: Option<bool>,
    pub(crate) is_ephemeral: Option<bool>,
    pub(crate) current_role_ids: Option<SchemaUserRoles>,
}

#[cfg(feature = "compat")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SchemaFrontend {
    pub(crate) id: Option<String>,
    pub(crate) ip: Option<String>,
    pub(crate) http_port: Option<i32>,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct SchemaScanContext {
    pub(crate) table_name: String,
    pub(crate) db: Option<String>,
    pub(crate) table: Option<String>,
    pub(crate) wild: Option<String>,
    pub(crate) user: Option<String>,
    pub(crate) ip: Option<String>,
    pub(crate) port: Option<i32>,
    pub(crate) thread_id: Option<i64>,
    pub(crate) user_ip: Option<String>,
    #[cfg(feature = "compat")]
    pub(crate) current_user_ident: Option<SchemaUserIdentity>,
    pub(crate) catalog_name: Option<String>,
    pub(crate) table_id: Option<i64>,
    pub(crate) partition_id: Option<i64>,
    pub(crate) tablet_id: Option<i64>,
    pub(crate) txn_id: Option<i64>,
    pub(crate) job_id: Option<i64>,
    pub(crate) label: Option<String>,
    pub(crate) type_: Option<String>,
    pub(crate) state: Option<String>,
    pub(crate) limit: Option<i64>,
    pub(crate) log_start_ts: Option<i64>,
    pub(crate) log_end_ts: Option<i64>,
    pub(crate) log_level: Option<String>,
    pub(crate) log_pattern: Option<String>,
    pub(crate) log_limit: Option<i64>,
    #[cfg(feature = "compat")]
    pub(crate) frontends: Vec<SchemaFrontend>,
}

impl SchemaScanContext {
    pub(crate) fn limit_as_usize(&self) -> Option<usize> {
        self.limit
            .and_then(|value| usize::try_from(value).ok())
            .filter(|value| *value > 0)
    }
}
