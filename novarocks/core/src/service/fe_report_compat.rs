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

//! StarRocks FE report compatibility facade.

#![cfg(feature = "compat")]

pub(crate) use crate::service::fe_report::{is_query_gone_status, mark_fe_query_gone};

#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::service::fe_report::{
    list_report_instances, test_insert_report_instance, test_is_fe_query_gone,
    test_report_registry_lock, test_reset_report_registry,
};
