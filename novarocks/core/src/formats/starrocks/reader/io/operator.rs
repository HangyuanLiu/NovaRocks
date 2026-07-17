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
//! OpenDAL operator builder for native reader.
//!
use opendal::Operator;

use crate::connector::starrocks::ObjectStoreProfile;
use crate::formats::starrocks::fs_access::resolve_format_tablet_access;

/// Build an object operator for native segment reads.
pub(crate) fn build_operator(
    tablet_root_path: &str,
    object_store_profile: Option<&ObjectStoreProfile>,
) -> Result<Operator, String> {
    resolve_format_tablet_access(tablet_root_path, object_store_profile)
        .map(|access| access.operator())
}
