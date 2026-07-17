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

use crate::thrift::types;

pub(crate) type TabletCommitInfo = types::TTabletCommitInfo;
pub(crate) type TabletFailInfo = types::TTabletFailInfo;

pub(crate) fn tablet_commit_info(tablet_id: i64, backend_id: i64) -> TabletCommitInfo {
    types::TTabletCommitInfo::new(
        tablet_id,
        backend_id,
        Option::<Vec<String>>::None,
        Option::<Vec<String>>::None,
        Option::<Vec<i64>>::None,
    )
}

pub(crate) fn tablet_fail_info(tablet_id: i64, backend_id: i64) -> TabletFailInfo {
    types::TTabletFailInfo::new(Some(tablet_id), Some(backend_id))
}
