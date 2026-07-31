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

use crate::connector::starrocks::lake::service_domain::LakeTransactionInfo;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AbortTxnLogSource {
    Combined,
    PerTablet,
}

pub(crate) fn should_skip_abort_cleanup(skip_cleanup: Option<bool>) -> bool {
    skip_cleanup.unwrap_or(false)
}

pub(crate) fn decide_abort_txn_log_source(txn_info: &LakeTransactionInfo) -> AbortTxnLogSource {
    if txn_info.combined_txn_log {
        AbortTxnLogSource::Combined
    } else {
        AbortTxnLogSource::PerTablet
    }
}
