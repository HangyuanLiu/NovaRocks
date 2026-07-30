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

use novarocks::thrift::data_sinks::TPlanFragmentDestination;
use novarocks::thrift::types::TNetworkAddress;

/// Selects the destination endpoint across the current and historical wire shapes.
///
/// Older FEs populated `deprecated_server`; current FEs populate `brpc_server`.
/// When both fields are present, `brpc_server` wins. This rule can be removed once
/// the minimum supported FE version no longer emits `deprecated_server`.
pub(crate) fn destination_address(
    destination: &TPlanFragmentDestination,
) -> Option<&TNetworkAddress> {
    destination_address_with_field(destination).map(|(address, _)| address)
}

/// Applies the endpoint compatibility rule and reports which wire field won.
///
/// `brpc_server` is the current field and takes precedence over the historical
/// `deprecated_server` field. This diagnostic form can be removed together with
/// the fallback once the minimum supported FE no longer emits `deprecated_server`.
pub(crate) fn destination_address_with_field(
    destination: &TPlanFragmentDestination,
) -> Option<(&TNetworkAddress, &'static str)> {
    if let Some(address) = destination.brpc_server.as_ref() {
        return Some((address, "brpc_server"));
    }
    destination
        .deprecated_server
        .as_ref()
        .map(|address| (address, "deprecated_server"))
}
