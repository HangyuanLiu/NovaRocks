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

//! NovaRocks extension of the upstream Iceberg REST client.
//!
//! The upstream `RestCatalog` owns the `/v1/config` handshake, config merge
//! and its session cache, plus the few read requests that need no NovaRocks
//! semantics. Everything that needs NovaRocks semantics -- every mutation and
//! its evidence, single-page listing, access delegation, credentials, views and
//! the backend acquisition -- is issued here from the one session binding the
//! injected auth owner captures while upstream establishes its session.
//!
//! Dependency direction: `evidence`, `json` and `minimal_view` are pure;
//! `http` depends only on the observation types in `evidence`; endpoint and
//! mutation modules compose the rest. The upstream REST crate appears only in
//! the auth owner, the upstream boundary and the transitional adapter.

pub(crate) mod adapter;
pub(crate) mod attempt;
pub(crate) mod auth;
pub(crate) mod binding;
pub(crate) mod boundary;
pub(crate) mod deferred;
pub(crate) mod endpoints;
pub(crate) mod evidence;
pub(crate) mod http;
pub(crate) mod json;
pub(crate) mod minimal_view;
pub(crate) mod mutation;
pub(crate) mod routing;
#[cfg(test)]
pub(crate) mod test_server;
