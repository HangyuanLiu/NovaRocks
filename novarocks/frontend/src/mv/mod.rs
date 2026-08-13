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

pub(crate) mod activity;
// The acquisition surface here is consumed when the three refresh entry points
// are switched onto it. The repository-side fence it feeds is already live, so
// it lands first and is wired next rather than being held back.
#[allow(dead_code)]
mod coordination;
mod create;
pub(crate) mod maintenance;
pub(crate) mod maintenance_worker;
mod recovery;
mod refresh;
pub mod repository;
pub(crate) mod scheduler;
mod service;

pub use recovery::FrontendMvRecoverySummary;
pub(crate) use refresh::FrontendMvRefreshProviderActivationPort;
pub use service::FrontendMvService;
