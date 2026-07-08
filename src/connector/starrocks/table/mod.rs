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

//! StarRocks table subsystem: config, catalog rebuild/reconcile, DDL,
//! transactional INSERT + publish, SQLite-backed metadata persistence,
//! and materialized-view lifecycle. Migrated here from the former standalone
//! lake module during the standalone/connector decoupling refactor
//! (2026-04-24).
//!
//! Files will be added incrementally by the next tasks in this plan.

pub(crate) mod backend;
pub(crate) mod catalog;
pub(crate) mod config;
pub(crate) mod ddl;
pub(crate) mod erase;
pub(crate) mod ivm_change_stream;
pub(crate) mod ivm_delta_aggregate;
pub(crate) mod ivm_delta_source;
pub(crate) mod ivm_row_identity;
pub(crate) mod model;
pub(crate) mod mv_apply_policy;
pub(crate) mod mv_ddl;
pub(crate) mod mv_refresh;
pub(crate) mod mv_refresh_strategy;
pub(crate) mod refresh_pin;
pub(crate) mod scan_planner;
pub(crate) mod schema_adapter;
pub(crate) mod txn;

pub(crate) use catalog::{
    StarRocksTableCatalog, register_starrocks_tables_in_catalog, runtime_registered,
};
pub(crate) use config::StarRocksTableConfig;
pub(crate) use scan_planner::StarRocksTableScanPlanner;
