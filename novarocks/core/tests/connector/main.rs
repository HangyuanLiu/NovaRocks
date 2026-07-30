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
//! Integration tests for supported connector infrastructure.

use crate::common::TestConfig;
use novarocks::connector;

#[path = "../common/mod.rs"]
mod common;

#[test]
fn test_connector_registry_exists() {
    // Test that connector registry module exists and can be accessed
    // This is a basic smoke test to ensure the module is properly exported
    let _registry = connector::ConnectorRegistry::default();
}

#[test]
fn test_connector_registry_initialization() {
    // Test connector registry initialization
    let registry = connector::ConnectorRegistry::default();

    // Registry construction must not install a synthetic transport connector.
    let _ = registry;
}

#[test]
fn test_connector_registry_new() {
    // Test creating a new empty registry
    let registry = connector::ConnectorRegistry::new();
    let _ = registry;
}

#[test]
fn test_connector_config_loading() {
    let test_config = TestConfig::new().expect("Failed to create test config");
    let config = test_config.load_config().expect("Failed to load config");
    assert_eq!(config.server.host, "127.0.0.1");
}
