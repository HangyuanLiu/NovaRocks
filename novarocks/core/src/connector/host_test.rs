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

use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorBatchReader, ConnectorBeginScanRequest, ConnectorError, ConnectorInstance,
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorOpenReaderRequest,
    ConnectorProviderId, ConnectorRead, ConnectorScan, ConnectorScanHandle, ConnectorSplit,
    ConnectorSplitPlanningRequest, ConnectorTableHandle,
};

use super::ConnectorRegistry;
use super::host::{ConnectorHost, ConnectorHostErrorKind};

struct TestRead {
    instance_id: ConnectorInstanceId,
}

impl TestRead {
    fn new(instance_id: &str) -> Self {
        Self {
            instance_id: ConnectorInstanceId::parse(instance_id).expect("instance ID"),
        }
    }
}

impl ConnectorRead for TestRead {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn begin_scan(
        &self,
        _table: &ConnectorTableHandle,
        _request: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError> {
        unreachable!("host tests do not execute a read")
    }

    fn plan_splits(
        &self,
        _scan: &ConnectorScanHandle,
        _request: ConnectorSplitPlanningRequest,
    ) -> Result<Vec<ConnectorSplit>, ConnectorError> {
        unreachable!("host tests do not execute a read")
    }

    fn open_reader(
        &self,
        _split: &ConnectorSplit,
        _request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        unreachable!("host tests do not execute a read")
    }
}

fn instance(instance_id: &str) -> ConnectorInstance {
    let descriptor = ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("test").expect("provider ID"),
        instance_id: ConnectorInstanceId::parse(instance_id).expect("instance ID"),
    };
    ConnectorInstance::try_new(descriptor, None, Arc::new(TestRead::new(instance_id)))
        .expect("matching read capability")
}

#[test]
fn host_resolves_a_registered_instance_by_typed_id() {
    let mut host = ConnectorHost::default();
    host.register(instance("lake.catalog"))
        .expect("first registration");

    let resolved = host
        .resolve(&ConnectorInstanceId::parse("LAKE.CATALOG").expect("instance ID"))
        .expect("registered instance");
    assert_eq!(resolved.descriptor().instance_id.as_str(), "lake.catalog");
}

#[test]
fn host_rejects_duplicate_instance_registration() {
    let mut host = ConnectorHost::default();
    host.register(instance("lake.catalog"))
        .expect("first registration");

    assert_eq!(
        host.register(instance("lake.catalog"))
            .err()
            .expect("duplicate registration must fail")
            .kind(),
        ConnectorHostErrorKind::DuplicateInstance
    );
}

#[test]
fn host_reports_unknown_instances_without_a_fallback() {
    let host = ConnectorHost::default();

    assert_eq!(
        host.resolve(&ConnectorInstanceId::parse("missing").expect("instance ID"))
            .err()
            .expect("unknown instance must fail")
            .kind(),
        ConnectorHostErrorKind::UnknownInstance
    );
}

#[test]
fn host_unregisters_an_instance_before_its_backing_catalog_is_dropped() {
    let mut host = ConnectorHost::default();
    host.register(instance("lake.catalog"))
        .expect("first registration");
    let instance_id = ConnectorInstanceId::parse("lake.catalog").expect("instance ID");

    let removed = host
        .unregister(&instance_id)
        .expect("registered instance may be removed during catalog drop");
    assert_eq!(removed.descriptor().instance_id, instance_id);
    assert_eq!(
        host.resolve(&instance_id)
            .err()
            .expect("removed instance must not remain resolvable")
            .kind(),
        ConnectorHostErrorKind::UnknownInstance
    );
}

#[test]
fn registry_exposes_only_typed_connector_instance_resolution() {
    let mut registry = ConnectorRegistry::new();
    registry
        .register_connector_instance(instance("lake.catalog"))
        .expect("first typed instance registration");

    let resolved = registry
        .connector_instance(&ConnectorInstanceId::parse("lake.catalog").expect("instance ID"))
        .expect("typed instance resolution");
    assert_eq!(resolved.descriptor().provider_id.as_str(), "test");
}
