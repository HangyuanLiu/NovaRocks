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

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorBatchReader, ConnectorBeginScanRequest, ConnectorError, ConnectorInstance,
    ConnectorInstanceDeclaration, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorInstanceIncarnation, ConnectorInstanceInstaller, ConnectorOpenReaderRequest,
    ConnectorProviderId, ConnectorRead, ConnectorRequestContext, ConnectorScan,
    ConnectorScanHandle, ConnectorSplit, ConnectorSplitPlanningRequest, ConnectorTableHandle,
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

struct TestInstaller {
    provider_id: ConnectorProviderId,
}

impl TestInstaller {
    fn new() -> Self {
        Self {
            provider_id: ConnectorProviderId::parse("test").expect("provider ID"),
        }
    }
}

impl ConnectorInstanceInstaller for TestInstaller {
    fn provider_id(&self) -> &ConnectorProviderId {
        &self.provider_id
    }

    fn install(
        &self,
        declaration: &ConnectorInstanceDeclaration,
        _context: &ConnectorRequestContext,
    ) -> Result<ConnectorInstance, ConnectorError> {
        Ok(instance(declaration.descriptor().instance_id.as_str()))
    }
}

fn declaration(
    incarnation: ConnectorInstanceIncarnation,
    payload: &'static [u8],
) -> ConnectorInstanceDeclaration {
    ConnectorInstanceDeclaration::try_new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("test").expect("provider ID"),
            instance_id: ConnectorInstanceId::parse("lake.catalog").expect("instance ID"),
        },
        incarnation,
        Bytes::from_static(payload),
    )
    .expect("bounded declaration")
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

#[test]
fn host_installs_a_declared_instance_idempotently() {
    let mut host = ConnectorHost::default();
    host.register_installer(Arc::new(TestInstaller::new()))
        .expect("register installer");
    let declaration = declaration(
        ConnectorInstanceIncarnation::from_bytes([1; 16]),
        b"binding=default",
    );
    let context = crate::connector::test_request_context();

    let first = host.install(&declaration, &context).expect("first install");
    let second = host
        .install(&declaration, &context)
        .expect("idempotent install");

    assert!(Arc::ptr_eq(&first, &second));
}

#[test]
fn host_rejects_conflicting_declarations_for_one_incarnation() {
    let mut host = ConnectorHost::default();
    host.register_installer(Arc::new(TestInstaller::new()))
        .expect("register installer");
    let context = crate::connector::test_request_context();
    let incarnation = ConnectorInstanceIncarnation::from_bytes([1; 16]);
    host.install(&declaration(incarnation, b"binding=default"), &context)
        .expect("first install");

    let error = host
        .install(&declaration(incarnation, b"binding=other"), &context)
        .err()
        .expect("same incarnation must not change declaration");
    assert_eq!(error.kind(), ConnectorHostErrorKind::ConflictingDeclaration);
}

#[test]
fn host_replaces_a_newer_incarnation_and_retires_it() {
    let mut host = ConnectorHost::default();
    host.register_installer(Arc::new(TestInstaller::new()))
        .expect("register installer");
    let context = crate::connector::test_request_context();
    let first_incarnation = ConnectorInstanceIncarnation::from_bytes([1; 16]);
    let second_incarnation = ConnectorInstanceIncarnation::from_bytes([2; 16]);
    let first = host
        .install(
            &declaration(first_incarnation, b"binding=default"),
            &context,
        )
        .expect("first install");
    let second = host
        .install(
            &declaration(second_incarnation, b"binding=default"),
            &context,
        )
        .expect("new incarnation install");
    assert!(!Arc::ptr_eq(&first, &second));

    let instance_id = ConnectorInstanceId::parse("lake.catalog").expect("instance ID");
    let error = host
        .install(
            &declaration(first_incarnation, b"binding=default"),
            &context,
        )
        .err()
        .expect("stale incarnation must not replace active instance");
    assert_eq!(error.kind(), ConnectorHostErrorKind::StaleIncarnation);
    host.retire(&instance_id, second_incarnation)
        .expect("retire current incarnation");
    let error = host
        .resolve(&instance_id)
        .err()
        .expect("retired instance must reject new readers");
    assert_eq!(error.kind(), ConnectorHostErrorKind::RetiringInstance);
    assert_eq!(first.descriptor().instance_id, instance_id);
}
