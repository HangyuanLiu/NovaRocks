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

//! One read, negotiated and frozen against a real provider.
//!
//! Everything else about this path is decided in isolation - which owner
//! answers, what a disposition means, what the reader is told. This is the one
//! place the whole sequence runs end to end against a provider that answers,
//! so the parts have to agree with each other rather than each being right on
//! its own.

use std::sync::Arc;

use novarocks_physical_plan::{ProviderReadOccurrenceId, ValueType};
use novarocks_query_application::preparation::{ProviderReadFactPort, ReadAccessSink};
use novarocks_spi::connector::read_stack::{ConnectorSession, ConnectorValueType};
use novarocks_sql::compiler::{
    ProviderReadNeed, ProviderReadRelationNeed, ProviderReadVersionNeed, fixtures,
};
use novarocks_sql::test_support::{
    NativeScanFixture, native_scan_fixture_binding, native_scan_plan,
};
use novarocks_types::naming::TableIdentity;

use super::{data_file, fixture_control_role_host, fixture_query_table_bindings, registry};
use crate::query_execution::provider_read_facts::FrontendProviderReadFacts;

fn session() -> ConnectorSession {
    ConnectorSession::try_new(
        "fixture-query",
        "fixture-user",
        "UTC",
        "en_US",
        std::time::SystemTime::UNIX_EPOCH,
    )
    .expect("fixture connector session")
}

fn need(
    identity: TableIdentity,
    binding: novarocks_sql::binding::SqlTableBindingId,
) -> ProviderReadNeed {
    let column = fixtures::provider_read_column_need(
        0,
        "id",
        ValueType::new(arrow::datatypes::DataType::Int64, false),
        ConnectorValueType::BigInt,
    )
    .expect("column need");
    fixtures::provider_read_need(
        1,
        ProviderReadOccurrenceId::new(1),
        binding,
        ProviderReadRelationNeed::Data {
            relation: identity,
            version: ProviderReadVersionNeed::Current,
        },
        vec![column],
        Vec::new(),
        None,
    )
    .expect("provider read need")
}

/// The freeze answers the request it was asked to commit, and leaves behind
/// exactly one capability for the occurrence that asked.
#[test]
fn one_read_is_negotiated_frozen_and_accounted_for() {
    let connectors = registry(vec![data_file("s3://bucket/current.parquet")]);
    let controls = crate::connector::FixtureControlResolver::new(connectors);
    let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
        .expect("sealed ordinary fixture");
    let admitted = native_scan_fixture_binding(&plan).expect("fixture scan binding");
    let store = Arc::new(fixture_query_table_bindings(&plan, &controls));
    let host = fixture_control_role_host(&plan, &controls);
    let (binding, _) = store
        .captured_bindings()
        .into_iter()
        .next()
        .expect("the fixture admitted one binding");
    let identity = TableIdentity::new(&admitted.catalog, &admitted.namespace, &admitted.table);
    let need = need(identity, binding);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("runtime");
    let facts = FrontendProviderReadFacts::new(
        host,
        store,
        session(),
        crate::connector::test_request_context(),
        crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor::new(
            runtime.handle().clone(),
            novarocks_native_adapter::connector_blocking_io::ConnectorBlockingIoBudget::try_new(
                2, 1,
            )
            .expect("blocking budget"),
        ),
    );
    let taken = ReadAccessSink::new();
    let frozen = runtime
        .block_on(facts.resolve_provider_reads(std::slice::from_ref(&need), &taken))
        .expect("the fixture provider answers one read");

    assert_eq!(frozen.len(), 1);
    let fact = &frozen[0];
    assert_eq!(fact.occurrence(), need.occurrence());
    assert_eq!(fact.binding(), need.binding());
    // The contract echoes the request it answers, which is what lets the
    // compiler resume against the read it actually asked for.
    assert_eq!(fact.contract().schema.len(), 1);
    assert_eq!(fact.contract().schema[0].request_ordinal(), 0);
    assert_eq!(fact.contract().sql_binding, need.binding());

    // One freeze, one capability, and the encoding half kept beside it.
    let access = match taken.try_into_access() {
        Ok(access) => access,
        Err((error, _)) => panic!("one occurrence, one freeze: {error}"),
    };
    assert_eq!(access.len(), 1);
    let kept = access
        .get(need.occurrence())
        .expect("the occurrence that asked");
    assert_eq!(kept.binding, need.binding());
    assert_eq!(kept.access.encoding.assignments.len(), 1);
    assert_eq!(kept.access.encoding.columns.len(), 1);
    assert_eq!(kept.access.encoding.columns[0].1.name(), "id");
}
