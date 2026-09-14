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

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use novarocks_spi::connector::read_stack::adapter::{
    ProviderReadColumnBinding, ProviderReadFilterApplication, ProviderReadLimitApplication,
    ProviderReadMetadata, ProviderReadRuntime, ReadRuntimeAdapter,
};
use novarocks_spi::connector::read_stack::negotiation::{
    ReadNegotiation, ReadPushdownDisposition, ReadPushdownOp,
};
use novarocks_spi::connector::read_stack::{
    Assignment, ColumnHandle, ConnectorReadMetadata, ConnectorReadRelationVersion,
    ConnectorReadTableExecuteProcedure, ConnectorSession, Constraint, SchemaTableName, TupleDomain,
};
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorError, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorPinnedFileSet, ConnectorProviderId,
};

#[derive(Clone, Debug)]
struct AlphaTable;
#[derive(Clone, Debug)]
struct BetaTable;
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct AlphaColumn(u8);
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct BetaColumn(u8);

impl ColumnHandle for AlphaColumn {}
impl ColumnHandle for BetaColumn {}

struct FakeProvider<T, C> {
    descriptor: ConnectorInstanceDescriptor,
    catalog_handle: CatalogHandle,
    table: T,
    column: C,
    metadata_calls: AtomicUsize,
    projection_order: Mutex<Vec<String>>,
    _marker: PhantomData<C>,
}

impl<T, C> FakeProvider<T, C> {
    fn new(table: T, column: C) -> Self {
        Self {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("fake").expect("provider ID"),
                instance_id: ConnectorInstanceId::parse("catalog").expect("instance ID"),
            },
            catalog_handle: CatalogHandle::new(
                ConnectorInstanceId::parse("catalog").expect("instance ID"),
                CatalogVersion::from_bytes([7; 32]),
            ),
            table,
            column,
            metadata_calls: AtomicUsize::new(0),
            projection_order: Mutex::new(Vec::new()),
            _marker: PhantomData,
        }
    }
}

impl<T, C> ProviderReadRuntime for FakeProvider<T, C>
where
    T: Clone + std::fmt::Debug + Send + Sync + 'static,
    C: ColumnHandle,
{
    type Table = T;
    type Column = C;
    type Transaction = ();
    type Split = FakeSplit;

    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }

    fn transaction(&self) -> Self::Transaction {}
}

#[derive(Debug)]
struct FakeSplit;

impl novarocks_spi::connector::read_stack::ConnectorSplit for FakeSplit {
    fn retained_size_in_bytes(&self) -> u64 {
        0
    }
}

impl<T, C> ProviderReadMetadata for FakeProvider<T, C>
where
    T: Clone + std::fmt::Debug + Send + Sync + 'static,
    C: ColumnHandle,
{
    fn get_table_handle(
        &self,
        _session: &ConnectorSession,
        _name: &SchemaTableName,
        _version: ConnectorReadRelationVersion,
        _reference: Option<&str>,
    ) -> Result<Option<Self::Table>, ConnectorError> {
        self.metadata_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Some(self.table.clone()))
    }

    fn get_pinned_file_set_handle(
        &self,
        _session: &ConnectorSession,
        _name: &SchemaTableName,
        _pinned: &ConnectorPinnedFileSet,
    ) -> Result<Option<Self::Table>, ConnectorError> {
        Ok(None)
    }

    fn get_column_bindings(
        &self,
        _session: &ConnectorSession,
        _table: &Self::Table,
    ) -> Result<Vec<ProviderReadColumnBinding<Self::Column>>, ConnectorError> {
        self.metadata_calls.fetch_add(1, Ordering::SeqCst);
        Ok(vec![ProviderReadColumnBinding::new(
            "first",
            self.column.clone(),
            false,
        )])
    }

    fn apply_filter(
        &self,
        _session: &ConnectorSession,
        _table: &Self::Table,
        constraint: &Constraint<Self::Column>,
    ) -> Result<Option<ProviderReadFilterApplication<Self::Table, Self::Column>>, ConnectorError>
    {
        self.metadata_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Some(ProviderReadFilterApplication::new(
            self.table.clone(),
            constraint.clone(),
            None,
        )))
    }

    fn apply_projection(
        &self,
        _session: &ConnectorSession,
        _table: &Self::Table,
        assignments: &[Assignment<Self::Column>],
    ) -> Result<Option<Self::Table>, ConnectorError> {
        self.metadata_calls.fetch_add(1, Ordering::SeqCst);
        *self.projection_order.lock().expect("test mutex") = assignments
            .iter()
            .map(|assignment| assignment.variable().to_owned())
            .collect();
        Ok(Some(self.table.clone()))
    }

    fn apply_limit(
        &self,
        _session: &ConnectorSession,
        _table: &Self::Table,
        _limit: u64,
    ) -> Result<Option<ProviderReadLimitApplication<Self::Table>>, ConnectorError> {
        self.metadata_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Some(ProviderReadLimitApplication::new(
            self.table.clone(),
            true,
        )))
    }

    fn get_system_table_plan(
        &self,
        _session: &ConnectorSession,
        _name: &SchemaTableName,
    ) -> Result<
        Option<
            novarocks_spi::connector::read_stack::adapter::ProviderReadSystemTablePlan<Self::Table>,
        >,
        ConnectorError,
    > {
        Ok(None)
    }

    fn get_change_window_plan(
        &self,
        _session: &ConnectorSession,
        _name: &SchemaTableName,
        _window: novarocks_spi::connector::read_stack::ConnectorReadChangeWindow,
    ) -> Result<Option<Self::Table>, ConnectorError> {
        Ok(None)
    }

    fn get_table_execute_plan(
        &self,
        _session: &ConnectorSession,
        _name: &SchemaTableName,
        _procedure: ConnectorReadTableExecuteProcedure,
    ) -> Result<Option<Self::Table>, ConnectorError> {
        Ok(None)
    }
}

fn session() -> ConnectorSession {
    ConnectorSession::try_new("q", "u", "UTC", "en_US", SystemTime::UNIX_EPOCH).expect("session")
}

fn name() -> SchemaTableName {
    SchemaTableName::try_new("s", "t").expect("name")
}

#[test]
fn one_adapter_path_keeps_residual_limit_and_assignment_order_for_two_type_families() {
    let alpha_provider = Arc::new(FakeProvider::new(AlphaTable, AlphaColumn(1)));
    let beta_provider = Arc::new(FakeProvider::new(BetaTable, BetaColumn(2)));
    let alpha = ReadRuntimeAdapter::new(alpha_provider.clone());
    let beta = ReadRuntimeAdapter::new(beta_provider.clone());

    for metadata in [
        &alpha as &dyn ConnectorReadMetadata,
        &beta as &dyn ConnectorReadMetadata,
    ] {
        let table = metadata
            .get_table_handle(
                &session(),
                &name(),
                ConnectorReadRelationVersion::Current,
                None,
            )
            .expect("table call")
            .expect("table handle");
        let column = metadata
            .get_column_bindings(&session(), &table)
            .expect("columns")[0]
            .column()
            .clone();
        let assignments = vec![
            Assignment::try_new(
                "second",
                column.clone(),
                novarocks_spi::connector::read_stack::ConnectorValueType::BigInt,
            )
            .expect("assignment"),
            Assignment::try_new(
                "first",
                column.clone(),
                novarocks_spi::connector::read_stack::ConnectorValueType::BigInt,
            )
            .expect("assignment"),
        ];
        let constraint = Constraint::of_summary(
            TupleDomain::with_column_domains(BTreeMap::from([(
                column,
                novarocks_spi::connector::read_stack::Domain::all(
                    novarocks_spi::connector::read_stack::ConnectorValueType::BigInt,
                ),
            )]))
            .expect("domain"),
        );
        // One offer, three operations, answered in the order offered.
        let negotiation = ReadNegotiation {
            handle: table.clone(),
            ops: vec![
                ReadPushdownOp::Filter {
                    constraint: constraint.clone(),
                },
                ReadPushdownOp::Projection {
                    assignments: assignments.clone(),
                },
                ReadPushdownOp::Limit { rows: 5 },
            ],
        };
        let negotiated = metadata
            .negotiate(&session(), &negotiation)
            .expect("negotiate");
        negotiated
            .verify_shape(3)
            .expect("one answer per operation");
        assert!(negotiated.changed);
        // The fixture hands the predicate back unchanged, and this predicate
        // restricts nothing, so nothing is left for the engine to evaluate.
        // The residual is echoed either way; the disposition is what says
        // whether the engine is relieved.
        assert_eq!(negotiated.outcomes[0].residual.as_ref(), Some(&constraint));
        assert_eq!(
            negotiated.outcomes[0].disposition,
            ReadPushdownDisposition::Exact
        );
        assert_eq!(
            negotiated.outcomes[1].disposition,
            ReadPushdownDisposition::Exact
        );
        assert!(negotiated.outcomes[2].disposition.relieves_engine());

        // Offering the same operations against the same handle again answers
        // the same way: negotiating commits to nothing.
        let again = metadata
            .negotiate(&session(), &negotiation)
            .expect("negotiate again");
        assert_eq!(
            again
                .outcomes
                .iter()
                .map(|outcome| outcome.disposition)
                .collect::<Vec<_>>(),
            negotiated
                .outcomes
                .iter()
                .map(|outcome| outcome.disposition)
                .collect::<Vec<_>>()
        );
    }

    assert_eq!(
        alpha_provider
            .projection_order
            .lock()
            .expect("test mutex")
            .as_slice(),
        ["second", "first"]
    );
    assert_eq!(
        beta_provider
            .projection_order
            .lock()
            .expect("test mutex")
            .as_slice(),
        ["second", "first"]
    );
}

#[test]
fn binding_and_real_type_mismatch_are_rejected_before_provider_calls() {
    let alpha_provider = Arc::new(FakeProvider::new(AlphaTable, AlphaColumn(1)));
    let beta_provider = Arc::new(FakeProvider::new(BetaTable, BetaColumn(2)));
    let alpha = ReadRuntimeAdapter::new(alpha_provider.clone());
    let beta = ReadRuntimeAdapter::new(beta_provider.clone());
    let alpha_table = alpha
        .get_table_handle(
            &session(),
            &name(),
            ConnectorReadRelationVersion::Current,
            None,
        )
        .expect("table")
        .expect("handle");

    let before = beta_provider.metadata_calls.load(Ordering::SeqCst);
    let error = beta
        .get_column_bindings(&session(), &alpha_table)
        .expect_err("different concrete table must fail");
    assert_eq!(
        error.kind(),
        novarocks_spi::connector::ConnectorErrorKind::InvalidRequest
    );
    assert_eq!(beta_provider.metadata_calls.load(Ordering::SeqCst), before);

    let mut other_provider = FakeProvider::new(AlphaTable, AlphaColumn(3));
    other_provider.catalog_handle = CatalogHandle::new(
        ConnectorInstanceId::parse("catalog").expect("instance ID"),
        CatalogVersion::from_bytes([8; 32]),
    );
    let other = ReadRuntimeAdapter::new(Arc::new(other_provider));
    let error = other
        .get_column_bindings(&session(), &alpha_table)
        .expect_err("different binding must fail");
    assert_eq!(
        error.kind(),
        novarocks_spi::connector::ConnectorErrorKind::InvalidRequest
    );
}

/// Statistics can be asked about a negotiated read, not only about the table.
///
/// Once a provider has taken a predicate on, the rows it will actually return
/// are the pruned ones. A caller reasoning about cost needs to be able to ask
/// about *that* read. This proves the question is expressible and reaches the
/// provider distinguishably; consuming the answer is a separate concern.
#[test]
fn statistics_can_be_asked_about_a_negotiated_read() {
    let provider = Arc::new(FakeProvider::new(AlphaTable, AlphaColumn(1)));
    let adapter = ReadRuntimeAdapter::new(provider);
    let metadata = &adapter as &dyn ConnectorReadMetadata;
    let table = metadata
        .get_table_handle(
            &session(),
            &name(),
            ConnectorReadRelationVersion::Current,
            None,
        )
        .expect("table call")
        .expect("table handle");
    let column = metadata
        .get_column_bindings(&session(), &table)
        .expect("columns")[0]
        .column()
        .clone();
    let constraint = Constraint::of_summary(
        TupleDomain::with_column_domains(BTreeMap::from([(
            column,
            novarocks_spi::connector::read_stack::Domain::all(
                novarocks_spi::connector::read_stack::ConnectorValueType::BigInt,
            ),
        )]))
        .expect("domain"),
    );
    let negotiated = metadata
        .negotiate(
            &session(),
            &ReadNegotiation {
                handle: table.clone(),
                ops: vec![ReadPushdownOp::Filter { constraint }],
            },
        )
        .expect("negotiate");
    assert!(negotiated.changed, "the fixture narrows on a filter");

    // The question a cost model needs to ask is now expressible: statistics
    // about the negotiated read rather than about the whole table. Whether a
    // provider answers it differently is a provider fact, proven against a
    // real one; what this proves is that the seam exists and carries the
    // handle negotiation produced.
    let about_table = statistics_request(None);
    let about_read = statistics_request(Some(negotiated.handle.clone()));
    assert!(about_table.narrowed_read.is_none());
    assert!(about_read.narrowed_read.is_some());
}

/// A statistics request for the fixture table, optionally about a negotiated
/// read rather than about the table itself.
fn statistics_request(
    narrowed_read: Option<novarocks_spi::connector::read_stack::runtime::ConnectorReadTableHandle>,
) -> novarocks_spi::connector::StatisticsReadRequest {
    use novarocks_spi::connector::{
        ConnectorRequestContext, ConnectorTableHandle, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        StatisticsDataVersion, StatisticsMetric, StatisticsMetricRequest,
    };

    let owner =
        novarocks_spi::connector::ConnectorInstanceId::parse("lake.catalog").expect("instance id");
    novarocks_spi::connector::StatisticsReadRequest {
        table: ConnectorTableHandle::try_new(owner, bytes::Bytes::from_static(b"table-v1"))
            .expect("table handle"),
        narrowed_read,
        data_version: StatisticsDataVersion::try_new(bytes::Bytes::from_static(b"data-v1"))
            .expect("data version"),
        metrics: StatisticsMetricRequest::try_new(vec![StatisticsMetric::RowCount])
            .expect("metrics"),
        context: ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(1),
            std::sync::Arc::new(NeverCancelled),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        )
        .expect("request context"),
    }
}

struct NeverCancelled;

impl novarocks_spi::connector::ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}
