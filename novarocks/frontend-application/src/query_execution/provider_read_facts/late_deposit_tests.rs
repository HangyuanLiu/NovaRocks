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

use super::*;
use crate::catalog_application::query_bindings::{
    QueryScanMaterialization, QueryTableBinding, QueryTableBindingAdmission, QueryTableBindingKey,
};
use bytes::Bytes;
use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
use novarocks_spi::connector::read_stack::adapter::{
    ProviderReadColumnBinding, ProviderReadFilterApplication, ProviderReadLimitApplication,
    ProviderReadMetadata, ProviderReadRuntime, ProviderReadSplitManager, ProviderReadSplitSource,
    ProviderReadSystemTablePlan, ReadRuntimeAdapter,
};
use novarocks_spi::connector::read_stack::*;
use novarocks_spi::connector::*;
use novarocks_workload_control::{CancellationReason, ResultWindowClass};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, mpsc};
use std::time::{Duration, Instant, SystemTime};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct LateColumn;
impl ColumnHandle for LateColumn {}
#[derive(Debug)]
struct LateSplit;
impl novarocks_spi::connector::read_stack::ConnectorSplit for LateSplit {
    fn retained_size_in_bytes(&self) -> u64 {
        0
    }
}

struct ActualFrozenTable {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    destroyed: Arc<AtomicBool>,
}
impl std::fmt::Debug for ActualFrozenTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ActualFrozenTableDropWitness")
    }
}
impl Drop for ActualFrozenTable {
    fn drop(&mut self) {
        let _ = self.entered.send(());
        let _ = self.release.get_mut().unwrap().recv();
        self.destroyed.store(true, Ordering::Release);
    }
}
struct LateProvider {
    descriptor: ConnectorInstanceDescriptor,
    catalog: CatalogHandle,
    table: Mutex<Option<Arc<ActualFrozenTable>>>,
    freeze_started: mpsc::Sender<()>,
    freeze_release: Mutex<mpsc::Receiver<()>>,
    opens: AtomicUsize,
    freezes: AtomicUsize,
}
impl ProviderReadRuntime for LateProvider {
    type Table = Arc<ActualFrozenTable>;
    type Column = LateColumn;
    type Transaction = ();
    type Split = LateSplit;
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog
    }
    fn transaction(&self) -> Self::Transaction {}
}
impl ProviderReadMetadata for LateProvider {
    fn get_table_handle(
        &self,
        _: &ConnectorSession,
        name: &SchemaTableName,
        _: ConnectorReadRelationVersion,
        _: Option<&str>,
    ) -> Result<Option<Self::Table>, ConnectorError> {
        // The fixture owns exactly one table; no fallback/private payload reconstruction.
        self.opens.fetch_add(1, Ordering::AcqRel);
        if name.schema_name() != "db" || name.table_name() != "orders" {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "wrong read relation",
            ));
        }
        Ok(self.table.lock().unwrap().take())
    }
    fn get_pinned_file_set_handle(
        &self,
        _: &ConnectorSession,
        _: &SchemaTableName,
        _: &ConnectorPinnedFileSet,
    ) -> Result<Option<Self::Table>, ConnectorError> {
        Ok(None)
    }
    fn get_column_bindings(
        &self,
        _: &ConnectorSession,
        _: &Self::Table,
    ) -> Result<Vec<ProviderReadColumnBinding<Self::Column>>, ConnectorError> {
        Ok(vec![ProviderReadColumnBinding::new("k", LateColumn, false)])
    }
    fn final_static_facts(
        &self,
        _: &ConnectorSession,
        _: &Self::Table,
    ) -> Result<ConnectorReadStaticFacts<Self::Column>, ConnectorError> {
        self.freezes.fetch_add(1, Ordering::AcqRel);
        let _ = self.freeze_started.send(());
        let _ = self.freeze_release.lock().unwrap().recv();
        ConnectorReadStaticFacts::try_new(
            ConnectorReadInputVersion::try_new(Arc::<[u8]>::from(&b"input-v1"[..]))?,
            [9; 32],
            ConnectorReadProperties::try_new(
                ConnectorReadDistribution::Unconstrained,
                Vec::<ConnectorReadOrderingKey<LateColumn>>::new(),
            )?,
            ConnectorReadArtifactCoverage::NoArtifactInputs,
            Arc::<[u8]>::from(&b"coverage"[..]),
        )
    }
    fn apply_filter(
        &self,
        _: &ConnectorSession,
        _: &Self::Table,
        _: &Constraint<Self::Column>,
    ) -> Result<Option<ProviderReadFilterApplication<Self::Table, Self::Column>>, ConnectorError>
    {
        Ok(None)
    }
    fn apply_projection(
        &self,
        _: &ConnectorSession,
        _: &Self::Table,
        _: &[Assignment<Self::Column>],
    ) -> Result<Option<Self::Table>, ConnectorError> {
        Ok(None)
    }
    fn apply_limit(
        &self,
        _: &ConnectorSession,
        _: &Self::Table,
        _: u64,
    ) -> Result<Option<ProviderReadLimitApplication<Self::Table>>, ConnectorError> {
        Ok(None)
    }
    fn get_system_table_plan(
        &self,
        _: &ConnectorSession,
        _: &SchemaTableName,
    ) -> Result<Option<ProviderReadSystemTablePlan<Self::Table>>, ConnectorError> {
        Ok(None)
    }
    fn get_change_window_plan(
        &self,
        _: &ConnectorSession,
        _: &SchemaTableName,
        _: ConnectorReadChangeWindow,
    ) -> Result<Option<Self::Table>, ConnectorError> {
        Ok(None)
    }
    fn get_table_execute_plan(
        &self,
        _: &ConnectorSession,
        _: &SchemaTableName,
        _: ConnectorReadTableExecuteProcedure,
    ) -> Result<Option<Self::Table>, ConnectorError> {
        Ok(None)
    }
}
impl ProviderReadSplitManager for LateProvider {
    fn get_splits(
        &self,
        _: &ConnectorSession,
        _: &Self::Table,
        _: &[Assignment<Self::Column>],
        _: &BTreeSet<Self::Column>,
        _: &Constraint<Self::Column>,
    ) -> Result<Box<dyn ProviderReadSplitSource<Self>>, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "fixture does not enumerate splits",
        ))
    }
}

struct LateAccessSealer {
    adapter: Arc<ReadRuntimeAdapter<LateProvider>>,
    seals: Arc<AtomicUsize>,
}
impl ConnectorReadAttemptAccessSealer for LateAccessSealer {
    fn seal(
        &self,
        _: &ConnectorReadTableHandle,
        mint: ConnectorReadAttemptAccessMint,
    ) -> Result<ConnectorReadAttemptAccessSource, ConnectorError> {
        self.seals.fetch_add(1, Ordering::AcqRel);
        Ok(mint.seal(Arc::new(LateReacquirer(self.adapter.clone()))))
    }
}
struct LateReacquirer(Arc<ReadRuntimeAdapter<LateProvider>>);
impl ConnectorReadAttemptAccessReacquirer for LateReacquirer {
    fn for_attempt(
        &self,
        _: &ConnectorAttemptContext,
    ) -> Result<ConnectorReadAttemptRuntime, ConnectorError> {
        Ok(ConnectorReadAttemptRuntime::new(self.0.clone()))
    }
}
struct LateRequestFactory {
    adapter: Arc<ReadRuntimeAdapter<LateProvider>>,
    seals: Arc<AtomicUsize>,
}
impl ConnectorReadRequestControlFactory for LateRequestFactory {
    fn for_planning(
        &self,
        _: &ConnectorPlanningContext,
    ) -> Result<ConnectorReadRequestControl, ConnectorError> {
        Ok(
            ConnectorReadRequestControl::provider_reacquire_attempt_access(
                self.adapter.clone(),
                self.adapter.clone(),
                Arc::new(LateAccessSealer {
                    adapter: self.adapter.clone(),
                    seals: self.seals.clone(),
                }),
            ),
        )
    }
}
struct LateEncoder {
    catalog: CatalogHandle,
    relation_calls: Arc<AtomicUsize>,
}
impl LateEncoder {
    fn unused() -> ConnectorCodecError {
        ConnectorCodecError::new(
            ConnectorFieldPath::root("fixture"),
            ConnectorCodecErrorKind::Unsupported,
            "closed plan must not encode a relation",
        )
    }
}
impl ConnectorReadWireEncoder for LateEncoder {
    fn owner(&self) -> &str {
        "iceberg"
    }
    fn encode_relation_payload(
        &self,
        _: &ConnectorReadRelation,
    ) -> Result<ConnectorReadRelationPayload, ConnectorCodecError> {
        self.relation_calls.fetch_add(1, Ordering::AcqRel);
        Err(Self::unused())
    }
    fn encode_column_payload(
        &self,
        _: &ConnectorReadColumnHandle,
    ) -> Result<ConnectorEncodedPayload, ConnectorCodecError> {
        Ok(ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                ConnectorProviderId::parse("iceberg").unwrap(),
                self.catalog.clone(),
                ConnectorCodecCategory::ReadColumn,
                ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            Bytes::from_static(b"fixture-column-k"),
        ))
    }
    fn encode_transaction_payload(
        &self,
        _: &ConnectorReadTransactionHandle,
    ) -> Result<ConnectorEncodedPayload, ConnectorCodecError> {
        Err(Self::unused())
    }
    fn encode_split_payload(
        &self,
        _: &ConnectorReadSplit,
    ) -> Result<ConnectorReadSplitPayload, ConnectorCodecError> {
        Err(Self::unused())
    }
}

#[test]
fn actual_provider_freeze_keeps_local_backing_and_rejects_a_closed_plan_deposit() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let blocking = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
    let (control, root, window) =
        crate::task_execution::blocking_io::tests::admitted_class(ResultWindowClass::Local);
    let capacity =
        QueryResultCapacityBinding::try_new(&root.owner.scope(), window.retain_alias()).unwrap();
    let (freeze_started, freeze_entered) = mpsc::channel();
    let (release_freeze, freeze_held) = mpsc::channel();
    let (drop_started, drop_entered) = mpsc::channel();
    let (release_drop, drop_held) = mpsc::channel();
    let destroyed = Arc::new(AtomicBool::new(false));
    let (disarm, watch) = mpsc::channel();
    let rescue_freeze = release_freeze.clone();
    let rescue_drop = release_drop.clone();
    let watchdog = std::thread::spawn(move || {
        if watch.recv_timeout(Duration::from_secs(6)).is_err() {
            let _ = rescue_freeze.send(());
            let _ = rescue_drop.send(());
        }
    });
    let instance = ConnectorInstanceId::parse("catalog.analytics").unwrap();
    let catalog = CatalogHandle::new(instance.clone(), CatalogVersion::from_bytes([7; 32]));
    let provider = Arc::new(LateProvider {
        descriptor: ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: instance.clone(),
        },
        catalog: catalog.clone(),
        table: Mutex::new(Some(Arc::new(ActualFrozenTable {
            entered: drop_started,
            release: Mutex::new(drop_held),
            destroyed: destroyed.clone(),
        }))),
        freeze_started,
        freeze_release: Mutex::new(freeze_held),
        opens: AtomicUsize::new(0),
        freezes: AtomicUsize::new(0),
    });
    let adapter = Arc::new(ReadRuntimeAdapter::new(provider.clone()));
    let seals = Arc::new(AtomicUsize::new(0));
    let relation_calls = Arc::new(AtomicUsize::new(0));
    let props = CatalogProperties::new(
        catalog.clone(),
        provider.descriptor.provider_id.clone(),
        1,
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let generic = Arc::new(
        novarocks_catalog_application::test_support::test_control_binding_for(instance.clone(), 7)
            .with_catalog_properties(props.clone())
            .unwrap(),
    );
    let host = Arc::new(ConnectorControlHost::new());
    host.register_role_binding(
        ConnectorControlRoleBinding::try_new(
            NormalizedCatalogProperties::try_new(props).unwrap(),
            generic,
            Some(ConnectorControlReadBinding::new(
                adapter.clone(),
                adapter.clone(),
                Some(Arc::new(LateRequestFactory {
                    adapter: adapter.clone(),
                    seals: seals.clone(),
                })),
                Arc::new(LateEncoder {
                    catalog: catalog.clone(),
                    relation_calls: relation_calls.clone(),
                }),
            )),
            None,
        )
        .unwrap(),
    )
    .unwrap();
    let lease = host.acquire_current(&instance).unwrap();
    let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Int64, false),
    ]));
    let binding = bindings
        .resolve_or_insert_with_id(
            QueryTableBindingKey::strict_base(instance.as_str(), "db", "orders"),
            |id| {
                let resolved = novarocks_sql::planning::catalog::materialize_connector_read_table(
                    novarocks_sql::planning::catalog::ConnectorReadTableFacts {
                        catalog: instance.as_str().into(),
                        namespace: "db".into(),
                        table: "orders".into(),
                        columns: vec![novarocks_types::schema::ColumnDef {
                            name: "k".into(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        }],
                        iceberg_row_lineage_metadata_columns: Vec::new(),
                        schema: schema.clone(),
                        binding: id,
                        selector: ConnectorReadSelector::Current,
                        planning_facts: ConnectorTablePlanningFacts::empty(),
                    },
                )
                .unwrap()
                .into_resolved_table();
                let mut value = QueryTableBinding::local(resolved, id);
                value.admission = QueryTableBindingAdmission::Exact(lease.clone());
                value.scan_materialization = Some(QueryScanMaterialization {
                    table: novarocks_spi::connector::ConnectorTableHandle::try_new(
                        instance.clone(),
                        Bytes::from_static(b"orders"),
                    )
                    .unwrap(),
                    catalog_handle: catalog.clone(),
                    schema: schema.clone(),
                    selector: ConnectorReadSelector::Current,
                    mv_partition_selection: None,
                    statistics_pin: None,
                    planning_lease: lease.clone(),
                });
                Ok(value)
            },
        )
        .unwrap();
    let stop = ConnectorStopOwner::new();
    let context = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(10),
        stop.view(),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .unwrap();
    let port = FrontendProviderReadFacts::new(
        host.clone(),
        bindings.clone(),
        ConnectorSession::try_new("actual-port", "u", "UTC", "en_US", SystemTime::UNIX_EPOCH)
            .unwrap(),
        context,
        blocking.clone(),
        capacity.clone(),
    );
    let column = novarocks_sql::compiler::fixtures::provider_read_column_need(
        0,
        "k",
        novarocks_physical_plan::ValueType::new(arrow::datatypes::DataType::Int64, false),
        ConnectorValueType::BigInt,
    )
    .unwrap();
    let need = novarocks_sql::compiler::fixtures::provider_read_need(
        1,
        novarocks_physical_plan::ProviderReadOccurrenceId::new(1),
        binding,
        ProviderReadRelationNeed::Data {
            relation: TableIdentity::new(instance.as_str(), "db", "orders"),
            version: ProviderReadVersionNeed::Current,
        },
        vec![column],
        Vec::new(),
        None,
    )
    .unwrap();
    let delivered = Arc::new(AtomicBool::new(false));
    let published = delivered.clone();
    // Only this future owns the real plan sink; its destruction closes deposits.
    let mut waiter = runtime.spawn(async move {
        let sink = ReadAccessSink::new();
        let answer = port.resolve_provider_reads(&[need], &sink).await;
        published.store(true, Ordering::Release);
        (answer, sink.into_taken())
    });
    let entered = freeze_entered.recv_timeout(Duration::from_secs(2)).is_ok();
    stop.request_stop();
    root.owner.cancel(CancellationReason::Requested);
    control.expire_deadlines();
    // Retiring the original generation forbids new acquisition; it must not
    // substitute a new generation for the lease the in-flight read already owns.
    let retired = host.retire_current(&instance).is_ok();
    let no_new_generation = host.acquire_current(&instance).is_err();
    waiter.abort();
    let joined =
        runtime.block_on(async { tokio::time::timeout(Duration::from_secs(2), &mut waiter).await });
    let cancelled = matches!(joined, Ok(Err(ref e)) if e.is_cancelled());
    drop(joined);
    drop(waiter);
    drop(capacity);
    drop(bindings);
    drop(lease);
    root.owner.complete_after_terminal_cancel_settled();
    root.business.release();
    drop(window);
    let held_call = control.snapshot();
    let _ = release_freeze.send(());
    let table_drop_entered = drop_entered.recv_timeout(Duration::from_secs(2)).is_ok();
    let held_output = control.snapshot();
    let held = !destroyed.load(Ordering::Acquire);
    let _ = release_drop.send(());
    let _ = disarm.send(());
    let watchdog_joined = watchdog.join().is_ok();
    let deadline = Instant::now() + Duration::from_secs(2);
    let after = loop {
        let facts = control.snapshot();
        if facts.scopes.is_empty() || Instant::now() >= deadline {
            break facts;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    control.close_admission();
    let shutdown = control.shutdown();
    if let Some(error) = blocking.take_original_failure() {
        blocking.retire_original_failure(error);
    }
    drop(adapter);
    drop(host);
    drop(blocking);
    drop(runtime);
    assert!(
        entered
            && cancelled
            && retired
            && no_new_generation
            && table_drop_entered
            && held
            && watchdog_joined
    );
    for facts in [&held_call, &held_output] {
        assert_eq!(facts.root_responsibilities, 1);
        assert_eq!(facts.result_windows.held_positions, [0, 1, 0, 0]);
        assert!(
            facts
                .scopes
                .iter()
                .any(|s| s.parent.is_some() && !s.own_completed)
        );
    }
    assert_eq!(provider.opens.load(Ordering::Acquire), 1);
    assert_eq!(provider.freezes.load(Ordering::Acquire), 1);
    assert_eq!(
        seals.load(Ordering::Acquire),
        1,
        "actual freeze never reached capability sealing"
    );
    assert_eq!(
        relation_calls.load(Ordering::Acquire),
        0,
        "closed plan accepted a late capability and reached post-deposit relation encoding"
    );
    assert!(!delivered.load(Ordering::Acquire));
    assert!(destroyed.load(Ordering::Acquire));
    assert_eq!(after.result_windows.held_positions, [0; 4]);
    assert!(after.scopes.is_empty() && shutdown.is_ok());
}
