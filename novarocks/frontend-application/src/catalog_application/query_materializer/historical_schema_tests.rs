// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use novarocks_spi::connector::*;
use std::sync::atomic::{AtomicUsize, Ordering};

struct SelectedMetadata {
    instance: ConnectorInstanceId,
    current_loads: AtomicUsize,
    selectors: Mutex<Vec<ConnectorReadSelector>>,
    reject_history: bool,
}

fn unsupported() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "selected metadata fixture refusal",
    )
}

fn selected_schema(historical: bool) -> Arc<Schema> {
    let reused = Field::new(
        "reused",
        if historical {
            DataType::Int16
        } else {
            DataType::Int32
        },
        true,
    )
    .with_metadata(HashMap::from([(
        "PARQUET:field_id".into(),
        if historical { "7" } else { "8" }.into(),
    )]));
    let json = Field::new("note", DataType::Utf8, true).with_metadata(HashMap::from([
        (
            novarocks_types::logical::NR_LOGICAL_TYPE_KEY.into(),
            "json".into(),
        ),
        ("PARQUET:field_id".into(), "6".into()),
    ]));
    Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::Struct(vec![Arc::new(json), Arc::new(reused)].into()),
        true,
    )]))
}

impl SelectedMetadata {
    fn metadata(&self, request: ConnectorTableRequest, historical: bool) -> ConnectorTableMetadata {
        assert_eq!(request.table.table.as_ref(), "orders");
        let schema = selected_schema(historical);
        let planning_facts = if historical {
            ConnectorTablePlanningFacts::try_new(
                &schema,
                vec![ConnectorTableColumnPlanningFact::new(
                    0,
                    ConnectorTableColumnVisibility::Sql,
                    ConnectorTableColumnSemanticKind::None,
                    ConnectorTableColumnRole::Ordinary,
                )],
                Vec::new(),
                Vec::new(),
                Vec::new(),
                &request.context,
            )
            .unwrap()
        } else {
            ConnectorTablePlanningFacts::empty()
        };
        let token = if historical {
            Bytes::from_static(b"snapshot-42")
        } else {
            Bytes::from_static(b"current")
        };
        ConnectorTableMetadata {
            identity: request.table,
            schema,
            planning_facts,
            definition_facts: ConnectorTableDefinitionFacts::empty(),
            version: Some(token.clone()),
            statistics_data_version: Some(StatisticsDataVersion::try_new(token.clone()).unwrap()),
            table: ConnectorTableHandle::try_new(self.instance.clone(), token).unwrap(),
        }
    }
}

impl ConnectorMetadata for SelectedMetadata {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance
    }
    fn namespace_exists(&self, _: ConnectorNamespaceRequest) -> Result<bool, ConnectorError> {
        Err(unsupported())
    }
    fn table_exists(&self, _: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        Err(unsupported())
    }
    fn list_tables(
        &self,
        _: ConnectorListTablesRequest,
    ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
        Err(unsupported())
    }
    fn load_table(
        &self,
        request: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError> {
        self.current_loads.fetch_add(1, Ordering::SeqCst);
        Ok(self.metadata(request, false))
    }
    fn load_table_for_read(
        &self,
        request: ConnectorTableRequest,
        selector: ConnectorReadSelector,
    ) -> Result<ConnectorTableMetadata, ConnectorError> {
        self.selectors.lock().unwrap().push(selector);
        match selector {
            ConnectorReadSelector::Current => self.load_table(request),
            ConnectorReadSelector::SnapshotId(42) if !self.reject_history => {
                Ok(self.metadata(request, true))
            }
            _ => Err(unsupported()),
        }
    }
}

impl ConnectorScanPlanning for SelectedMetadata {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance
    }
    fn begin_scan(
        &self,
        _: &ConnectorTableHandle,
        _: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError> {
        Err(unsupported())
    }
    fn plan_splits(
        &self,
        _: &ConnectorScanHandle,
        _: ConnectorSplitPlanningRequest,
    ) -> Result<ConnectorSplitPlanningResult, ConnectorError> {
        Err(unsupported())
    }
}

impl ConnectorExecutionDistribution for SelectedMetadata {
    fn declaration(
        &self,
        _: &ConnectorRequestContext,
    ) -> Result<ConnectorProviderBinding, ConnectorError> {
        ConnectorProviderBinding::iceberg(self.instance.as_str(), [0; 16], "default").map_err(
            |error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string()),
        )
    }
}

fn loader_fixture(
    reject_history: bool,
) -> (
    Arc<SelectedMetadata>,
    crate::connector::FixtureControlResolver,
) {
    let instance = ConnectorInstanceId::parse("history_fixture").unwrap();
    let provider = Arc::new(SelectedMetadata {
        instance: instance.clone(),
        current_loads: AtomicUsize::new(0),
        selectors: Mutex::new(Vec::new()),
        reject_history,
    });
    let provider_id = ConnectorProviderId::parse("iceberg").unwrap();
    let binding = ConnectorControlBinding::try_new(
        ConnectorInstanceDescriptor {
            provider_id: provider_id.clone(),
            instance_id: instance.clone(),
        },
        ProviderBindingEpoch::from_bytes([0; 16]),
        provider.clone(),
        provider.clone(),
        provider.clone(),
        None,
    )
    .unwrap()
    .with_catalog_properties(
        CatalogProperties::new(
            CatalogHandle::new(instance, CatalogVersion::from_bytes([1; 32])),
            provider_id,
            1,
            Vec::new(),
            Vec::new(),
        )
        .unwrap(),
    )
    .unwrap();
    let registry = crate::connector::FixtureConnectorRegistry::new();
    registry.register_fixture_control(binding);
    (
        provider,
        crate::connector::FixtureControlResolver::new(registry),
    )
}

fn context() -> ConnectorRequestContext {
    crate::connector::connector_request_context(None, ConnectorStopOwner::new().view()).unwrap()
}

fn binding_id() -> SqlTableBindingId {
    novarocks_sql::binding::SqlTableBindingAllocator::try_new_for_test(
        std::num::NonZeroU64::new(77).unwrap(),
    )
    .unwrap()
    .allocate()
    .unwrap()
}

#[test]
fn historical_schema_overlay_freezes_selected_sql_scan_source_and_statistics_facts() {
    let (provider, controls) = loader_fixture(false);
    let loader = iceberg_table_binding_loader(&controls, context());
    let binding = loader
        .load_strict_base_table(
            "history_fixture",
            "db",
            "__sqlx1_tt_orders_42",
            binding_id(),
        )
        .unwrap();
    assert_eq!(
        *provider.selectors.lock().unwrap(),
        vec![ConnectorReadSelector::SnapshotId(42)]
    );
    assert_eq!(
        provider.current_loads.load(Ordering::SeqCst),
        0,
        "history must not first load current"
    );
    let expected = selected_schema(true);
    let sql = novarocks_sql::planning::catalog::catalog_table(&binding.resolved);
    assert_eq!(
        sql.columns[0].data_type,
        expected.field(0).data_type().clone()
    );
    let scan = binding.scan_materialization.as_ref().unwrap();
    assert_eq!(scan.schema.as_ref(), expected.as_ref());
    assert_eq!(scan.selector, ConnectorReadSelector::SnapshotId(42));
    let source = binding.source_metadata.as_ref().unwrap();
    assert_eq!(source.schema.as_ref(), expected.as_ref());
    assert_eq!(source.version.as_ref().unwrap().as_ref(), b"snapshot-42");
    assert_eq!(source.planning_facts.column_facts().len(), 1);
    let statistics = binding.statistics_pin.as_ref().unwrap();
    assert_eq!(statistics.data_version.as_bytes().as_ref(), b"snapshot-42");
    assert_eq!(statistics.table, source.table);
    assert_eq!(scan.table, source.table);
}

#[test]
fn historical_schema_current_loader_retains_the_existing_current_contract() {
    let (provider, controls) = loader_fixture(false);
    let materialization = crate::catalog_application::query_catalog::load_connector_table_materialization_with_lease_typed(
        &controls, context(), "history_fixture", "db", "orders",
    ).unwrap();
    assert_eq!(
        *provider.selectors.lock().unwrap(),
        vec![ConnectorReadSelector::Current]
    );
    assert_eq!(provider.current_loads.load(Ordering::SeqCst), 1);
    assert_eq!(
        materialization.read_selector,
        ConnectorReadSelector::Current
    );
    assert_eq!(
        materialization.read_schema.as_ref(),
        selected_schema(false).as_ref()
    );
    assert_eq!(
        materialization.source_metadata.version.unwrap().as_ref(),
        b"current"
    );
}

#[test]
fn historical_schema_provider_refusal_never_retries_as_current() {
    let (provider, controls) = loader_fixture(true);
    let loader = iceberg_table_binding_loader(&controls, context());
    let result = loader.load_strict_base_table(
        "history_fixture",
        "db",
        "__sqlx1_tt_orders_42",
        binding_id(),
    );
    let error = match result {
        Ok(_) => panic!("historical refusal must fail"),
        Err(error) => error,
    };
    assert!(matches!(error, CatalogResolutionError::Failed { .. }));
    assert!(error.message().contains("Unsupported"));
    assert_eq!(
        *provider.selectors.lock().unwrap(),
        vec![ConnectorReadSelector::SnapshotId(42)]
    );
    assert_eq!(provider.current_loads.load(Ordering::SeqCst), 0);
}
