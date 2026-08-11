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
pub(crate) mod backend;
pub mod cleanup_maintenance;
pub(crate) mod data_mutation;
pub mod distributed_rewrite_application;
pub mod file_execution;
pub mod iceberg;
pub mod metadata_maintenance;
pub mod mutation;
pub mod runtime;
pub(crate) mod scan_model;
pub mod schema;
pub(crate) mod stats;
pub(crate) mod unified_statistics;

pub(crate) use backend::MvBackend;
#[cfg(test)]
pub(crate) use iceberg::catalog::load_table as load_iceberg_table;
pub(crate) use iceberg::catalog::{
    IcebergCatalogRegistry, namespace_exists as iceberg_namespace_exists,
};
#[cfg(test)]
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use novarocks_spi::connector::{
    ConnectorCancellation, ConnectorInstanceId, ConnectorListNamespacesRequest,
    ConnectorNamespaceIdentity, ConnectorReadReferenceFacts, ConnectorReadReferenceFactsRequest,
    ConnectorRequestContext, ConnectorTableIdentity, ConnectorTableRequest,
    ConnectorTableResolution,
};

struct RequestConnectorCancellation {
    signal: Arc<AtomicBool>,
}

impl ConnectorCancellation for RequestConnectorCancellation {
    fn is_cancelled(&self) -> bool {
        self.signal.load(Ordering::SeqCst)
    }
}

struct QueryConnectorCancellation {
    cancellation: crate::query_execution::cancellation::QueryCancellationView,
}

impl ConnectorCancellation for QueryConnectorCancellation {
    fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

fn build_connector_request_context(
    query_options: Option<&novarocks_execution::runtime::query_options::QueryOptions>,
    cancellation: Arc<dyn ConnectorCancellation>,
) -> Result<ConnectorRequestContext, String> {
    let (_, query_expire) =
        novarocks_execution::runtime::query_options::query_expire_durations(query_options);
    ConnectorRequestContext::try_new(
        Instant::now() + query_expire,
        cancellation,
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map_err(|error| error.to_string())
}

pub(crate) fn connector_request_context(
    query_options: Option<&novarocks_execution::runtime::query_options::QueryOptions>,
    cancellation_signal: Arc<AtomicBool>,
) -> Result<ConnectorRequestContext, String> {
    build_connector_request_context(
        query_options,
        Arc::new(RequestConnectorCancellation {
            signal: cancellation_signal,
        }),
    )
}

pub(crate) fn connector_request_context_for_query(
    query_options: Option<&novarocks_execution::runtime::query_options::QueryOptions>,
    cancellation: crate::query_execution::cancellation::QueryCancellationView,
) -> Result<ConnectorRequestContext, String> {
    build_connector_request_context(
        query_options,
        Arc::new(QueryConnectorCancellation { cancellation }),
    )
}

/// Derive connector admission from the immutable query execution captured by
/// the frontend. A request deadline is authoritative; only requests without an
/// admission deadline use the bounded connector fallback.
pub fn connector_request_context_for_execution(
    query_options: Option<&novarocks_execution::runtime::query_options::QueryOptions>,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
) -> Result<ConnectorRequestContext, String> {
    let cancellation: Arc<dyn ConnectorCancellation> = Arc::new(QueryConnectorCancellation {
        cancellation: execution.cancellation().clone(),
    });
    match execution.deadline() {
        Some(deadline) => ConnectorRequestContext::try_new(
            deadline,
            cancellation,
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .map_err(|error| error.to_string()),
        None => build_connector_request_context(query_options, cancellation),
    }
}

pub(crate) fn validate_request_context(context: &ConnectorRequestContext) -> Result<(), String> {
    if context.cancellation().is_cancelled() {
        return Err("connector request was cancelled".to_string());
    }
    if Instant::now() >= context.deadline() {
        return Err("connector request deadline elapsed".to_string());
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn test_request_context() -> ConnectorRequestContext {
    connector_request_context(None, Arc::new(AtomicBool::new(false)))
        .expect("test connector request context")
}

#[cfg(test)]
mod request_context_tests {
    use std::time::{Duration, Instant};

    use super::connector_request_context_for_execution;
    use crate::common::app_config::ClusterRole;
    use crate::query_execution::backend::BackendTopologySnapshot;
    use crate::query_execution::cancellation::{QueryCancellationReason, QueryCancellationSource};
    use crate::query_execution::request_context::{RequestAdmission, RequestContext};
    use crate::sql::optimizer::options::SessionOptimizerSettings;

    #[test]
    fn connector_context_preserves_admitted_deadline_and_cancellation() {
        let cancellation = QueryCancellationSource::new();
        let deadline = Instant::now() + Duration::from_secs(17);
        let request = RequestContext::admit(RequestAdmission::new(
            None,
            "db".to_string(),
            ClusterRole::Fe,
            BackendTopologySnapshot::empty(41),
            Some(deadline),
            cancellation.view(),
            SessionOptimizerSettings::default(),
        ));

        let connector = connector_request_context_for_execution(None, request.execution()).unwrap();
        assert_eq!(connector.deadline(), deadline);
        assert!(!connector.cancellation().is_cancelled());

        cancellation.request(QueryCancellationReason::ClientDisconnected);
        assert!(request.execution().cancellation().is_cancelled());
        assert!(connector.cancellation().is_cancelled());
    }

    #[test]
    fn connector_context_without_admitted_deadline_uses_bounded_fallback() {
        let request = RequestContext::admit(RequestAdmission::new(
            None,
            "db".to_string(),
            ClusterRole::Fe,
            BackendTopologySnapshot::empty(43),
            None,
            QueryCancellationSource::new().view(),
            SessionOptimizerSettings::default(),
        ));
        let before = Instant::now();
        let connector = connector_request_context_for_execution(None, request.execution()).unwrap();
        assert!(connector.deadline() > before);
    }
}

fn metadata_binding(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: &str,
) -> Result<novarocks_spi::connector::ConnectorControlPlanningLease, String> {
    let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())?;
    controls
        .acquire_current(&instance_id)
        .map_err(|error| error.to_string())
}

pub(crate) fn metadata_namespace_exists(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
) -> Result<bool, String> {
    let binding = metadata_binding(controls, catalog)?;
    let instance_id = binding.binding().descriptor().instance_id.clone();
    binding
        .binding()
        .metadata()
        .namespace_exists(novarocks_spi::connector::ConnectorNamespaceRequest {
            namespace: novarocks_spi::connector::ConnectorNamespaceIdentity {
                instance_id,
                namespace: Arc::from(namespace),
            },
            context,
        })
        .map_err(|error| error.to_string())
}

pub(crate) fn metadata_table_exists(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> Result<bool, String> {
    let binding = metadata_binding(controls, catalog)?;
    metadata_table_exists_with_planning_lease(binding, context, namespace, table)
}

/// Resolve table existence through an admission-frozen planning lease.  A
/// caller that performs a table-or-view decision must retain this lease for
/// every metadata lookup in that decision.
pub(crate) fn metadata_table_exists_with_planning_lease(
    binding: novarocks_spi::connector::ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
    namespace: &str,
    table: &str,
) -> Result<bool, String> {
    let instance_id = binding.binding().descriptor().instance_id.clone();
    binding
        .binding()
        .metadata()
        .table_exists(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(namespace),
                table: Arc::from(table),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context,
        })
        .map_err(|error| error.to_string())
}

/// Enumerate namespaces through an admission-frozen connector control lease.
/// Ordering and duplicate handling stay application-owned so providers only
/// expose their authoritative catalog facts.
pub(crate) fn metadata_list_namespaces_with_planning_lease(
    binding: novarocks_spi::connector::ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
) -> Result<Vec<ConnectorNamespaceIdentity>, String> {
    let instance_id = binding.binding().descriptor().instance_id.clone();
    binding
        .binding()
        .metadata()
        .list_namespaces(ConnectorListNamespacesRequest {
            instance_id,
            context,
        })
        .map_err(|error| error.to_string())
}

/// Read immutable branch/tag/snapshot facts through the same exact lease that
/// admitted the table.  SQL owns the projection of these neutral facts.
pub(crate) fn metadata_read_reference_facts_with_planning_lease(
    binding: novarocks_spi::connector::ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
    namespace: &str,
    table: &str,
) -> Result<ConnectorReadReferenceFacts, String> {
    let instance_id = binding.binding().descriptor().instance_id.clone();
    binding
        .binding()
        .metadata()
        .read_reference_facts(ConnectorReadReferenceFactsRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(namespace),
                table: Arc::from(table),
            },
            context,
        })
        .map_err(|error| error.to_string())
}

pub(crate) fn metadata_load_table(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
    resolution: ConnectorTableResolution,
) -> Result<(backend::ResolvedTable, Option<i32>), String> {
    let binding = metadata_binding(controls, catalog)?;
    metadata_load_table_with_planning_lease(binding, context, namespace, table, resolution)
}

/// Resolve metadata through an admission-frozen planning lease.  Write
/// callers use this instead of reopening `acquire_current` after target
/// admission.
pub(crate) fn metadata_load_table_with_planning_lease(
    binding: novarocks_spi::connector::ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
    namespace: &str,
    table: &str,
    resolution: ConnectorTableResolution,
) -> Result<(backend::ResolvedTable, Option<i32>), String> {
    let instance_id = binding.binding().descriptor().instance_id.clone();
    let metadata = binding
        .binding()
        .metadata()
        .load_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(namespace),
                table: Arc::from(table),
            },
            resolution,
            context,
        })
        .map_err(|error| error.to_string())?;
    let columns = sql_columns_from_connector_schema(&metadata.schema, &metadata.planning_facts);
    let schema_id = metadata.version.as_ref().and_then(|version| {
        <[u8; 4]>::try_from(version.as_ref())
            .ok()
            .map(i32::from_le_bytes)
    });
    Ok((
        backend::ResolvedTable {
            catalog: metadata.identity.instance_id.as_str().to_string(),
            namespace: metadata.identity.namespace.to_string(),
            table: metadata.identity.table.to_string(),
            columns,
            statistics_pin: metadata
                .statistics_data_version
                .clone()
                .map(|data_version| backend::ResolvedTableStatisticsPin {
                    table: metadata.table.clone(),
                    data_version,
                }),
        },
        schema_id,
    ))
}

/// Load the exact connector metadata admitted by a retained planning lease.
///
/// Consumers that need provider-owned interpretation must pass this value to
/// a composition-injected application port.  They must not decode the opaque
/// table handle or reopen the current connector generation themselves.
pub(crate) fn metadata_load_connector_table_with_planning_lease(
    binding: &novarocks_spi::connector::ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
    namespace: &str,
    table: &str,
    resolution: ConnectorTableResolution,
) -> Result<novarocks_spi::connector::ConnectorTableMetadata, String> {
    let instance_id = binding.binding().descriptor().instance_id.clone();
    binding
        .binding()
        .metadata()
        .load_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(namespace),
                table: Arc::from(table),
            },
            resolution,
            context,
        })
        .map_err(|error| error.to_string())
}

fn sql_columns_from_connector_schema(
    schema: &arrow::datatypes::Schema,
    planning_facts: &novarocks_spi::connector::ConnectorTablePlanningFacts,
) -> Vec<novarocks_catalog::schema::ColumnDef> {
    schema
        .fields()
        .iter()
        .enumerate()
        // The ordinal must index the frozen schema, not this filtered view:
        // planning facts are aligned to the schema Core received, and hidden
        // fields keep their position in it.
        .filter(|(_, field)| {
            field
                .metadata()
                .get(novarocks_spi::connector::CONNECTOR_FIELD_HIDDEN_FROM_SQL)
                .is_none_or(|value| !value.eq_ignore_ascii_case("true"))
        })
        .map(|(ordinal, field)| novarocks_catalog::schema::ColumnDef {
            name: field.name().clone(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            write_default: connector_write_default_at(planning_facts, ordinal),
            logical_type: None,
        })
        .collect()
}

/// Read the write default a provider published for one frozen schema ordinal.
///
/// Facts are optional by contract: a provider with no column defaults returns
/// empty facts, and the column then behaves exactly as it did before defaults
/// were expressible.
pub(crate) fn connector_write_default_at(
    planning_facts: &novarocks_spi::connector::ConnectorTablePlanningFacts,
    ordinal: usize,
) -> Option<novarocks_catalog::schema::ColumnDefault> {
    planning_facts
        .column_facts()
        .get(ordinal)
        .and_then(|fact| fact.write_default())
        .map(connector_default_to_column_default)
}

/// Project the sealed SPI default value onto the neutral catalog value.
///
/// The two vocabularies are variant-for-variant identical; they are separate
/// types only because the SPI dependency ceiling admits no application value
/// crate.
pub(crate) fn connector_default_to_column_default(
    value: &novarocks_spi::connector::ConnectorColumnDefault,
) -> novarocks_catalog::schema::ColumnDefault {
    use novarocks_catalog::schema::ColumnDefault;
    use novarocks_spi::connector::ConnectorColumnDefault as Spi;

    match value {
        Spi::Null => ColumnDefault::Null,
        Spi::Boolean(value) => ColumnDefault::Boolean(*value),
        Spi::Int32(value) => ColumnDefault::Int32(*value),
        Spi::Int64(value) => ColumnDefault::Int64(*value),
        Spi::Float32 { bits } => ColumnDefault::Float32 { bits: *bits },
        Spi::Float64 { bits } => ColumnDefault::Float64 { bits: *bits },
        Spi::Decimal {
            unscaled,
            precision,
            scale,
        } => ColumnDefault::Decimal {
            unscaled: *unscaled,
            precision: *precision,
            scale: *scale,
        },
        Spi::String(text) => ColumnDefault::String(text.to_string()),
        Spi::Binary(bytes) => ColumnDefault::Binary(bytes.to_vec()),
        Spi::Date { days_since_epoch } => ColumnDefault::Date {
            days_since_epoch: *days_since_epoch,
        },
        Spi::TimeMicros {
            micros_since_midnight,
        } => ColumnDefault::TimeMicros {
            micros_since_midnight: *micros_since_midnight,
        },
        Spi::TimestampMicros { micros_since_epoch } => ColumnDefault::TimestampMicros {
            micros_since_epoch: *micros_since_epoch,
        },
        Spi::TimestamptzMicros { micros_since_epoch } => ColumnDefault::TimestamptzMicros {
            micros_since_epoch: *micros_since_epoch,
        },
        Spi::TimestampNanos { nanos_since_epoch } => ColumnDefault::TimestampNanos {
            nanos_since_epoch: *nanos_since_epoch,
        },
        Spi::TimestamptzNanos { nanos_since_epoch } => ColumnDefault::TimestamptzNanos {
            nanos_since_epoch: *nanos_since_epoch,
        },
        Spi::Uuid(bytes) => ColumnDefault::Uuid(*bytes),
        Spi::Fixed { size, bytes } => ColumnDefault::Fixed {
            size: *size,
            bytes: bytes.to_vec(),
        },
        Spi::Struct(fields) => ColumnDefault::Struct(
            fields
                .iter()
                .map(|(name, field_value)| {
                    (
                        name.to_string(),
                        connector_default_to_column_default(field_value),
                    )
                })
                .collect(),
        ),
        Spi::Array(elements) => ColumnDefault::Array(
            elements
                .iter()
                .map(connector_default_to_column_default)
                .collect(),
        ),
        Spi::Map(entries) => ColumnDefault::Map(
            entries
                .iter()
                .map(|(key, entry_value)| {
                    (
                        connector_default_to_column_default(key),
                        connector_default_to_column_default(entry_value),
                    )
                })
                .collect(),
        ),
    }
}

pub(crate) fn acquire_metadata_planning_lease(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: &str,
) -> Result<novarocks_spi::connector::ConnectorControlPlanningLease, String> {
    metadata_binding(controls, catalog)
}

pub(crate) use novarocks_execution::exec::min_max_predicate::{
    MinMaxPredicate, MinMaxPredicateValue,
};

pub use crate::connector::file_execution::FileScanRange;
pub use crate::formats::orc::OrcScanConfig;
pub use crate::formats::parquet::ParquetScanConfig;

#[cfg(test)]
mod iceberg_provider_test;
#[cfg(test)]
mod runtime_test;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn standalone_catalog_service_keeps_internal_entry_after_backend_registration() {
        let state = Arc::new(crate::engine::StandaloneState::default());
        super::register_standalone_backends(&state);

        let registry = state
            .catalog_service
            .registry()
            .read()
            .expect("catalog service registry");
        assert!(registry.get_catalog("default_catalog").is_ok());
    }

    #[test]
    fn spi5b_sql_target_columns_exclude_connector_hidden_read_fields() {
        let schema = Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("_row_id", DataType::Int64, false).with_metadata(HashMap::from([(
                novarocks_spi::connector::CONNECTOR_FIELD_HIDDEN_FROM_SQL.to_string(),
                "true".to_string(),
            )])),
        ]);

        let columns = super::sql_columns_from_connector_schema(
            &schema,
            &novarocks_spi::connector::ConnectorTablePlanningFacts::empty(),
        );
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name, "value");
    }

    /// SPI-5G equivalence gate.
    ///
    /// Every write default now reaches Core through the provider's planning
    /// facts instead of a Core-side Iceberg literal decode. This asserts the
    /// full `Iceberg literal -> provider -> SPI value -> catalog value` round
    /// trip is lossless for every variant the neutral vocabulary admits, so the
    /// value INSERT materializes for an omitted column is unchanged.
    #[test]
    fn spi5g_write_default_round_trip_matches_direct_iceberg_decode() {
        use novarocks_catalog::schema::ColumnDefault;
        use novarocks_connector_iceberg::default_value::{
            column_default_to_connector_default, iceberg_literal_to_column_default,
        };
        use novarocks_connector_iceberg::iceberg::spec::{
            Literal, NestedField, PrimitiveType, Type,
        };

        let cases: Vec<(&str, Type, Literal)> = vec![
            (
                "bool",
                Type::Primitive(PrimitiveType::Boolean),
                Literal::bool(true),
            ),
            ("i32", Type::Primitive(PrimitiveType::Int), Literal::int(-7)),
            (
                "i64",
                Type::Primitive(PrimitiveType::Long),
                Literal::long(1_i64 << 40),
            ),
            (
                "f32",
                Type::Primitive(PrimitiveType::Float),
                Literal::float(1.5f32),
            ),
            (
                "f64",
                Type::Primitive(PrimitiveType::Double),
                Literal::double(-2.25f64),
            ),
            (
                "text",
                Type::Primitive(PrimitiveType::String),
                Literal::string("fallback"),
            ),
            (
                "blob",
                Type::Primitive(PrimitiveType::Binary),
                Literal::binary(vec![0u8, 255u8]),
            ),
            (
                "day",
                Type::Primitive(PrimitiveType::Date),
                Literal::int(19_000),
            ),
        ];

        for (name, iceberg_type, literal) in cases {
            let direct: ColumnDefault = iceberg_literal_to_column_default(&literal, &iceberg_type)
                .unwrap_or_else(|error| panic!("column `{name}` direct decode failed: {error}"));

            // The path SPI-5G introduces: provider projects onto the sealed SPI
            // value, Core projects it back onto the neutral catalog value.
            let round_tripped = super::connector_default_to_column_default(
                &column_default_to_connector_default(&direct),
            );

            assert_eq!(
                round_tripped, direct,
                "column `{name}` changed while crossing the SPI boundary"
            );

            // The field is only used to prove the fixture is a legal Iceberg
            // column carrying this default.
            let _ = NestedField::optional(1, name, iceberg_type).with_write_default(literal);
        }
    }

    /// Nested defaults are the ones a flat projection would silently truncate.
    #[test]
    fn spi5g_nested_write_default_round_trip_is_lossless() {
        use novarocks_catalog::schema::ColumnDefault;
        use novarocks_connector_iceberg::default_value::column_default_to_connector_default;

        let nested = ColumnDefault::Struct(vec![
            (
                "list".to_string(),
                ColumnDefault::Array(vec![
                    ColumnDefault::Int32(1),
                    ColumnDefault::String("two".to_string()),
                ]),
            ),
            (
                "map".to_string(),
                ColumnDefault::Map(vec![(
                    ColumnDefault::String("k".to_string()),
                    ColumnDefault::Fixed {
                        size: 3,
                        bytes: vec![1, 2, 3],
                    },
                )]),
            ),
            ("uuid".to_string(), ColumnDefault::Uuid([9u8; 16])),
        ]);

        assert_eq!(
            super::connector_default_to_column_default(&column_default_to_connector_default(
                &nested
            )),
            nested
        );
    }

    /// Non-finite floats survive because both vocabularies carry the raw bit
    /// pattern rather than the float value.
    #[test]
    fn spi5g_non_finite_write_default_round_trip_preserves_bits() {
        use novarocks_catalog::schema::ColumnDefault;
        use novarocks_connector_iceberg::default_value::column_default_to_connector_default;

        for value in [
            ColumnDefault::Float32 {
                bits: f32::NAN.to_bits(),
            },
            ColumnDefault::Float64 {
                bits: f64::NEG_INFINITY.to_bits(),
            },
        ] {
            assert_eq!(
                super::connector_default_to_column_default(&column_default_to_connector_default(
                    &value
                )),
                value
            );
        }
    }
}

#[derive(Clone)]
pub struct ConnectorRegistry {
    mv_backends: HashMap<&'static str, Arc<dyn MvBackend>>,
    #[cfg(test)]
    fixture_controls: Arc<
        Mutex<
            BTreeMap<ConnectorInstanceId, Arc<novarocks_spi::connector::ConnectorControlBinding>>,
        >,
    >,
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self {
            mv_backends: HashMap::new(),
            #[cfg(test)]
            fixture_controls: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    #[cfg(test)]
    pub(crate) fn register_fixture_control(
        &self,
        binding: novarocks_spi::connector::ConnectorControlBinding,
    ) {
        self.fixture_controls
            .lock()
            .expect("fixture connector control lock")
            .insert(binding.descriptor().instance_id.clone(), Arc::new(binding));
    }

    #[cfg(test)]
    fn acquire_fixture_control(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorControlPlanningLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .fixture_controls
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "fixture connector control lock poisoned",
                )
            })?
            .get(instance_id)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    "test fixture did not register a connector control binding",
                )
            })?;
        Ok(novarocks_spi::connector::ConnectorControlPlanningLease::new(binding, || {}))
    }

    pub(crate) fn register_mv_backend(&mut self, backend: Arc<dyn MvBackend>) {
        self.mv_backends.insert(backend.name(), backend);
    }

    pub(crate) fn mv_backend(&self, name: &str) -> Result<Arc<dyn MvBackend>, String> {
        self.mv_backends
            .get(name)
            .cloned()
            .ok_or_else(|| format!("unknown MV backend: {name}"))
    }

    pub(crate) fn mv_backends(&self) -> Vec<Arc<dyn MvBackend>> {
        let mut entries: Vec<_> = self.mv_backends.iter().collect();
        entries.sort_by(|(left, _), (right, _)| left.cmp(right));
        entries
            .into_iter()
            .map(|(_, backend)| Arc::clone(backend))
            .collect()
    }
}

/// Test-only resolver for fixtures that explicitly register a control binding.
#[cfg(test)]
pub(crate) struct FixtureControlResolver {
    registry: ConnectorRegistry,
}

#[cfg(test)]
impl FixtureControlResolver {
    pub(crate) fn new(registry: ConnectorRegistry) -> Self {
        Self { registry }
    }
}

#[cfg(test)]
impl novarocks_spi::connector::ConnectorControlResolver for FixtureControlResolver {
    fn observe_current_binding(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorExecutionBindingKey,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .registry
            .fixture_controls
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "fixture connector control lock poisoned",
                )
            })?
            .get(instance_id)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    "test fixture did not register a connector control binding",
                )
            })?;
        Ok(novarocks_spi::connector::ConnectorExecutionBindingKey {
            instance_id: binding.descriptor().instance_id.clone(),
            incarnation: binding.incarnation(),
        })
    }

    fn acquire_current(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorControlPlanningLease,
        novarocks_spi::connector::ConnectorError,
    > {
        self.registry.acquire_fixture_control(instance_id)
    }
}

pub(crate) fn register_standalone_backends(state: &Arc<crate::engine::StandaloneState>) {
    {
        let mut connectors = state
            .connectors
            .write()
            .expect("standalone connector registry write lock");
        connectors.register_mv_backend(Arc::new(
            crate::engine::mv::iceberg_backend::IcebergMvBackend::new(state),
        ));
    }
}

impl Default for ConnectorRegistry {
    fn default() -> Self {
        ConnectorRegistry::new()
    }
}

impl std::fmt::Debug for ConnectorRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut mv_backends: Vec<_> = self.mv_backends.keys().copied().collect();
        mv_backends.sort();
        f.debug_struct("ConnectorRegistry")
            .field("mv_backends", &mv_backends)
            .finish()
    }
}
