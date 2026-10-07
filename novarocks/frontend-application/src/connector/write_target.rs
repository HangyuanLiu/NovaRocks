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

//! Neutral write-target binding for SQL write statements.
//!
//! One statement resolves its write target exactly once, against a single
//! provider generation, and carries:
//!
//!   - the [`ConnectorTableMetadata`] that generation produced (neutral Arrow
//!     schema, bounded planning facts, opaque table handle);
//!   - the [`ConnectorControlPlanningLease`] that produced it, retained so the
//!     write lease derived from it acts on that same generation.
//!
//! Core never interprets the opaque handle. Physical write facts a writer
//! needs — staging location, sequence numbers, partition spec objects, commit
//! vocabulary, abort cleanup — are deliberately absent: they belong to Provider
//! write preparation, reached through the derived write lease.
//!
//! This is the write-path sibling of the MV refresh binding in
//! Frontend MV refresh target binding. The two are deliberately separate
//! types: the MV one additionally carries MV refresh-ledger identity
//! (refresh markers, bootstrap state, main-ancestor lineage) that has no
//! meaning for an INSERT or a row mutation.

use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorControlResolver, ConnectorRequestContext,
    ConnectorRowMutationIntent, ConnectorRowMutationPreparation,
    ConnectorRowMutationPreparationOutcome, ConnectorRowMutationPreparationRequest,
    ConnectorTableColumnRole, ConnectorTableColumnVisibility, ConnectorTableCurrentWriteFacts,
    ConnectorTableHandle, ConnectorTableIdentity, ConnectorTableMetadata, ConnectorTableResolution,
    ConnectorWriteLease, ConnectorWriteOperationId, ConnectorWriteTargetRef,
};

/// One write target, resolved once against a single provider generation.
///
/// Cloning is cheap: the metadata's schema is an `Arc` and the lease is a
/// handle onto an already-resolved generation.
///
/// A binding exists only for Current metadata: construction requires the
/// planning facts to carry the Current write authority and retains it, so a
/// historical read-only load can never become a write target.
///
/// Deliberately not `Debug`: neither [`ConnectorTableMetadata`] nor
/// [`ConnectorControlPlanningLease`] is `Debug`, precisely so an opaque
/// provider handle and a live generation cannot end up in a log line.
#[derive(Clone)]
pub struct ConnectorWriteTargetBinding {
    metadata: ConnectorTableMetadata,
    lease: ConnectorControlPlanningLease,
    /// The Current write authority of `metadata`, proven at construction.
    write_facts: ConnectorTableCurrentWriteFacts,
}

impl ConnectorWriteTargetBinding {
    /// Bind a write target to the metadata one generation produced.
    ///
    /// Historical read-only planning facts carry no write authority and are
    /// refused here, before any write lease, preparation or provider call.
    pub fn try_new(
        metadata: ConnectorTableMetadata,
        lease: ConnectorControlPlanningLease,
    ) -> Result<Self, String> {
        let write_facts = metadata
            .planning_facts
            .current_write_facts()
            .map_err(|refusal| {
                format!(
                    "connector write target `{}.{}` is not Current metadata: {refusal}",
                    metadata.identity.namespace, metadata.identity.table
                )
            })?
            .clone();
        Ok(Self {
            metadata,
            lease,
            write_facts,
        })
    }

    /// The Current write authority: write defaults, write-target types and
    /// partition source membership of this target.
    pub const fn current_write_facts(&self) -> &ConnectorTableCurrentWriteFacts {
        &self.write_facts
    }

    /// Names of the Current partition source columns.
    ///
    /// The ordinals index the target's own frozen schema, the schema the
    /// provider aligned them to, so each one is resolved there rather than
    /// against a filtered or strategy-specific column list.
    pub fn partition_source_column_names(&self) -> Result<Vec<String>, String> {
        self.write_facts
            .partition_source_column_ordinals()
            .iter()
            .map(|ordinal| {
                self.metadata
                    .schema
                    .fields()
                    .get(*ordinal as usize)
                    .map(|field| field.name().to_string())
                    .ok_or_else(|| {
                        format!(
                            "connector write target partition source ordinal {ordinal} is outside its frozen schema"
                        )
                    })
            })
            .collect()
    }

    /// The exact generation that produced every fact in this binding.
    ///
    /// Write preparation must derive its lease from this one rather than
    /// re-resolving `latest`, otherwise a concurrent commit could split one
    /// statement across two generations.
    pub const fn lease(&self) -> &ConnectorControlPlanningLease {
        &self.lease
    }

    pub const fn metadata(&self) -> &ConnectorTableMetadata {
        &self.metadata
    }

    /// Opaque provider handle. Core passes it through and never decodes it.
    pub const fn handle(&self) -> &ConnectorTableHandle {
        &self.metadata.table
    }

    pub const fn identity(&self) -> &ConnectorTableIdentity {
        &self.metadata.identity
    }

    /// The write target's neutral Arrow schema.
    ///
    /// This replaces reading `current_schema()` off a concrete provider table:
    /// column shaping, projection and default filling all work from here.
    pub fn arrow_schema(&self) -> &arrow::datatypes::SchemaRef {
        &self.metadata.schema
    }

    /// SQL-visible columns in the shape row DML must write.
    ///
    /// Visibility, row-lineage ownership and provider-declared write type
    /// overrides are all bounded neutral planning facts. Keeping this
    /// projection here avoids loading or decoding a concrete provider table in
    /// statement planning.
    pub fn dml_target_columns(&self) -> Vec<novarocks_types::schema::ColumnDef> {
        self.metadata
            .schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(ordinal, field)| {
                let fact = self.metadata.planning_facts.column_facts().get(ordinal);
                if matches!(
                    fact.map(|fact| fact.visibility()),
                    Some(ConnectorTableColumnVisibility::Hidden)
                ) || matches!(
                    fact.map(|fact| fact.role()),
                    Some(ConnectorTableColumnRole::RowLineageSystem)
                ) {
                    return None;
                }
                Some(novarocks_types::schema::ColumnDef {
                    name: field.name().to_string(),
                    data_type: self
                        .write_facts
                        .write_target_type(ordinal)
                        .cloned()
                        .unwrap_or_else(|| field.data_type().clone()),
                    nullable: field.is_nullable(),
                    write_default: None,
                    logical_type: None,
                })
            })
            .collect()
    }

    /// Derive the write lease for this statement from the same generation.
    pub fn derive_write_lease(&self) -> Result<ConnectorWriteLease, String> {
        self.lease
            .derive_write_lease()
            .map_err(|error| error.to_string())
    }

    /// Derive the NCP-6 write-stack lease from the same generation that
    /// resolved this target.
    ///
    /// It names the generation explicitly rather than resolving whatever is
    /// active now, because between planning and commit the active generation
    /// can be replaced, and committing through the replacement would attach
    /// staged work to a runtime that never admitted it.
    pub(crate) fn derive_write_stack_lease(
        &self,
        host: &novarocks_catalog_application::ConnectorControlHost,
    ) -> Result<novarocks_catalog_application::ConnectorWriteStackLease, String> {
        derive_write_stack_lease(host, &self.lease)
    }

    /// Ask the exact generation that resolved this target to sign one row
    /// mutation. The opaque table handle is passed through unchanged.
    pub fn prepare_row_mutation(
        &self,
        target_ref: &str,
        operation_id: ConnectorWriteOperationId,
        intent: ConnectorRowMutationIntent,
        context: ConnectorRequestContext,
    ) -> Result<(ConnectorWriteLease, ConnectorRowMutationPreparation), String> {
        let lease = self
            .derive_write_lease()
            .map_err(|error| format!("derive connector row-mutation write lease: {error}"))?;
        let preparation = match lease
            .prepare_row_mutation(ConnectorRowMutationPreparationRequest {
                operation_id,
                table: self.handle().clone(),
                target_ref: ConnectorWriteTargetRef::parse(target_ref).map_err(|error| {
                    format!("validate connector row-mutation target ref: {error}")
                })?,
                intent,
                context,
            })
            .map_err(|error| format!("prepare connector row mutation: {error}"))?
        {
            ConnectorRowMutationPreparationOutcome::Prepared(preparation) => preparation,
            ConnectorRowMutationPreparationOutcome::Denied(error) => {
                return Err(format!("connector row-mutation admission denied: {error}"));
            }
        };
        Ok((lease, preparation))
    }

    /// Resolve the current head used by the existing durable DML journal.
    ///
    /// This intentionally preserves the historical RefHead observation rather
    /// than claiming it is the opaque base sealed into a write preparation.
    /// Those two facts can differ if the external ref moves; harmonizing them
    /// is a separate lifecycle change.
    pub fn journal_ref_head_snapshot_id(
        &self,
        target_ref: &str,
        context: ConnectorRequestContext,
    ) -> Result<Option<i64>, String> {
        let facts = super::metadata_read_reference_facts_with_planning_lease(
            self.lease.clone(),
            context,
            self.identity().namespace.as_ref(),
            self.identity().table.as_ref(),
        )?;
        if target_ref == "main" {
            return Ok(facts.current_snapshot_id());
        }
        facts
            .named_references()
            .iter()
            .find(|reference| reference.name.as_ref() == target_ref)
            .map(|reference| Some(reference.snapshot_id))
            .ok_or_else(|| {
                format!("iceberg ref: branch '{target_ref}' not found in table metadata")
            })
    }
}

/// Resolve a SQL write target into a neutral binding.
///
/// Mirrors `load_mv_target_binding`: acquire one planning lease, then load the
/// table metadata through that same lease, so the schema, planning facts and
/// opaque handle cannot drift apart. The load is the Current load, and the
/// binding refuses anything but Current facts.
pub fn load_write_target_binding(
    controls: &dyn ConnectorControlResolver,
    catalog: &str,
    namespace: &str,
    table: &str,
    resolution: ConnectorTableResolution,
    context: ConnectorRequestContext,
) -> Result<ConnectorWriteTargetBinding, String> {
    let lease = super::acquire_metadata_planning_lease(controls, catalog)?;
    let metadata = super::metadata_load_connector_table_with_planning_lease(
        &lease, context, namespace, table, resolution,
    )?;
    ConnectorWriteTargetBinding::try_new(metadata, lease)
}

/// Derive the NCP-6 write-stack lease from a retained planning generation.
///
/// Statements that never build a [`ConnectorWriteTargetBinding`] -- the ones
/// that resolve their target through a row-mutation, rewrite, or MV binding --
/// reach the same generation through this function, so every write commits
/// through the incarnation that planned it rather than through whichever one
/// happens to be active at commit time.
pub(crate) fn derive_write_stack_lease(
    host: &novarocks_catalog_application::ConnectorControlHost,
    planning_lease: &ConnectorControlPlanningLease,
) -> Result<novarocks_catalog_application::ConnectorWriteStackLease, String> {
    host.acquire_exact_write_stack(planning_lease.control_runtime_id())
        .map_err(|error| format!("derive connector write-stack lease: {error}"))
}

/// Project a provider-signed input shape back into the request that produced
/// it.
///
/// A begin session must describe the same input the admission already signed.
/// Rebuilding the request from field names would let the two drift; projecting
/// the signed shape cannot.
pub(crate) fn write_input_request_for_shape(
    shape: &novarocks_spi::connector::ConnectorWriteInputShape,
) -> novarocks_spi::connector::ConnectorWriteInputRequest {
    use novarocks_spi::connector::{
        ConnectorWriteFieldRequest, ConnectorWriteInputRequest, ConnectorWriteInputShape,
    };

    fn requests(
        fields: &[novarocks_spi::connector::ConnectorWriteFieldBinding],
    ) -> Vec<ConnectorWriteFieldRequest> {
        fields
            .iter()
            .map(|field| ConnectorWriteFieldRequest::new(field.field().clone()))
            .collect()
    }

    match shape {
        ConnectorWriteInputShape::Data { fields } => ConnectorWriteInputRequest::Data {
            fields: requests(fields),
        },
        ConnectorWriteInputShape::RowLineage {
            data_fields,
            row_identity_fields,
        } => ConnectorWriteInputRequest::RowLineage {
            data_fields: requests(data_fields),
            row_identity_fields: requests(row_identity_fields),
        },
        ConnectorWriteInputShape::PositionDelete {
            identity_fields,
            partition_source_fields,
        } => ConnectorWriteInputRequest::PositionDelete {
            identity_fields: requests(identity_fields),
            partition_source_fields: requests(partition_source_fields),
        },
        ConnectorWriteInputShape::DeletionVector {
            identity_fields,
            partition_source_fields,
        } => ConnectorWriteInputRequest::DeletionVector {
            identity_fields: requests(identity_fields),
            partition_source_fields: requests(partition_source_fields),
        },
        ConnectorWriteInputShape::EqualityDelete { equality_fields } => {
            ConnectorWriteInputRequest::EqualityDelete {
                equality_fields: requests(equality_fields),
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorBeginScanRequest, ConnectorColumnDefault, ConnectorControlBinding, ConnectorError,
        ConnectorErrorKind, ConnectorExecutionDistribution, ConnectorInstanceDescriptor,
        ConnectorInstanceId, ConnectorListTablesRequest, ConnectorMetadata,
        ConnectorNamespaceRequest, ConnectorProviderBinding, ConnectorProviderId, ConnectorScan,
        ConnectorScanHandle, ConnectorScanPlanning, ConnectorSplitPlanningRequest,
        ConnectorSplitPlanningResult, ConnectorTableColumnPlanningFact,
        ConnectorTableDefinitionFacts, ConnectorTablePlanningFacts, ConnectorTableRequest,
        ConnectorWriteAuthorityRefusal, ProviderBindingEpoch,
    };

    use super::*;

    const CATALOG: &str = "ice";

    fn fact(ordinal: u32, role: ConnectorTableColumnRole) -> ConnectorTableColumnPlanningFact {
        let visibility = match role {
            ConnectorTableColumnRole::RowLineageSystem => ConnectorTableColumnVisibility::Hidden,
            _ => ConnectorTableColumnVisibility::Sql,
        };
        ConnectorTableColumnPlanningFact::new(
            ordinal,
            visibility,
            novarocks_spi::connector::ConnectorTableColumnSemanticKind::None,
            role,
        )
    }

    /// Metadata of `db.orders(id BIGINT, future INT)` plus one hidden
    /// row-lineage column. Current facts declare `future` as the only
    /// partition source and a write default on `id`; historical facts describe
    /// the same columns read-only.
    pub(crate) fn future_partition_metadata(historical: bool) -> ConnectorTableMetadata {
        let instance_id = ConnectorInstanceId::parse(CATALOG).expect("instance ID");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("future", DataType::Int32, true),
            Field::new("_row_id", DataType::Int64, true),
        ]));
        let context = crate::connector::test_request_context();
        let planning_facts = if historical {
            ConnectorTablePlanningFacts::try_new_historical_read_only(
                &schema,
                vec![
                    fact(0, ConnectorTableColumnRole::Ordinary),
                    fact(1, ConnectorTableColumnRole::Ordinary),
                    fact(2, ConnectorTableColumnRole::RowLineageSystem),
                ],
                Vec::new(),
                Vec::new(),
                &context,
            )
        } else {
            ConnectorTablePlanningFacts::try_new(
                &schema,
                vec![
                    fact(0, ConnectorTableColumnRole::Ordinary)
                        .with_write_default(Some(ConnectorColumnDefault::Int64(7))),
                    fact(1, ConnectorTableColumnRole::Ordinary),
                    fact(2, ConnectorTableColumnRole::RowLineageSystem),
                ],
                Vec::new(),
                Vec::new(),
                vec![1],
                &context,
            )
        }
        .expect("planning facts");
        ConnectorTableMetadata {
            identity: ConnectorTableIdentity {
                instance_id: instance_id.clone(),
                namespace: Arc::from("db"),
                table: Arc::from("orders"),
            },
            schema,
            planning_facts,
            definition_facts: ConnectorTableDefinitionFacts::empty(),
            version: None,
            statistics_data_version: None,
            table: ConnectorTableHandle::try_new(instance_id, Bytes::from_static(b"orders"))
                .expect("table handle"),
        }
    }

    pub(crate) fn test_lease() -> ConnectorControlPlanningLease {
        ConnectorControlPlanningLease::new(
            Arc::new(
                novarocks_catalog_application::test_support::test_control_binding_for(
                    ConnectorInstanceId::parse(CATALOG).expect("instance ID"),
                    7,
                ),
            ),
            || {},
        )
    }

    #[test]
    fn a_current_binding_retains_the_current_write_authority() {
        let binding =
            ConnectorWriteTargetBinding::try_new(future_partition_metadata(false), test_lease())
                .expect("Current metadata binds");

        assert_eq!(
            binding
                .current_write_facts()
                .partition_source_column_ordinals(),
            &[1]
        );
        assert_eq!(
            binding.partition_source_column_names(),
            Ok(vec!["future".to_string()])
        );
        assert_eq!(
            binding
                .dml_target_columns()
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "future"]
        );
        assert_eq!(
            binding.current_write_facts().write_default(0),
            Some(&ConnectorColumnDefault::Int64(7))
        );
    }

    #[test]
    fn a_historical_load_can_never_become_a_write_target() {
        let error =
            ConnectorWriteTargetBinding::try_new(future_partition_metadata(true), test_lease())
                .err()
                .expect("historical read-only facts carry no write authority");

        assert!(
            error.contains("connector write target `db.orders` is not Current metadata"),
            "{error}"
        );
        assert!(
            error.contains(&ConnectorWriteAuthorityRefusal.to_string()),
            "{error}"
        );
    }

    /// A provider whose Current load wrongly answers with historical facts.
    /// It counts loads and installs no write capability: the refusal must
    /// happen while binding, before any lease or preparation exists.
    struct HistoricalAnsweringProvider {
        instance_id: ConnectorInstanceId,
        incarnation: ProviderBindingEpoch,
        loads: AtomicUsize,
    }

    fn unsupported() -> ConnectorError {
        ConnectorError::new(ConnectorErrorKind::Unsupported, "not used by this test")
    }

    impl ConnectorMetadata for HistoricalAnsweringProvider {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.instance_id
        }

        fn namespace_exists(
            &self,
            _request: ConnectorNamespaceRequest,
        ) -> Result<bool, ConnectorError> {
            Err(unsupported())
        }

        fn table_exists(&self, _request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
            Err(unsupported())
        }

        fn list_tables(
            &self,
            _request: ConnectorListTablesRequest,
        ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
            Err(unsupported())
        }

        fn load_table(
            &self,
            _request: ConnectorTableRequest,
        ) -> Result<ConnectorTableMetadata, ConnectorError> {
            self.loads.fetch_add(1, Ordering::SeqCst);
            Ok(future_partition_metadata(true))
        }
    }

    impl ConnectorScanPlanning for HistoricalAnsweringProvider {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.instance_id
        }

        fn begin_scan(
            &self,
            _table: &ConnectorTableHandle,
            _request: ConnectorBeginScanRequest,
        ) -> Result<ConnectorScan, ConnectorError> {
            Err(unsupported())
        }

        fn plan_splits(
            &self,
            _scan: &ConnectorScanHandle,
            _request: ConnectorSplitPlanningRequest,
        ) -> Result<ConnectorSplitPlanningResult, ConnectorError> {
            Err(unsupported())
        }
    }

    impl ConnectorExecutionDistribution for HistoricalAnsweringProvider {
        fn declaration(
            &self,
            _context: &ConnectorRequestContext,
        ) -> Result<ConnectorProviderBinding, ConnectorError> {
            ConnectorProviderBinding::iceberg(
                self.instance_id.as_str(),
                self.incarnation.to_bytes(),
                "default",
            )
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
            })
        }
    }

    #[test]
    fn load_write_target_binding_refuses_historical_facts_before_any_effect() {
        let provider = Arc::new(HistoricalAnsweringProvider {
            instance_id: ConnectorInstanceId::parse(CATALOG).expect("instance ID"),
            incarnation: ProviderBindingEpoch::from_bytes([9; 16]),
            loads: AtomicUsize::new(0),
        });
        let binding = ConnectorControlBinding::try_new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
                instance_id: provider.instance_id.clone(),
            },
            provider.incarnation,
            provider.clone(),
            provider.clone(),
            provider.clone(),
            None,
        )
        .expect("control binding");
        let registry = crate::connector::fixture::FixtureConnectorRegistry::new();
        registry.register_fixture_control(binding);
        let controls = crate::connector::fixture::FixtureControlResolver::new(registry);

        let error = load_write_target_binding(
            &controls,
            CATALOG,
            "db",
            "orders",
            ConnectorTableResolution::StrictBaseTable,
            crate::connector::test_request_context(),
        )
        .err()
        .expect("a write path given historical facts is refused");

        assert!(
            error.contains("is not Current metadata")
                && error.contains("historical read-only and carry no current write authority"),
            "{error}"
        );
        assert_eq!(provider.loads.load(Ordering::SeqCst), 1);
    }
}
