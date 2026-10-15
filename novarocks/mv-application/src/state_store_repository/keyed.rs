// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Keyed projections on the single Accelerator owner and canonical indexes.
use super::codec::{decode_keyed_projection, encode_keyed_projection};
use super::*;
use crate::persistence::keyed_documents::{MvKeyedDocumentProjection, StoredKeyedMvProjection};
use crate::repository::MvProjectionVersion;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadedKeyedMvProjection {
    pub projection: StoredKeyedMvProjection,
    pub version: MvProjectionVersion,
}

impl StateStoreMvRepository {
    pub async fn find_keyed_by_target(
        &self,
        target: &MvTarget,
        budget: crate::persistence::validation::PersistenceDecodeBudget,
    ) -> Result<Option<LoadedKeyedMvProjection>, MvRepositoryError> {
        let key = target_key(target).map_err(corruption)?;
        let Some(lookup) = self.read_record(&key).await? else {
            return Ok(None);
        };
        let lookup: DecodedMvRecord<MvTargetLookup> =
            decode_record(&key, &lookup.value).map_err(corruption)?;
        let key = projection_by_id_key(lookup.value.mv_id).map_err(corruption)?;
        let row = self
            .read_record(&key)
            .await?
            .ok_or_else(|| corruption("keyed MV lookup has no projection"))?;
        let decoded = decode_keyed_projection(&key, &row.value, budget).map_err(corruption)?;
        if decoded.value.facts.target() != *target {
            return Err(corruption("keyed MV lookup disagrees with its projection"));
        }
        Ok(Some(LoadedKeyedMvProjection {
            projection: decoded.value,
            version: MvProjectionVersion::from_store(row.version),
        }))
    }
    /// The existing sequence, lookup and dependency indexes are updated in the
    /// same transaction. A stale expected root is a conflict, never overwritten.
    pub async fn install_keyed_projection(
        &self,
        operation_id: Uuid,
        facts: MvKeyedDocumentProjection,
        expected: Option<LoadedKeyedMvProjection>,
    ) -> Result<LoadedKeyedMvProjection, MvRepositoryError> {
        let target = facts.target();
        if expected
            .as_ref()
            .is_some_and(|loaded| loaded.projection.facts.target() != target)
        {
            return Err(invalid("keyed projection replacement target differs"));
        }
        let next = facts.clone();
        let expected_clone = expected.clone();
        let page_size = self.store.limits().max_page_size;
        operation::run(
            self.store.as_ref(),
            &self.runner_metrics,
            self.run_policy,
            "install keyed MV projection",
            move |transaction| {
                let facts = next.clone();
                let expected = expected_clone.clone();
                Box::pin(async move {
                    let target = facts.target();
                    let index = target_key(&target).map_err(invalid_state_store)?;
                    let (id, condition) = match expected {
                        Some(expected) => {
                            let root = projection_by_id_key(expected.projection.mv_id)
                                .map_err(invalid_state_store)?;
                            let record = transaction.get(&root).await?.ok_or_else(|| {
                                conflict_state_store("keyed projection vanished before CAS")
                            })?;
                            if record.version != *expected.version.store_version() {
                                return Err(conflict_state_store(
                                    "keyed projection changed before CAS",
                                ));
                            }
                            let current = transaction.get(&index).await?.ok_or_else(|| {
                                conflict_state_store("keyed target index vanished")
                            })?;
                            let lookup: DecodedMvRecord<MvTargetLookup> =
                                decode_record(&index, &current.value)
                                    .map_err(invalid_state_store)?;
                            if lookup.value.mv_id != expected.projection.mv_id {
                                return Err(conflict_state_store("keyed target index changed"));
                            }
                            (
                                expected.projection.mv_id,
                                Precondition::Version(record.version),
                            )
                        }
                        None => {
                            if transaction.get(&index).await?.is_some() {
                                return Err(conflict_state_store(
                                    "MV target exists before keyed create",
                                ));
                            }
                            let sequence = sequence_key().map_err(invalid_state_store)?;
                            let (id, condition) = match transaction.get(&sequence).await? {
                                Some(row) => {
                                    let old: DecodedMvRecord<MvSequence> =
                                        decode_record(&sequence, &row.value)
                                            .map_err(invalid_state_store)?;
                                    (
                                        old.value.last_allocated_id.checked_add(1).ok_or_else(
                                            || invalid_state_store("MV ID sequence exhausted"),
                                        )?,
                                        Precondition::Version(row.version),
                                    )
                                }
                                None => (1, Precondition::Absent),
                            };
                            put_sequence(
                                transaction,
                                operation_id,
                                MvSequence {
                                    last_allocated_id: id,
                                },
                                condition,
                            )
                            .await?;
                            put_target_lookup(
                                transaction,
                                operation_id,
                                index,
                                id,
                                Precondition::Absent,
                            )
                            .await?;
                            (id, Precondition::Absent)
                        }
                    };
                    let dependencies = keyed_dependencies(id, &facts);
                    replace_dependency_indexes(
                        transaction,
                        operation_id,
                        id,
                        &dependencies,
                        page_size,
                    )
                    .await?;
                    let root = projection_by_id_key(id).map_err(invalid_state_store)?;
                    let value = encode_keyed_projection(
                        operation_id,
                        &StoredKeyedMvProjection { mv_id: id, facts },
                    )
                    .map_err(invalid_state_store)?;
                    transaction.put(root, value, condition).await?;
                    Ok(())
                })
            },
        )
        .await?;
        self.find_keyed_by_target(
            &target,
            crate::persistence::validation::PersistenceDecodeBudget::default(),
        )
        .await?
        .ok_or_else(|| corruption("installed keyed projection is missing"))
    }
}
pub(super) fn keyed_dependencies(
    id: i64,
    facts: &MvKeyedDocumentProjection,
) -> Vec<StoredMvDependency> {
    use crate::dependency::{MvDependencyObjectType, MvDependencyStorageEngine};
    facts
        .definition()
        .relation_occurrences
        .iter()
        .map(|r| StoredMvDependency {
            downstream_mv_id: id,
            occurrence_id: r.occurrence_id,
            upstream_object_id: serde_bytes::ByteBuf::from(r.object_id.as_bytes().to_vec()),
            upstream: MvDependencyObjectRef {
                catalog: Some(r.catalog_at_binding.clone()),
                database_or_namespace: r.namespace_at_binding.clone(),
                name: r.relation_at_binding.clone(),
                object_type: MvDependencyObjectType::Unclassified,
                storage_engine: MvDependencyStorageEngine::Unclassified,
            },
            created_at_ms: facts.definition().created_at_ms,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::{keyed_documents::*, validation::PersistenceDecodeBudget};
    #[tokio::test]
    async fn keyed_projection_roundtrips_on_existing_owner_with_guarded_cas() {
        let (_, repository) = super::super::tests_definition::repository().await;
        let (catalog, view) = crate::persistence::keyed_documents::tests::fixture(Some(17), true);
        let observed =
            decode_current_keyed_documents(view, catalog, PersistenceDecodeBudget::default())
                .unwrap();
        let facts = MvKeyedDocumentProjection::try_from_current(observed).unwrap();
        let target = facts.target();
        let first = repository
            .install_keyed_projection(Uuid::now_v7(), facts.clone(), None)
            .await
            .unwrap();
        assert_eq!(
            repository
                .find_keyed_by_target(&target, PersistenceDecodeBudget::default())
                .await
                .unwrap(),
            Some(first.clone())
        );
        let second = repository
            .install_keyed_projection(Uuid::now_v7(), facts.clone(), Some(first.clone()))
            .await
            .unwrap();
        assert_eq!(second.projection.mv_id, first.projection.mv_id);
        assert_eq!(
            repository
                .install_keyed_projection(Uuid::now_v7(), facts, Some(first))
                .await
                .unwrap_err()
                .kind(),
            MvRepositoryErrorKind::Conflict
        );
    }
}

impl StateStoreMvRepository {
    pub async fn list_keyed_projections(
        &self,
    ) -> Result<Vec<LoadedKeyedMvProjection>, MvRepositoryError> {
        let mut rows = Vec::new();
        for row in self
            .scan_prefix(projection_prefix().map_err(corruption)?)
            .await?
        {
            let value = decode_keyed_projection(
                &row.key,
                &row.value,
                crate::persistence::validation::PersistenceDecodeBudget::default(),
            )
            .map_err(corruption)?
            .value;
            if row.key != projection_by_id_key(value.mv_id).map_err(corruption)? {
                return Err(corruption(
                    "keyed projection key differs from root identity",
                ));
            }
            rows.push(LoadedKeyedMvProjection {
                projection: value,
                version: MvProjectionVersion::from_store(row.version),
            });
        }
        Ok(rows)
    }
    pub async fn list_keyed_projection_inventory(
        &self,
        bound: MvProjectionInventoryBound,
    ) -> Result<Vec<MvProjectionInventoryEntry>, MvRepositoryError> {
        let mut tx = self
            .store
            .begin_read()
            .await
            .map_err(operation::state_store_error)?;
        let result = bounded_keyed_inventory(tx.as_mut(), bound).await;
        let close = tx.abort().await.map_err(operation::state_store_error);
        match (result, close) {
            (Err(e), _) => Err(e),
            (_, Err(e)) => Err(e),
            (Ok(v), Ok(())) => Ok(v),
        }
    }
    pub async fn find_keyed_by_target_bounded(
        &self,
        target: &MvTarget,
        bound: MvProjectionInventoryBound,
    ) -> Result<Option<LoadedKeyedMvProjection>, MvRepositoryError> {
        MvProjectionInventoryBuilder::new(bound)?;
        let index = target_key(target).map_err(corruption)?;
        let Some(row) = self.read_record(&index).await? else {
            return Ok(None);
        };
        check_inventory_raw_record(&row, bound)?;
        let lookup: DecodedMvRecord<MvTargetLookup> =
            decode_record(&index, &row.value).map_err(corruption)?;
        let root = projection_by_id_key(lookup.value.mv_id).map_err(corruption)?;
        let row = self
            .read_record(&root)
            .await?
            .ok_or_else(|| corruption("keyed target lookup has no root"))?;
        check_inventory_raw_record(&row, bound)?;
        let facts = decode_keyed_projection(&root, &row.value, bound.decode)
            .map_err(corruption)?
            .value;
        if facts.facts.target() != *target {
            return Err(corruption("keyed lookup disagrees with root"));
        }
        Ok(Some(LoadedKeyedMvProjection {
            projection: facts,
            version: MvProjectionVersion::from_store(row.version),
        }))
    }
    pub async fn delete_keyed_projection(
        &self,
        operation_id: Uuid,
        expected: LoadedKeyedMvProjection,
    ) -> Result<bool, MvRepositoryError> {
        let page_size = self.store.limits().max_page_size;
        operation::run(
            self.store.as_ref(),
            &self.runner_metrics,
            self.run_policy,
            "delete keyed MV projection",
            move |tx| {
                let expected = expected.clone();
                Box::pin(async move {
                    let key = projection_by_id_key(expected.projection.mv_id)
                        .map_err(invalid_state_store)?;
                    let Some(row) = tx.get(&key).await? else {
                        return Ok(false);
                    };
                    if row.version != *expected.version.store_version() {
                        return Err(conflict_state_store("keyed MV root changed before delete"));
                    }
                    let current = decode_keyed_projection(
                        &key,
                        &row.value,
                        crate::persistence::validation::PersistenceDecodeBudget::default(),
                    )
                    .map_err(invalid_state_store)?
                    .value;
                    if current.facts.source_revision()
                        != expected.projection.facts.source_revision()
                    {
                        return Err(conflict_state_store(
                            "keyed MV source changed before delete",
                        ));
                    }
                    delete_indexes_for_target(
                        tx,
                        &current.facts.target(),
                        current.mv_id,
                        page_size,
                    )
                    .await?;
                    tx.delete(key, Precondition::Version(row.version)).await?;
                    let _ = operation_id;
                    Ok(true)
                })
            },
        )
        .await
    }
    pub async fn list_keyed_dependencies_by_downstream_bounded(
        &self,
        loaded: &LoadedKeyedMvProjection,
        bound: MvDependencyReadBound,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        use crate::bounded_dependencies::{
            DependencyCollector, classify, validate_canonical_parts,
        };
        let mut tx = self
            .store
            .begin_read()
            .await
            .map_err(operation::state_store_error)?;
        let result = async {
            let root = projection_by_id_key(loaded.projection.mv_id).map_err(corruption)?;
            let row = tx
                .get(&root)
                .await
                .map_err(operation::state_store_error)?
                .ok_or_else(|| corruption("keyed dependency root disappeared"))?;
            if row.version != *loaded.version.store_version() {
                return Err(MvRepositoryError::new(
                    MvRepositoryErrorKind::Conflict,
                    "keyed dependency root changed",
                ));
            }
            check_inventory_raw_record(&row, bound.inventory)?;
            let projection = decode_keyed_projection(&root, &row.value, bound.inventory.decode)
                .map_err(corruption)?
                .value;
            drop(row);
            let mut collector = DependencyCollector::new(bound)?;
            let mut range = BoundedRecordRange::new(
                dependency_by_downstream_prefix(projection.mv_id).map_err(corruption)?,
            )?;
            while let Some(row) = range.next(tx.as_mut(), bound.inventory).await? {
                let dependency = decode_dependency_with_budget(
                    &row.key,
                    &row.value,
                    bound.inventory.decode.max_working_set_bytes,
                )
                .map_err(corruption)?
                .value;
                if row.key
                    != dependency_by_downstream_key(
                        dependency.downstream_mv_id,
                        &dependency.upstream,
                        dependency.occurrence_id,
                    )
                    .map_err(corruption)?
                {
                    return Err(corruption(
                        "keyed dependency index key differs from occurrence",
                    ));
                }
                collector.push(dependency)?;
            }
            let mut dependencies = collector.finish();
            validate_canonical_parts(
                projection.mv_id,
                projection.facts.definition(),
                &dependencies,
            )?;
            drop(projection);
            let inventory = bounded_keyed_inventory(tx.as_mut(), bound.inventory).await?;
            classify(&mut dependencies, &inventory);
            Ok(dependencies)
        }
        .await;
        let close = tx.abort().await.map_err(operation::state_store_error);
        match (result, close) {
            (Err(e), _) => Err(e),
            (_, Err(e)) => Err(e),
            (Ok(v), Ok(())) => Ok(v),
        }
    }
}
async fn bounded_keyed_inventory(
    tx: &mut dyn ReadTransaction,
    bound: MvProjectionInventoryBound,
) -> Result<Vec<MvProjectionInventoryEntry>, MvRepositoryError> {
    let mut inventory = MvProjectionInventoryBuilder::new(bound)?;
    let mut range = BoundedRecordRange::new(projection_prefix().map_err(corruption)?)?;
    while let Some(row) = range.next(tx, bound).await? {
        let projection = decode_keyed_projection(&row.key, &row.value, bound.decode)
            .map_err(corruption)?
            .value;
        if row.key != projection_by_id_key(projection.mv_id).map_err(corruption)? {
            return Err(corruption("keyed inventory key differs from projection"));
        }
        inventory.push_facts(
            projection.mv_id,
            &projection.facts.target(),
            &projection.facts.source_revision().target_object_id,
        )?;
    }
    Ok(inventory.finish())
}

impl StateStoreMvRepository {
    pub async fn find_keyed_by_target_object(
        &self,
        object: &novarocks_spi::connector::ConnectorTableObjectId,
    ) -> Result<Option<LoadedKeyedMvProjection>, MvRepositoryError> {
        let mut found = None;
        for row in self.list_keyed_projections().await? {
            if &row.projection.facts.source_revision().target_object_id == object {
                if found.is_some() {
                    return Err(corruption(
                        "target object has more than one keyed projection",
                    ));
                }
                found = Some(row);
            }
        }
        Ok(found)
    }
    pub async fn list_keyed_dependencies_by_downstream(
        &self,
        loaded: &LoadedKeyedMvProjection,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        let current = self
            .find_keyed_by_target(
                &loaded.projection.facts.target(),
                crate::persistence::validation::PersistenceDecodeBudget::default(),
            )
            .await?;
        if current.as_ref().map(|v| &v.version) != Some(&loaded.version) {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                "keyed dependency root changed",
            ));
        }
        let mut dependencies = Vec::new();
        for row in self
            .scan_prefix(
                dependency_by_downstream_prefix(loaded.projection.mv_id).map_err(corruption)?,
            )
            .await?
        {
            let dependency: DecodedMvRecord<StoredMvDependency> =
                decode_record(&row.key, &row.value).map_err(corruption)?;
            if row.key
                != dependency_by_downstream_key(
                    dependency.value.downstream_mv_id,
                    &dependency.value.upstream,
                    dependency.value.occurrence_id,
                )
                .map_err(corruption)?
            {
                return Err(corruption(
                    "keyed dependency key differs from canonical occurrence",
                ));
            }
            dependencies.push(dependency.value);
        }
        crate::bounded_dependencies::validate_canonical_parts(
            loaded.projection.mv_id,
            loaded.projection.facts.definition(),
            &dependencies,
        )?;
        let inventory = self
            .list_keyed_projections()
            .await?
            .into_iter()
            .map(|p| MvProjectionInventoryEntry {
                mv_id: p.projection.mv_id,
                target: p.projection.facts.target(),
                object_id: p
                    .projection
                    .facts
                    .source_revision()
                    .target_object_id
                    .clone(),
            })
            .collect::<Vec<_>>();
        crate::bounded_dependencies::classify(&mut dependencies, &inventory);
        Ok(dependencies)
    }
}
