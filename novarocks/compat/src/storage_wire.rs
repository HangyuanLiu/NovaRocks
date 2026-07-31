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

//! StarRocks lake-storage protobuf codec.

use crate::proto::starrocks::{
    AggStateDescPb, BinaryPredicatePb, BundleTabletMetadataPb, ColumnPb, CombinedTxnLogPb,
    DeletePredicatePb, DelfileWithRowsetId, DeltaColumnGroupColumnIdsPb,
    DeltaColumnGroupMetadataPb, DeltaColumnGroupVerPb, DelvecMetadataPb, DelvecPagePb, FileMetaPb,
    FlatJsonConfigPb, FooterPointerPb, InPredicatePb, IsNullPredicatePb, KeysType,
    MetadataUpdateInfoPb, PScalarType, PStructField, PTypeDesc, PTypeNode, PUniqueId,
    PagePointerPb, PersistentIndexSstableMetaPb, PersistentIndexSstablePb,
    PersistentIndexSstablePredicatePb, RecordPredicatePb, RowsetMetadataPb, RowsetTxnMetaPb,
    SegmentMetadataPb, TableSchemaKeyPb, TabletIndexPb, TabletMetadataPb, TabletSchemaPb, TuplePb,
    TxnLogPb, VariantPb, VariantTypePb, record_predicate_pb, txn_log_pb,
};
use novarocks::connector::starrocks::lake::storage_domain::{
    StorageAlterMetadataOperation, StorageBinaryPredicate, StorageBundleFile,
    StorageBundleMetadata, StorageColumnHashCongruence, StorageCombinedTransactionLog,
    StorageCompactionOperation, StorageDeleteFile, StorageDeletePredicate,
    StorageDeltaColumnGroupMetadata, StorageDeltaColumnGroupVersion, StorageDelvecMetadata,
    StorageDelvecPage, StorageFile, StorageFlatJsonConfig, StorageFooterPointer,
    StorageInPredicate, StorageIsNullPredicate, StorageMetadataUpdate, StoragePagePointer,
    StoragePersistentIndexSstable, StoragePersistentIndexSstableMeta,
    StoragePersistentIndexSstablePredicate, StorageRecordPredicate, StorageRowset,
    StorageRowsetTxnMeta, StorageSchemaChangeOperation, StorageSchemaKey, StorageSegment,
    StorageTabletMetadata, StorageTransactionLog, StorageTuple, StorageVariant, StorageVariantKind,
    StorageWriteOperation,
};
use novarocks::connector::starrocks::schema::{
    StarRocksAggStateDesc, StarRocksColumnSchema, StarRocksKeysType, StarRocksScalarType,
    StarRocksStructField, StarRocksTabletIndex, StarRocksTabletSchema, StarRocksTypeDesc,
    StarRocksTypeNode,
};
use prost::Message;
use std::sync::Arc;

pub(crate) fn storage_metadata_provider()
-> Arc<dyn novarocks::connector::starrocks::ports::StorageMetadataProvider> {
    Arc::new(CompatStorageMetadataProvider)
}

struct CompatStorageMetadataProvider;

impl novarocks::connector::starrocks::ports::StorageMetadataProvider
    for CompatStorageMetadataProvider
{
    fn encode_tablet_schema(&self, schema: &StarRocksTabletSchema) -> Result<Vec<u8>, String> {
        Ok(encode_schema(schema).encode_to_vec())
    }

    fn decode_tablet_schema(&self, bytes: &[u8]) -> Result<StarRocksTabletSchema, String> {
        let schema = TabletSchemaPb::decode(bytes)
            .map_err(|error| format!("decode StarRocks tablet schema protobuf failed: {error}"))?;
        decode_schema(schema)
    }

    fn decode_tablet_metadata(&self, bytes: &[u8]) -> Result<StorageTabletMetadata, String> {
        decode_tablet_metadata(bytes)
    }

    fn encode_tablet_metadata(&self, metadata: &StorageTabletMetadata) -> Result<Vec<u8>, String> {
        encode_tablet_metadata(metadata)
    }

    fn decode_bundle_metadata(&self, bytes: &[u8]) -> Result<StorageBundleMetadata, String> {
        decode_bundle_metadata(bytes)
    }

    fn decode_bundle_file(&self, bytes: &[u8]) -> Result<StorageBundleFile, String> {
        decode_bundle_file(bytes)
    }

    fn encode_bundle_file(&self, bundle: &StorageBundleFile) -> Result<Vec<u8>, String> {
        encode_bundle_file(bundle)
    }

    fn rewrite_tablet_metadata_version(
        &self,
        bytes: &[u8],
        version: i64,
    ) -> Result<Vec<u8>, String> {
        rewrite_tablet_metadata_version(bytes, version)
    }

    fn decode_transaction_log(&self, bytes: &[u8]) -> Result<StorageTransactionLog, String> {
        decode_transaction_log(bytes)
    }

    fn encode_transaction_log(&self, log: &StorageTransactionLog) -> Result<Vec<u8>, String> {
        encode_transaction_log(log)
    }

    fn decode_combined_transaction_log(
        &self,
        bytes: &[u8],
    ) -> Result<StorageCombinedTransactionLog, String> {
        decode_combined_transaction_log(bytes)
    }

    fn encode_combined_transaction_log(
        &self,
        log: &StorageCombinedTransactionLog,
    ) -> Result<Vec<u8>, String> {
        encode_combined_transaction_log(log)
    }
}

pub(crate) fn decode_tablet_metadata(bytes: &[u8]) -> Result<StorageTabletMetadata, String> {
    decode_tablet_metadata_pb(
        TabletMetadataPb::decode(bytes).map_err(|error| {
            format!("decode StarRocks tablet metadata protobuf failed: {error}")
        })?,
    )
}

pub(crate) fn decode_transaction_log(bytes: &[u8]) -> Result<StorageTransactionLog, String> {
    decode_transaction_log_pb(
        TxnLogPb::decode(bytes).map_err(|error| {
            format!("decode StarRocks transaction log protobuf failed: {error}")
        })?,
    )
}

pub(crate) fn encode_transaction_log(log: &StorageTransactionLog) -> Result<Vec<u8>, String> {
    Ok(encode_transaction_log_pb(log)?.encode_to_vec())
}

pub(crate) fn decode_combined_transaction_log(
    bytes: &[u8],
) -> Result<StorageCombinedTransactionLog, String> {
    let combined = CombinedTxnLogPb::decode(bytes).map_err(|error| {
        format!("decode StarRocks combined transaction log protobuf failed: {error}")
    })?;
    Ok(StorageCombinedTransactionLog {
        transaction_logs: combined
            .txn_logs
            .into_iter()
            .map(decode_transaction_log_pb)
            .collect::<Result<_, _>>()?,
    })
}

pub(crate) fn encode_combined_transaction_log(
    log: &StorageCombinedTransactionLog,
) -> Result<Vec<u8>, String> {
    Ok(CombinedTxnLogPb {
        txn_logs: log
            .transaction_logs
            .iter()
            .map(encode_transaction_log_pb)
            .collect::<Result<_, _>>()?,
    }
    .encode_to_vec())
}

fn decode_transaction_log_pb(log: TxnLogPb) -> Result<StorageTransactionLog, String> {
    Ok(StorageTransactionLog {
        tablet_id: log.tablet_id,
        txn_id: log.txn_id,
        write: log.op_write.map(decode_write_operation),
        compaction: log.op_compaction.map(decode_compaction_operation),
        schema_change: log.op_schema_change.map(decode_schema_change_operation),
        alter_metadata: log
            .op_alter_metadata
            .map(decode_alter_metadata_operation)
            .transpose()?,
        replication: encode_opaque(log.op_replication),
        partition_id: log.partition_id,
        load_id: log.load_id.map(|id| (id.hi, id.lo)),
    })
}

fn encode_transaction_log_pb(log: &StorageTransactionLog) -> Result<TxnLogPb, String> {
    Ok(TxnLogPb {
        tablet_id: log.tablet_id,
        txn_id: log.txn_id,
        op_write: log.write.as_ref().map(encode_write_operation).transpose()?,
        op_compaction: log
            .compaction
            .as_ref()
            .map(encode_compaction_operation)
            .transpose()?,
        op_schema_change: log
            .schema_change
            .as_ref()
            .map(encode_schema_change_operation)
            .transpose()?,
        op_alter_metadata: log
            .alter_metadata
            .as_ref()
            .map(encode_alter_metadata_operation)
            .transpose()?,
        op_replication: decode_opaque(log.replication.as_deref(), "transaction replication")?,
        partition_id: log.partition_id,
        load_id: log.load_id.map(|(hi, lo)| PUniqueId { hi, lo }),
    })
}

fn decode_write_operation(write: txn_log_pb::OpWrite) -> StorageWriteOperation {
    StorageWriteOperation {
        rowset: write.rowset.map(decode_rowset),
        txn_meta: write.txn_meta.map(decode_rowset_txn_meta),
        dels: write.dels,
        rewrite_segments: write.rewrite_segments,
        del_encryption_metas: write.del_encryption_metas,
        ssts: write.ssts.into_iter().map(decode_file).collect(),
        schema_key: write.schema_key.map(decode_schema_key),
    }
}

fn encode_write_operation(write: &StorageWriteOperation) -> Result<txn_log_pb::OpWrite, String> {
    Ok(txn_log_pb::OpWrite {
        rowset: write.rowset.as_ref().map(encode_rowset).transpose()?,
        txn_meta: write.txn_meta.as_ref().map(encode_rowset_txn_meta),
        dels: write.dels.clone(),
        rewrite_segments: write.rewrite_segments.clone(),
        del_encryption_metas: write.del_encryption_metas.clone(),
        ssts: write.ssts.iter().map(encode_file).collect(),
        schema_key: write.schema_key.as_ref().map(encode_schema_key),
    })
}

fn decode_compaction_operation(op: txn_log_pb::OpCompaction) -> StorageCompactionOperation {
    StorageCompactionOperation {
        input_rowsets: op.input_rowsets,
        output_rowset: op.output_rowset.map(decode_rowset),
        input_sstables: op
            .input_sstables
            .into_iter()
            .map(decode_persistent_index_sstable)
            .collect(),
        output_sstable: op.output_sstable.map(decode_persistent_index_sstable),
        compact_version: op.compact_version,
        new_segment_offset: op.new_segment_offset,
        new_segment_count: op.new_segment_count,
        ssts: op.ssts.into_iter().map(decode_file).collect(),
    }
}

fn encode_compaction_operation(
    op: &StorageCompactionOperation,
) -> Result<txn_log_pb::OpCompaction, String> {
    Ok(txn_log_pb::OpCompaction {
        input_rowsets: op.input_rowsets.clone(),
        output_rowset: op.output_rowset.as_ref().map(encode_rowset).transpose()?,
        input_sstables: op
            .input_sstables
            .iter()
            .map(encode_persistent_index_sstable)
            .collect(),
        output_sstable: op
            .output_sstable
            .as_ref()
            .map(encode_persistent_index_sstable),
        compact_version: op.compact_version,
        new_segment_offset: op.new_segment_offset,
        new_segment_count: op.new_segment_count,
        ssts: op.ssts.iter().map(encode_file).collect(),
    })
}

fn decode_schema_change_operation(op: txn_log_pb::OpSchemaChange) -> StorageSchemaChangeOperation {
    StorageSchemaChangeOperation {
        rowsets: op.rowsets.into_iter().map(decode_rowset).collect(),
        linked_segment: op.linked_segment,
        alter_version: op.alter_version,
        delvec_meta: op.delvec_meta.map(decode_delvec_metadata),
    }
}

fn encode_schema_change_operation(
    op: &StorageSchemaChangeOperation,
) -> Result<txn_log_pb::OpSchemaChange, String> {
    Ok(txn_log_pb::OpSchemaChange {
        rowsets: op
            .rowsets
            .iter()
            .map(encode_rowset)
            .collect::<Result<_, _>>()?,
        linked_segment: op.linked_segment,
        alter_version: op.alter_version,
        delvec_meta: op.delvec_meta.as_ref().map(encode_delvec_metadata),
    })
}

fn decode_alter_metadata_operation(
    op: txn_log_pb::OpAlterMetadata,
) -> Result<StorageAlterMetadataOperation, String> {
    Ok(StorageAlterMetadataOperation {
        metadata_updates: op
            .metadata_update_infos
            .into_iter()
            .map(decode_metadata_update)
            .collect::<Result<_, _>>()?,
    })
}

fn encode_alter_metadata_operation(
    op: &StorageAlterMetadataOperation,
) -> Result<txn_log_pb::OpAlterMetadata, String> {
    Ok(txn_log_pb::OpAlterMetadata {
        metadata_update_infos: op
            .metadata_updates
            .iter()
            .map(encode_metadata_update)
            .collect::<Result<_, _>>()?,
    })
}

fn decode_metadata_update(value: MetadataUpdateInfoPb) -> Result<StorageMetadataUpdate, String> {
    Ok(StorageMetadataUpdate {
        enable_persistent_index: value.enable_persistent_index,
        persistent_index_type: value.persistent_index_type,
        bundle_tablet_metadata: value.bundle_tablet_metadata,
        compaction_strategy: value.compaction_strategy,
        flat_json_config: value.flat_json_config.map(decode_flat_json_config),
        tablet_schema: value.tablet_schema.map(decode_schema).transpose()?,
    })
}

fn encode_metadata_update(value: &StorageMetadataUpdate) -> Result<MetadataUpdateInfoPb, String> {
    Ok(MetadataUpdateInfoPb {
        enable_persistent_index: value.enable_persistent_index,
        persistent_index_type: value.persistent_index_type,
        bundle_tablet_metadata: value.bundle_tablet_metadata,
        compaction_strategy: value.compaction_strategy,
        flat_json_config: value.flat_json_config.as_ref().map(encode_flat_json_config),
        tablet_schema: value.tablet_schema.as_ref().map(encode_schema),
    })
}

/// Decode a lake-service schema key at the compat wire boundary.
pub(crate) fn decode_schema_key(key: TableSchemaKeyPb) -> StorageSchemaKey {
    StorageSchemaKey {
        db_id: key.db_id,
        table_id: key.table_id,
        schema_id: key.schema_id,
    }
}

fn encode_schema_key(key: &StorageSchemaKey) -> TableSchemaKeyPb {
    TableSchemaKeyPb {
        db_id: key.db_id,
        table_id: key.table_id,
        schema_id: key.schema_id,
    }
}

fn decode_rowset_txn_meta(meta: RowsetTxnMetaPb) -> StorageRowsetTxnMeta {
    StorageRowsetTxnMeta {
        partial_update_column_ids: meta.partial_update_column_ids,
        partial_update_column_unique_ids: meta.partial_update_column_unique_ids,
        partial_rowset_footers: meta
            .partial_rowset_footers
            .into_iter()
            .map(|pointer| StorageFooterPointer {
                position: pointer.position,
                size: pointer.size,
            })
            .collect(),
        merge_condition: meta.merge_condition,
        auto_increment_partial_update_column_id: meta.auto_increment_partial_update_column_id,
        partial_update_mode: meta.partial_update_mode,
        auto_increment_partial_update_column_uid: meta.auto_increment_partial_update_column_uid,
        column_to_expr_value: meta.column_to_expr_value,
    }
}

fn encode_rowset_txn_meta(meta: &StorageRowsetTxnMeta) -> RowsetTxnMetaPb {
    RowsetTxnMetaPb {
        partial_update_column_ids: meta.partial_update_column_ids.clone(),
        partial_update_column_unique_ids: meta.partial_update_column_unique_ids.clone(),
        partial_rowset_footers: meta
            .partial_rowset_footers
            .iter()
            .map(|pointer| FooterPointerPb {
                position: pointer.position,
                size: pointer.size,
            })
            .collect(),
        merge_condition: meta.merge_condition.clone(),
        auto_increment_partial_update_column_id: meta.auto_increment_partial_update_column_id,
        partial_update_mode: meta.partial_update_mode,
        auto_increment_partial_update_column_uid: meta.auto_increment_partial_update_column_uid,
        column_to_expr_value: meta.column_to_expr_value.clone(),
    }
}

pub(crate) fn encode_tablet_metadata(metadata: &StorageTabletMetadata) -> Result<Vec<u8>, String> {
    metadata.validate()?;
    Ok(encode_tablet_metadata_pb(metadata)?.encode_to_vec())
}

pub(crate) fn decode_bundle_metadata(bytes: &[u8]) -> Result<StorageBundleMetadata, String> {
    let bundle = BundleTabletMetadataPb::decode(bytes)
        .map_err(|error| format!("decode StarRocks bundle metadata protobuf failed: {error}"))?;
    Ok(StorageBundleMetadata {
        tablet_to_schema: bundle.tablet_to_schema,
        schemas: bundle
            .schemas
            .into_iter()
            .map(|(id, schema)| decode_schema(schema).map(|schema| (id, schema)))
            .collect::<Result<_, _>>()?,
        tablet_meta_pages: bundle
            .tablet_meta_pages
            .into_iter()
            .map(|(id, pointer)| {
                (
                    id,
                    StoragePagePointer {
                        offset: pointer.offset,
                        size: pointer.size,
                    },
                )
            })
            .collect(),
    })
}

pub(crate) fn encode_bundle_metadata(bundle: &StorageBundleMetadata) -> BundleTabletMetadataPb {
    BundleTabletMetadataPb {
        tablet_to_schema: bundle.tablet_to_schema.clone(),
        schemas: bundle
            .schemas
            .iter()
            .map(|(id, schema)| (*id, encode_schema(schema)))
            .collect(),
        tablet_meta_pages: bundle
            .tablet_meta_pages
            .iter()
            .map(|(id, pointer)| {
                (
                    *id,
                    PagePointerPb {
                        offset: pointer.offset,
                        size: pointer.size,
                    },
                )
            })
            .collect(),
    }
}

const BUNDLE_METADATA_FOOTER_SIZE: usize = 8;

pub(crate) fn decode_bundle_file(bytes: &[u8]) -> Result<StorageBundleFile, String> {
    if bytes.len() < BUNDLE_METADATA_FOOTER_SIZE {
        return Err("invalid StarRocks bundle metadata file: too small".to_string());
    }
    let footer_offset = bytes.len() - BUNDLE_METADATA_FOOTER_SIZE;
    let bundle_size = u64::from_le_bytes(
        bytes[footer_offset..]
            .try_into()
            .map_err(|_| "decode StarRocks bundle metadata footer failed".to_string())?,
    ) as usize;
    if bundle_size == 0 || bundle_size > footer_offset {
        return Err(format!(
            "invalid StarRocks bundle metadata footer: file_size={} bundle_size={}",
            bytes.len(),
            bundle_size
        ));
    }
    let bundle_offset = footer_offset - bundle_size;
    let metadata = decode_bundle_metadata(&bytes[bundle_offset..footer_offset])?;
    let mut tablet_metadata_pages = std::collections::HashMap::new();
    for (tablet_id, page) in &metadata.tablet_meta_pages {
        let start = page.offset as usize;
        let end = start.saturating_add(page.size as usize);
        if end > bundle_offset {
            return Err(format!(
                "StarRocks bundle tablet page out of range: tablet_id={} offset={} size={} bundle_offset={}",
                tablet_id, page.offset, page.size, bundle_offset
            ));
        }
        tablet_metadata_pages.insert(*tablet_id, bytes[start..end].to_vec());
    }
    Ok(StorageBundleFile {
        tablet_metadata_pages,
        tablet_to_schema: metadata.tablet_to_schema,
        schemas: metadata.schemas,
    })
}

pub(crate) fn encode_bundle_file(bundle: &StorageBundleFile) -> Result<Vec<u8>, String> {
    let mut tablet_ids = bundle
        .tablet_metadata_pages
        .keys()
        .copied()
        .collect::<Vec<_>>();
    tablet_ids.sort_unstable();

    let mut tablet_meta_pages = std::collections::HashMap::with_capacity(tablet_ids.len());
    let mut bytes = Vec::new();
    for tablet_id in tablet_ids {
        let page = bundle
            .tablet_metadata_pages
            .get(&tablet_id)
            .ok_or_else(|| format!("bundle file missing tablet page for tablet_id={tablet_id}"))?;
        let offset = bytes.len() as u64;
        bytes.extend_from_slice(page);
        tablet_meta_pages.insert(
            tablet_id,
            StoragePagePointer {
                offset,
                size: page.len() as u32,
            },
        );
    }
    let metadata = StorageBundleMetadata {
        tablet_to_schema: bundle.tablet_to_schema.clone(),
        schemas: bundle.schemas.clone(),
        tablet_meta_pages,
    };
    let footer = encode_bundle_metadata(&metadata).encode_to_vec();
    bytes.extend_from_slice(&footer);
    bytes.extend_from_slice(&(footer.len() as u64).to_le_bytes());
    Ok(bytes)
}

pub(crate) fn rewrite_tablet_metadata_version(
    bytes: &[u8],
    version: i64,
) -> Result<Vec<u8>, String> {
    let mut metadata = TabletMetadataPb::decode(bytes).map_err(|error| {
        format!("decode StarRocks tablet metadata for version rewrite failed: {error}")
    })?;
    metadata.version = Some(version);
    Ok(metadata.encode_to_vec())
}

fn encode_tablet_metadata_pb(metadata: &StorageTabletMetadata) -> Result<TabletMetadataPb, String> {
    Ok(TabletMetadataPb {
        id: metadata.id,
        version: metadata.version,
        schema: metadata.schema.as_ref().map(encode_schema),
        rowsets: metadata
            .rowsets
            .iter()
            .map(encode_rowset)
            .collect::<Result<_, _>>()?,
        next_rowset_id: metadata.next_rowset_id,
        cumulative_point: metadata.cumulative_point,
        delvec_meta: metadata.delvec_meta.as_ref().map(encode_delvec_metadata),
        compaction_inputs: metadata
            .compaction_inputs
            .iter()
            .map(encode_rowset)
            .collect::<Result<_, _>>()?,
        prev_garbage_version: metadata.prev_garbage_version,
        orphan_files: metadata.orphan_files.iter().map(encode_file).collect(),
        enable_persistent_index: metadata.enable_persistent_index,
        persistent_index_type: metadata.persistent_index_type,
        commit_time: metadata.commit_time,
        source_schema: metadata.source_schema.as_ref().map(encode_schema),
        sstable_meta: metadata
            .sstable_meta
            .as_ref()
            .map(encode_persistent_index_sstable_meta),
        dcg_meta: metadata
            .dcg_meta
            .as_ref()
            .map(encode_delta_column_group_metadata),
        historical_schemas: metadata
            .historical_schemas
            .iter()
            .map(|(id, schema)| (*id, encode_schema(schema)))
            .collect(),
        rowset_to_schema: metadata.rowset_to_schema.clone(),
        gtid: metadata.gtid,
        compaction_strategy: metadata.compaction_strategy,
        flat_json_config: metadata
            .flat_json_config
            .as_ref()
            .map(encode_flat_json_config),
    })
}

fn decode_tablet_metadata_pb(metadata: TabletMetadataPb) -> Result<StorageTabletMetadata, String> {
    let decoded = StorageTabletMetadata {
        id: metadata.id,
        version: metadata.version,
        schema: metadata.schema.map(decode_schema).transpose()?,
        rowsets: metadata.rowsets.into_iter().map(decode_rowset).collect(),
        next_rowset_id: metadata.next_rowset_id,
        cumulative_point: metadata.cumulative_point,
        delvec_meta: metadata.delvec_meta.map(decode_delvec_metadata),
        compaction_inputs: metadata
            .compaction_inputs
            .into_iter()
            .map(decode_rowset)
            .collect(),
        prev_garbage_version: metadata.prev_garbage_version,
        orphan_files: metadata.orphan_files.into_iter().map(decode_file).collect(),
        enable_persistent_index: metadata.enable_persistent_index,
        persistent_index_type: metadata.persistent_index_type,
        commit_time: metadata.commit_time,
        source_schema: metadata.source_schema.map(decode_schema).transpose()?,
        sstable_meta: metadata
            .sstable_meta
            .map(decode_persistent_index_sstable_meta),
        dcg_meta: metadata.dcg_meta.map(decode_delta_column_group_metadata),
        historical_schemas: metadata
            .historical_schemas
            .into_iter()
            .map(|(id, schema)| decode_schema(schema).map(|schema| (id, schema)))
            .collect::<Result<_, _>>()?,
        rowset_to_schema: metadata.rowset_to_schema,
        gtid: metadata.gtid,
        compaction_strategy: metadata.compaction_strategy,
        flat_json_config: metadata.flat_json_config.map(decode_flat_json_config),
    };
    decoded.validate()?;
    Ok(decoded)
}

fn decode_flat_json_config(value: FlatJsonConfigPb) -> StorageFlatJsonConfig {
    StorageFlatJsonConfig {
        enabled: value.flat_json_enable,
        null_factor: value.flat_json_null_factor,
        sparsity_factor: value.flat_json_sparsity_factor,
        max_column_max: value.flat_json_max_column_max,
    }
}

fn encode_flat_json_config(value: &StorageFlatJsonConfig) -> FlatJsonConfigPb {
    FlatJsonConfigPb {
        flat_json_enable: value.enabled,
        flat_json_null_factor: value.null_factor,
        flat_json_sparsity_factor: value.sparsity_factor,
        flat_json_max_column_max: value.max_column_max,
    }
}

fn decode_record_predicate(value: RecordPredicatePb) -> StorageRecordPredicate {
    StorageRecordPredicate {
        kind: value.r#type,
        children: value
            .children
            .into_iter()
            .map(decode_record_predicate)
            .collect(),
        column_hash_is_congruent: value.column_hash_is_congruent.map(|predicate| {
            StorageColumnHashCongruence {
                modulus: predicate.modulus,
                remainder: predicate.remainder,
                column_names: predicate.column_names,
            }
        }),
    }
}

fn encode_record_predicate(value: &StorageRecordPredicate) -> RecordPredicatePb {
    RecordPredicatePb {
        r#type: value.kind,
        children: value.children.iter().map(encode_record_predicate).collect(),
        column_hash_is_congruent: value.column_hash_is_congruent.as_ref().map(|predicate| {
            record_predicate_pb::ColumnHashIsCongruentPb {
                modulus: predicate.modulus,
                remainder: predicate.remainder,
                column_names: predicate.column_names.clone(),
            }
        }),
    }
}

fn decode_persistent_index_sstable(
    value: PersistentIndexSstablePb,
) -> StoragePersistentIndexSstable {
    StoragePersistentIndexSstable {
        version: value.version,
        filename: value.filename,
        filesize: value.filesize,
        max_rss_rowid: value.max_rss_rowid,
        encryption_meta: value.encryption_meta,
        shared: value.shared,
        predicate: value
            .predicate
            .map(|predicate| StoragePersistentIndexSstablePredicate {
                record_predicate: predicate.record_predicate.map(decode_record_predicate),
            }),
        shared_rssid: value.shared_rssid,
        shared_version: value.shared_version,
        delvec: value.delvec.map(decode_delvec_page),
    }
}

fn encode_persistent_index_sstable(
    value: &StoragePersistentIndexSstable,
) -> PersistentIndexSstablePb {
    PersistentIndexSstablePb {
        version: value.version,
        filename: value.filename.clone(),
        filesize: value.filesize,
        max_rss_rowid: value.max_rss_rowid,
        encryption_meta: value.encryption_meta.clone(),
        shared: value.shared,
        predicate: value
            .predicate
            .as_ref()
            .map(|predicate| PersistentIndexSstablePredicatePb {
                record_predicate: predicate
                    .record_predicate
                    .as_ref()
                    .map(encode_record_predicate),
            }),
        shared_rssid: value.shared_rssid,
        shared_version: value.shared_version,
        delvec: value.delvec.as_ref().map(encode_delvec_page),
    }
}

fn decode_persistent_index_sstable_meta(
    value: PersistentIndexSstableMetaPb,
) -> StoragePersistentIndexSstableMeta {
    StoragePersistentIndexSstableMeta {
        sstables: value
            .sstables
            .into_iter()
            .map(decode_persistent_index_sstable)
            .collect(),
    }
}

fn encode_persistent_index_sstable_meta(
    value: &StoragePersistentIndexSstableMeta,
) -> PersistentIndexSstableMetaPb {
    PersistentIndexSstableMetaPb {
        sstables: value
            .sstables
            .iter()
            .map(encode_persistent_index_sstable)
            .collect(),
    }
}

fn decode_delta_column_group_metadata(
    value: DeltaColumnGroupMetadataPb,
) -> StorageDeltaColumnGroupMetadata {
    StorageDeltaColumnGroupMetadata {
        groups: value
            .dcgs
            .into_iter()
            .map(|(segment_id, group)| {
                (
                    segment_id,
                    StorageDeltaColumnGroupVersion {
                        unique_column_ids: group
                            .unique_column_ids
                            .into_iter()
                            .map(|columns| columns.column_ids)
                            .collect(),
                        column_files: group.column_files,
                        versions: group.versions,
                        encryption_metas: group.encryption_metas,
                        shared_files: group.shared_files,
                    },
                )
            })
            .collect(),
    }
}

fn encode_delta_column_group_metadata(
    value: &StorageDeltaColumnGroupMetadata,
) -> DeltaColumnGroupMetadataPb {
    DeltaColumnGroupMetadataPb {
        dcgs: value
            .groups
            .iter()
            .map(|(segment_id, group)| {
                (
                    *segment_id,
                    DeltaColumnGroupVerPb {
                        unique_column_ids: group
                            .unique_column_ids
                            .iter()
                            .map(|column_ids| DeltaColumnGroupColumnIdsPb {
                                column_ids: column_ids.clone(),
                            })
                            .collect(),
                        column_files: group.column_files.clone(),
                        versions: group.versions.clone(),
                        encryption_metas: group.encryption_metas.clone(),
                        shared_files: group.shared_files.clone(),
                    },
                )
            })
            .collect(),
    }
}

fn encode_rowset(rowset: &StorageRowset) -> Result<RowsetMetadataPb, String> {
    Ok(RowsetMetadataPb {
        id: rowset.id,
        overlapped: rowset.overlapped,
        segments: rowset.segments.clone(),
        num_rows: rowset.num_rows,
        data_size: rowset.data_size,
        delete_predicate: rowset
            .delete_predicate
            .as_ref()
            .map(encode_delete_predicate),
        num_dels: rowset.num_dels,
        segment_size: rowset.segment_size.clone(),
        max_compact_input_rowset_id: rowset.max_compact_input_rowset_id,
        version: rowset.version,
        del_files: rowset.del_files.iter().map(encode_delete_file).collect(),
        segment_encryption_metas: rowset.segment_encryption_metas.clone(),
        next_compaction_offset: rowset.next_compaction_offset,
        bundle_file_offsets: rowset.bundle_file_offsets.clone(),
        shared_segments: rowset.shared_segments.clone(),
        record_predicate: rowset
            .record_predicate
            .as_ref()
            .map(encode_record_predicate),
        segment_metas: rowset
            .segment_metas
            .iter()
            .map(encode_segment)
            .collect::<Result<_, _>>()?,
    })
}

fn decode_rowset(rowset: RowsetMetadataPb) -> StorageRowset {
    StorageRowset {
        id: rowset.id,
        overlapped: rowset.overlapped,
        segments: rowset.segments,
        num_rows: rowset.num_rows,
        data_size: rowset.data_size,
        delete_predicate: rowset.delete_predicate.map(decode_delete_predicate),
        num_dels: rowset.num_dels,
        segment_size: rowset.segment_size,
        max_compact_input_rowset_id: rowset.max_compact_input_rowset_id,
        version: rowset.version,
        del_files: rowset
            .del_files
            .into_iter()
            .map(decode_delete_file)
            .collect(),
        segment_encryption_metas: rowset.segment_encryption_metas,
        next_compaction_offset: rowset.next_compaction_offset,
        bundle_file_offsets: rowset.bundle_file_offsets,
        shared_segments: rowset.shared_segments,
        record_predicate: rowset.record_predicate.map(decode_record_predicate),
        segment_metas: rowset
            .segment_metas
            .into_iter()
            .map(decode_segment)
            .collect(),
    }
}

fn encode_delete_predicate(predicate: &StorageDeletePredicate) -> DeletePredicatePb {
    DeletePredicatePb {
        version: predicate.version,
        sub_predicates: predicate.sub_predicates.clone(),
        in_predicates: predicate
            .in_predicates
            .iter()
            .map(|value| InPredicatePb {
                column_name: value.column_name.clone(),
                is_not_in: value.is_not_in,
                values: value.values.clone(),
            })
            .collect(),
        binary_predicates: predicate
            .binary_predicates
            .iter()
            .map(|value| BinaryPredicatePb {
                column_name: value.column_name.clone(),
                op: value.op.clone(),
                value: value.value.clone(),
            })
            .collect(),
        is_null_predicates: predicate
            .is_null_predicates
            .iter()
            .map(|value| IsNullPredicatePb {
                column_name: value.column_name.clone(),
                is_not_null: value.is_not_null,
            })
            .collect(),
    }
}

/// Decode a lake-service delete predicate at the compat wire boundary.
pub(crate) fn decode_delete_predicate(predicate: DeletePredicatePb) -> StorageDeletePredicate {
    StorageDeletePredicate {
        version: predicate.version,
        sub_predicates: predicate.sub_predicates,
        in_predicates: predicate
            .in_predicates
            .into_iter()
            .map(|value| StorageInPredicate {
                column_name: value.column_name,
                is_not_in: value.is_not_in,
                values: value.values,
            })
            .collect(),
        binary_predicates: predicate
            .binary_predicates
            .into_iter()
            .map(|value| StorageBinaryPredicate {
                column_name: value.column_name,
                op: value.op,
                value: value.value,
            })
            .collect(),
        is_null_predicates: predicate
            .is_null_predicates
            .into_iter()
            .map(|value| StorageIsNullPredicate {
                column_name: value.column_name,
                is_not_null: value.is_not_null,
            })
            .collect(),
    }
}

fn encode_delvec_metadata(metadata: &StorageDelvecMetadata) -> DelvecMetadataPb {
    DelvecMetadataPb {
        version_to_file: metadata
            .version_to_file
            .iter()
            .map(|(version, file)| (*version, encode_file(file)))
            .collect(),
        delvecs: metadata
            .delvecs
            .iter()
            .map(|(segment_id, page)| (*segment_id, encode_delvec_page(page)))
            .collect(),
    }
}

fn decode_delvec_metadata(metadata: DelvecMetadataPb) -> StorageDelvecMetadata {
    StorageDelvecMetadata {
        version_to_file: metadata
            .version_to_file
            .into_iter()
            .map(|(version, file)| (version, decode_file(file)))
            .collect(),
        delvecs: metadata
            .delvecs
            .into_iter()
            .map(|(segment_id, page)| (segment_id, decode_delvec_page(page)))
            .collect(),
    }
}

fn encode_delvec_page(page: &StorageDelvecPage) -> DelvecPagePb {
    DelvecPagePb {
        version: page.version,
        offset: page.offset,
        size: page.size,
        crc32c: page.crc32c,
        crc32c_gen_version: page.crc32c_gen_version,
    }
}

fn decode_delvec_page(page: DelvecPagePb) -> StorageDelvecPage {
    StorageDelvecPage {
        version: page.version,
        offset: page.offset,
        size: page.size,
        crc32c: page.crc32c,
        crc32c_gen_version: page.crc32c_gen_version,
    }
}

fn encode_file(file: &StorageFile) -> FileMetaPb {
    FileMetaPb {
        name: file.name.clone(),
        size: file.size,
        shared: file.shared,
        encryption_meta: file.encryption_meta.clone(),
    }
}

fn decode_file(file: FileMetaPb) -> StorageFile {
    StorageFile {
        name: file.name,
        size: file.size,
        shared: file.shared,
        encryption_meta: file.encryption_meta,
    }
}

fn encode_delete_file(file: &StorageDeleteFile) -> DelfileWithRowsetId {
    DelfileWithRowsetId {
        name: file.name.clone(),
        origin_rowset_id: file.origin_rowset_id,
        op_offset: file.op_offset,
        encryption_meta: file.encryption_meta.clone(),
        shared: file.shared,
    }
}

fn decode_delete_file(file: DelfileWithRowsetId) -> StorageDeleteFile {
    StorageDeleteFile {
        name: file.name,
        origin_rowset_id: file.origin_rowset_id,
        op_offset: file.op_offset,
        encryption_meta: file.encryption_meta,
        shared: file.shared,
    }
}

fn encode_segment(segment: &StorageSegment) -> Result<SegmentMetadataPb, String> {
    Ok(SegmentMetadataPb {
        sort_key_min: segment.sort_key_min.as_ref().map(encode_tuple),
        sort_key_max: segment.sort_key_max.as_ref().map(encode_tuple),
        num_rows: segment.num_rows,
    })
}

fn decode_segment(segment: SegmentMetadataPb) -> StorageSegment {
    StorageSegment {
        sort_key_min: segment.sort_key_min.map(decode_tuple),
        sort_key_max: segment.sort_key_max.map(decode_tuple),
        num_rows: segment.num_rows,
    }
}

fn encode_tuple(value: &StorageTuple) -> TuplePb {
    TuplePb {
        values: value.values.iter().map(encode_variant).collect(),
    }
}

fn decode_tuple(value: TuplePb) -> StorageTuple {
    StorageTuple {
        values: value.values.into_iter().map(decode_variant).collect(),
    }
}

fn encode_variant(value: &StorageVariant) -> VariantPb {
    VariantPb {
        r#type: value.type_desc.as_ref().map(encode_type_desc),
        value: value.value.clone(),
        variant_type: value.kind.map(encode_variant_kind),
    }
}

fn decode_variant(value: VariantPb) -> StorageVariant {
    StorageVariant {
        type_desc: value.r#type.map(decode_type_desc),
        value: value.value,
        kind: value.variant_type.map(decode_variant_kind),
    }
}

fn encode_variant_kind(kind: StorageVariantKind) -> i32 {
    match kind {
        StorageVariantKind::Null => VariantTypePb::NullValue as i32,
        StorageVariantKind::Normal => VariantTypePb::NormalValue as i32,
        StorageVariantKind::Unknown(value) => value,
    }
}

fn decode_variant_kind(kind: i32) -> StorageVariantKind {
    if kind == VariantTypePb::NullValue as i32 {
        StorageVariantKind::Null
    } else if kind == VariantTypePb::NormalValue as i32 {
        StorageVariantKind::Normal
    } else {
        StorageVariantKind::Unknown(kind)
    }
}

fn encode_schema(schema: &StarRocksTabletSchema) -> TabletSchemaPb {
    TabletSchemaPb {
        keys_type: schema.keys_type.map(encode_keys_type),
        column: schema.column.iter().map(encode_column).collect(),
        num_short_key_columns: schema.num_short_key_columns,
        num_rows_per_row_block: schema.num_rows_per_row_block,
        bf_fpp: schema.bf_fpp,
        next_column_unique_id: schema.next_column_unique_id,
        deprecated_is_in_memory: schema.deprecated_is_in_memory,
        deprecated_id: schema.deprecated_id,
        compression_type: schema.compression_type,
        sort_key_idxes: schema.sort_key_idxes.clone(),
        schema_version: schema.schema_version,
        sort_key_unique_ids: schema.sort_key_unique_ids.clone(),
        table_indices: schema
            .table_indices
            .iter()
            .map(encode_tablet_index)
            .collect(),
        compression_level: schema.compression_level,
        id: schema.id,
    }
}

fn decode_schema(schema: TabletSchemaPb) -> Result<StarRocksTabletSchema, String> {
    let decoded = StarRocksTabletSchema {
        keys_type: schema.keys_type.map(decode_keys_type).transpose()?,
        column: schema.column.into_iter().map(decode_column).collect(),
        num_short_key_columns: schema.num_short_key_columns,
        num_rows_per_row_block: schema.num_rows_per_row_block,
        bf_fpp: schema.bf_fpp,
        next_column_unique_id: schema.next_column_unique_id,
        deprecated_is_in_memory: schema.deprecated_is_in_memory,
        deprecated_id: schema.deprecated_id,
        compression_type: schema.compression_type,
        sort_key_idxes: schema.sort_key_idxes,
        schema_version: schema.schema_version,
        sort_key_unique_ids: schema.sort_key_unique_ids,
        table_indices: schema
            .table_indices
            .into_iter()
            .map(decode_tablet_index)
            .collect(),
        compression_level: schema.compression_level,
        id: schema.id,
    };
    decoded.validate()?;
    Ok(decoded)
}

fn encode_keys_type(value: StarRocksKeysType) -> i32 {
    match value {
        StarRocksKeysType::Duplicate => KeysType::DupKeys as i32,
        StarRocksKeysType::Unique => KeysType::UniqueKeys as i32,
        StarRocksKeysType::Aggregate => KeysType::AggKeys as i32,
        StarRocksKeysType::Primary => KeysType::PrimaryKeys as i32,
    }
}

fn decode_keys_type(value: i32) -> Result<StarRocksKeysType, String> {
    match KeysType::try_from(value) {
        Ok(KeysType::DupKeys) => Ok(StarRocksKeysType::Duplicate),
        Ok(KeysType::UniqueKeys) => Ok(StarRocksKeysType::Unique),
        Ok(KeysType::AggKeys) => Ok(StarRocksKeysType::Aggregate),
        Ok(KeysType::PrimaryKeys) => Ok(StarRocksKeysType::Primary),
        Err(_) => Err(format!("unknown StarRocks keys type {value}")),
    }
}

fn encode_column(value: &StarRocksColumnSchema) -> ColumnPb {
    ColumnPb {
        unique_id: value.unique_id,
        name: value.name.clone(),
        r#type: value.r#type.clone(),
        is_key: value.is_key,
        aggregation: value.aggregation.clone(),
        is_nullable: value.is_nullable,
        default_value: value.default_value.clone(),
        precision: value.precision,
        frac: value.frac,
        length: value.length,
        index_length: value.index_length,
        is_bf_column: value.is_bf_column,
        referenced_column_id: value.referenced_column_id,
        referenced_column: value.referenced_column.clone(),
        has_bitmap_index: value.has_bitmap_index,
        visible: value.visible,
        children_columns: value.children_columns.iter().map(encode_column).collect(),
        is_auto_increment: value.is_auto_increment,
        agg_state_desc: value.agg_state_desc.as_ref().map(encode_agg_state),
    }
}

fn decode_column(value: ColumnPb) -> StarRocksColumnSchema {
    StarRocksColumnSchema {
        unique_id: value.unique_id,
        name: value.name,
        r#type: value.r#type,
        is_key: value.is_key,
        aggregation: value.aggregation,
        is_nullable: value.is_nullable,
        default_value: value.default_value,
        precision: value.precision,
        frac: value.frac,
        length: value.length,
        index_length: value.index_length,
        is_bf_column: value.is_bf_column,
        referenced_column_id: value.referenced_column_id,
        referenced_column: value.referenced_column,
        has_bitmap_index: value.has_bitmap_index,
        visible: value.visible,
        children_columns: value
            .children_columns
            .into_iter()
            .map(decode_column)
            .collect(),
        is_auto_increment: value.is_auto_increment,
        agg_state_desc: value.agg_state_desc.map(decode_agg_state),
    }
}

fn encode_agg_state(value: &StarRocksAggStateDesc) -> AggStateDescPb {
    AggStateDescPb {
        agg_func_name: value.agg_func_name.clone(),
        arg_types: value.arg_types.iter().map(encode_type_desc).collect(),
        ret_type: value.ret_type.as_ref().map(encode_type_desc),
        is_result_nullable: value.is_result_nullable,
        func_version: value.func_version,
    }
}

fn decode_agg_state(value: AggStateDescPb) -> StarRocksAggStateDesc {
    StarRocksAggStateDesc {
        agg_func_name: value.agg_func_name,
        arg_types: value.arg_types.into_iter().map(decode_type_desc).collect(),
        ret_type: value.ret_type.map(decode_type_desc),
        is_result_nullable: value.is_result_nullable,
        func_version: value.func_version,
    }
}

fn encode_type_desc(value: &StarRocksTypeDesc) -> PTypeDesc {
    PTypeDesc {
        types: value.types.iter().map(encode_type_node).collect(),
    }
}

fn decode_type_desc(value: PTypeDesc) -> StarRocksTypeDesc {
    StarRocksTypeDesc {
        types: value.types.into_iter().map(decode_type_node).collect(),
    }
}

fn encode_type_node(value: &StarRocksTypeNode) -> PTypeNode {
    PTypeNode {
        r#type: value.r#type,
        scalar_type: value.scalar_type.map(|scalar| PScalarType {
            r#type: scalar.r#type,
            len: scalar.len,
            precision: scalar.precision,
            scale: scalar.scale,
        }),
        struct_fields: value
            .struct_fields
            .iter()
            .map(|field| PStructField {
                name: field.name.clone(),
                comment: field.comment.clone(),
            })
            .collect(),
    }
}

fn decode_type_node(value: PTypeNode) -> StarRocksTypeNode {
    StarRocksTypeNode {
        r#type: value.r#type,
        scalar_type: value.scalar_type.map(|scalar| StarRocksScalarType {
            r#type: scalar.r#type,
            len: scalar.len,
            precision: scalar.precision,
            scale: scalar.scale,
        }),
        struct_fields: value
            .struct_fields
            .into_iter()
            .map(|field| StarRocksStructField {
                name: field.name,
                comment: field.comment,
            })
            .collect(),
    }
}

fn encode_tablet_index(value: &StarRocksTabletIndex) -> TabletIndexPb {
    TabletIndexPb {
        index_id: value.index_id,
        index_name: value.index_name.clone(),
        index_type: value.index_type,
        col_unique_id: value.col_unique_id.clone(),
        index_properties: value.index_properties.clone(),
    }
}

fn decode_tablet_index(value: TabletIndexPb) -> StarRocksTabletIndex {
    StarRocksTabletIndex {
        index_id: value.index_id,
        index_name: value.index_name,
        index_type: value.index_type,
        col_unique_id: value.col_unique_id,
        index_properties: value.index_properties,
    }
}

fn encode_opaque<M: Message>(value: Option<M>) -> Option<Vec<u8>> {
    value.map(|value| value.encode_to_vec())
}

fn decode_opaque<M: Message + Default>(
    bytes: Option<&[u8]>,
    field: &str,
) -> Result<Option<M>, String> {
    bytes
        .map(|bytes| {
            M::decode(bytes).map_err(|error| {
                format!("decode StarRocks {field} protobuf from storage domain failed: {error}")
            })
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tablet_metadata_round_trip_preserves_domain_rowset_facts() {
        let metadata = StorageTabletMetadata {
            id: Some(9),
            version: Some(4),
            rowsets: vec![StorageRowset {
                id: Some(3),
                segments: vec!["segment-3.dat".to_string()],
                num_rows: Some(17),
                shared_segments: vec![true],
                segment_metas: vec![StorageSegment {
                    sort_key_min: Some(StorageTuple {
                        values: vec![
                            StorageVariant {
                                value: Some("3".to_string()),
                                kind: Some(StorageVariantKind::Normal),
                                ..StorageVariant::default()
                            },
                            StorageVariant {
                                kind: Some(StorageVariantKind::Null),
                                ..StorageVariant::default()
                            },
                            StorageVariant {
                                value: Some("future-kind".to_string()),
                                kind: Some(StorageVariantKind::Unknown(99)),
                                ..StorageVariant::default()
                            },
                        ],
                    }),
                    sort_key_max: Some(StorageTuple {
                        values: vec![StorageVariant {
                            value: Some("17".to_string()),
                            kind: Some(StorageVariantKind::Normal),
                            ..StorageVariant::default()
                        }],
                    }),
                    num_rows: Some(17),
                }],
                ..StorageRowset::default()
            }],
            sstable_meta: Some(StoragePersistentIndexSstableMeta {
                sstables: vec![StoragePersistentIndexSstable {
                    filename: Some("index.sst".to_string()),
                    shared: Some(true),
                    ..StoragePersistentIndexSstable::default()
                }],
            }),
            dcg_meta: Some(StorageDeltaColumnGroupMetadata {
                groups: std::collections::HashMap::from([(
                    3,
                    StorageDeltaColumnGroupVersion {
                        unique_column_ids: vec![vec![5, 7]],
                        column_files: vec!["delta-3.dat".to_string()],
                        versions: vec![11],
                        encryption_metas: vec![vec![13]],
                        shared_files: vec![false],
                    },
                )]),
            }),
            ..StorageTabletMetadata::default()
        };

        let bytes = encode_tablet_metadata(&metadata).expect("encode domain metadata");
        assert_eq!(
            decode_tablet_metadata(&bytes).expect("decode domain metadata"),
            metadata
        );
    }

    #[test]
    fn tablet_metadata_round_trip_preserves_record_predicate_facts() {
        let metadata = StorageTabletMetadata {
            id: Some(9),
            version: Some(4),
            rowsets: vec![StorageRowset {
                record_predicate: Some(StorageRecordPredicate {
                    kind: Some(1),
                    column_hash_is_congruent: Some(StorageColumnHashCongruence {
                        modulus: Some(7),
                        remainder: Some(3),
                        column_names: vec!["k1".to_string()],
                    }),
                    ..StorageRecordPredicate::default()
                }),
                ..StorageRowset::default()
            }],
            ..StorageTabletMetadata::default()
        };

        let bytes = encode_tablet_metadata(&metadata).expect("encode domain metadata");
        assert_eq!(
            decode_tablet_metadata(&bytes).expect("decode domain metadata"),
            metadata
        );
    }

    #[test]
    fn bundle_file_round_trip_preserves_pages_and_schema_facts() {
        let first_page = encode_tablet_metadata(&StorageTabletMetadata {
            id: Some(1),
            version: Some(3),
            ..StorageTabletMetadata::default()
        })
        .expect("encode first tablet metadata");
        let second_page = encode_tablet_metadata(&StorageTabletMetadata {
            id: Some(2),
            version: Some(3),
            ..StorageTabletMetadata::default()
        })
        .expect("encode second tablet metadata");
        let bundle = StorageBundleFile {
            tablet_metadata_pages: std::collections::HashMap::from([
                (1, first_page),
                (2, second_page),
            ]),
            tablet_to_schema: std::collections::HashMap::from([(1, 7), (2, 7)]),
            schemas: std::collections::HashMap::from([(
                7,
                StarRocksTabletSchema {
                    id: Some(7),
                    keys_type: Some(StarRocksKeysType::Duplicate),
                    column: vec![StarRocksColumnSchema {
                        unique_id: 1,
                        name: Some("k1".to_string()),
                        r#type: "BIGINT".to_string(),
                        is_key: Some(true),
                        ..StarRocksColumnSchema::default()
                    }],
                    ..StarRocksTabletSchema::default()
                },
            )]),
        };

        let bytes = encode_bundle_file(&bundle).expect("encode bundle file");
        assert_eq!(
            decode_bundle_file(&bytes).expect("decode bundle file"),
            bundle
        );
    }

    #[test]
    fn provider_backed_bundle_writer_encodes_domain_metadata_pages() {
        let root = std::env::temp_dir().join(format!(
            "novarocks-compat-storage-bundle-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
        let root = root.to_string_lossy().into_owned();
        let provider = storage_metadata_provider();
        let schema = StarRocksTabletSchema {
            id: Some(7),
            keys_type: Some(StarRocksKeysType::Duplicate),
            column: vec![StarRocksColumnSchema {
                unique_id: 1,
                name: Some("k1".to_string()),
                r#type: "BIGINT".to_string(),
                is_key: Some(true),
                ..StarRocksColumnSchema::default()
            }],
            ..StarRocksTabletSchema::default()
        };
        let metadata = StorageTabletMetadata {
            id: Some(11),
            version: Some(3),
            ..StorageTabletMetadata::default()
        };

        novarocks::formats::starrocks::writer::bundle_meta::write_bundle_meta_file_with_provider(
            &root,
            11,
            3,
            &schema,
            &metadata,
            provider.as_ref(),
        )
        .expect("write provider-backed bundle");

        let bundle =
            novarocks::formats::starrocks::writer::bundle_meta::load_bundle_file_with_provider(
                &root,
                3,
                provider.as_ref(),
            )
            .expect("load provider-backed bundle")
            .expect("provider-backed bundle exists");
        assert_eq!(bundle.tablet_to_schema.get(&11), Some(&7));
        assert_eq!(bundle.schemas.get(&7), Some(&schema));
        assert_eq!(
            novarocks::formats::starrocks::writer::bundle_meta::decode_bundle_tablet_metadata_with_provider(
                &bundle,
                11,
                provider.as_ref(),
            )
            .expect("decode tablet page"),
            metadata
        );
        std::fs::remove_dir_all(&root).expect("remove temporary bundle directory");
    }

    #[test]
    fn provider_backed_initial_metadata_writer_round_trips_domain_metadata() {
        let root = std::env::temp_dir().join(format!(
            "novarocks-compat-storage-initial-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
        let root = root.to_string_lossy().into_owned();
        let provider = storage_metadata_provider();
        let metadata = StorageTabletMetadata {
            id: Some(17),
            version: Some(1),
            ..StorageTabletMetadata::default()
        };

        novarocks::formats::starrocks::writer::bundle_meta::write_initial_meta_file_with_provider(
            &root,
            &metadata,
            provider.as_ref(),
        )
        .expect("write provider-backed initial metadata");

        let (version, loaded) = novarocks::formats::starrocks::writer::bundle_meta::load_latest_tablet_metadata_with_provider(
            &root,
            17,
            provider.as_ref(),
        )
        .expect("load provider-backed initial metadata");
        assert_eq!(version, 1);
        assert_eq!(loaded, metadata);
        std::fs::remove_dir_all(&root).expect("remove temporary initial metadata directory");
    }

    #[test]
    fn rewrite_tablet_metadata_version_changes_only_version() {
        let bytes = encode_tablet_metadata(&StorageTabletMetadata {
            id: Some(9),
            version: Some(2),
            ..StorageTabletMetadata::default()
        })
        .expect("encode tablet metadata");
        let rewritten = rewrite_tablet_metadata_version(&bytes, 4).expect("rewrite version");
        assert_eq!(
            decode_tablet_metadata(&rewritten)
                .expect("decode rewritten tablet metadata")
                .version,
            Some(4)
        );
    }

    #[test]
    fn transaction_log_round_trip_preserves_write_domain_facts() {
        let log = StorageTransactionLog {
            tablet_id: Some(7),
            txn_id: Some(11),
            partition_id: Some(13),
            load_id: Some((17, 19)),
            write: Some(StorageWriteOperation {
                rowset: Some(StorageRowset {
                    id: Some(23),
                    segments: vec!["segment-23.dat".to_string()],
                    num_rows: Some(29),
                    ..StorageRowset::default()
                }),
                schema_key: Some(StorageSchemaKey {
                    db_id: Some(31),
                    table_id: Some(37),
                    schema_id: Some(41),
                }),
                txn_meta: Some(StorageRowsetTxnMeta {
                    partial_update_column_ids: vec![43],
                    merge_condition: Some("k1".to_string()),
                    ..StorageRowsetTxnMeta::default()
                }),
                dels: vec!["delete-1.dat".to_string()],
                ..StorageWriteOperation::default()
            }),
            ..StorageTransactionLog::default()
        };

        let bytes = encode_transaction_log(&log).expect("encode transaction log");
        assert_eq!(
            decode_transaction_log(&bytes).expect("decode transaction log"),
            log
        );
    }

    #[test]
    fn transaction_log_round_trip_preserves_non_write_operations() {
        let flat_json_config = StorageFlatJsonConfig {
            enabled: Some(true),
            null_factor: Some(0.25),
            sparsity_factor: Some(0.75),
            max_column_max: Some(64),
        };
        let sstable = StoragePersistentIndexSstable {
            filename: Some("index.sst".to_string()),
            shared: Some(true),
            predicate: Some(StoragePersistentIndexSstablePredicate {
                record_predicate: Some(StorageRecordPredicate {
                    kind: Some(1),
                    ..StorageRecordPredicate::default()
                }),
            }),
            ..StoragePersistentIndexSstable::default()
        };
        let log = StorageTransactionLog {
            tablet_id: Some(7),
            txn_id: Some(11),
            compaction: Some(StorageCompactionOperation {
                input_rowsets: vec![3, 5],
                output_rowset: Some(StorageRowset {
                    num_rows: Some(17),
                    ..StorageRowset::default()
                }),
                input_sstables: vec![sstable.clone()],
                output_sstable: Some(sstable),
                compact_version: Some(19),
                new_segment_offset: Some(23),
                new_segment_count: Some(29),
                ssts: vec![StorageFile {
                    name: Some("segment.dat".to_string()),
                    ..StorageFile::default()
                }],
            }),
            schema_change: Some(StorageSchemaChangeOperation {
                rowsets: vec![StorageRowset {
                    num_rows: Some(31),
                    ..StorageRowset::default()
                }],
                linked_segment: Some(true),
                alter_version: Some(37),
                ..StorageSchemaChangeOperation::default()
            }),
            alter_metadata: Some(StorageAlterMetadataOperation {
                metadata_updates: vec![StorageMetadataUpdate {
                    enable_persistent_index: Some(true),
                    persistent_index_type: Some(2),
                    bundle_tablet_metadata: Some(true),
                    compaction_strategy: Some(3),
                    flat_json_config: Some(flat_json_config),
                    ..StorageMetadataUpdate::default()
                }],
            }),
            ..StorageTransactionLog::default()
        };

        let bytes = encode_transaction_log(&log).expect("encode transaction log");
        assert_eq!(
            decode_transaction_log(&bytes).expect("decode transaction log"),
            log
        );
    }

    #[test]
    fn provider_backed_transaction_log_files_preserve_domain_facts() {
        let root = std::env::temp_dir().join(format!(
            "novarocks-compat-storage-log-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
        let root = root.to_string_lossy().into_owned();
        let provider = storage_metadata_provider();
        let log = StorageTransactionLog {
            tablet_id: Some(7),
            txn_id: Some(11),
            write: Some(StorageWriteOperation {
                rowset: Some(StorageRowset {
                    id: Some(13),
                    segments: vec!["segment-13.dat".to_string()],
                    num_rows: Some(17),
                    ..StorageRowset::default()
                }),
                ..StorageWriteOperation::default()
            }),
            ..StorageTransactionLog::default()
        };
        let combined = StorageCombinedTransactionLog {
            transaction_logs: vec![log.clone()],
        };
        let log_path = format!("{root}/log/txn.log");
        let combined_path = format!("{root}/log/combined.log");

        novarocks::formats::starrocks::writer::io::write_transaction_log_with_provider(
            &log_path,
            &log,
            provider.as_ref(),
        )
        .expect("write provider-backed transaction log");
        novarocks::formats::starrocks::writer::io::write_combined_transaction_log_with_provider(
            &combined_path,
            &combined,
            provider.as_ref(),
        )
        .expect("write provider-backed combined transaction log");

        assert_eq!(
            novarocks::formats::starrocks::writer::io::read_transaction_log_if_exists_with_provider(
                &log_path,
                provider.as_ref(),
            )
            .expect("read provider-backed transaction log"),
            Some(log)
        );
        assert_eq!(
            novarocks::formats::starrocks::writer::io::read_combined_transaction_log_if_exists_with_provider(
                &combined_path,
                provider.as_ref(),
            )
            .expect("read provider-backed combined transaction log"),
            Some(combined)
        );
        assert_eq!(
            novarocks::formats::starrocks::writer::io::read_transaction_log_if_exists_with_provider(
                &format!("{root}/log/missing.log"),
                provider.as_ref(),
            )
            .expect("read missing transaction log"),
            None
        );
        std::fs::remove_dir_all(&root).expect("remove temporary log directory");
    }
}
