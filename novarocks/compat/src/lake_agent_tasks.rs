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

//! StarRocks BackendService lake-agent request adapter.

use std::sync::Arc;

use novarocks::connector::starrocks::lake::schema_adapter::{
    build_create_tablet_schema, build_tablet_schema_from_thrift,
};
use novarocks::connector::starrocks::lake::schema_change::{
    CompiledRollupExpression, LakeAlterTabletMode, LakeAlterTabletTask, LakeTabletMetadataUpdate,
    LakeUpdateTabletMetaTask, RollupExpressionProgram, RollupInputSlot,
    RollupMaterializedViewParam,
};
use novarocks::connector::starrocks::lake::storage_domain::{
    StorageFlatJsonConfig, StorageMetadataUpdate,
};
use novarocks::exec::expr::ExprArena;
use novarocks::runtime::starlet_shard_registry::StarletShardInfo;
use novarocks::service::grpc_client::proto::starrocks::{
    CompactionStrategyPb, PersistentIndexTypePb,
};
use novarocks::thrift::agent_service::{
    TAlterJobType, TAlterTabletReqV2, TCompactionStrategy, TCreateTabletReq, TPersistentIndexType,
    TTabletType, TUpdateTabletMetaInfoReq,
};
use novarocks::thrift::exprs::TExpr;

pub(crate) struct CompatLakeAgentTaskAdapter {
    storage_metadata_provider:
        Arc<dyn novarocks::connector::starrocks::ports::StorageMetadataProvider>,
}

impl CompatLakeAgentTaskAdapter {
    pub(crate) fn create_tablet(
        &self,
        request: &TCreateTabletReq,
        shard_info: &StarletShardInfo,
    ) -> Result<(), String> {
        novarocks::connector::starrocks::lake::execute_lake_create_tablet_task(
            adapt_create_tablet_task(request, shard_info)?,
            Arc::clone(&self.storage_metadata_provider),
        )
    }

    pub(crate) fn alter_tablet(&self, request: &TAlterTabletReqV2) -> Result<(), String> {
        let task = adapt_alter_tablet_task(request)?;
        novarocks::connector::starrocks::lake::schema_change::execute_lake_alter_tablet_task(
            task,
            Arc::clone(&self.storage_metadata_provider),
        )
    }

    pub(crate) fn update_tablet_meta_info(
        &self,
        request: &TUpdateTabletMetaInfoReq,
    ) -> Result<(), String> {
        novarocks::connector::starrocks::lake::execute_lake_update_tablet_meta_task(
            adapt_update_tablet_meta_task(request)?,
            Arc::clone(&self.storage_metadata_provider),
        )
    }
}

fn adapt_create_tablet_task(
    request: &TCreateTabletReq,
    shard_info: &StarletShardInfo,
) -> Result<novarocks::connector::starrocks::lake::LakeCreateTabletTask, String> {
    Ok(
        novarocks::connector::starrocks::lake::LakeCreateTabletTask {
            tablet_id: request.tablet_id,
            table_id: request.table_id.unwrap_or(0),
            tablet_root_path: shard_info.full_path().to_string(),
            tablet_schema: build_create_tablet_schema(request)?,
            s3_config: shard_info.s3().cloned(),
            enable_persistent_index: request.enable_persistent_index,
            persistent_index_type: request
                .persistent_index_type
                .map(map_create_tablet_persistent_index_type)
                .transpose()?,
            gtid: request.gtid.unwrap_or(0),
            compaction_strategy: request
                .compaction_strategy
                .map(map_create_tablet_compaction_strategy)
                .transpose()?
                .or(Some(CompactionStrategyPb::Default as i32)),
            flat_json_config: request
                .flat_json_config
                .as_ref()
                .map(|cfg| StorageFlatJsonConfig {
                    enabled: cfg.flat_json_enable,
                    null_factor: cfg.flat_json_null_factor.map(|value| value.0),
                    sparsity_factor: cfg.flat_json_sparsity_factor.map(|value| value.0),
                    max_column_max: cfg.flat_json_column_max,
                }),
            enable_tablet_creation_optimization: request
                .enable_tablet_creation_optimization
                .unwrap_or(false),
        },
    )
}

fn map_create_tablet_persistent_index_type(
    persistent_index_type: TPersistentIndexType,
) -> Result<i32, String> {
    if persistent_index_type == TPersistentIndexType::LOCAL {
        return Ok(PersistentIndexTypePb::Local as i32);
    }
    if persistent_index_type == TPersistentIndexType::CLOUD_NATIVE {
        return Ok(PersistentIndexTypePb::CloudNative as i32);
    }
    Err(format!(
        "unsupported create_tablet persistent_index_type={persistent_index_type:?}"
    ))
}

fn map_create_tablet_compaction_strategy(
    compaction_strategy: TCompactionStrategy,
) -> Result<i32, String> {
    if compaction_strategy == TCompactionStrategy::DEFAULT {
        return Ok(CompactionStrategyPb::Default as i32);
    }
    if compaction_strategy == TCompactionStrategy::REAL_TIME {
        return Ok(CompactionStrategyPb::RealTime as i32);
    }
    Err(format!(
        "unsupported create_tablet compaction_strategy={compaction_strategy:?}"
    ))
}

fn adapt_update_tablet_meta_task(
    request: &TUpdateTabletMetaInfoReq,
) -> Result<LakeUpdateTabletMetaTask, String> {
    let tablet_type = request.tablet_type.unwrap_or(TTabletType::TABLET_TYPE_DISK);
    if tablet_type != TTabletType::TABLET_TYPE_LAKE {
        return Err(format!(
            "update_tablet_meta_info unsupported tablet_type={tablet_type:?} (only TABLET_TYPE_LAKE is supported)"
        ));
    }
    let txn_id = request
        .txn_id
        .ok_or_else(|| "update_tablet_meta_info missing txn_id".to_string())?;
    if txn_id <= 0 {
        return Err(format!(
            "update_tablet_meta_info has invalid txn_id={txn_id}"
        ));
    }
    let tablet_meta_infos = request
        .tablet_meta_infos
        .as_ref()
        .ok_or_else(|| "update_tablet_meta_info missing tablet_meta_infos".to_string())?;
    let updates = tablet_meta_infos
        .iter()
        .map(|tablet_meta_info| {
            Ok(LakeTabletMetadataUpdate {
                tablet_id: tablet_meta_info.tablet_id.ok_or_else(|| {
                    "update_tablet_meta_info tablet_meta_info missing tablet_id".to_string()
                })?,
                metadata_update: StorageMetadataUpdate {
                    enable_persistent_index: tablet_meta_info.enable_persistent_index,
                    persistent_index_type: tablet_meta_info
                        .persistent_index_type
                        .map(map_update_tablet_meta_persistent_index_type)
                        .transpose()?,
                    bundle_tablet_metadata: tablet_meta_info.bundle_tablet_metadata,
                    compaction_strategy: tablet_meta_info
                        .compaction_strategy
                        .map(map_update_tablet_meta_compaction_strategy)
                        .transpose()?,
                    flat_json_config: tablet_meta_info.flat_json_config.as_ref().map(|cfg| {
                        StorageFlatJsonConfig {
                            enabled: cfg.flat_json_enable,
                            null_factor: cfg.flat_json_null_factor.map(|value| value.0),
                            sparsity_factor: cfg.flat_json_sparsity_factor.map(|value| value.0),
                            max_column_max: cfg.flat_json_column_max,
                        }
                    }),
                    tablet_schema: tablet_meta_info
                        .tablet_schema
                        .as_ref()
                        .map(build_tablet_schema_from_thrift)
                        .transpose()?,
                },
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(LakeUpdateTabletMetaTask { txn_id, updates })
}

fn map_update_tablet_meta_persistent_index_type(
    persistent_index_type: TPersistentIndexType,
) -> Result<i32, String> {
    if persistent_index_type == TPersistentIndexType::LOCAL {
        return Ok(PersistentIndexTypePb::Local as i32);
    }
    if persistent_index_type == TPersistentIndexType::CLOUD_NATIVE {
        return Ok(PersistentIndexTypePb::CloudNative as i32);
    }
    Err(format!(
        "update_tablet_meta_info unsupported persistent_index_type={persistent_index_type:?}"
    ))
}

fn map_update_tablet_meta_compaction_strategy(
    compaction_strategy: TCompactionStrategy,
) -> Result<i32, String> {
    if compaction_strategy == TCompactionStrategy::DEFAULT {
        return Ok(CompactionStrategyPb::Default as i32);
    }
    if compaction_strategy == TCompactionStrategy::REAL_TIME {
        return Ok(CompactionStrategyPb::RealTime as i32);
    }
    Err(format!(
        "update_tablet_meta_info unsupported compaction_strategy={compaction_strategy:?}"
    ))
}

fn adapt_alter_tablet_task(request: &TAlterTabletReqV2) -> Result<LakeAlterTabletTask, String> {
    if request.base_tablet_id <= 0 {
        return Err(format!(
            "alter task has non-positive base_tablet_id={}",
            request.base_tablet_id
        ));
    }
    if request.new_tablet_id <= 0 {
        return Err(format!(
            "alter task has non-positive new_tablet_id={}",
            request.new_tablet_id
        ));
    }
    let tablet_type = request.tablet_type.unwrap_or(TTabletType::TABLET_TYPE_DISK);
    if tablet_type != TTabletType::TABLET_TYPE_LAKE {
        return Err(format!(
            "alter task unsupported tablet_type={tablet_type:?} (only TABLET_TYPE_LAKE is supported)"
        ));
    }
    let alter_version = request
        .alter_version
        .ok_or_else(|| "alter task missing alter_version".to_string())?;
    let txn_id = request
        .txn_id
        .ok_or_else(|| "alter task missing txn_id".to_string())?;
    let alter_job_type = request
        .alter_job_type
        .unwrap_or(TAlterJobType::SCHEMA_CHANGE);
    let (mode, rollup) = match alter_job_type {
        TAlterJobType::SCHEMA_CHANGE => {
            if request
                .materialized_view_params
                .as_ref()
                .is_some_and(|params| !params.is_empty())
            {
                return Err(
                    "alter task does not support materialized_view_params in SCHEMA_CHANGE V1"
                        .to_string(),
                );
            }
            if request.materialized_column_req.is_some() {
                return Err(
                    "alter task does not support materialized_column_req in SCHEMA_CHANGE V1"
                        .to_string(),
                );
            }
            if request.where_expr.is_some() {
                return Err(
                    "alter task does not support where_expr in SCHEMA_CHANGE V1".to_string()
                );
            }
            (LakeAlterTabletMode::SchemaChange, None)
        }
        TAlterJobType::ROLLUP => {
            if request.materialized_column_req.is_some() {
                return Err(
                    "alter task does not support materialized_column_req in ROLLUP V1".to_string(),
                );
            }
            if request.query_options.is_none() || request.query_globals.is_none() {
                return Err("alter task missing query_options/query_globals for ROLLUP".to_string());
            }
            if request.desc_tbl.is_none() {
                return Err("alter task missing desc_tbl for ROLLUP".to_string());
            }
            (
                LakeAlterTabletMode::Rollup,
                Some(compile_rollup_program(request)?),
            )
        }
        _ => {
            return Err(format!(
                "alter task unsupported alter_job_type={alter_job_type:?} (supported: SCHEMA_CHANGE, ROLLUP)"
            ));
        }
    };

    Ok(LakeAlterTabletTask {
        base_tablet_id: request.base_tablet_id,
        new_tablet_id: request.new_tablet_id,
        base_schema_hash: request.base_schema_hash,
        new_schema_hash: request.new_schema_hash,
        alter_version,
        txn_id,
        mode,
        base_tablet_read_schema: request
            .base_tablet_read_schema
            .as_ref()
            .map(build_tablet_schema_from_thrift)
            .transpose()?,
        rollup,
        columns_len: request.columns.as_ref().map_or(0, Vec::len),
        base_table_column_names_len: request.base_table_column_names.as_ref().map_or(0, Vec::len),
    })
}

fn compile_rollup_program(request: &TAlterTabletReqV2) -> Result<RollupExpressionProgram, String> {
    let desc_tbl = request
        .desc_tbl
        .as_ref()
        .ok_or_else(|| "rollup expression evaluation requires desc_tbl".to_string())?;
    let input_slots = desc_tbl
        .slot_descriptors
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter_map(|slot| {
            let tuple_id = slot.parent?;
            let slot_id = slot.id?;
            let name = slot
                .col_name
                .as_ref()
                .filter(|value| !value.trim().is_empty())
                .or_else(|| {
                    slot.col_physical_name
                        .as_ref()
                        .filter(|value| !value.trim().is_empty())
                })?
                .clone();
            Some(RollupInputSlot {
                tuple_id,
                slot_id,
                name,
                nullable: slot.is_nullable,
            })
        })
        .collect::<Vec<_>>();
    let layout = crate::protocol::starrocks::decode::layout::Layout {
        order: input_slots
            .iter()
            .map(|slot| (slot.tuple_id, slot.slot_id))
            .collect(),
        index: input_slots
            .iter()
            .enumerate()
            .map(|(index, slot)| ((slot.tuple_id, slot.slot_id), index))
            .collect(),
    };
    let where_expression = request
        .where_expr
        .as_ref()
        .map(|expr| compile_rollup_expression(expr, &layout, "where_expr"))
        .transpose()?;
    let materialized_view_params = request
        .materialized_view_params
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|param| {
            Ok(RollupMaterializedViewParam {
                column_name: param.column_name.clone(),
                origin_column_name: param.origin_column_name.clone(),
                expression: param
                    .mv_expr
                    .as_ref()
                    .map(|expr| {
                        compile_rollup_expression(expr, &layout, "materialized_view_params.mv_expr")
                    })
                    .transpose()?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(RollupExpressionProgram {
        input_slots,
        where_expression,
        materialized_view_params,
    })
}

fn compile_rollup_expression(
    expr: &TExpr,
    layout: &crate::protocol::starrocks::decode::layout::Layout,
    context: &str,
) -> Result<CompiledRollupExpression, String> {
    let mut arena = ExprArena::default();
    let root =
        crate::protocol::starrocks::decode::decode_expression_for_layout(expr, &mut arena, layout)
            .map_err(|error| {
                format!("rollup lower expression failed: context={context} error={error}")
            })?;
    Ok(CompiledRollupExpression { arena, root })
}

pub(crate) fn lake_agent_task_adapter(
    storage_metadata_provider: Arc<
        dyn novarocks::connector::starrocks::ports::StorageMetadataProvider,
    >,
) -> Arc<CompatLakeAgentTaskAdapter> {
    Arc::new(CompatLakeAgentTaskAdapter {
        storage_metadata_provider,
    })
}
