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

//! Compat composition for the temporary BackendService lake-agent callback.

use std::sync::Arc;

use novarocks::connector::starrocks::lake::schema_adapter::build_tablet_schema_from_thrift;
use novarocks::connector::starrocks::lake::schema_change::{
    CompiledRollupExpression, LakeAlterTabletMode, LakeAlterTabletTask, RollupExpressionProgram,
    RollupInputSlot, RollupMaterializedViewParam,
};
use novarocks::exec::expr::ExprArena;
use novarocks::runtime::starlet_shard_registry::StarletShardInfo;
use novarocks::service::backend_service::{self, LakeAgentTaskAdapter};
use novarocks::thrift::agent_service::{
    TAlterJobType, TAlterTabletReqV2, TCreateTabletReq, TTabletType, TUpdateTabletMetaInfoReq,
};
use novarocks::thrift::exprs::TExpr;

struct CompatLakeAgentTaskAdapter {
    storage_metadata_provider:
        Arc<dyn novarocks::connector::starrocks::ports::StorageMetadataProvider>,
}

impl LakeAgentTaskAdapter for CompatLakeAgentTaskAdapter {
    fn create_tablet(
        &self,
        request: &TCreateTabletReq,
        shard_info: &StarletShardInfo,
    ) -> Result<(), String> {
        backend_service::execute_lake_create_tablet(
            request,
            shard_info,
            Arc::clone(&self.storage_metadata_provider),
        )
    }

    fn alter_tablet(&self, request: &TAlterTabletReqV2) -> Result<(), String> {
        let task = adapt_alter_tablet_task(request)?;
        backend_service::execute_lake_alter_tablet(
            task,
            Arc::clone(&self.storage_metadata_provider),
        )
    }

    fn update_tablet_meta_info(&self, request: &TUpdateTabletMetaInfoReq) -> Result<(), String> {
        backend_service::execute_lake_update_tablet_meta_info(
            request,
            Arc::clone(&self.storage_metadata_provider),
        )
    }
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
) -> Arc<dyn LakeAgentTaskAdapter> {
    Arc::new(CompatLakeAgentTaskAdapter {
        storage_metadata_provider,
    })
}
