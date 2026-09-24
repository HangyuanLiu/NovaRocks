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

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::Int32Array;
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use novarocks_connector_paimon::metadata::freeze_table;
use novarocks_connector_paimon::resources::PaimonRequestControl;
use novarocks_connector_paimon::split_source::{
    PaimonSplitPlanningLimits, PaimonSplitSource, plan_splits,
};
use novarocks_spi::connector::ConnectorErrorKind;
use novarocks_spi::connector::read_stack::{ConnectorSplitSource, SchemaTableName};
use paimon::Table;
use paimon::catalog::Identifier;
use paimon::io::FileIOBuilder;
use paimon::spec::{DataType, IntType, Schema, TableSchema};

fn control(
    cancellation: &Arc<novarocks_spi::connector::ConnectorStopOwner>,
) -> PaimonRequestControl {
    PaimonRequestControl::new(
        cancellation.view(),
        Instant::now() + Duration::from_secs(60),
    )
}

fn append_table(path: &str) -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .build()
        .unwrap();
    Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("db", "append_t"),
        path.to_string(),
        TableSchema::new(0, &schema),
        None,
    )
}

fn primary_key_table(path: &str) -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::with_nullable(false)))
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .build()
        .unwrap();
    Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("db", "pk_t"),
        path.to_string(),
        TableSchema::new(0, &schema),
        None,
    )
}

fn batch(ids: &[i32], values: &[i32]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("value", ArrowDataType::Int32, true),
        ])),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(Int32Array::from(values.to_vec())),
        ],
    )
    .unwrap()
}

async fn write(table: &Table, rows: RecordBatch, user: &str) {
    let schema_path = table.schema_manager().schema_path(table.schema().id());
    let schema_dir = schema_path.rsplit_once('/').unwrap().0;
    table.file_io().mkdirs(schema_dir).await.unwrap();
    table
        .file_io()
        .new_output(&schema_path)
        .unwrap()
        .write(bytes::Bytes::from(
            serde_json::to_vec(table.schema()).unwrap(),
        ))
        .await
        .unwrap();
    let builder = table.new_write_builder().with_commit_user(user).unwrap();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&rows).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    builder.new_commit().commit(messages).await.unwrap();
}

#[tokio::test]
async fn frozen_s1_plan_never_switches_to_newly_published_s2() {
    let table = append_table("memory://snapshot-pin/db/append_t");
    write(&table, batch(&[1], &[10]), "s1").await;
    let cancellation = Arc::new(novarocks_spi::connector::ConnectorStopOwner::new());
    let frozen = freeze_table(
        table.clone(),
        SchemaTableName::try_new("db", "append_t").unwrap(),
        control(&cancellation),
    )
    .await
    .unwrap();
    let s1 = frozen.view().snapshot_id().unwrap();

    write(&table, batch(&[2], &[20]), "s2").await;
    assert!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap()
            .unwrap()
            > s1
    );
    let planned = plan_splits(
        &frozen,
        control(&cancellation),
        PaimonSplitPlanningLimits::default(),
    )
    .await
    .unwrap();
    assert!(!planned.is_empty());
    assert!(
        planned
            .iter()
            .all(|split| split.sdk_split().snapshot_id() == s1)
    );
    assert_eq!(
        planned
            .iter()
            .map(|split| split.sdk_split().row_count())
            .sum::<i64>(),
        1
    );
    drop(planned);
    drop(frozen);
}

#[tokio::test]
async fn merge_tree_group_larger_than_target_is_not_resplit() {
    let table = primary_key_table("memory://merge-group/db/pk_t");
    write(&table, batch(&[1], &[10]), "first").await;
    write(&table, batch(&[1], &[20]), "second").await;
    let cancellation = Arc::new(novarocks_spi::connector::ConnectorStopOwner::new());
    let frozen = freeze_table(
        table,
        SchemaTableName::try_new("db", "pk_t").unwrap(),
        control(&cancellation),
    )
    .await
    .unwrap();
    let planned = plan_splits(
        &frozen,
        control(&cancellation),
        PaimonSplitPlanningLimits {
            target_split_bytes: 1,
            ..PaimonSplitPlanningLimits::default()
        },
    )
    .await
    .unwrap();
    assert!(!planned.is_empty());
    for group in &planned {
        assert_eq!(
            group.split().files().len(),
            group.sdk_split().data_files().len()
        );
        assert_eq!(group.split().snapshot_id(), group.sdk_split().snapshot_id());
    }
    drop(planned);
    drop(frozen);
}

#[tokio::test]
async fn table_without_snapshot_is_a_finished_zero_split_source() {
    let table = append_table("memory://empty/db/append_t");
    let cancellation = Arc::new(novarocks_spi::connector::ConnectorStopOwner::new());
    let frozen = freeze_table(
        table,
        SchemaTableName::try_new("db", "append_t").unwrap(),
        control(&cancellation),
    )
    .await
    .unwrap();
    assert_eq!(frozen.view().snapshot_id(), None);
    let planned = plan_splits(
        &frozen,
        control(&cancellation),
        PaimonSplitPlanningLimits::default(),
    )
    .await
    .unwrap();
    let mut source = PaimonSplitSource::new(planned, control(&cancellation)).unwrap();
    assert!(source.is_finished());
    let batch = source.next_planned_batch(16).unwrap();
    assert!(batch.is_empty());
    assert!(batch.no_more_splits());
    source.close().unwrap();
    source.close().unwrap();
    drop(source);
    drop(frozen);
}

#[tokio::test]
async fn cancellation_reaches_split_source() {
    let table = append_table("memory://budget/db/append_t");
    write(&table, batch(&[1], &[10]), "s1").await;
    let cancellation = Arc::new(novarocks_spi::connector::ConnectorStopOwner::new());
    let frozen = freeze_table(
        table,
        SchemaTableName::try_new("db", "append_t").unwrap(),
        control(&cancellation),
    )
    .await
    .unwrap();
    let planned = plan_splits(
        &frozen,
        control(&cancellation),
        PaimonSplitPlanningLimits::default(),
    )
    .await
    .unwrap();
    let mut source = PaimonSplitSource::new(planned, control(&cancellation)).unwrap();
    cancellation.request_stop();
    assert_eq!(
        source.next_planned_batch(1).unwrap_err().kind(),
        ConnectorErrorKind::Cancelled
    );
    source.close().unwrap();
    drop(source);
    drop(frozen);
}
