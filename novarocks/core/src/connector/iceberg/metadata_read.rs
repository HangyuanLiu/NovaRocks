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

//! Core compatibility adapter for provider-owned Iceberg metadata walks.

use novarocks_connector_iceberg::iceberg::io::FileIO;
use novarocks_connector_iceberg::iceberg::table::Table;

use crate::connector::iceberg::IcebergMetadataTableType;

fn provider_type(
    ty: IcebergMetadataTableType,
) -> Result<novarocks_connector_iceberg::metadata_read::MetadataTableType, String> {
    match ty {
        IcebergMetadataTableType::Files => {
            Ok(novarocks_connector_iceberg::metadata_read::MetadataTableType::Files)
        }
        IcebergMetadataTableType::Manifests => {
            Ok(novarocks_connector_iceberg::metadata_read::MetadataTableType::Manifests)
        }
        IcebergMetadataTableType::LogicalIcebergMetadata => Ok(
            novarocks_connector_iceberg::metadata_read::MetadataTableType::LogicalIcebergMetadata,
        ),
        other => Err(format!(
            "provider metadata walk does not support {:?}",
            other
        )),
    }
}

pub async fn read_metadata_table_rows(
    table: &Table,
    file_io: &FileIO,
    ty: IcebergMetadataTableType,
) -> Result<String, String> {
    novarocks_connector_iceberg::metadata_read::read_metadata_table_rows(
        table,
        file_io,
        provider_type(ty)?,
    )
    .await
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    #[tokio::test]
    async fn read_metadata_table_rows_empty_snapshot_returns_empty() {
        let fixture =
            crate::connector::iceberg::commit::test_helpers::empty_v3_iceberg_table().await;
        let file_io = fixture.table.file_io().clone();
        let payload =
            read_metadata_table_rows(&fixture.table, &file_io, IcebergMetadataTableType::Files)
                .await
                .expect("read_metadata_table_rows on empty table should succeed");
        let actual: Value = serde_json::from_str(&payload).expect("payload must be valid JSON");
        let expected: Value = json!({ "version": 1, "rows": [] });
        assert_eq!(actual, expected);
    }
}
