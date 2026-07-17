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

//! Shared metadata-only commit for an iceberg Puffin StatisticsFile.

use iceberg::spec::StatisticsFile;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};

/// Apply `stats_file` to `table` via a metadata-only `update_statistics`
/// transaction and commit it through `catalog`. Returns an error on apply or
/// commit failure (callers decide whether to surface or log it).
pub(crate) async fn commit_statistics_file(
    table: &Table,
    catalog: &dyn iceberg::Catalog,
    stats_file: StatisticsFile,
) -> Result<(), String> {
    let tx = Transaction::new(table);
    let action = tx.update_statistics().set_statistics(stats_file);
    let tx = action
        .apply(tx)
        .map_err(|e| format!("iceberg update_statistics apply failed: {e}"))?;
    tx.commit(catalog)
        .await
        .map_err(|e| format!("iceberg update_statistics commit failed: {e}"))?;
    Ok(())
}
