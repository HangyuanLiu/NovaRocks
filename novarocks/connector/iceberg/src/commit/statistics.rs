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
//! Design: ADR-0082 (docs/adr/ADR-0082-same-snapshot-statistics-publication-arbitration.md)

use crate::iceberg::spec::StatisticsFile;
use crate::iceberg::table::Table;
use crate::iceberg::transaction::{ApplyTransactionAction, Transaction};
use crate::stats_assembler::StatisticsCoverageMark;

/// How many times a losing commit race is retried before giving up.
///
/// Statistics commits are metadata-only and cheap to redo, but they are also
/// never worth blocking on: the data they describe is already published.
const MAX_STATISTICS_COMMIT_ATTEMPTS: usize = 5;

/// What a publication attempt did to the table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsCommitOutcome {
    /// The entry is now registered against its snapshot.
    Registered,
    /// A more complete entry already covers this snapshot, so this one stood
    /// down. Not a failure — an incremental union has nothing to add to a full
    /// scan of the same rows.
    YieldedToFullerCoverage,
}

/// Registers `stats_file`, deciding against whatever currently occupies its
/// snapshot and re-deciding after every lost race.
///
/// Iceberg keeps one statistics file per snapshot and `set_statistics`
/// replaces it, so two writers aiming at the same snapshot need an order. A
/// full visible-row scan outranks an incremental union; between equals the
/// later commit wins, since they describe the same rows and the newer one is
/// simply fresher evidence.
///
/// The re-decision after a conflict is the point: a blind retry would let an
/// incremental result overwrite a full scan that landed in between, which is
/// exactly the race the ranking exists to prevent.
pub async fn commit_statistics_file(
    table: &Table,
    catalog: &dyn crate::iceberg::Catalog,
    stats_file: StatisticsFile,
    coverage: StatisticsCoverageMark,
) -> Result<StatisticsCommitOutcome, String> {
    let mut table = table.clone();
    for attempt in 0..MAX_STATISTICS_COMMIT_ATTEMPTS {
        if !supersedes_registered_entry(&table, &stats_file, coverage) {
            return Ok(StatisticsCommitOutcome::YieldedToFullerCoverage);
        }
        let tx = Transaction::new(&table);
        let action = tx.update_statistics().set_statistics(stats_file.clone());
        let tx = action
            .apply(tx)
            .map_err(|e| format!("iceberg update_statistics apply failed: {e}"))?;
        match tx.commit(catalog).await {
            Ok(committed) => {
                let _ = committed;
                return Ok(StatisticsCommitOutcome::Registered);
            }
            Err(error) if attempt + 1 < MAX_STATISTICS_COMMIT_ATTEMPTS => {
                // Reload before deciding again: the winner of this race may
                // have registered something this entry must not replace.
                table = crate::iceberg::Catalog::load_table(catalog, table.identifier())
                    .await
                    .map_err(|reload| {
                        format!(
                            "iceberg update_statistics commit failed ({error}); \
                             reloading the table to re-decide also failed: {reload}"
                        )
                    })?;
            }
            Err(error) => {
                return Err(format!("iceberg update_statistics commit failed: {error}"));
            }
        }
    }
    Err("iceberg update_statistics exhausted its commit attempts".to_string())
}

/// Whether `candidate` should replace whatever is registered for its snapshot.
fn supersedes_registered_entry(
    table: &Table,
    candidate: &StatisticsFile,
    coverage: StatisticsCoverageMark,
) -> bool {
    let Some(registered) = table
        .metadata()
        .statistics_for_snapshot(candidate.snapshot_id)
    else {
        return true;
    };
    if registered.statistics_path == candidate.statistics_path {
        // Re-registering the identical artifact, e.g. a reconcile after an
        // uncertain commit.
        return true;
    }
    match (StatisticsCoverageMark::of(registered), coverage) {
        // Only this pairing stands down; everything else is same-rank, where
        // the later writer is the fresher evidence.
        (StatisticsCoverageMark::AllVisibleRows, StatisticsCoverageMark::IncrementalUnion) => false,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::iceberg::spec::{BlobMetadata, StatisticsFile};
    use crate::stats_assembler::{
        STATISTICS_COVERAGE_ALL_VISIBLE_ROWS, STATISTICS_COVERAGE_PROPERTY,
    };

    use super::*;

    fn entry(path: &str, coverage: Option<&str>) -> StatisticsFile {
        StatisticsFile {
            snapshot_id: 7,
            statistics_path: path.to_string(),
            file_size_in_bytes: 1,
            file_footer_size_in_bytes: 1,
            key_metadata: None,
            blob_metadata: vec![BlobMetadata {
                r#type: "apache-datasketches-theta-v1".to_string(),
                snapshot_id: 7,
                sequence_number: 1,
                fields: vec![1],
                properties: coverage
                    .map(|value| {
                        HashMap::from([(
                            STATISTICS_COVERAGE_PROPERTY.to_string(),
                            value.to_string(),
                        )])
                    })
                    .unwrap_or_default(),
            }],
        }
    }

    #[test]
    fn an_entry_without_the_marker_reads_as_incremental() {
        // Conservative by design: the unknown side is the one that yields.
        assert_eq!(
            StatisticsCoverageMark::of(&entry("s.puffin", None)),
            StatisticsCoverageMark::IncrementalUnion
        );
        assert_eq!(
            StatisticsCoverageMark::of(&entry(
                "s.puffin",
                Some(STATISTICS_COVERAGE_ALL_VISIBLE_ROWS)
            )),
            StatisticsCoverageMark::AllVisibleRows
        );
    }
}
