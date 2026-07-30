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

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use crate::connector::starrocks::ports::{SinkFrontendAddress, SinkFrontendProvider};
use crate::connector::starrocks::sink::plan::FrontendAddress;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AutoIncrementInterval {
    next: i64,
    end: i64,
}

static AUTO_INCREMENT_INTERVALS: OnceLock<Mutex<HashMap<i64, AutoIncrementInterval>>> =
    OnceLock::new();

fn interval_cache() -> &'static Mutex<HashMap<i64, AutoIncrementInterval>> {
    AUTO_INCREMENT_INTERVALS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn allocate_auto_increment_ids(
    provider: Option<&dyn SinkFrontendProvider>,
    fe_addr: &FrontendAddress,
    table_id: i64,
    rows: usize,
) -> Result<Vec<i64>, String> {
    if rows == 0 {
        return Ok(Vec::new());
    }
    if table_id <= 0 {
        return Err(format!("invalid table_id for auto increment: {table_id}"));
    }

    let mut result = Vec::with_capacity(rows);
    let mut remaining = rows;
    let mut guard = interval_cache()
        .lock()
        .map_err(|_| "lock auto increment cache failed".to_string())?;

    while remaining > 0 {
        let usable = guard
            .get_mut(&table_id)
            .and_then(|interval| (interval.next < interval.end).then_some(interval));
        if let Some(interval) = usable {
            let available = usize::try_from(interval.end - interval.next).unwrap_or(0);
            if available > 0 {
                let take = remaining.min(available);
                let start = interval.next;
                let end = start
                    .checked_add(i64::try_from(take).map_err(|_| {
                        format!("auto increment take size overflow: table_id={table_id}, take={take}")
                    })?)
                    .ok_or_else(|| {
                        format!(
                            "auto increment range overflow while assigning ids: table_id={table_id}, start={start}, take={take}"
                        )
                    })?;
                result.extend(start..end);
                interval.next = end;
                remaining -= take;
                continue;
            }
        }

        let request_rows = remaining.max(1024);
        let provider = provider.ok_or_else(|| {
            "OLAP_TABLE_SINK auto_increment requires StarRocks FE capability".to_string()
        })?;
        let interval = provider.allocate_auto_increment_range(
            &SinkFrontendAddress {
                host: fe_addr.hostname.clone(),
                port: fe_addr.port,
            },
            table_id,
            request_rows,
        )?;
        let interval = AutoIncrementInterval {
            next: interval.start,
            end: interval.end,
        };
        guard.insert(table_id, interval);
    }

    Ok(result)
}

pub(crate) fn clear_auto_increment_cache_for_table(table_id: i64) {
    if table_id <= 0 {
        return;
    }
    if let Ok(mut guard) = interval_cache().lock() {
        guard.remove(&table_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::starrocks::ports::{
        AutoIncrementRange, AutomaticPartitionRequest, AutomaticPartitionResult,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct RecordingFrontendProvider {
        calls: AtomicUsize,
    }

    impl SinkFrontendProvider for RecordingFrontendProvider {
        fn create_automatic_partitions(
            &self,
            _request: &AutomaticPartitionRequest,
        ) -> Result<AutomaticPartitionResult, String> {
            unreachable!("auto increment test does not create partitions")
        }

        fn allocate_auto_increment_range(
            &self,
            _frontend: &SinkFrontendAddress,
            _table_id: i64,
            _rows: usize,
        ) -> Result<AutoIncrementRange, String> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(AutoIncrementRange {
                start: 100,
                end: 1_124,
            })
        }
    }

    #[test]
    fn native_mode_rejects_auto_increment_without_frontend_capability() {
        const TABLE_ID: i64 = 9_001;
        clear_auto_increment_cache_for_table(TABLE_ID);
        let frontend = FrontendAddress {
            hostname: "127.0.0.1".to_string(),
            port: 9_030,
        };

        let error = allocate_auto_increment_ids(None, &frontend, TABLE_ID, 1)
            .expect_err("auto increment must require the injected FE capability");

        assert_eq!(
            error,
            "OLAP_TABLE_SINK auto_increment requires StarRocks FE capability"
        );
    }

    #[test]
    fn uses_injected_frontend_provider_for_auto_increment_ranges() {
        const TABLE_ID: i64 = 9_002;
        clear_auto_increment_cache_for_table(TABLE_ID);
        let frontend = FrontendAddress {
            hostname: "127.0.0.1".to_string(),
            port: 9_030,
        };
        let provider = RecordingFrontendProvider {
            calls: AtomicUsize::new(0),
        };

        let ids = allocate_auto_increment_ids(Some(&provider), &frontend, TABLE_ID, 3)
            .expect("injected provider should supply auto increment ids");

        assert_eq!(ids, vec![100, 101, 102]);
        assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
        clear_auto_increment_cache_for_table(TABLE_ID);
    }
}
