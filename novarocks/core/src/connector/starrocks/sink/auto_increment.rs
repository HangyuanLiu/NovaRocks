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

use crate::connector::starrocks::sink::frontend_wire::{
    AutoIncrementInterval, allocate_auto_increment_interval,
};
use crate::connector::starrocks::sink::plan::FrontendAddress;

static AUTO_INCREMENT_INTERVALS: OnceLock<Mutex<HashMap<i64, AutoIncrementInterval>>> =
    OnceLock::new();

fn interval_cache() -> &'static Mutex<HashMap<i64, AutoIncrementInterval>> {
    AUTO_INCREMENT_INTERVALS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn allocate_auto_increment_ids(
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
        let interval = allocate_auto_increment_interval(fe_addr, table_id, request_rows)?;
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
