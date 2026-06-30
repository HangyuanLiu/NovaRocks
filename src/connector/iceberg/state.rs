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

use crate::runtime::descriptor_snapshot::DescriptorSnapshot;

static ICEBERG_TABLE_LOCATIONS: OnceLock<Mutex<HashMap<i64, String>>> = OnceLock::new();

fn iceberg_table_locations() -> &'static Mutex<HashMap<i64, String>> {
    ICEBERG_TABLE_LOCATIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn cache_iceberg_table_locations(snapshot: Option<&DescriptorSnapshot>) {
    let Some(snapshot) = snapshot else { return };
    let mut guard = iceberg_table_locations()
        .lock()
        .expect("iceberg_table_locations lock");
    for (table_id, location) in snapshot.iceberg_table_locations() {
        guard.insert(table_id, location.to_string());
    }
}

pub(crate) fn lookup_iceberg_table_location(table_id: i64) -> Option<String> {
    iceberg_table_locations()
        .lock()
        .expect("iceberg_table_locations lock")
        .get(&table_id)
        .cloned()
}

pub(crate) fn snapshot_iceberg_table_locations() -> HashMap<i64, String> {
    iceberg_table_locations()
        .lock()
        .expect("iceberg_table_locations lock")
        .clone()
}
