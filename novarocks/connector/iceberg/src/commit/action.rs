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

//! Resolve provider-owned snapshot summary declarations.

use std::collections::{BTreeMap, HashMap};

pub(super) fn merge_snapshot_summary_properties(
    mut built_in: HashMap<String, String>,
    snapshot_properties: &BTreeMap<String, String>,
) -> Result<HashMap<String, String>, String> {
    let provider_properties = snapshot_properties.clone();
    for key in provider_properties.keys() {
        if built_in.contains_key(key) {
            return Err(format!(
                "snapshot property {key} conflicts with built-in summary field"
            ));
        }
    }
    built_in.extend(provider_properties);
    Ok(built_in)
}
