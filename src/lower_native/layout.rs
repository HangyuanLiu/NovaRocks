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

//! Proto layout lowering placeholder.

use std::collections::HashMap;

#[derive(Clone, Debug, Default)]
pub(crate) struct Layout {
    order: Vec<crate::common::ids::SlotId>,
    index: HashMap<crate::common::ids::SlotId, usize>,
}

impl Layout {
    #[allow(dead_code)]
    pub(crate) fn for_slots(slots: impl IntoIterator<Item = crate::common::ids::SlotId>) -> Self {
        let mut order = Vec::new();
        let mut index = HashMap::new();
        for slot in slots {
            index.entry(slot).or_insert_with(|| {
                let idx = order.len();
                order.push(slot);
                idx
            });
        }
        Self { order, index }
    }

    pub(crate) fn contains_slot(&self, slot: crate::common::ids::SlotId) -> bool {
        self.index.contains_key(&slot)
    }

    pub(crate) fn resolve_column_id(
        &self,
        column_id: u32,
    ) -> Result<crate::common::ids::SlotId, String> {
        let slot = crate::common::ids::SlotId::new(column_id);
        if self.contains_slot(slot)
            && let Some(index) = self.index.get(&slot)
            && self.order.get(*index) == Some(&slot)
        {
            Ok(slot)
        } else {
            Err(format!(
                "ColumnRef column_id={} not found in input layout",
                column_id
            ))
        }
    }
}
