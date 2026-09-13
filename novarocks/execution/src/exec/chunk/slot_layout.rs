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

//! Carrier-neutral ordering and lookup for lowered execution slots.

use std::collections::HashMap;

use novarocks_types::SlotId;

/// The stable output-slot order for one lowered execution subtree.
///
/// The layout carries no wire, connector, or role-local state. Native adapters
/// map wire column IDs into this vocabulary at their boundary.
#[derive(Clone, Debug, Default)]
pub struct SlotLayout {
    order: Vec<SlotId>,
    index: HashMap<SlotId, usize>,
}

impl SlotLayout {
    /// Builds a layout while retaining the first occurrence of each slot.
    pub fn for_slots(slots: impl IntoIterator<Item = SlotId>) -> Self {
        let mut order = Vec::new();
        let mut index = HashMap::new();
        for slot in slots {
            index.entry(slot).or_insert_with(|| {
                let index = order.len();
                order.push(slot);
                index
            });
        }
        Self { order, index }
    }

    pub fn order(&self) -> &[SlotId] {
        &self.order
    }

    pub fn contains_slot(&self, slot: SlotId) -> bool {
        self.index.contains_key(&slot)
    }

    pub fn index_of_slot(&self, slot: SlotId) -> Option<usize> {
        self.index.get(&slot).copied()
    }

    pub fn index_of_column_id(&self, column_id: u32) -> Option<usize> {
        self.index_of_slot(SlotId::new(column_id))
    }

    pub fn resolve_column_id(&self, column_id: u32) -> Option<SlotId> {
        let slot = SlotId::new(column_id);
        self.contains_slot(slot).then_some(slot)
    }
}

#[cfg(test)]
mod tests {
    use super::SlotLayout;
    use novarocks_types::SlotId;

    #[test]
    fn preserves_first_slot_order_and_lookup() {
        let layout = SlotLayout::for_slots([SlotId::new(7), SlotId::new(3), SlotId::new(7)]);

        assert_eq!(layout.order(), &[SlotId::new(7), SlotId::new(3)]);
        assert_eq!(layout.index_of_slot(SlotId::new(7)), Some(0));
        assert_eq!(layout.index_of_column_id(3), Some(1));
        assert_eq!(layout.resolve_column_id(7), Some(SlotId::new(7)));
        assert_eq!(layout.resolve_column_id(9), None);
    }
}
