// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::{ParamParser, ProtocolLimits, StatementData};
use std::io;

/// Conservative allocation coverage for one protocol input owner. Index and payload coverage
/// uses complete Vec capacities, including every empty/spare slot.
/// The adapter must retain the complete connection_input_bytes guard throughout
/// IO/alias lifetime. These observations do not declare physical reclamation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProtocolInputUsage {
    pub command_backing_bytes: usize,
    pub input_control_bytes: usize,
    pub statement_count: usize,
    pub parameter_count: usize,
    pub long_data_entries: usize,
    pub long_data_payload_bytes: usize,
    pub long_data_capacity_bytes: usize,
    pub bound_type_capacity_bytes: usize,
    pub bookkeeping_capacity_bytes: usize,
    pub total_capacity_bytes: usize,
}
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "MySQL connection input capacity exceeds limit",
    )
}
fn add(a: usize, b: usize) -> io::Result<usize> {
    a.checked_add(b).ok_or_else(invalid)
}
fn vec_bytes<T>(capacity: usize) -> io::Result<usize> {
    capacity
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(invalid)
}

/// One exact slot index, allocated from the bounded declared parameter count.
/// Empty bindings use an already reserved slot and no payload allocation.
#[derive(Default)]
pub(crate) struct LongData {
    slots: Vec<Option<Vec<u8>>>,
}
impl LongData {
    fn new(params: usize) -> io::Result<Self> {
        let mut owner = Self::default();
        owner.ensure_slots(params)?;
        Ok(owner)
    }
    pub fn ensure_slots(&mut self, params: usize) -> io::Result<()> {
        if params > crate::ProtocolLimits::default().columns {
            return Err(invalid());
        }
        if self.slots.len() < params {
            self.slots
                .try_reserve_exact(params - self.slots.len())
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        "long data index allocation failed",
                    )
                })?;
            self.slots.resize_with(params, || None);
        }
        Ok(())
    }
    pub fn get(&self, param: &u16) -> Option<&Vec<u8>> {
        self.slots.get(*param as usize).and_then(Option::as_ref)
    }
    pub fn contains_key(&self, param: &u16) -> bool {
        self.get(param).is_some()
    }
    pub fn get_or_insert(&mut self, param: u16) -> io::Result<&mut Vec<u8>> {
        Ok(self
            .slots
            .get_mut(param as usize)
            .ok_or_else(invalid)?
            .get_or_insert_with(Vec::new))
    }
    pub fn values(&self) -> impl Iterator<Item = &Vec<u8>> {
        self.slots.iter().filter_map(Option::as_ref)
    }
    fn len(&self) -> usize {
        self.values().count()
    }
    fn capacity(&self) -> usize {
        self.slots.capacity()
    }
    fn clear(&mut self) {
        for slot in &mut self.slots {
            *slot = None;
        }
    }
}
#[cfg(test)]
impl std::ops::Index<&u16> for LongData {
    type Output = Vec<u8>;
    fn index(&self, param: &u16) -> &Self::Output {
        self.get(param).expect("internal long data test index")
    }
}

pub(crate) struct PreparedStatements {
    states: Vec<(u32, StatementData)>,
    limits: ProtocolLimits,
}
impl PreparedStatements {
    fn state(&self, id: u32) -> Option<&StatementData> {
        self.states
            .iter()
            .find(|(key, _)| *key == id)
            .map(|(_, state)| state)
    }
    fn state_mut(&mut self, id: u32) -> Option<&mut StatementData> {
        self.states
            .iter_mut()
            .find(|(key, _)| *key == id)
            .map(|(_, state)| state)
    }
    pub fn new(limits: ProtocolLimits) -> io::Result<Self> {
        let peak = add(
            add(limits.command_bytes, 1024)?,
            vec_bytes::<(u32, StatementData)>(limits.prepared_statements)?,
        )?;
        if peak > limits.connection_input_bytes {
            return Err(invalid());
        }
        let mut states = Vec::new();
        states
            .try_reserve_exact(limits.prepared_statements)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "statement index allocation failed",
                )
            })?;
        let owner = Self { states, limits };
        owner.check(owner.usage()?)?;
        Ok(owner)
    }
    pub fn usage(&self) -> io::Result<ProtocolInputUsage> {
        let mut usage = ProtocolInputUsage {
            command_backing_bytes: self.limits.command_bytes,
            input_control_bytes: 1024,
            statement_count: self.states.len(),
            bookkeeping_capacity_bytes: add(
                1024,
                vec_bytes::<(u32, StatementData)>(self.states.capacity())?,
            )?,
            ..Default::default()
        };
        for (_, state) in &self.states {
            usage.parameter_count = add(usage.parameter_count, state.params as usize)?;
            usage.long_data_entries = add(usage.long_data_entries, state.long_data.len())?;
            usage.bookkeeping_capacity_bytes = add(
                usage.bookkeeping_capacity_bytes,
                vec_bytes::<Option<Vec<u8>>>(state.long_data.capacity())?,
            )?;
            usage.bound_type_capacity_bytes = add(
                usage.bound_type_capacity_bytes,
                vec_bytes::<(crate::ColumnType, bool)>(state.bound_types.capacity())?,
            )?;
            for data in state.long_data.values() {
                usage.long_data_payload_bytes = add(usage.long_data_payload_bytes, data.len())?;
                usage.long_data_capacity_bytes =
                    add(usage.long_data_capacity_bytes, data.capacity())?;
            }
        }
        usage.bookkeeping_capacity_bytes = add(
            usage.bookkeeping_capacity_bytes,
            add(
                usage.bound_type_capacity_bytes,
                usage.long_data_capacity_bytes,
            )?,
        )?;
        usage.total_capacity_bytes = add(
            usage.command_backing_bytes,
            usage.bookkeeping_capacity_bytes,
        )?;
        Ok(usage)
    }
    fn check(&self, usage: ProtocolInputUsage) -> io::Result<()> {
        if usage.total_capacity_bytes > self.limits.connection_input_bytes
            || usage.statement_count > self.limits.prepared_statements
            || usage.parameter_count > self.limits.prepared_parameters
            || usage.long_data_entries > self.limits.long_data_entries
            || usage.long_data_payload_bytes > self.limits.long_data_bytes
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub fn prepare(&mut self, id: u32, params: usize) -> io::Result<()> {
        let usage = self.usage()?;
        let old_params = self.state(id).map_or(0, |state| state.params as usize);
        if params > self.limits.columns
            || (self.state(id).is_none() && self.states.len() >= self.limits.prepared_statements)
            || add(usage.parameter_count - old_params, params)? > self.limits.prepared_parameters
        {
            return Err(invalid());
        }
        // Reserve each declared statement's complete bounded parameter indexes
        // once. Execute/long-data insertion cannot trigger index/type growth.
        let maps = vec_bytes::<Option<Vec<u8>>>(params)?;
        let types = vec_bytes::<(crate::ColumnType, bool)>(params)?;
        // Replacement's old and new allocations coexist until insert drops old.
        if add(usage.total_capacity_bytes, add(maps, types)?)? > self.limits.connection_input_bytes
        {
            return Err(invalid());
        }
        let long_data = LongData::new(params)?;
        let mut bound_types = Vec::new();
        bound_types.try_reserve_exact(params).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "parameter type allocation failed",
            )
        })?;
        let state = StatementData {
            long_data,
            bound_types,
            params: params as u16,
        };
        if let Some(index) = self.states.iter().position(|(key, _)| *key == id) {
            self.states[index] = (id, state);
        } else {
            self.states.push((id, state));
        }
        self.check(self.usage()?)
    }
    pub fn append_long_data(&mut self, id: u32, param: u16, data: &[u8]) -> io::Result<()> {
        let usage = self.usage()?;
        let state = self.state(id).ok_or_else(invalid)?;
        if param >= state.params {
            return Err(invalid());
        }
        if add(usage.long_data_payload_bytes, data.len())? > self.limits.long_data_bytes
            || (!state.long_data.contains_key(&param)
                && usage.long_data_entries >= self.limits.long_data_entries)
        {
            return Err(invalid());
        }
        let current = state.long_data.get(&param);
        let len = add(current.map_or(0, Vec::len), data.len())?;
        let capacity = current.map_or(0, Vec::capacity);
        // A reallocation may keep the old allocation while allocating the new.
        if len > capacity
            && add(usage.total_capacity_bytes, len)? > self.limits.connection_input_bytes
        {
            return Err(invalid());
        }
        let limit = self.limits.long_data_bytes;
        self.state_mut(id)
            .ok_or_else(invalid)?
            .append_long_data(param, data, limit)?;
        self.check(self.usage()?)
    }
    pub fn parser<'a>(&'a mut self, id: u32, input: &'a [u8]) -> io::Result<ParamParser<'a>> {
        let state = self.state_mut(id).ok_or_else(invalid)?;
        if state.bound_types.capacity() < state.params as usize {
            return Err(invalid());
        }
        ParamParser::new(input, state)
    }
    pub fn clear_long_data(&mut self, id: u32) {
        if let Some(state) = self.state_mut(id) {
            state.long_data.clear();
        }
    }
    pub fn remove(&mut self, id: u32) {
        if let Some(index) = self.states.iter().position(|(key, _)| *key == id) {
            self.states.swap_remove(index);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn spare_payload_capacity_counts_toward_complete_connection_bound() {
        let limits = ProtocolLimits::default();
        let mut states = PreparedStatements::new(limits).unwrap();
        states.prepare(1, 2).unwrap();
        states.append_long_data(1, 0, b"a").unwrap();
        states
            .state_mut(1)
            .unwrap()
            .long_data
            .get_or_insert(0)
            .unwrap()
            .reserve_exact(700_000);
        let before = states.usage().unwrap();
        assert_eq!(before.long_data_payload_bytes, 1);
        assert!(before.long_data_capacity_bytes >= 700_000);
        assert!(states.append_long_data(1, 1, &vec![0; 900_000]).is_err());
        assert_eq!(states.usage().unwrap(), before);
    }
}
