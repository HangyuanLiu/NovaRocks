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

//! Borrowed JSON shape receipt for the actual frozen contract decode.
use super::cow_begin::{Cause, CowBeginScope, add, mul, tree_upper};
use serde::de::{DeserializeSeed, Error, MapAccess, SeqAccess, Visitor};
use std::{cell::RefCell, fmt, mem::size_of};

struct Counter<'a, 'b> {
    scope: &'a CowBeginScope<'b>,
    failure: RefCell<Option<Cause>>,
    largest_slot: u64,
}
#[derive(Clone, Copy)]
struct Seed<'a, 'b>(&'a Counter<'a, 'b>);
impl<'de> DeserializeSeed<'de> for Seed<'_, '_> {
    type Value = u64;
    fn deserialize<D: serde::Deserializer<'de>>(self, de: D) -> Result<u64, D::Error> {
        de.deserialize_any(self)
    }
}
impl Seed<'_, '_> {
    fn checked<E: Error>(&self, n: Result<u64, Cause>) -> Result<u64, E> {
        let n = self.0.scope.active().and(n);
        match n {
            Ok(n) => Ok(n),
            Err(e) => {
                *self.0.failure.borrow_mut() = Some(e);
                Err(E::custom("original COW JSON receipt failed"))
            }
        }
    }
}
impl<'de> Visitor<'de> for Seed<'_, '_> {
    type Value = u64;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("frozen COW canonical JSON")
    }
    fn visit_bool<E: Error>(self, _: bool) -> Result<u64, E> {
        self.checked(Ok(0))
    }
    fn visit_i64<E: Error>(self, _: i64) -> Result<u64, E> {
        self.checked(Ok(0))
    }
    fn visit_u64<E: Error>(self, _: u64) -> Result<u64, E> {
        self.checked(Ok(0))
    }
    fn visit_f64<E: Error>(self, _: f64) -> Result<u64, E> {
        self.checked(Ok(0))
    }
    fn visit_unit<E: Error>(self) -> Result<u64, E> {
        self.checked(Ok(0))
    }
    fn visit_none<E: Error>(self) -> Result<u64, E> {
        self.checked(Ok(0))
    }
    fn visit_str<E: Error>(self, s: &str) -> Result<u64, E> {
        self.checked(mul(s.len() as u64, 3))
    }
    fn visit_borrowed_str<E: Error>(self, s: &'de str) -> Result<u64, E> {
        self.visit_str(s)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<u64, A::Error> {
        let mut n = 0u64;
        let mut count = 0u64;
        while let Some(child) = a.next_element_seed(self)? {
            n = self.checked(add(n, child))?;
            count = self.checked(add(count, 1))?;
        }
        // All concrete frozen DTO array elements fit largest_slot. Old and new
        // geometric Vec buffers and minimum-capacity allocation coexist here.
        self.checked(mul(count.max(4), 3 * self.0.largest_slot).and_then(|slots| add(n, slots)))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<u64, A::Error> {
        let mut n = 0u64;
        let mut count = 0u64;
        while let Some(key) = a.next_key_seed(self)? {
            let value = a.next_value_seed(self)?;
            n = self.checked(add(key, value).and_then(|child| add(n, child)))?;
            count = self.checked(add(count, 1))?;
        }
        // Covers property/default maps plus headers of concrete typed objects.
        self.checked(
            tree_upper::<String, serde_json::Value>(count)
                .and_then(|nodes| add(self.0.largest_slot, nodes))
                .and_then(|nodes| add(n, nodes)),
        )
    }
}
pub(crate) fn decoded_upper(bytes: &[u8], scope: &CowBeginScope<'_>) -> Result<u64, Cause> {
    // Deserializer's escaped-key scratch may grow before the borrowed visit;
    // authorize that finite parser buffer independently of the returned graph.
    scope.reserve(mul(bytes.len().max(8) as u64, 3)?)?;
    let largest_slot = [
        size_of::<crate::metadata::IcebergTablePayload>(),
        size_of::<crate::metadata::IcebergTableInfo>(),
        size_of::<crate::scan_model::IcebergDataFileInfo>(),
        size_of::<crate::scan_model::IcebergSchemaFieldDef>(),
        size_of::<serde_json::Value>(),
    ]
    .into_iter()
    .max()
    .unwrap() as u64;
    let counter = Counter {
        scope,
        failure: RefCell::new(None),
        largest_slot,
    };
    let mut de = serde_json::Deserializer::from_slice(bytes);
    let result = Seed(&counter).deserialize(&mut de);
    if let Some(e) = counter.failure.into_inner() {
        return Err(e);
    }
    let upper = result
        .map_err(|_| super::corrupt("Iceberg copy-on-write match contract table is invalid"))?;
    de.end().map_err(|_| {
        super::corrupt("Iceberg copy-on-write match contract table has trailing JSON")
    })?;
    Ok(upper)
}
