// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Borrowed structural preflight of the closed StoredMvProjectionV3 datum.
//! The envelope's registered schema and fingerprint are checked by the caller.
//! No Avro Value, ByteBuf or document model exists until this check succeeds.

use crate::persistence::codec::preflight_current_document_set;
use crate::persistence::validation::PersistenceDecodeBudget;

pub(super) fn projection_v3(
    payload: &[u8],
    budget: PersistenceDecodeBudget,
) -> Result<PersistenceDecodeBudget, String> {
    let mut input = Datum { remaining: payload };
    input.long()?; // mv_id
    let definition = input.bytes()?;
    let interpretation = input.bytes()?;
    let configuration = input.bytes()?;
    let publication = input.optional(Datum::bytes)?;
    input.provider_version()?;
    input.optional(Datum::provider_version)?;
    input.optional(Datum::long)?;
    // MvAcceleratorSourceRevisionV3, in registered schema order.
    input.string()?;
    input.string()?;
    input.string()?;
    input.bytes()?; // opaque target object ID
    input.version_revision()?;
    input.string()?;
    input.string()?;
    input.optional(Datum::string)?;
    input.optional(Datum::version_revision)?;
    input.string()?;
    input.string()?;
    input.string()?;
    if !input.remaining.is_empty() {
        return Err("MV Accelerator Avro payload has trailing bytes".into());
    }
    // Avro datum, serde materialization and the immutable source revision can
    // overlap with the document codec's own six-copy structural envelope.
    // The outer schema has no collections: four payload copies plus fixed
    // record/string headers cover its simultaneous owned representations.
    let outer = payload
        .len()
        .checked_mul(4)
        .and_then(|n| n.checked_add(4096))
        .ok_or("MV projection outer decode working set overflows")?;
    let available = budget
        .max_working_set_bytes
        .checked_sub(outer)
        .ok_or("MV projection exceeds its outer decode working set bound")?;
    let documents = PersistenceDecodeBudget {
        max_working_set_bytes: available,
        ..budget
    };
    preflight_current_document_set(
        definition,
        interpretation,
        publication,
        configuration,
        documents,
    )
    .map_err(|error| error.to_string())?;
    Ok(documents)
}

/// The registered dependency V3 is a fixed record, with no collections.
pub(super) fn dependency_v3(payload: &[u8], working_set_bytes: usize) -> Result<(), String> {
    let mut input = Datum { remaining: payload };
    input.long()?;
    input.long()?;
    input.bytes()?;
    input.optional(Datum::string)?;
    input.string()?;
    input.string()?;
    if !(0..=2).contains(&input.long()?) || !(0..=3).contains(&input.long()?) {
        return Err("MV dependency Avro enum index is invalid".into());
    }
    input.long()?;
    if !input.remaining.is_empty() {
        return Err("MV dependency Avro payload has trailing bytes".into());
    }
    if payload
        .len()
        .checked_mul(4)
        .and_then(|n| n.checked_add(4096))
        .is_none_or(|n| n > working_set_bytes)
    {
        return Err("MV dependency exceeds its decode working set bound".into());
    }
    Ok(())
}

struct Datum<'a> {
    remaining: &'a [u8],
}

impl<'a> Datum<'a> {
    fn long(&mut self) -> Result<i64, String> {
        let mut value = 0u64;
        for shift in (0..70).step_by(7) {
            let (&byte, rest) = self
                .remaining
                .split_first()
                .ok_or("MV Accelerator Avro integer is truncated")?;
            self.remaining = rest;
            if shift == 63 && byte > 1 {
                return Err("MV Accelerator Avro integer overflows".into());
            }
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(((value >> 1) as i64) ^ -((value & 1) as i64));
            }
        }
        Err("MV Accelerator Avro integer overflows".into())
    }

    fn bytes(&mut self) -> Result<&'a [u8], String> {
        let length = usize::try_from(self.long()?)
            .map_err(|_| "MV Accelerator Avro byte length is negative or overflows")?;
        if length > self.remaining.len() {
            return Err("MV Accelerator Avro bytes are truncated".into());
        }
        let (value, rest) = self.remaining.split_at(length);
        self.remaining = rest;
        Ok(value)
    }

    fn string(&mut self) -> Result<&'a str, String> {
        std::str::from_utf8(self.bytes()?)
            .map_err(|_| "MV Accelerator Avro string is not UTF-8".into())
    }

    fn optional<T>(
        &mut self,
        read: impl FnOnce(&mut Self) -> Result<T, String>,
    ) -> Result<Option<T>, String> {
        match self.long()? {
            0 => Ok(None),
            1 => read(self).map(Some),
            _ => Err("MV Accelerator Avro optional union index is invalid".into()),
        }
    }

    fn provider_version(&mut self) -> Result<(), String> {
        self.bytes()?;
        self.optional(Datum::long)?;
        Ok(())
    }

    fn version_revision(&mut self) -> Result<(), String> {
        self.string()?;
        self.optional(Datum::long)?;
        Ok(())
    }
}
