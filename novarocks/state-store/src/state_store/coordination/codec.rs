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

use bytes::Bytes;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{Key, OperationId, StoreIdentity, Value};

use super::{
    AttemptId, ControlPlaneIncarnation, ControlPlaneMode, CoordinationError, FencingToken,
    HolderId, ResourceEpoch, ResourceKey,
};

const V1: u8 = 1;
const CONTROL_KEY_PREFIX: &[u8] = b"\0novarocks/cp/v1/control";
const LEASE_KEY_PREFIX: &[u8] = b"\0novarocks/cp/v1/lease/";
const HELD_STATE: u8 = 1;
const RELEASED_STATE: u8 = 2;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ControlRecord {
    pub(crate) store_id: Uuid,
    pub(crate) cluster_id: String,
    pub(crate) incarnation: ControlPlaneIncarnation,
    pub(crate) mode: ControlPlaneMode,
    pub(crate) last_operation_id: OperationId,
}

impl ControlRecord {
    pub(crate) fn from_identity(
        identity: &StoreIdentity,
        incarnation: ControlPlaneIncarnation,
        mode: ControlPlaneMode,
        last_operation_id: OperationId,
    ) -> Self {
        Self {
            store_id: identity.store_id,
            cluster_id: identity.cluster_id.clone(),
            incarnation,
            mode,
            last_operation_id,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LeaseState {
    Held,
    Released,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LeaseRecord {
    pub(crate) resource: ResourceKey,
    pub(crate) state: LeaseState,
    pub(crate) holder: HolderId,
    pub(crate) attempt: AttemptId,
    pub(crate) incarnation: ControlPlaneIncarnation,
    pub(crate) epoch: ResourceEpoch,
    pub(crate) deadline_ms: u64,
    pub(crate) renewed_ms: u64,
    pub(crate) last_operation_id: OperationId,
}

pub(crate) fn control_storage_key() -> Result<Key, CoordinationError> {
    Key::try_from(Bytes::from_static(CONTROL_KEY_PREFIX))
        .map_err(CoordinationError::from_state_store)
}

pub(crate) fn lease_storage_key(resource: &ResourceKey) -> Result<Key, CoordinationError> {
    let digest = Sha256::digest(resource.as_bytes());
    let mut key = Vec::with_capacity(LEASE_KEY_PREFIX.len() + digest.len());
    key.extend_from_slice(LEASE_KEY_PREFIX);
    key.extend_from_slice(&digest);
    Key::try_from(Bytes::from(key)).map_err(CoordinationError::from_state_store)
}

pub(crate) fn encode_control(record: &ControlRecord) -> Result<Value, CoordinationError> {
    let mut value = Vec::with_capacity(1 + 16 + 4 + record.cluster_id.len() + 8 + 1 + 16);
    value.push(V1);
    value.extend_from_slice(record.store_id.as_bytes());
    write_bytes(&mut value, record.cluster_id.as_bytes())?;
    write_u64(&mut value, record.incarnation.get());
    value.push(encode_mode(record.mode));
    value.extend_from_slice(record.last_operation_id.as_uuid().as_bytes());
    Value::try_from(Bytes::from(value)).map_err(CoordinationError::from_state_store)
}

pub(crate) fn decode_control(value: &Value) -> Result<ControlRecord, CoordinationError> {
    let mut reader = Reader::new(value.as_bytes());
    require_version(&mut reader)?;
    let store_id = read_uuid(&mut reader)?;
    let cluster_id = String::from_utf8(reader.read_bytes()?.to_vec()).map_err(|_| corruption())?;
    if cluster_id.is_empty() {
        return Err(corruption());
    }
    let incarnation = ControlPlaneIncarnation::new(reader.read_u64()?).map_err(|_| corruption())?;
    let mode = decode_mode(reader.read_u8()?)?;
    let last_operation_id = OperationId::from(read_uuid(&mut reader)?);
    reader.finish()?;
    Ok(ControlRecord {
        store_id,
        cluster_id,
        incarnation,
        mode,
        last_operation_id,
    })
}

pub(crate) fn encode_lease(record: &LeaseRecord) -> Result<Value, CoordinationError> {
    let mut value = Vec::with_capacity(
        1 + 4
            + record.resource.as_bytes().len()
            + 1
            + 4
            + record.holder.as_bytes().len()
            + 16
            + 8
            + 8
            + 8
            + 8
            + 16,
    );
    value.push(V1);
    write_bytes(&mut value, record.resource.as_bytes())?;
    value.push(encode_lease_state(record.state));
    write_bytes(&mut value, record.holder.as_bytes())?;
    value.extend_from_slice(record.attempt.as_uuid().as_bytes());
    write_u64(&mut value, record.incarnation.get());
    write_u64(&mut value, record.epoch.get());
    write_u64(&mut value, record.deadline_ms);
    write_u64(&mut value, record.renewed_ms);
    value.extend_from_slice(record.last_operation_id.as_uuid().as_bytes());
    Value::try_from(Bytes::from(value)).map_err(CoordinationError::from_state_store)
}

pub(crate) fn decode_lease(
    storage_key: &Key,
    value: &Value,
) -> Result<LeaseRecord, CoordinationError> {
    let mut reader = Reader::new(value.as_bytes());
    require_version(&mut reader)?;
    let resource = ResourceKey::try_from(Bytes::copy_from_slice(reader.read_bytes()?))
        .map_err(|_| corruption())?;
    let state = decode_lease_state(reader.read_u8()?)?;
    let holder = HolderId::try_from(Bytes::copy_from_slice(reader.read_bytes()?))
        .map_err(|_| corruption())?;
    let attempt = AttemptId::try_from(read_uuid(&mut reader)?).map_err(|_| corruption())?;
    let incarnation = ControlPlaneIncarnation::new(reader.read_u64()?).map_err(|_| corruption())?;
    let epoch = ResourceEpoch::new(reader.read_u64()?).map_err(|_| corruption())?;
    let deadline_ms = reader.read_u64()?;
    let renewed_ms = reader.read_u64()?;
    let last_operation_id = OperationId::from(read_uuid(&mut reader)?);
    reader.finish()?;

    let expected_key = lease_storage_key(&resource)?;
    if storage_key.as_bytes() != expected_key.as_bytes() {
        return Err(corruption());
    }

    Ok(LeaseRecord {
        resource,
        state,
        holder,
        attempt,
        incarnation,
        epoch,
        deadline_ms,
        renewed_ms,
        last_operation_id,
    })
}

impl FencingToken {
    pub fn encode_v1(&self) -> Result<Bytes, CoordinationError> {
        let mut value = Vec::with_capacity(1 + 4 + self.cluster_id().len() + 8 + 8);
        value.push(V1);
        write_bytes(&mut value, self.cluster_id().as_bytes())?;
        write_u64(&mut value, self.control_plane_incarnation().get());
        write_u64(&mut value, self.resource_epoch().get());
        Ok(Bytes::from(value))
    }

    pub fn decode_v1(value: Bytes) -> Result<Self, CoordinationError> {
        let mut reader = Reader::new(value.as_ref());
        require_version(&mut reader)?;
        let cluster_id =
            String::from_utf8(reader.read_bytes()?.to_vec()).map_err(|_| corruption())?;
        let incarnation =
            ControlPlaneIncarnation::new(reader.read_u64()?).map_err(|_| corruption())?;
        let epoch = ResourceEpoch::new(reader.read_u64()?).map_err(|_| corruption())?;
        reader.finish()?;
        FencingToken::new(cluster_id, incarnation, epoch).map_err(|_| corruption())
    }
}

fn write_bytes(output: &mut Vec<u8>, bytes: &[u8]) -> Result<(), CoordinationError> {
    let length = u32::try_from(bytes.len()).map_err(|_| {
        CoordinationError::limit_exceeded("coordination encoded field exceeds the v1 byte limit")
    })?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

fn write_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn encode_mode(mode: ControlPlaneMode) -> u8 {
    match mode {
        ControlPlaneMode::Reconciling => 1,
        ControlPlaneMode::WriteOpen => 2,
    }
}

fn decode_mode(value: u8) -> Result<ControlPlaneMode, CoordinationError> {
    match value {
        1 => Ok(ControlPlaneMode::Reconciling),
        2 => Ok(ControlPlaneMode::WriteOpen),
        _ => Err(corruption()),
    }
}

fn encode_lease_state(state: LeaseState) -> u8 {
    match state {
        LeaseState::Held => HELD_STATE,
        LeaseState::Released => RELEASED_STATE,
    }
}

fn decode_lease_state(value: u8) -> Result<LeaseState, CoordinationError> {
    match value {
        HELD_STATE => Ok(LeaseState::Held),
        RELEASED_STATE => Ok(LeaseState::Released),
        _ => Err(corruption()),
    }
}

fn require_version(reader: &mut Reader<'_>) -> Result<(), CoordinationError> {
    if reader.read_u8()? != V1 {
        return Err(corruption());
    }
    Ok(())
}

fn read_uuid(reader: &mut Reader<'_>) -> Result<Uuid, CoordinationError> {
    let bytes = reader.read_exact(16)?;
    Uuid::from_slice(bytes).map_err(|_| corruption())
}

fn corruption() -> CoordinationError {
    CoordinationError::corruption()
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, CoordinationError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, CoordinationError> {
        let bytes: [u8; 4] = self.read_exact(4)?.try_into().map_err(|_| corruption())?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, CoordinationError> {
        let bytes: [u8; 8] = self.read_exact(8)?.try_into().map_err(|_| corruption())?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn read_bytes(&mut self) -> Result<&'a [u8], CoordinationError> {
        let length = usize::try_from(self.read_u32()?).map_err(|_| corruption())?;
        self.read_exact(length)
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], CoordinationError> {
        let end = self.position.checked_add(length).ok_or_else(corruption)?;
        let bytes = self.bytes.get(self.position..end).ok_or_else(corruption)?;
        self.position = end;
        Ok(bytes)
    }

    fn finish(self) -> Result<(), CoordinationError> {
        if self.position != self.bytes.len() {
            return Err(corruption());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use uuid::Uuid;

    use super::{LeaseRecord, LeaseState, decode_lease, encode_lease, lease_storage_key};
    use crate::coordination::{
        AttemptId, ControlPlaneIncarnation, CoordinationErrorKind, HolderId, ResourceEpoch,
        ResourceKey,
    };
    use crate::{OperationId, Value};

    fn resource(bytes: &'static [u8]) -> ResourceKey {
        ResourceKey::try_from(Bytes::from_static(bytes)).expect("valid resource")
    }

    fn record() -> LeaseRecord {
        LeaseRecord {
            resource: resource(&[0, 0xff]),
            state: LeaseState::Held,
            holder: HolderId::try_from(Bytes::from_static(b"holder-a")).expect("valid holder"),
            attempt: AttemptId::try_from(Uuid::now_v7()).expect("UUIDv7 attempt"),
            incarnation: ControlPlaneIncarnation::new(3).expect("nonzero incarnation"),
            epoch: ResourceEpoch::new(7).expect("nonzero epoch"),
            deadline_ms: 100,
            renewed_ms: 90,
            last_operation_id: OperationId::new_v7(),
        }
    }

    #[test]
    fn v1_lease_round_trips_binary_resource_and_uses_stable_digest_key() {
        let record = record();
        let key = lease_storage_key(&record.resource).expect("storage key");
        assert_eq!(
            key.as_bytes(),
            &[
                b'\0', b'n', b'o', b'v', b'a', b'r', b'o', b'c', b'k', b's', b'/', b'c', b'p',
                b'/', b'v', b'1', b'/', b'l', b'e', b'a', b's', b'e', b'/', 0x06, 0xeb, 0x7d, 0x6a,
                0x69, 0xee, 0x19, 0xe5, 0xfb, 0xdf, 0x74, 0x90, 0x18, 0xd3, 0xd2, 0xab, 0xfa, 0x04,
                0xbc, 0xbd, 0x13, 0x65, 0xdb, 0x31, 0x2e, 0xb8, 0x6d, 0xc7, 0x16, 0x93, 0x89, 0xb8,
            ]
        );

        let value = encode_lease(&record).expect("encode lease");
        assert_eq!(decode_lease(&key, &value).expect("decode lease"), record);
    }

    #[test]
    fn v1_lease_decoder_fails_closed_for_malformed_or_mismatched_records() {
        let record = record();
        let key = lease_storage_key(&record.resource).expect("storage key");
        let value = encode_lease(&record).expect("encode lease");

        let malformed = [
            Bytes::from_static(&[2]),
            Bytes::copy_from_slice(&value.as_bytes()[..value.as_bytes().len() - 1]),
            {
                let mut bytes = value.as_bytes().to_vec();
                bytes.push(0);
                Bytes::from(bytes)
            },
            with_u64(
                &value,
                1 + 4
                    + record.resource.as_bytes().len()
                    + 1
                    + 4
                    + record.holder.as_bytes().len()
                    + 16,
                0,
            ),
            with_u64(
                &value,
                1 + 4
                    + record.resource.as_bytes().len()
                    + 1
                    + 4
                    + record.holder.as_bytes().len()
                    + 16
                    + 8,
                0,
            ),
            with_uuid(
                &value,
                1 + 4 + record.resource.as_bytes().len() + 1 + 4 + record.holder.as_bytes().len(),
                Uuid::new_v4(),
            ),
        ];
        for malformed_value in malformed {
            let error = decode_lease(
                &key,
                &Value::try_from(malformed_value).expect("nonempty value"),
            )
            .expect_err("malformed lease must fail closed");
            assert_eq!(error.kind(), CoordinationErrorKind::Corruption);
        }

        let other = resource(b"other-resource");
        let other_key = lease_storage_key(&other).expect("other storage key");
        let error =
            decode_lease(&other_key, &value).expect_err("digest/value mismatch must fail closed");
        assert_eq!(error.kind(), CoordinationErrorKind::Corruption);
    }

    fn with_u64(value: &Value, offset: usize, replacement: u64) -> Bytes {
        let mut bytes = value.as_bytes().to_vec();
        bytes[offset..offset + 8].copy_from_slice(&replacement.to_be_bytes());
        Bytes::from(bytes)
    }

    fn with_uuid(value: &Value, offset: usize, replacement: Uuid) -> Bytes {
        let mut bytes = value.as_bytes().to_vec();
        bytes[offset..offset + 16].copy_from_slice(replacement.as_bytes());
        Bytes::from(bytes)
    }
}
