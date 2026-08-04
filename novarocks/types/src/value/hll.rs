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

//! Canonical StarRocks-compatible HLL scalar bytes and hash primitive.

pub const HLL_DATA_EMPTY: u8 = 0;
pub const HLL_DATA_EXPLICIT: u8 = 1;
pub const MURMUR_SEED: u32 = 0xadc8_3b19;

const MURMUR_PRIME: u64 = 0xc6a4_a793_5bd1_e995;

pub fn encode_hll_empty() -> Vec<u8> {
    vec![HLL_DATA_EMPTY]
}

pub fn encode_hll_single(hash: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + std::mem::size_of::<u64>());
    out.push(HLL_DATA_EXPLICIT);
    out.push(1);
    out.extend_from_slice(&hash.to_le_bytes());
    out
}

pub fn murmur_hash64a(data: &[u8], seed: u32) -> u64 {
    let r: u32 = 47;
    let mut h = (seed as u64) ^ (data.len() as u64).wrapping_mul(MURMUR_PRIME);

    let mut offset = 0usize;
    while offset + 8 <= data.len() {
        let mut block = [0u8; 8];
        block.copy_from_slice(&data[offset..offset + 8]);
        let mut k = u64::from_le_bytes(block);
        k = k.wrapping_mul(MURMUR_PRIME);
        k ^= k >> r;
        k = k.wrapping_mul(MURMUR_PRIME);
        h ^= k;
        h = h.wrapping_mul(MURMUR_PRIME);
        offset += 8;
    }

    for (idx, byte) in data[offset..].iter().enumerate() {
        h ^= (*byte as u64) << (idx * 8);
    }
    if offset < data.len() {
        h = h.wrapping_mul(MURMUR_PRIME);
    }

    h ^= h >> r;
    h = h.wrapping_mul(MURMUR_PRIME);
    h ^= h >> r;
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlx2_value_codec_hll_empty_and_single_are_stable() {
        assert_eq!(encode_hll_empty(), vec![0]);
        assert_eq!(
            encode_hll_single(0x0102_0304_0506_0708),
            vec![1, 1, 8, 7, 6, 5, 4, 3, 2, 1]
        );
    }

    #[test]
    fn sqlx2_value_codec_hll_murmur_is_stable() {
        assert_eq!(
            murmur_hash64a(b"novarocks", MURMUR_SEED),
            7_139_930_336_803_328_733
        );
    }
}
