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

pub use novarocks_functions::hll::{
    HLL_DATA_EMPTY, HLL_DATA_EXPLICIT, MURMUR_SEED, encode_hll_empty, encode_hll_single,
    murmur_hash64a,
};

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
