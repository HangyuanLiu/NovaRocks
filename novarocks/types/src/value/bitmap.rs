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

pub use novarocks_functions::bitmap_value::*;

#[cfg(test)]
use std::collections::BTreeSet;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlx2_value_codec_bitmap_empty_single_and_set_are_stable() {
        assert_eq!(
            encode_internal_bitmap(&BTreeSet::new()).unwrap(),
            vec![BITMAP_TYPE_EMPTY]
        );
        assert_eq!(
            encode_bitmap_single(7),
            vec![BITMAP_TYPE_SINGLE32, 7, 0, 0, 0]
        );
        assert_eq!(
            decode_bitmap(&[BITMAP_TYPE_SET, 2, 0, 0, 0, 1, 172, 2]).unwrap(),
            BTreeSet::from([1, 300])
        );
    }

    #[test]
    fn sqlx2_value_codec_bitmap_string_and_malformed_payloads_are_checked() {
        assert_eq!(
            parse_bitmap_string(" 1, 2, 1 ").unwrap(),
            BTreeSet::from([1, 2])
        );
        assert!(parse_bitmap_string("one").is_err());
        assert!(decode_internal_bitmap(&[BITMAP_TYPE_SINGLE32]).is_err());
    }
}
