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

//! Stable row-identity helpers used by execution expressions and MV flows.

use sha2::{Digest, Sha256};

/// Produce a deterministic identifier for a pair of stable source rows.
///
/// This deliberately has no SQL, catalog, or provider dependency. Callers
/// decide how the identity is persisted or surfaced; execution owns only the
/// canonical byte layout and digest rendering.
pub fn stable_join_row_key(
    left_object_id: &[u8],
    left_row_id: i64,
    right_object_id: &[u8],
    right_row_id: i64,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.join_row_key");
    hasher.update([2]);
    hash_length_delimited(&mut hasher, left_object_id);
    hasher.update(left_row_id.to_be_bytes());
    hash_length_delimited(&mut hasher, right_object_id);
    hasher.update(right_row_id.to_be_bytes());
    let digest = hasher.finalize();
    let mut output = String::with_capacity("v2:".len() + 32);
    output.push_str("v2:");
    for byte in &digest[..16] {
        use std::fmt::Write;
        write!(&mut output, "{byte:02x}").expect("write to String cannot fail");
    }
    output
}

fn hash_length_delimited(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use super::stable_join_row_key;

    #[test]
    fn stable_join_row_key_is_deterministic_and_versioned() {
        let first = stable_join_row_key(b"left-object", 11, b"right-object", 22);
        assert_eq!(
            first,
            stable_join_row_key(b"left-object", 11, b"right-object", 22)
        );
        assert!(first.starts_with("v2:"));
    }

    #[test]
    fn stable_join_row_key_distinguishes_row_identity() {
        let base = stable_join_row_key(b"left-object", 11, b"right-object", 22);
        assert_ne!(
            base,
            stable_join_row_key(b"other-left", 11, b"right-object", 22)
        );
        assert_ne!(
            base,
            stable_join_row_key(b"left-object", 12, b"right-object", 22)
        );
        assert_ne!(
            base,
            stable_join_row_key(b"left-object", 11, b"other-right", 22)
        );
        assert_ne!(
            base,
            stable_join_row_key(b"left-object", 11, b"right-object", 23)
        );
    }

    #[test]
    fn stable_join_row_key_accepts_non_utf8_object_ids_without_ambiguous_framing() {
        let non_utf8 = [0xff, 0x00, 0x80];
        let different_bytes = [0xff, 0x00, 0x81];
        let key = stable_join_row_key(&non_utf8, 11, b"right\0object", 22);

        assert!(key.starts_with("v2:"));
        assert_ne!(
            key,
            stable_join_row_key(&different_bytes, 11, b"right\0object", 22)
        );
        assert_ne!(
            key,
            stable_join_row_key(b"\xff", 11, b"\0\x80right\0object", 22),
            "length-delimited object identifiers must not depend on NUL separators"
        );
    }
}
