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

//! Pointer-free allocation identity stored in an unaligned eight-byte tail.

/// Identity does not confer access rights. A live owner, allocation or slot pin
/// must separately protect every access to the corresponding record.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RecordRef {
    pub index: u32,
    pub generation: u32,
}
impl RecordRef {
    pub const NONE: Self = Self {
        index: u32::MAX,
        generation: u32::MAX,
    };
    pub const fn is_none(self) -> bool {
        self.index == u32::MAX
    }
    pub const fn to_bytes(self) -> [u8; 8] {
        let i = self.index.to_ne_bytes();
        let g = self.generation.to_ne_bytes();
        [i[0], i[1], i[2], i[3], g[0], g[1], g[2], g[3]]
    }
    pub const fn from_bytes(b: [u8; 8]) -> Self {
        Self {
            index: u32::from_ne_bytes([b[0], b[1], b[2], b[3]]),
            generation: u32::from_ne_bytes([b[4], b[5], b[6], b[7]]),
        }
    }
    /// # Safety
    /// `tail` must denote eight writable bytes in the live allocation.
    pub unsafe fn write(self, tail: *mut u8) {
        // SAFETY: the caller supplies eight bytes; no alignment is required.
        unsafe { tail.cast::<Self>().write_unaligned(self) };
    }
    /// # Safety
    /// `tail` must denote eight initialized token bytes in the live allocation.
    pub unsafe fn read(tail: *const u8) -> Self {
        // SAFETY: the caller supplies initialized token bytes.
        unsafe { tail.cast::<Self>().read_unaligned() }
    }
}
const _: () = assert!(std::mem::size_of::<RecordRef>() == 8);

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    #[test]
    fn token_round_trip_and_unaligned_tail() {
        let token = RecordRef {
            index: 0x12345678,
            generation: 0xfedcba98,
        };
        assert_eq!(RecordRef::from_bytes(token.to_bytes()), token);
        let mut bytes = [0u8; 9];
        // SAFETY: the last eight bytes are writable and then initialized.
        unsafe {
            token.write(bytes.as_mut_ptr().add(1));
            assert_eq!(RecordRef::read(bytes.as_ptr().add(1)), token);
        }
        assert!(RecordRef::NONE.is_none());
    }
}
