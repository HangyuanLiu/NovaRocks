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

//! Locked library sources for allocation-request and work projections.

const fn at(source: &[u8], position: usize, needle: &[u8]) -> bool {
    if position > source.len() || needle.len() > source.len() - position {
        return false;
    }
    let mut index = 0;
    while index < needle.len() {
        if source[position + index] != needle[index] {
            return false;
        }
        index += 1;
    }
    true
}
pub(crate) const fn locked_family(source: &[u8]) -> bool {
    let names: [&[u8]; 8] = [
        b"\nname = \"arrow\"\n",
        b"\nname = \"arrow-array\"\n",
        b"\nname = \"arrow-buffer\"\n",
        b"\nname = \"arrow-data\"\n",
        b"\nname = \"arrow-ipc\"\n",
        b"\nname = \"arrow-schema\"\n",
        b"\nname = \"num-bigint\"\n",
        b"\nname = \"flatbuffers\"\n",
    ];
    let mut counts = [0usize; 8];
    let mut index = 0;
    while index < source.len() {
        if source[index] == b'\n' && at(source, index, b"\nname = ") {
            let mut member = 0;
            while member < names.len() {
                if at(source, index, names[member]) {
                    counts[member] += 1;
                    if !at(
                        source,
                        index + names[member].len(),
                        if member == 6 {
                            b"version = \"0.4.6\"\n"
                        } else if member == 7 {
                            b"version = \"25.9.23\"\n"
                        } else {
                            b"version = \"58.2.0\"\n"
                        },
                    ) {
                        return false;
                    }
                }
                member += 1;
            }
        }
        index += 1;
    }
    let mut member = 0;
    while member < counts.len() {
        if counts[member] != 1 {
            return false;
        }
        member += 1;
    }
    true
}
pub(crate) const LOCKED_FAMILY: bool = locked_family(include_bytes!("../../../Cargo.lock"));
pub(crate) use novarocks_type_contract::owned_resources::profile::LOCKED_TOOLCHAIN;

#[cfg(test)]
mod tests {
    use super::locked_family;

    #[test]
    fn lockfile_resource_sources_reject_flatbuffers_drift_and_ambiguity() {
        let source = include_str!("../../../Cargo.lock");
        assert!(locked_family(source.as_bytes()));
        let changed = source.replacen(
            "name = \"flatbuffers\"\nversion = \"25.9.23\"",
            "name = \"flatbuffers\"\nversion = \"25.12.19\"",
            1,
        );
        assert!(!locked_family(changed.as_bytes()));
        let duplicate = format!("{source}\nname = \"flatbuffers\"\nversion = \"25.9.23\"\n");
        assert!(!locked_family(duplicate.as_bytes()));
    }
}
