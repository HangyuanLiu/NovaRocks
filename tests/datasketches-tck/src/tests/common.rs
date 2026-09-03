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

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

pub const CUSTOM_SEED: u64 = 123_456_789;

#[derive(Debug)]
pub struct Case<'a> {
    pub path: &'a str,
    pub producer: &'a str,
    pub producer_version: &'a str,
    pub producer_commit: &'a str,
    pub profile: &'a str,
    pub seed: &'a str,
    pub lg_k: u8,
    pub input: &'a str,
    pub serialization: &'a str,
    pub bytes_sha256: &'a str,
    pub facts: BTreeMap<&'a str, &'a str>,
}

pub fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures")
}

pub fn fixture(path: &str) -> Vec<u8> {
    fs::read(fixture_root().join(path)).unwrap_or_else(|error| panic!("read {path}: {error}"))
}

pub fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn cases() -> Vec<Case<'static>> {
    include_str!("../../fixtures/manifest.tsv")
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let columns: Vec<_> = line.split('\t').collect();
            assert_eq!(columns.len(), 11, "malformed manifest row: {line}");
            let facts = columns[10]
                .split(';')
                .map(|fact| fact.split_once('=').expect("key=value fact"))
                .collect();
            Case {
                path: columns[0],
                producer: columns[1],
                producer_version: columns[2],
                producer_commit: columns[3],
                profile: columns[4],
                seed: columns[5],
                lg_k: columns[6].parse().expect("lg_k"),
                input: columns[7],
                serialization: columns[8],
                bytes_sha256: columns[9],
                facts,
            }
        })
        .collect()
}

pub fn expected_cardinality(input: &str) -> Option<usize> {
    if input == "empty" {
        return Some(0);
    }
    if input == "range_i32_0_1000_repeated_5" {
        return Some(1000);
    }
    let (_, end) = input.rsplit_once('_')?;
    let end: usize = end.parse().ok()?;
    let start = input
        .strip_prefix("range_i32_")?
        .split('_')
        .next()?
        .parse::<usize>()
        .ok()?;
    Some(end - start)
}

pub fn fact<'a>(case: &'a Case<'_>, key: &str) -> &'a str {
    case.facts
        .get(key)
        .copied()
        .unwrap_or_else(|| panic!("{} missing {key}", case.path))
}
