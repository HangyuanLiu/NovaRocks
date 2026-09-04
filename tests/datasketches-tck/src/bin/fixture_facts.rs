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

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use datasketches::hll::HllSketch;
use datasketches::theta::CompactThetaSketch;
use sha2::{Digest, Sha256};

const CUSTOM_SEED: u64 = 123_456_789;

fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn files_under(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for family in ["theta", "hll"] {
        let directory = root.join(family);
        if !directory.exists() {
            continue;
        }
        for entry in fs::read_dir(directory).expect("read fixture directory") {
            let path = entry.expect("read fixture entry").path();
            if path.extension().is_some_and(|extension| extension == "sk") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

fn main() {
    let root = env::args()
        .nth(1)
        .expect("usage: fixture-facts FIXTURE_DIR");
    let root = Path::new(&root);
    println!("path\tbytes_sha256\tdecoded_facts");
    for path in files_under(root) {
        let bytes = fs::read(&path).expect("read fixture");
        let relative = path
            .strip_prefix(root)
            .expect("fixture beneath root")
            .display();
        if path
            .parent()
            .and_then(Path::file_name)
            .is_some_and(|name| name == "theta")
        {
            let sketch = CompactThetaSketch::deserialize(&bytes)
                .or_else(|_| CompactThetaSketch::deserialize_with_seed(&bytes, CUSTOM_SEED))
                .expect("deserialize theta fixture");
            let mut hashes: Vec<_> = sketch.iter().map(|entry| entry.hash()).collect();
            hashes.sort_unstable();
            let hash_bytes: Vec<_> = hashes.iter().flat_map(|hash| hash.to_le_bytes()).collect();
            println!(
                "{relative}\t{}\tempty={};estimation={};ordered={};seed_hash={};retained={};theta64={};estimate={:.17};hashes_sha256={}",
                digest(&bytes),
                sketch.is_empty(),
                sketch.is_estimation_mode(),
                sketch.is_ordered(),
                sketch.seed_hash(),
                sketch.num_retained(),
                sketch.theta64(),
                sketch.estimate(),
                digest(&hash_bytes),
            );
        } else {
            let sketch = HllSketch::deserialize(&bytes).expect("deserialize HLL fixture");
            println!(
                "{relative}\t{}\ttype={:?};lg_k={};estimate={:.17};mode={}",
                digest(&bytes),
                sketch.target_type(),
                sketch.lg_config_k(),
                sketch.estimate(),
                match bytes.get(7).map(|byte| byte & 3) {
                    Some(0) => "list",
                    Some(1) => "set",
                    Some(2) => "hll",
                    _ => "invalid",
                },
            );
        }
    }
}
