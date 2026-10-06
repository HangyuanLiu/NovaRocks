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

//! The byte-exact manifest is frozen before any measurement.
use serde::Deserialize;
use sha2::{Digest, Sha256};

pub const MANIFEST_JSON: &str = include_str!("../manifest.json");
pub const MANIFEST_SHA256: &str =
    "63d72bbbca8934689f5618d8801738a7496a04adfa6849d47b1224cb27280fc1";
#[derive(Clone, Copy, Debug, Deserialize)]
pub struct Parameters {
    pub warmup_iterations: usize,
    pub throughput_iterations_per_worker: usize,
    pub latency_iterations_per_worker: usize,
    pub live_hold_iterations: usize,
}
#[derive(Debug, Deserialize)]
pub struct Manifest {
    pub schema_version: u32,
    pub attribution_threshold_bytes: usize,
    pub token_bytes: usize,
    pub modes: Modes,
    pub alignment_bytes: Vec<usize>,
    pub workloads: Workloads,
}
#[derive(Debug, Deserialize)]
pub struct Modes {
    pub smoke: Parameters,
    pub formal: Parameters,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub struct Workloads {
    pub w1: SizeSweep,
    pub w2: ChunkShape,
    pub w3: Mixed,
    pub w4: Resize,
    pub w5: Small,
}
#[derive(Debug, Deserialize)]
pub struct SizeSweep {
    pub sizes_bytes: Vec<usize>,
}
#[derive(Debug, Deserialize)]
pub struct ChunkShape {
    pub rows: usize,
    pub buffer_sizes_bytes: Vec<usize>,
    pub small_objects: usize,
    pub small_sizes_bytes: Vec<usize>,
}
#[derive(Debug, Deserialize)]
pub struct Mixed {
    pub worker_counts: Vec<usize>,
    pub release_threads: usize,
    pub remote_release_every: usize,
    pub sizes_bytes: Vec<usize>,
}
#[derive(Debug, Deserialize)]
pub struct Resize {
    pub vec_initial_capacity: usize,
    pub vec_grow_len: usize,
    pub vec_shrink_len: usize,
    pub r1_resize_sizes_bytes: Vec<usize>,
}
#[derive(Debug, Deserialize)]
pub struct Small {
    pub worker_counts: Vec<usize>,
    pub sizes_bytes: Vec<usize>,
}
pub fn load() -> Result<Manifest, String> {
    let actual = format!("{:x}", Sha256::digest(MANIFEST_JSON.as_bytes()));
    if actual != MANIFEST_SHA256 {
        return Err("frozen manifest SHA mismatch".into());
    }
    let manifest: Manifest = serde_json::from_str(MANIFEST_JSON).map_err(|e| e.to_string())?;
    if manifest.schema_version != 1
        || manifest.attribution_threshold_bytes
            != novarocks_memory::attribution::ATTRIBUTION_THRESHOLD_BYTES
        || manifest.token_bytes != novarocks_memory::attribution::ATTRIBUTION_TOKEN_BYTES
    {
        return Err("frozen manifest disagrees with allocator representation".into());
    }
    Ok(manifest)
}
#[cfg(test)]
mod tests {
    #[test]
    fn manifest_bytes_and_representation_are_frozen() {
        super::load().unwrap();
    }
}
