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
use std::path::Path;

use datasketches::hash::value::natural_extend;
use datasketches::hll::{HllSketch, HllType};
use datasketches::theta::ThetaSketchBuilder;

fn main() {
    let output = env::args()
        .nth(1)
        .expect("usage: generate-rust-fixtures OUTPUT_DIR");
    let output = Path::new(&output);
    fs::create_dir_all(output.join("theta")).expect("create theta output");
    fs::create_dir_all(output.join("hll")).expect("create hll output");

    let mut exact = ThetaSketchBuilder::default()
        .lg_k(12)
        .build()
        .expect("theta builder");
    for value in 0_u32..1000 {
        exact.update(natural_extend::from_u32(value));
    }
    fs::write(
        output.join("theta/rust_quickselect_n1000_ordered_v3.sk"),
        exact.compact(true).serialize(),
    )
    .expect("write theta v3");

    let mut estimation = ThetaSketchBuilder::default()
        .lg_k(12)
        .build()
        .expect("theta builder");
    for value in 0_u32..100_000 {
        estimation.update(natural_extend::from_u32(value));
    }
    fs::write(
        output.join("theta/rust_quickselect_n100000_ordered_v4.sk"),
        estimation.compact(true).serialize_compressed(),
    )
    .expect("write theta v4");

    for (hll_type, name) in [
        (HllType::Hll4, "hll4"),
        (HllType::Hll6, "hll6"),
        (HllType::Hll8, "hll8"),
    ] {
        let mut sketch = HllSketch::new(12, hll_type).expect("HLL builder");
        for value in 0_u32..10_000 {
            sketch.update(natural_extend::from_u32(value));
        }
        fs::write(
            output.join(format!("hll/rust_{name}_n10000.sk")),
            sketch.serialize(),
        )
        .expect("write HLL fixture");
    }
}
