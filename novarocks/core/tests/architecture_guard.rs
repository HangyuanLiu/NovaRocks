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

use std::fs;
use std::path::{Path, PathBuf};

fn rust_sources(root: &Path) -> Vec<PathBuf> {
    let mut sources = Vec::new();
    for entry in fs::read_dir(root).expect("model directory should be readable") {
        let path = entry
            .expect("model directory entry should be readable")
            .path();
        if path.is_dir() {
            sources.extend(rust_sources(&path));
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            sources.push(path);
        }
    }
    sources
}

#[test]
fn runtime_filter_model_does_not_depend_on_sql_planner() {
    let model_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/runtime_filter/model");
    for source_path in rust_sources(&model_root) {
        let source = fs::read_to_string(&source_path).expect("model source should be readable");
        assert!(
            !source.contains("crate::sql::planner"),
            "{} must not depend on the SQL planner",
            source_path.display()
        );
    }
}
