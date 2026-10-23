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

//! Design: ADR-0196 (docs/adr/ADR-0196-hadoop-test-catalog-complete-version-installation.md)
//! A conservative, catalog-neutral deletion guard. Metadata version slots
//! and their hint remain discovery roots even when absent from metadata logs.

pub(crate) fn is_catalog_metadata_root(path: &str) -> bool {
    let Ok(location) = novarocks_fs::FsLocation::parse(path) else {
        return false;
    };
    let mut components = std::path::Path::new(location.path()).components().rev();
    let mut parents = 0usize;
    let mut next_normal = || loop {
        match components.next()? {
            std::path::Component::ParentDir => parents += 1,
            std::path::Component::Normal(_) if parents > 0 => parents -= 1,
            std::path::Component::Normal(value) => break value.to_str(),
            _ => (),
        }
    };
    let Some(name) = next_normal() else {
        return false;
    };
    next_normal() == Some("metadata")
        && (name == "version-hint.text"
            || crate::hadoop_catalog::versions::version_number(name).is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    // Independent path examples protect discovery roots without asserting
    // catalog kind, metadata-log membership, or a storage listing.
    #[test]
    fn protects_only_direct_metadata_versions_and_hint_conservatively() {
        for path in [
            "/warehouse/t/metadata/v1.metadata.json",
            "s3://bucket/other/metadata/v27.gz.metadata.json",
            "s3a://bucket/t/metadata/v2147483647.metadata.json.gz",
            "file:///warehouse/t/metadata/version-hint.text",
            "metadata/./v1.metadata.json",
            "other/metadata/sub/../v1.metadata.json",
        ] {
            assert!(is_catalog_metadata_root(path), "{path}");
        }
        for path in [
            "data/v1.metadata.json",
            "t/metadata/sub/v1.metadata.json",
            "t/metadata/v0.metadata.json",
            "t/metadata/v01.metadata.json",
            "t/metadata/v2147483648.metadata.json",
            "t/metadata/install-123.tmp",
            "t/metadata/123.metadata.json",
            "t/data/version-hint.text",
        ] {
            assert!(!is_catalog_metadata_root(path), "{path}");
        }
    }
}
