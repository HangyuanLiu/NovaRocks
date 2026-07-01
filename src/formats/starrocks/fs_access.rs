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

use opendal::Operator;

use crate::connector::starrocks::ObjectStoreProfile;
use crate::connector::starrocks::fs_access::{
    StarRocksFsAccess, resolve_runtime_path, resolve_with_profile,
};
use crate::fs::access::FsScheme;

#[derive(Clone, Debug)]
pub(crate) struct StarRocksFormatPathAccess {
    access: StarRocksFsAccess,
}

impl StarRocksFormatPathAccess {
    pub(crate) fn scheme(&self) -> FsScheme {
        self.access.scheme()
    }

    pub(crate) fn operator(&self) -> Operator {
        self.access.operator()
    }

    pub(crate) fn single_relative_path(&self) -> Result<&str, String> {
        self.access.single_relative_path()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StarRocksFormatTabletAccess {
    root_location: String,
    root_relative_path: String,
    access: StarRocksFsAccess,
}

impl StarRocksFormatTabletAccess {
    pub(crate) fn scheme(&self) -> FsScheme {
        self.access.scheme()
    }

    pub(crate) fn operator(&self) -> Operator {
        self.access.operator()
    }

    pub(crate) fn join_relative_path(&self, rel_path: &str) -> String {
        join_path(&self.root_location, rel_path)
    }

    pub(crate) fn operator_relative_path(&self, rel_path: &str) -> String {
        join_path(&self.root_relative_path, rel_path)
    }
}

pub(crate) fn resolve_format_path(path: &str) -> Result<StarRocksFormatPathAccess, String> {
    let access = resolve_runtime_path(path)?;
    Ok(StarRocksFormatPathAccess { access })
}

pub(crate) fn resolve_format_tablet_access(
    tablet_root_path: &str,
    object_store_profile: Option<&ObjectStoreProfile>,
) -> Result<StarRocksFormatTabletAccess, String> {
    let access = resolve_with_profile(tablet_root_path, object_store_profile)?;
    let root_relative_path = access.single_relative_path()?.to_string();
    let root_location = tablet_root_path.trim().trim_end_matches('/').to_string();
    Ok(StarRocksFormatTabletAccess {
        root_location,
        root_relative_path,
        access,
    })
}

fn join_path(base: &str, rel_path: &str) -> String {
    let base = base.trim_end_matches('/');
    let rel_path = rel_path.trim_start_matches('/');
    if rel_path.is_empty() {
        return base.to_string();
    }
    if base.is_empty() {
        return rel_path.to_string();
    }
    format!("{base}/{rel_path}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_relative_path_appends_to_object_store_root() {
        assert_eq!(
            join_path("s3://bucket/warehouse/tablet", "meta/1.meta"),
            "s3://bucket/warehouse/tablet/meta/1.meta"
        );
    }

    #[test]
    fn join_relative_path_appends_to_local_root() {
        assert_eq!(
            join_path("/tmp/warehouse/tablet/", "/meta/1.meta"),
            "/tmp/warehouse/tablet/meta/1.meta"
        );
    }
}
