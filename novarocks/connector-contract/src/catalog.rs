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

use std::fmt::Write;

use crate::ConnectorInstanceId;

pub const CATALOG_VERSION_BYTES: usize = 32;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CatalogVersion([u8; CATALOG_VERSION_BYTES]);

impl CatalogVersion {
    pub const fn from_bytes(bytes: [u8; CATALOG_VERSION_BYTES]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; CATALOG_VERSION_BYTES] {
        &self.0
    }

    /// A bounded rendering suitable for diagnostics, never a metric label.
    pub fn short_hex(self) -> String {
        let mut result = String::with_capacity(16);
        for byte in &self.0[..8] {
            write!(&mut result, "{byte:02x}").expect("writing to String cannot fail");
        }
        result
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CatalogHandle {
    catalog_name: ConnectorInstanceId,
    version: CatalogVersion,
}

impl CatalogHandle {
    pub const fn new(catalog_name: ConnectorInstanceId, version: CatalogVersion) -> Self {
        Self {
            catalog_name,
            version,
        }
    }

    pub const fn catalog_name(&self) -> &ConnectorInstanceId {
        &self.catalog_name
    }

    pub const fn version(&self) -> CatalogVersion {
        self.version
    }
}
