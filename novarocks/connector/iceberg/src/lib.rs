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

//! Iceberg provider dependency boundary.
//!
//! Provider-owned Iceberg facts and table-format value conversions.
//!
//! The external Iceberg, catalog, object-store, and provider-specific value
//! modules live here. Consumer crates depend on these typed facts at their
//! boundaries; the provider crate does not depend on aggregate Core, SQL, or
//! execution modules.

/// Stable provider identity used by server composition and SPI declarations.
pub const PROVIDER_ID: &str = "iceberg";

pub mod commit;
pub mod default_value;
pub mod delete_file;
pub mod delta;
pub mod file_reader;
pub mod ref_snapshot;
pub mod scan_model;
pub mod theta_sketch;

pub mod iceberg {
    pub use ::iceberg::*;
}

pub mod iceberg_catalog_rest {
    pub use ::iceberg_catalog_rest::*;
}

pub mod iceberg_catalog_hms {
    pub use ::iceberg_catalog_hms::*;
}

pub mod opendal {
    pub use ::opendal::*;
}

pub use novarocks_fs;
pub use novarocks_spi;
