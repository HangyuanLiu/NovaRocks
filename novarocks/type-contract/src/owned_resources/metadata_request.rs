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

//! Closed numerical request facts. These grant no memory or source validity.

use super::metadata_materialization::MetadataMaterializationError;
use crate::{CompileControlError, ValueTypeError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompleteMetadataRequestFacts {
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataRequestError {
    Control(CompileControlError),
    Arithmetic,
    SourceModel(&'static str),
    Metadata(MetadataMaterializationError),
    ValueType(ValueTypeError),
}
impl std::fmt::Display for MetadataRequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::Arithmetic => f.write_str("metadata request arithmetic exceeds its width"),
            Self::SourceModel(detail) => f.write_str(detail),
            Self::Metadata(error) => write!(f, "metadata request source: {error:?}"),
            Self::ValueType(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for MetadataRequestError {}
