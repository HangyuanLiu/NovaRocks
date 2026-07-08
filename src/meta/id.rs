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

use crate::meta::{MetaError, MetaErrorKind};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct IdScope(String);

impl IdScope {
    pub fn new(value: impl Into<String>) -> Result<Self, MetaError> {
        let value = value.into();
        if value.is_empty() {
            return Err(MetaError::new(
                MetaErrorKind::InvalidRequest,
                "metadata id scope must not be empty",
            ));
        }
        if value.chars().any(|ch| ch.is_control()) {
            return Err(MetaError::new(
                MetaErrorKind::InvalidRequest,
                format!("metadata id scope `{value}` contains a control character"),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}
