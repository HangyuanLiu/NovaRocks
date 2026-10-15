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

//! Native fragment ingress error adaptation.

use std::fmt;

/// Why a fragment could not be recovered from its wire form.
#[derive(Debug)]
pub struct NativeFragmentIngressError {
    message: String,
    construction: Option<crate::fragment_error::NativeTypeConstructionError>,
    specialization: Option<novarocks_functions::FunctionSpecializationFailure>,
}

impl NativeFragmentIngressError {
    pub(crate) fn from_decode(error: crate::fragment_error::NativeFragmentDecodeError) -> Self {
        let message = error.to_string();
        let (construction, specialization) = match error {
            crate::fragment_error::NativeFragmentDecodeError::Construction(cause) => {
                (Some(cause), None)
            }
            crate::fragment_error::NativeFragmentDecodeError::Specialization(cause) => {
                (None, Some(cause))
            }
            _ => (None, None),
        };
        Self {
            message,
            construction,
            specialization,
        }
    }

    pub fn specialization_cause(
        &self,
    ) -> Option<&novarocks_functions::FunctionSpecializationFailure> {
        self.specialization.as_ref()
    }

    pub fn construction_cause(
        &self,
    ) -> Option<&crate::fragment_error::NativeTypeConstructionError> {
        self.construction.as_ref()
    }

    pub fn new(error: impl fmt::Display) -> Self {
        Self {
            message: error.to_string(),
            construction: None,
            specialization: None,
        }
    }
}

impl fmt::Display for NativeFragmentIngressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for NativeFragmentIngressError {}
