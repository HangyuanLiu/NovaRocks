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

//! Move-only checked COW begin custody. Ordinary writes keep their entrypoint.

use super::write_stack::session::ConnectorWriteSessionPlan;
use super::{ConnectorError, ConnectorOriginalResultScope, OriginalResultCheckError};
use std::{error::Error, fmt};

pub enum ConnectorCowBeginCause {
    Provider(ConnectorError),
    OriginalResult(OriginalResultCheckError),
}
impl From<ConnectorError> for ConnectorCowBeginCause {
    fn from(e: ConnectorError) -> Self {
        Self::Provider(e)
    }
}
impl From<OriginalResultCheckError> for ConnectorCowBeginCause {
    fn from(e: OriginalResultCheckError) -> Self {
        Self::OriginalResult(e)
    }
}
impl fmt::Debug for ConnectorCowBeginCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Provider(_) => "COW provider failure",
            Self::OriginalResult(_) => "COW original result failure",
        })
    }
}
impl fmt::Display for ConnectorCowBeginCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("copy-on-write begin failed")
    }
}
impl Error for ConnectorCowBeginCause {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(match self {
            Self::Provider(e) => e,
            Self::OriginalResult(e) => e,
        })
    }
}

pub struct ConnectorCowBeginFailure {
    cause: ConnectorCowBeginCause,
    // Original cause destroys before its last neutral holder.
    original: ConnectorOriginalResultScope,
}
impl ConnectorCowBeginFailure {
    pub fn new(cause: ConnectorCowBeginCause, original: ConnectorOriginalResultScope) -> Self {
        Self { cause, original }
    }
    pub fn cause(&self) -> &ConnectorCowBeginCause {
        &self.cause
    }
    pub fn original(&self) -> &ConnectorOriginalResultScope {
        &self.original
    }
}
impl fmt::Debug for ConnectorCowBeginFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ConnectorCowBeginFailure(original cause retained)")
    }
}
impl fmt::Display for ConnectorCowBeginFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("copy-on-write begin failed")
    }
}
impl Error for ConnectorCowBeginFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.cause)
    }
}

pub struct ConnectorCowBeginPlan {
    plan: ConnectorWriteSessionPlan,
    simultaneous_upper: u64,
    original: ConnectorOriginalResultScope,
}
impl ConnectorCowBeginPlan {
    /// First-party constructors must have completed their prospective recipe
    /// and the late-publication check before constructing this envelope.
    pub fn new(
        plan: ConnectorWriteSessionPlan,
        original: ConnectorOriginalResultScope,
        simultaneous_upper: u64,
    ) -> Self {
        Self {
            plan,
            original,
            simultaneous_upper,
        }
    }
    pub fn simultaneous_upper(&self) -> u64 {
        self.simultaneous_upper
    }
    pub fn plan(&self) -> &ConnectorWriteSessionPlan {
        &self.plan
    }
    pub fn original(&self) -> &ConnectorOriginalResultScope {
        &self.original
    }
    /// The caller must move both outputs into the same actual consumer owner.
    pub fn into_parts(self) -> (ConnectorWriteSessionPlan, ConnectorOriginalResultScope) {
        (self.plan, self.original)
    }
}
