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

//! Opaque caller-owned lifetime retention. This module acquires no capacity
//! and grants no authority; the caller supplies its already-admitted holder.

use std::fmt;
use std::sync::Arc;

#[derive(Clone)]
pub struct ConnectorPayloadRetentionGuard {
    _holder: Arc<dyn Send + Sync>,
}

impl ConnectorPayloadRetentionGuard {
    pub fn new<T: Send + Sync + 'static>(holder: T) -> Self {
        Self {
            _holder: Arc::new(holder),
        }
    }
}

// The erased holder is never inspected or mutated by this component. Its only
// operation after an unwind is ordinary Arc destruction.
impl std::panic::RefUnwindSafe for ConnectorPayloadRetentionGuard {}

impl fmt::Debug for ConnectorPayloadRetentionGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ConnectorPayloadRetentionGuard")
    }
}
