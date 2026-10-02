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

//! Explicit external upper bounds share one domain's backing, and conversion
//! subtracts O before publishing the matching L under the same local gate.
use crate::{domain::FundingDomain, error::CapacityError, lane::FactToken};
#[derive(Debug)]
pub struct ExternalBound {
    domain: FundingDomain,
    remaining: u64,
}
impl FundingDomain {
    pub fn external_bound(&self, bytes: u64) -> Result<ExternalBound, CapacityError> {
        let mut s = self.0.state.lock().unwrap();
        if s.sealed || s.residual {
            return Err(CapacityError::Closed {
                account: s.account.id(),
            });
        }
        if s.active {
            return Err(CapacityError::Invalid {
                detail: "external authorization requires an inactive lane",
            });
        }
        if bytes
            > s.authorized
                .saturating_sub(self.0.lane.live_bytes().saturating_add(s.external))
        {
            return Err(CapacityError::Invalid {
                detail: "external bound exceeds local backing",
            });
        }
        s.external += bytes;
        // Even a zero-byte bound can publish an allocation origin later.
        s.external_handles += 1;
        self.0
            .lane
            .record()
            .sequence
            .fetch_add(1, crate::sync::Ordering::Release);
        Ok(ExternalBound {
            domain: self.clone(),
            remaining: bytes,
        })
    }
}
impl ExternalBound {
    pub fn remaining_bytes(&self) -> u64 {
        self.remaining
    }
    /// Establishes proven successful allocation in place of exactly these
    /// external bytes. It does not establish a second commitment.
    pub fn convert_to_live(&mut self, bytes: u64) -> Result<FactToken, CapacityError> {
        if bytes > self.remaining {
            return Err(CapacityError::Invalid {
                detail: "conversion exceeds external bound",
            });
        }
        let mut s = self.domain.0.state.lock().unwrap();
        s.external -= bytes;
        self.remaining -= bytes;
        let token = self.domain.0.lane.publish_fact(bytes);
        // Accept this exact O -> L conversion only. Other unpublished scope
        // debt remains dirty until the common settlement boundary accepts it.
        s.settled_live = s
            .settled_live
            .checked_add(bytes)
            .expect("valid converted bytes");
        Ok(token)
    }
}
impl Drop for ExternalBound {
    fn drop(&mut self) {
        let mut state = self.domain.0.state.lock().unwrap();
        state.external -= self.remaining;
        state.external_handles -= 1;
        self.domain
            .0
            .lane
            .record()
            .sequence
            .fetch_add(1, crate::sync::Ordering::Release);
    }
}
