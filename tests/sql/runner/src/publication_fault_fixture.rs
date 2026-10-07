// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Closed runner API over the shared transparent REST fault owner.
//!
//! The downstream catalog remains authoritative. This API discards one
//! successful commit response and returns a retryable HTTP failure; it does
//! not claim that a TCP reset occurred or manufacture a provider verdict.

use crate::publication_catalog::{FixtureControl, FixtureFaultGuard, FixtureHandle};
use anyhow::Result;
use std::time::Instant;

pub struct CatalogResponseLossFixture(FixtureHandle);

impl CatalogResponseLossFixture {
    pub fn start(downstream: String) -> Result<Self> {
        FixtureHandle::start(downstream).map(Self)
    }

    pub fn uri(&self) -> &str {
        self.0.uri()
    }

    pub fn control(&self, deadline: Instant) -> Result<CatalogResponseLossControl> {
        self.0
            .control_with_deadline(Some(deadline))
            .map(CatalogResponseLossControl)
    }
}

#[derive(Clone)]
pub struct CatalogResponseLossControl(FixtureControl);

impl CatalogResponseLossControl {
    pub fn arm_next_table_commit(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<SuccessfulCommitResponseLoss> {
        self.0
            .arm_next_for_table(namespace, table)
            .map(SuccessfulCommitResponseLoss)
    }

    pub fn successful_table_commits(&self, namespace: &str, table: &str) -> usize {
        self.0
            .mutation_counts(namespace, table)
            .table_commit_succeeded
    }
}

pub struct SuccessfulCommitResponseLoss(FixtureFaultGuard);

impl SuccessfulCommitResponseLoss {
    /// Fails unless the real catalog committed and its success response was
    /// discarded. The bounded, secret-free trace belongs to this exact arm.
    pub fn finish(self, deadline: Instant) -> Result<String> {
        self.0
            .finish_before(deadline)
            .map(|evidence| evidence.summary())
    }
}
