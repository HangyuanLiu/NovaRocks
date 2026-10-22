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

//! Catalog-scoped engine wrappers around validated connector carriers.

use novarocks_spi::connector::read_stack::ConnectorReadSplit;

/// One split as the coordinator sees it, for placement only.
///
/// It carries no catalog because provider identity belongs to the scan's
/// frozen facts, not to the assignment that routes this split.
#[derive(Clone, Debug)]
pub(crate) struct Split {
    split: ConnectorReadSplit,
}

impl Split {
    pub(crate) fn new(split: ConnectorReadSplit) -> Self {
        Self { split }
    }

    pub(crate) const fn split(&self) -> &ConnectorReadSplit {
        &self.split
    }

    /// Relative scheduling cost. Weight only influences placement; it never
    /// changes what the split reads.
    pub(crate) fn weight_raw(&self) -> u64 {
        self.split.facts().split_weight().raw_value()
    }
}
