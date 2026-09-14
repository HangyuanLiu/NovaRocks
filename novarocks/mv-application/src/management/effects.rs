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

use super::{ManagedMvTarget, ManagementTimestamp, ProcessIncarnation};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EffectIdentity([u8; 16]);

impl EffectIdentity {
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

/// External paths have separate guarantees. A catalog guarantee never covers
/// object deletion, and a delete guarantee never protects a catalog commit.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EffectPath {
    CatalogCommit,
    ObjectDeletion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EffectScope {
    catalog_commit: bool,
    object_deletion: bool,
}

impl EffectScope {
    pub const CATALOG_COMMIT: Self = Self {
        catalog_commit: true,
        object_deletion: false,
    };
    pub const OBJECT_DELETION: Self = Self {
        catalog_commit: false,
        object_deletion: true,
    };
    pub const CATALOG_AND_OBJECT_DELETION: Self = Self {
        catalog_commit: true,
        object_deletion: true,
    };

    pub const fn contains(self, path: EffectPath) -> bool {
        match path {
            EffectPath::CatalogCommit => self.catalog_commit,
            EffectPath::ObjectDeletion => self.object_deletion,
        }
    }

    pub const fn covers(self, required: Self) -> bool {
        (!required.catalog_commit || self.catalog_commit)
            && (!required.object_deletion || self.object_deletion)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectDisposition {
    KnownCommitted,
    KnownUncommitted,
    CommitUnknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EffectResponsibility {
    identity: EffectIdentity,
    target: ManagedMvTarget,
    dispatching_incarnation: ProcessIncarnation,
    scope: EffectScope,
    last_possible_dispatch_at: ManagementTimestamp,
}

impl EffectResponsibility {
    pub const fn new(
        identity: EffectIdentity,
        target: ManagedMvTarget,
        dispatching_incarnation: ProcessIncarnation,
        scope: EffectScope,
        last_possible_dispatch_at: ManagementTimestamp,
    ) -> Self {
        Self {
            identity,
            target,
            dispatching_incarnation,
            scope,
            last_possible_dispatch_at,
        }
    }

    pub const fn identity(&self) -> EffectIdentity {
        self.identity
    }

    pub const fn target(&self) -> &ManagedMvTarget {
        &self.target
    }

    pub const fn dispatching_incarnation(&self) -> &ProcessIncarnation {
        &self.dispatching_incarnation
    }

    pub const fn scope(&self) -> EffectScope {
        self.scope
    }

    pub const fn last_possible_dispatch_at(&self) -> ManagementTimestamp {
        self.last_possible_dispatch_at
    }

    pub fn record_terminal(self, disposition: EffectDisposition) -> EffectTerminalFact {
        match disposition {
            EffectDisposition::KnownCommitted => EffectTerminalFact::KnownCommitted(self),
            EffectDisposition::KnownUncommitted => EffectTerminalFact::KnownUncommitted(self),
            EffectDisposition::CommitUnknown => {
                EffectTerminalFact::CommitUnknown(UnsettledEffect {
                    responsibility: self,
                })
            }
        }
    }
}

/// The original terminal result is immutable. Readmission closes only the
/// responsibility for possible future effects; it does not rewrite this fact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EffectTerminalFact {
    KnownCommitted(EffectResponsibility),
    KnownUncommitted(EffectResponsibility),
    CommitUnknown(UnsettledEffect),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnsettledEffect {
    responsibility: EffectResponsibility,
}

impl UnsettledEffect {
    pub const fn responsibility(&self) -> &EffectResponsibility {
        &self.responsibility
    }

    pub const fn original_disposition(&self) -> EffectDisposition {
        EffectDisposition::CommitUnknown
    }
}
