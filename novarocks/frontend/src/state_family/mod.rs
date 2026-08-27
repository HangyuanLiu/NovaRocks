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

//! Closed manifest of frontend-local state families.
//!
//! Every piece of frontend state belongs to exactly one family, and every
//! family is registered here with exactly one classification, one authority or
//! rebuild source, one record version, and one retain/clone/wipe policy.  The
//! manifest exists because those facts used to live wherever each owner module
//! happened to put them: an owner declared its own `const PREFIX`, picked its
//! own schema version, and — unless the author thought of it — said nothing at
//! all about what a restart or a deployment clone should do to its records.
//! Adding a durable family cost nothing and broke nothing, which is exactly
//! why the frontend accumulated durable state nobody owned.
//!
//! Two structural properties do the enforcing, so the manifest is not another
//! convention that has to be remembered during review:
//!
//! 1. **A `ProcessRuntime` family cannot have a persistent prefix.** The prefix
//!    lives in the data of the `ExternalProjection` and `Accelerator` variants
//!    only, so the illegal state is not representable — see
//!    [`ProcessRuntimeContract`].
//! 2. **Only the manifest can mint a prefix.** [`PersistentKeyPrefix`] has a
//!    private literal and a constructor visible only inside this module tree,
//!    so an owner module can read a prefix from the manifest but cannot invent
//!    a second definition point for one.
//!
//! Owner modules keep their own suffix schemes and build keys through
//! [`PersistentKeyPrefix::key`] / [`PersistentKeyPrefix::key_with_suffix`].
//!
//! The manifest is a frontend application fact, not an SPI contract: the
//! StateStore boundary knows about keys and values, and has no opinion about
//! which frontend family owns them.

mod classification;
mod manifest;

pub use classification::{
    AcceleratorContract, AcceleratorRebuildAuthority, AcceleratorResidence, BootstrapFailureScope,
    ClonePolicy, DurabilityAdmission, ExternalProjectionContract, ExternalProjectionSource,
    PersistentKeyPrefix, ProcessRuntimeAuthority, ProcessRuntimeContract, RebuildDeterminism,
    SnapshotIdentity, StateFamilyClassification, WipeEntry,
};
pub use manifest::StateFamily;
