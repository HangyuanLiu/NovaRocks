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

//! Neutral identities.
//!
//! The memory core never names an application type. A caller that owns a
//! `QueryExecutionId`, a `WorkScope` or a service handle maps it to an
//! [`ExternalRef`] and keeps the mapping on its own side. Every identity here
//! is a plain integer so the core stays free of Arrow, SQL, scheduler and
//! transport vocabulary.

use std::fmt;
macro_rules! neutral_id {
    ($(#[$meta:meta])* $name:ident, $label:literal) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(u64);

        impl $name {
            /// Wraps a raw value, issued by its process authority.
            pub const fn new(raw: u64) -> Self {
                Self(raw)
            }

            /// Returns the raw value.
            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!($label, "#{}"), self.0)
            }
        }
    };
}

neutral_id!(
    /// Identifies one node of the strict account tree.
    AccountId,
    "account"
);
/// Version of the policy constraints installed on an account.
///
/// A snapshot reports the version its numbers were produced under, so a
/// consumer can tell a stale reading from a policy change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct PolicyVersion(u64);

impl PolicyVersion {
    /// The version an account carries before any policy is installed.
    pub const INITIAL: Self = Self(0);

    /// Wraps a raw version value.
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw version value.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next version.
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl fmt::Display for PolicyVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "policy-v{}", self.0)
    }
}

/// Version of the authority configuration a snapshot was taken under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ConfigVersion(u64);

impl ConfigVersion {
    /// Wraps a raw version value.
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw version value.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next version.
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl fmt::Display for ConfigVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "config-v{}", self.0)
    }
}

/// An opaque 128-bit reference to something outside the memory core.
///
/// The core stores it, reports it in snapshots, and compares it for equality.
/// It never interprets it. Callers use it for the work, attempt, service or
/// resource identity that their own domain owns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ExternalRef {
    high: u64,
    low: u64,
}

impl ExternalRef {
    /// A reference that names nothing; used where an account has no external
    /// identity yet, such as the process root.
    pub const NONE: Self = Self { high: 0, low: 0 };

    /// Builds a reference from two halves.
    pub const fn new(high: u64, low: u64) -> Self {
        Self { high, low }
    }

    /// Builds a reference from a 128-bit value, which is how a UUID-shaped
    /// application identity normally arrives.
    pub const fn from_u128(value: u128) -> Self {
        Self {
            high: (value >> 64) as u64,
            low: value as u64,
        }
    }

    /// Returns the high half.
    pub const fn high(self) -> u64 {
        self.high
    }

    /// Returns the low half.
    pub const fn low(self) -> u64 {
        self.low
    }

    /// Returns the reference as a 128-bit value.
    pub const fn to_u128(self) -> u128 {
        ((self.high as u128) << 64) | self.low as u128
    }

    /// Reports whether this reference names nothing.
    pub const fn is_none(self) -> bool {
        self.high == 0 && self.low == 0
    }
}

impl fmt::Display for ExternalRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_none() {
            f.write_str("external:none")
        } else {
            write!(f, "external:{:016x}{:016x}", self.high, self.low)
        }
    }
}

/// What an account stands for in the strict tree.
///
/// The kind carries no policy of its own: it labels the node so snapshots and
/// diagnostics are readable, and it lets the authority reject structurally
/// impossible parents (for example a second process root).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AccountKind {
    /// The single root of one operating-system process.
    Process,
    /// A resource group: the hard-limit branch a work belongs to.
    ResourceGroup,
    /// One unit of work, such as a statement or a refresh job.
    Work,
    /// One attempt of a work.
    Attempt,
    /// One locally created task of an attempt.
    Task,
    /// One allocation owner inside a task, such as an operator.
    Owner,
    /// A long-lived process service, such as a cache.
    Service,
    /// Work that exists before any attempt, such as compilation.
    Preparation,
}

impl AccountKind {
    /// Returns the label used in snapshots and diagnostics.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Process => "process",
            Self::ResourceGroup => "resource-group",
            Self::Work => "work",
            Self::Attempt => "attempt",
            Self::Task => "task",
            Self::Owner => "owner",
            Self::Service => "service",
            Self::Preparation => "preparation",
        }
    }
}

impl fmt::Display for AccountKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identities_display_with_their_own_label() {
        assert_eq!(AccountId::new(7).to_string(), "account#7");
        assert_eq!(PolicyVersion::new(3).to_string(), "policy-v3");
        assert_eq!(ConfigVersion::new(3).to_string(), "config-v3");
    }

    #[test]
    fn external_ref_round_trips_a_128_bit_identity() {
        let value = 0x0123_4567_89ab_cdef_fedc_ba98_7654_3210u128;
        let reference = ExternalRef::from_u128(value);
        assert_eq!(reference.to_u128(), value);
        assert_eq!(reference.high(), 0x0123_4567_89ab_cdef);
        assert_eq!(reference.low(), 0xfedc_ba98_7654_3210);
        assert!(!reference.is_none());
        assert!(ExternalRef::NONE.is_none());
        assert_eq!(ExternalRef::NONE.to_string(), "external:none");
    }

    #[test]
    fn policy_version_starts_at_initial_and_advances() {
        assert_eq!(PolicyVersion::INITIAL.get(), 0);
        assert_eq!(PolicyVersion::INITIAL.next().get(), 1);
        assert!(PolicyVersion::INITIAL < PolicyVersion::INITIAL.next());
    }

    #[test]
    fn account_kind_labels_are_stable() {
        assert_eq!(AccountKind::Process.label(), "process");
        assert_eq!(AccountKind::ResourceGroup.label(), "resource-group");
        assert_eq!(AccountKind::Preparation.to_string(), "preparation");
    }
}
