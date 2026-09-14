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

//! Registration and collision validation for application-owned durable families.
//!
//! A durable family belongs to the application that owns its record model.
//! This runtime intentionally has no built-in list of families: composition
//! supplies the owners it starts, and the runtime rejects ambiguous prefixes.

use bytes::Bytes;
use novarocks_state_store_api::Key;

/// One application's immutable StateStore record family.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PersistentStateFamily {
    owner_id: &'static str,
    prefix: &'static str,
    record_version: u8,
}

impl PersistentStateFamily {
    /// Declares one family at its owning application boundary.
    ///
    /// Validation is deliberately deferred to composition, where every active
    /// owner is available for collision checking. This constructor creates no
    /// global registry and has no side effects.
    pub const fn new(owner_id: &'static str, prefix: &'static str, record_version: u8) -> Self {
        Self {
            owner_id,
            prefix,
            record_version,
        }
    }

    pub const fn owner_id(self) -> &'static str {
        self.owner_id
    }

    pub const fn prefix(self) -> &'static str {
        self.prefix
    }

    pub const fn record_version(self) -> u8 {
        self.record_version
    }

    /// Returns this family's exact range-scan prefix.
    pub fn key(self) -> Result<Key, String> {
        Key::try_from(Bytes::from_static(self.prefix.as_bytes())).map_err(|error| {
            format!(
                "build StateStore key for durable family {}: {error}",
                self.owner_id
            )
        })
    }

    /// Appends an owner-defined suffix without normalizing frozen key bytes.
    pub fn key_with_suffix(self, suffix: &str) -> Result<Key, String> {
        let mut bytes = Vec::with_capacity(self.prefix.len() + suffix.len());
        bytes.extend_from_slice(self.prefix.as_bytes());
        bytes.extend_from_slice(suffix.as_bytes());
        Key::try_from(Bytes::from(bytes)).map_err(|error| {
            format!(
                "build StateStore key under durable family {}: {error}",
                self.owner_id
            )
        })
    }
}

/// A composition-time family registration is invalid.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PersistentStateFamilyError {
    EmptyOwnerId,
    EmptyPrefix {
        owner_id: &'static str,
    },
    DuplicateOwnerId {
        owner_id: &'static str,
    },
    OverlappingPrefixes {
        first_owner_id: &'static str,
        first_prefix: &'static str,
        second_owner_id: &'static str,
        second_prefix: &'static str,
    },
}

impl std::fmt::Display for PersistentStateFamilyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyOwnerId => {
                formatter.write_str("durable StateStore family owner id is empty")
            }
            Self::EmptyPrefix { owner_id } => {
                write!(
                    formatter,
                    "durable StateStore family {owner_id} has an empty prefix"
                )
            }
            Self::DuplicateOwnerId { owner_id } => {
                write!(
                    formatter,
                    "duplicate durable StateStore family owner id: {owner_id}"
                )
            }
            Self::OverlappingPrefixes {
                first_owner_id,
                first_prefix,
                second_owner_id,
                second_prefix,
            } => write!(
                formatter,
                "durable StateStore families {first_owner_id} ({first_prefix}) and \
                 {second_owner_id} ({second_prefix}) have overlapping prefixes"
            ),
        }
    }
}

impl std::error::Error for PersistentStateFamilyError {}

/// Verifies the set of durable families supplied by role composition.
///
/// No application may use this as a global enumerator: callers provide exactly
/// the owners composed in their process. Prefix containment is rejected as
/// well as equality because a range scan could otherwise attribute one record
/// to two owners.
pub fn validate_persistent_state_families(
    families: &[PersistentStateFamily],
) -> Result<(), PersistentStateFamilyError> {
    for (index, family) in families.iter().enumerate() {
        if family.owner_id.is_empty() {
            return Err(PersistentStateFamilyError::EmptyOwnerId);
        }
        if family.prefix.is_empty() {
            return Err(PersistentStateFamilyError::EmptyPrefix {
                owner_id: family.owner_id,
            });
        }
        for previous in &families[..index] {
            if family.owner_id == previous.owner_id {
                return Err(PersistentStateFamilyError::DuplicateOwnerId {
                    owner_id: family.owner_id,
                });
            }
            if family.prefix.starts_with(previous.prefix)
                || previous.prefix.starts_with(family.prefix)
            {
                return Err(PersistentStateFamilyError::OverlappingPrefixes {
                    first_owner_id: previous.owner_id,
                    first_prefix: previous.prefix,
                    second_owner_id: family.owner_id,
                    second_prefix: family.prefix,
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const CATALOG: PersistentStateFamily =
        PersistentStateFamily::new("catalog/desired-state", "novarocks/catalog/v1/", 3);
    const MV: PersistentStateFamily =
        PersistentStateFamily::new("mv/accelerator", "novarocks/mv/v1", 1);

    #[test]
    fn accepts_distinct_non_overlapping_owner_families() {
        validate_persistent_state_families(&[CATALOG, MV]).expect("unique family registrations");
    }

    #[test]
    fn rejects_a_nested_prefix_instead_of_attributing_by_order() {
        let nested = PersistentStateFamily::new(
            "catalog/attachments",
            "novarocks/catalog/v1/attachments/",
            1,
        );
        assert!(matches!(
            validate_persistent_state_families(&[CATALOG, nested]),
            Err(PersistentStateFamilyError::OverlappingPrefixes { .. })
        ));
    }

    #[test]
    fn preserves_an_owner_suffix_without_rewriting_frozen_separator_bytes() {
        let key = CATALOG
            .key_with_suffix("77617265686f757365")
            .expect("valid key");
        assert_eq!(key.as_bytes(), b"novarocks/catalog/v1/77617265686f757365");
    }
}
