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

use std::fmt;

/// Bit-exact identifier used for protocol, fragment, and execution identities.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct UniqueId {
    high: i64,
    low: i64,
}

impl UniqueId {
    pub const fn new(high: i64, low: i64) -> Self {
        Self { high, low }
    }

    pub const fn high(self) -> i64 {
        self.high
    }

    pub const fn low(self) -> i64 {
        self.low
    }

    pub fn to_uuid_string(self) -> String {
        format_uuid(self.high, self.low)
    }
}

impl fmt::Display for UniqueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_uuid(f, self.high, self.low)
    }
}

/// Query identity shared by coordinator and runtime ownership domains.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct QueryId {
    high: i64,
    low: i64,
}

impl QueryId {
    pub const fn new(high: i64, low: i64) -> Self {
        Self { high, low }
    }

    pub const fn high(self) -> i64 {
        self.high
    }

    pub const fn low(self) -> i64 {
        self.low
    }

    /// Returns the process attribution carried by a query id allocated by the
    /// native frontend allocator.  A non-positive low half is not an
    /// attributable local sequence and is therefore rejected rather than
    /// guessed.
    pub const fn process_attribution(self) -> Option<QueryIdAttribution> {
        QueryIdAttribution::from_query_id(self)
    }
}

impl fmt::Display for QueryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_uuid(f, self.high, self.low)
    }
}

/// Process-local, high-entropy namespace carried in the high half of a native
/// query identity.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct QueryProcessNamespace(u64);

impl QueryProcessNamespace {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn into_query_id_high(self) -> i64 {
        self.0 as i64
    }
}

impl fmt::Display for QueryProcessNamespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{:016x}", self.0)
    }
}

/// Strictly positive, never-reused sequence within one query process
/// namespace.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LocalQuerySequence(std::num::NonZeroU64);

impl LocalQuerySequence {
    pub const fn new(value: u64) -> Option<Self> {
        if value > i64::MAX as u64 {
            return None;
        }
        match std::num::NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }

    pub const fn into_query_id_low(self) -> i64 {
        self.get() as i64
    }
}

impl fmt::Display for LocalQuerySequence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Typed diagnostic view of the process namespace and local sequence embedded
/// in a native query id.
// Design: ADR-0092 (docs/adr/ADR-0092-process-scoped-query-execution-attribution.md)
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct QueryIdAttribution {
    namespace: QueryProcessNamespace,
    sequence: LocalQuerySequence,
}

impl QueryIdAttribution {
    pub const fn new(namespace: QueryProcessNamespace, sequence: LocalQuerySequence) -> Self {
        Self {
            namespace,
            sequence,
        }
    }

    pub const fn namespace(self) -> QueryProcessNamespace {
        self.namespace
    }

    pub const fn sequence(self) -> LocalQuerySequence {
        self.sequence
    }

    pub const fn into_query_id(self) -> QueryId {
        QueryId::new(
            self.namespace.into_query_id_high(),
            self.sequence.into_query_id_low(),
        )
    }

    pub const fn from_query_id(query_id: QueryId) -> Option<Self> {
        if query_id.low <= 0 {
            return None;
        }
        match LocalQuerySequence::new(query_id.low as u64) {
            Some(sequence) => Some(Self::new(
                QueryProcessNamespace::new(query_id.high as u64),
                sequence,
            )),
            None => None,
        }
    }
}

impl fmt::Display for QueryIdAttribution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "namespace={} sequence={}", self.namespace, self.sequence)
    }
}

pub fn format_uuid(high: i64, low: i64) -> String {
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        ((high as u64) >> 32) as u32,
        ((high as u64) >> 16) as u16,
        (high as u64) as u16,
        ((low as u64) >> 48) as u16,
        (low as u64) & 0x0000_FFFF_FFFF_FFFF
    )
}

fn write_uuid(f: &mut fmt::Formatter<'_>, high: i64, low: i64) -> fmt::Result {
    f.write_str(&format_uuid(high, low))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashSet};

    use super::{
        LocalQuerySequence, QueryId, QueryIdAttribution, QueryProcessNamespace, UniqueId,
        format_uuid,
    };

    #[test]
    fn identities_preserve_the_java_uuid_bit_layout() {
        let high = 116135542886790518;
        let low = -7531368976812794106;
        let expected = "019c98a9-3390-7576-977b-33d188ad1f06";

        assert_eq!(format_uuid(high, low), expected);
        assert_eq!(UniqueId::new(high, low).to_string(), expected);
        assert_eq!(QueryId::new(high, low).to_string(), expected);
    }

    #[test]
    fn identities_are_value_ordered_and_hashable() {
        let lower = UniqueId::new(1, -1);
        let higher = UniqueId::new(2, -1);
        assert!(lower < higher);

        let mut ordered = BTreeSet::new();
        ordered.insert(higher);
        ordered.insert(lower);
        assert_eq!(ordered.into_iter().collect::<Vec<_>>(), vec![lower, higher]);

        let mut hashed = HashSet::new();
        hashed.insert(QueryId::new(7, 9));
        assert!(hashed.contains(&QueryId::new(7, 9)));
    }

    #[test]
    fn query_process_attribution_round_trips_without_changing_query_id_display() {
        let namespace = QueryProcessNamespace::new(0xfedc_ba98_7654_3210);
        let sequence = LocalQuerySequence::new(7).expect("nonzero sequence");
        let attribution = QueryIdAttribution::new(namespace, sequence);
        let query_id = attribution.into_query_id();

        assert_eq!(query_id.high(), namespace.into_query_id_high());
        assert_eq!(query_id.low(), 7);
        assert_eq!(query_id.process_attribution(), Some(attribution));
        assert_eq!(
            attribution.to_string(),
            "namespace=0xfedcba9876543210 sequence=7"
        );
        assert_eq!(
            query_id.to_string(),
            format_uuid(namespace.into_query_id_high(), sequence.into_query_id_low())
        );
    }

    #[test]
    fn query_process_attribution_rejects_missing_or_non_local_sequence() {
        assert!(LocalQuerySequence::new(0).is_none());
        assert!(LocalQuerySequence::new((i64::MAX as u64) + 1).is_none());
        assert!(QueryId::new(1, 0).process_attribution().is_none());
        assert!(QueryId::new(1, -1).process_attribution().is_none());
    }
}
