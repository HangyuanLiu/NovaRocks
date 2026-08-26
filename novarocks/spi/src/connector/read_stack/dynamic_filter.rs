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

//! The dynamic filter contract shared by split enumeration and page sources.
//!
//! A dynamic filter is live state read through an interface; it never becomes
//! part of a table handle, a split, a scheduled split, or the wire, and it has
//! no identity, version, or digest.

use std::collections::BTreeSet;
use std::fmt::Debug;

use super::predicate::TupleDomain;

/// An immutable observation of a dynamic filter at one instant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DynamicFilterSnapshot<C: Ord + Clone + Debug> {
    current_predicate: TupleDomain<C>,
    complete: bool,
}

impl<C: Ord + Clone + Debug> DynamicFilterSnapshot<C> {
    pub const fn new(current_predicate: TupleDomain<C>, complete: bool) -> Self {
        Self {
            current_predicate,
            complete,
        }
    }

    /// The unconstrained, final snapshot used when no feedback exists.
    pub fn all_complete() -> Self {
        Self::new(TupleDomain::all(), true)
    }

    pub const fn current_predicate(&self) -> &TupleDomain<C> {
        &self.current_predicate
    }

    pub const fn is_complete(&self) -> bool {
        self.complete
    }
}

/// Live dynamic-filter state a connector may consult repeatedly.
pub trait DynamicFilter<C: Ord + Clone + Debug>: Send + Sync {
    /// The columns this filter can ever constrain.
    fn columns_covered(&self) -> &BTreeSet<C>;

    /// The predicate as of now. Must never become less restrictive over time
    /// in a way that would retroactively invalidate an earlier decision.
    fn current_predicate(&self) -> TupleDomain<C>;

    /// Whether the predicate can still tighten.
    fn is_complete(&self) -> bool;

    /// Whether a caller may usefully wait for a tighter predicate.
    ///
    /// A filter that reports `false` must never be waited on; reporting `true`
    /// while no feedback exists would fabricate a blocked scan.
    fn is_awaitable(&self) -> bool;

    /// Whether the filter is currently withholding a decision.
    fn is_blocked(&self) -> bool {
        false
    }

    fn snapshot(&self) -> DynamicFilterSnapshot<C> {
        DynamicFilterSnapshot::new(self.current_predicate(), self.is_complete())
    }
}

/// The truthful filter used where no runtime feedback is produced.
///
/// It reports the real covered columns, an unconstrained predicate, and a
/// complete, non-awaitable, non-blocked state.
#[derive(Clone, Debug)]
pub struct CompleteAllDynamicFilter<C: Ord + Clone + Debug> {
    columns_covered: BTreeSet<C>,
}

impl<C: Ord + Clone + Debug> CompleteAllDynamicFilter<C> {
    pub const fn new(columns_covered: BTreeSet<C>) -> Self {
        Self { columns_covered }
    }
}

impl<C: Ord + Clone + Debug + Send + Sync> DynamicFilter<C> for CompleteAllDynamicFilter<C> {
    fn columns_covered(&self) -> &BTreeSet<C> {
        &self.columns_covered
    }

    fn current_predicate(&self) -> TupleDomain<C> {
        TupleDomain::all()
    }

    fn is_complete(&self) -> bool {
        true
    }

    fn is_awaitable(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_no_feedback_filter_is_truthful_and_never_awaitable() {
        let mut covered = BTreeSet::new();
        covered.insert(7_u32);
        let filter = CompleteAllDynamicFilter::new(covered);
        assert!(filter.columns_covered().contains(&7));
        assert!(filter.current_predicate().is_all());
        assert!(filter.is_complete());
        assert!(!filter.is_awaitable());
        assert!(!filter.is_blocked());
        assert!(filter.snapshot().is_complete());
    }
}
