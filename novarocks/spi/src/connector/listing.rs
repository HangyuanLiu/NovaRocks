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

//! Source bounds for connector catalog listings.
//!
//! Every namespace, table and view listing is bounded at its source by a
//! caller-owned [`ConnectorListingBound`]. A listing that would exceed any
//! bound is refused with [`ConnectorErrorKind::ResourceExhausted`] naming the
//! exceeded bound. It is never truncated and never answered "best effort":
//! a partial enumeration is indistinguishable from an authoritative one.
//!
//! Providers whose source pages through a continuation token accumulate with
//! [`ConnectorListingCollector::accept_page`], which checks the page before
//! any entry is retained and before the token is followed. Providers whose
//! source returns one complete listing check it with
//! [`ConnectorListingBound::check_complete_listing`] or accumulate it with
//! [`ConnectorListingCollector::push`].

use super::{ConnectorError, ConnectorErrorKind};

/// Caller-owned bounds for one connector catalog listing.
///
/// No field has a hidden default: a request carries the bound explicitly, and
/// [`ConnectorListingBound::V1`] is both the production value and the ceiling
/// every bound is validated against.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorListingBound {
    /// Entries one whole listing may retain.
    pub entries: usize,
    /// Entries one source page may carry.
    pub page_entries: usize,
    /// Source pages one listing may consume.
    pub pages: usize,
    /// Bytes of one retained name or path.
    pub name_bytes: usize,
    /// Sum of the name bytes one whole listing snapshot may retain.
    pub total_name_bytes: usize,
    /// Bytes of one opaque continuation token the listing may follow.
    pub continuation_token_bytes: usize,
}

impl ConnectorListingBound {
    /// The frozen MEM-1 M07 profile v1 `local_source` listing bounds
    /// (`docs/testing/mem-1-m07/profile-v1.json`).
    pub const V1: Self = Self {
        entries: 65_536,
        page_entries: 256,
        pages: 1_024,
        name_bytes: 65_536,
        total_name_bytes: 16 * 1024 * 1024,
        continuation_token_bytes: 4_096,
    };

    /// Validate that this bound can make progress and never exceeds the frozen
    /// profile. A bound above [`ConnectorListingBound::V1`] is an invalid
    /// request, not a larger budget.
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.page_entries == 0 || self.pages == 0 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector listing bound must admit at least one page of at least one entry",
            ));
        }
        let ceiling = Self::V1;
        for (name, value, limit) in [
            ("entries", self.entries, ceiling.entries),
            ("page_entries", self.page_entries, ceiling.page_entries),
            ("pages", self.pages, ceiling.pages),
            ("name_bytes", self.name_bytes, ceiling.name_bytes),
            (
                "total_name_bytes",
                self.total_name_bytes,
                ceiling.total_name_bytes,
            ),
            (
                "continuation_token_bytes",
                self.continuation_token_bytes,
                ceiling.continuation_token_bytes,
            ),
        ] {
            if value > limit {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    format!(
                        "connector listing bound {name}={value} exceeds the frozen profile limit {limit}"
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Check one complete listing that an unpaged source has already returned.
    ///
    /// Use this where the source has no paging capability: the listing is
    /// refused as a whole when it exceeds the entry, name or snapshot bound.
    pub fn check_complete_listing<T: AsRef<str>>(
        &self,
        entries: &[T],
    ) -> Result<(), ConnectorError> {
        ConnectorListingBudget::new(*self)?.admit_names(entries.iter().map(AsRef::as_ref))
    }
}

/// Entry and name-byte accounting for one listing snapshot, which may span
/// several source listings.
///
/// Every admission is atomic: either all of its names are charged or none is.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorListingBudget {
    bound: ConnectorListingBound,
    entries: usize,
    total_name_bytes: usize,
}

impl ConnectorListingBudget {
    pub fn new(bound: ConnectorListingBound) -> Result<Self, ConnectorError> {
        bound.validate()?;
        Ok(Self {
            bound,
            entries: 0,
            total_name_bytes: 0,
        })
    }

    pub fn bound(&self) -> ConnectorListingBound {
        self.bound
    }

    /// Entries charged so far.
    pub fn entries(&self) -> usize {
        self.entries
    }

    /// Name bytes charged so far.
    pub fn total_name_bytes(&self) -> usize {
        self.total_name_bytes
    }

    /// The bound a further source listing inside this snapshot must observe:
    /// the same page, name and token limits with only the unspent entries and
    /// name bytes.
    pub fn remaining_bound(&self) -> ConnectorListingBound {
        ConnectorListingBound {
            entries: self.bound.entries - self.entries,
            total_name_bytes: self.bound.total_name_bytes - self.total_name_bytes,
            ..self.bound
        }
    }

    /// Admit one group of names atomically.
    pub fn admit_names<'a>(
        &mut self,
        names: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), ConnectorError> {
        self.admit(None, names)
    }

    /// Admit names that each retain their own copy of `qualifier`, such as a
    /// table name stored together with its namespace. Each entry is charged
    /// the bytes of both.
    pub fn admit_qualified_names<'a>(
        &mut self,
        qualifier: &str,
        names: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), ConnectorError> {
        self.admit(Some(qualifier), names)
    }

    fn admit<'a>(
        &mut self,
        qualifier: Option<&str>,
        names: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), ConnectorError> {
        let qualifier_bytes = match qualifier {
            Some(qualifier) => {
                self.check_name(qualifier)?;
                qualifier.len()
            }
            None => 0,
        };
        let mut entries = self.entries;
        let mut total_name_bytes = self.total_name_bytes;
        for name in names {
            self.check_name(name)?;
            entries = entries
                .checked_add(1)
                .filter(|entries| *entries <= self.bound.entries)
                .ok_or_else(|| exceeded("entries", self.bound.entries))?;
            total_name_bytes = qualifier_bytes
                .checked_add(name.len())
                .and_then(|entry_bytes| total_name_bytes.checked_add(entry_bytes))
                .filter(|bytes| *bytes <= self.bound.total_name_bytes)
                .ok_or_else(|| exceeded("total_name_bytes", self.bound.total_name_bytes))?;
        }
        self.entries = entries;
        self.total_name_bytes = total_name_bytes;
        Ok(())
    }

    fn check_name(&self, name: &str) -> Result<(), ConnectorError> {
        if name.len() > self.bound.name_bytes {
            return Err(exceeded("name_bytes", self.bound.name_bytes));
        }
        Ok(())
    }
}

/// A bounded accumulator for one connector listing.
///
/// Every check runs before the listing grows: a refused entry or page leaves
/// the retained entries, the page count and the continuation token exactly as
/// they were.
#[derive(Debug)]
pub struct ConnectorListingCollector<T> {
    budget: ConnectorListingBudget,
    entries: Vec<T>,
    pages: usize,
    continuation: Option<String>,
}

impl<T: AsRef<str>> ConnectorListingCollector<T> {
    pub fn new(bound: ConnectorListingBound) -> Result<Self, ConnectorError> {
        Ok(Self {
            budget: ConnectorListingBudget::new(bound)?,
            entries: Vec::new(),
            pages: 0,
            continuation: None,
        })
    }

    pub fn bound(&self) -> ConnectorListingBound {
        self.budget.bound()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn total_name_bytes(&self) -> usize {
        self.budget.total_name_bytes()
    }

    /// Source pages accepted so far.
    pub fn pages(&self) -> usize {
        self.pages
    }

    /// The continuation the next page request must replay unchanged, if the
    /// source announced one.
    pub fn continuation_token(&self) -> Option<&str> {
        self.continuation.as_deref()
    }

    /// Retain one entry of an unpaged source.
    pub fn push(&mut self, entry: T) -> Result<(), ConnectorError> {
        self.budget.admit_names(std::iter::once(entry.as_ref()))?;
        self.entries.push(entry);
        Ok(())
    }

    /// Accept one source page and the continuation it announces, atomically.
    ///
    /// The page is refused before any of its entries is retained when it
    /// carries more than `page_entries`, when it would exceed the listing's
    /// entry or name bounds, or when following its continuation would exceed
    /// the `pages` or `continuation_token_bytes` bound. A refused page does not
    /// advance the continuation token.
    pub fn accept_page(
        &mut self,
        page: Vec<T>,
        next_token: Option<String>,
    ) -> Result<(), ConnectorError> {
        let bound = self.budget.bound();
        let pages = self
            .pages
            .checked_add(1)
            .filter(|pages| *pages <= bound.pages)
            .ok_or_else(|| exceeded("pages", bound.pages))?;
        if page.len() > bound.page_entries {
            return Err(exceeded("page_entries", bound.page_entries));
        }
        if let Some(token) = next_token.as_deref() {
            if token.is_empty() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "connector listing source returned an empty continuation token",
                ));
            }
            if token.len() > bound.continuation_token_bytes {
                return Err(exceeded(
                    "continuation_token_bytes",
                    bound.continuation_token_bytes,
                ));
            }
            if self.continuation.as_deref() == Some(token) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "connector listing source repeated the continuation token it was given",
                ));
            }
            if pages >= bound.pages {
                return Err(exceeded("pages", bound.pages));
            }
        }
        self.budget
            .admit_names(page.iter().map(|entry| entry.as_ref()))?;
        self.entries.extend(page);
        self.pages = pages;
        self.continuation = next_token;
        Ok(())
    }

    /// Return the complete listing. A listing whose source still announces a
    /// continuation is incomplete and is refused rather than returned.
    pub fn finish(self) -> Result<Vec<T>, ConnectorError> {
        if self.continuation.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "connector listing finished with an unfollowed continuation token",
            ));
        }
        Ok(self.entries)
    }
}

fn exceeded(bound: &'static str, limit: usize) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        format!("connector listing refused: it exceeds the {bound} bound of {limit}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small() -> ConnectorListingBound {
        ConnectorListingBound {
            entries: 4,
            page_entries: 2,
            pages: 3,
            name_bytes: 3,
            total_name_bytes: 9,
            continuation_token_bytes: 4,
        }
    }

    fn names(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    fn token(value: &str) -> Option<String> {
        Some(value.to_string())
    }

    #[track_caller]
    fn assert_exhausted(error: ConnectorError, bound: &str) {
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
        assert!(
            error.to_string().contains(&format!("the {bound} bound")),
            "{error}"
        );
    }

    #[test]
    fn v1_matches_the_frozen_local_source_profile() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../docs/testing/mem-1-m07/profile-v1.json");
        let profile: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        let source = &profile["local_source"];
        let bound = ConnectorListingBound::V1;
        for (key, value) in [
            ("entries", bound.entries),
            ("page_entries", bound.page_entries),
            ("pages", bound.pages),
            ("single_name_path_bytes", bound.name_bytes),
            ("snapshot_bytes", bound.total_name_bytes),
            ("continuation_token_bytes", bound.continuation_token_bytes),
        ] {
            assert_eq!(source[key].as_u64(), Some(value as u64), "frozen key {key}");
        }
    }

    #[test]
    fn bounds_that_cannot_progress_or_exceed_the_profile_are_invalid() {
        ConnectorListingBound::V1.validate().unwrap();
        small().validate().unwrap();
        for bound in [
            ConnectorListingBound {
                page_entries: 0,
                ..small()
            },
            ConnectorListingBound {
                pages: 0,
                ..small()
            },
            ConnectorListingBound {
                entries: ConnectorListingBound::V1.entries + 1,
                ..small()
            },
            ConnectorListingBound {
                total_name_bytes: ConnectorListingBound::V1.total_name_bytes + 1,
                ..small()
            },
            ConnectorListingBound {
                continuation_token_bytes: ConnectorListingBound::V1.continuation_token_bytes + 1,
                ..small()
            },
        ] {
            let error = ConnectorListingCollector::<String>::new(bound).unwrap_err();
            assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        }
    }

    #[test]
    fn push_refuses_each_bound_before_growth_and_accepts_the_exact_limit() {
        let mut collector = ConnectorListingCollector::new(small()).unwrap();
        assert_exhausted(
            collector.push("abcd".to_string()).unwrap_err(),
            "name_bytes",
        );
        assert!(collector.is_empty());

        for name in ["abc", "def", "ghi"] {
            collector.push(name.to_string()).unwrap();
        }
        assert_eq!(collector.total_name_bytes(), 9);
        assert_exhausted(
            collector.push("x".to_string()).unwrap_err(),
            "total_name_bytes",
        );
        assert_eq!(collector.len(), 3);
        assert_eq!(collector.total_name_bytes(), 9);

        let mut collector = ConnectorListingCollector::new(ConnectorListingBound {
            total_name_bytes: 100,
            ..small()
        })
        .unwrap();
        for name in ["a", "b", "c", "d"] {
            collector.push(name.to_string()).unwrap();
        }
        assert_exhausted(collector.push("e".to_string()).unwrap_err(), "entries");
        assert_eq!(collector.finish().unwrap(), names(&["a", "b", "c", "d"]));
    }

    #[test]
    fn an_oversized_page_is_refused_without_advancing_the_token() {
        let mut collector = ConnectorListingCollector::new(small()).unwrap();
        collector.accept_page(names(&["a"]), token("t1")).unwrap();
        assert_exhausted(
            collector
                .accept_page(names(&["b", "c", "d"]), token("t2"))
                .unwrap_err(),
            "page_entries",
        );
        assert_eq!(collector.continuation_token(), Some("t1"));
        assert_eq!(collector.pages(), 1);
        assert_eq!(collector.len(), 1);
    }

    #[test]
    fn an_oversized_token_is_refused_without_retaining_its_page() {
        let mut collector = ConnectorListingCollector::new(small()).unwrap();
        collector.accept_page(names(&["a"]), token("t1")).unwrap();
        assert_exhausted(
            collector
                .accept_page(names(&["b"]), token("t2345"))
                .unwrap_err(),
            "continuation_token_bytes",
        );
        assert_eq!(collector.continuation_token(), Some("t1"));
        assert_eq!(collector.len(), 1);

        collector.accept_page(names(&["b"]), token("t234")).unwrap();
        assert_eq!(collector.continuation_token(), Some("t234"));
    }

    #[test]
    fn a_page_that_would_overflow_the_listing_is_refused_whole() {
        let mut collector = ConnectorListingCollector::new(small()).unwrap();
        collector
            .accept_page(names(&["abc", "def"]), token("t1"))
            .unwrap();
        // "ghi" fits the remaining bytes but "j" does not; neither is retained.
        assert_exhausted(
            collector
                .accept_page(names(&["ghi", "j"]), None)
                .unwrap_err(),
            "total_name_bytes",
        );
        assert_eq!(collector.len(), 2);
        assert_eq!(collector.total_name_bytes(), 6);
        assert_eq!(collector.continuation_token(), Some("t1"));

        collector.accept_page(names(&["ghi"]), None).unwrap();
        assert_eq!(collector.finish().unwrap(), names(&["abc", "def", "ghi"]));
    }

    #[test]
    fn the_page_bound_is_checked_before_a_token_is_followed() {
        let mut collector = ConnectorListingCollector::new(small()).unwrap();
        collector.accept_page(names(&["a"]), token("t1")).unwrap();
        collector.accept_page(names(&["b"]), token("t2")).unwrap();
        // A third page that announces a fourth is refused before the fourth is
        // requested.
        assert_exhausted(
            collector
                .accept_page(names(&["c"]), token("t3"))
                .unwrap_err(),
            "pages",
        );
        assert_eq!(collector.continuation_token(), Some("t2"));
        assert_eq!(collector.pages(), 2);

        // The third and last page is the exact limit.
        collector.accept_page(names(&["c"]), None).unwrap();
        assert_eq!(collector.pages(), 3);
        assert_exhausted(
            collector.accept_page(Vec::new(), None).unwrap_err(),
            "pages",
        );
        assert_eq!(collector.finish().unwrap(), names(&["a", "b", "c"]));
    }

    #[test]
    fn empty_or_repeated_tokens_are_corrupt() {
        let mut collector = ConnectorListingCollector::new(small()).unwrap();
        let error = collector.accept_page(names(&["a"]), token("")).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        collector.accept_page(names(&["a"]), token("t1")).unwrap();
        let error = collector
            .accept_page(names(&["b"]), token("t1"))
            .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert_eq!(collector.len(), 1);
    }

    #[test]
    fn an_unfollowed_continuation_is_never_returned_as_complete() {
        let mut collector = ConnectorListingCollector::new(small()).unwrap();
        collector.accept_page(names(&["a"]), token("t1")).unwrap();
        let error = collector.finish().unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Internal);
    }

    #[test]
    fn complete_listings_are_refused_whole() {
        small()
            .check_complete_listing(&names(&["abc", "def", "ghi"]))
            .unwrap();
        assert_exhausted(
            small()
                .check_complete_listing(&names(&["a", "b", "c", "d", "e"]))
                .unwrap_err(),
            "entries",
        );
        assert_exhausted(
            small()
                .check_complete_listing(&names(&["abcd"]))
                .unwrap_err(),
            "name_bytes",
        );
    }

    #[test]
    fn a_budget_spans_listings_and_charges_retained_qualifiers() {
        let mut budget = ConnectorListingBudget::new(ConnectorListingBound {
            total_name_bytes: 12,
            ..small()
        })
        .unwrap();
        budget.admit_names(["db"]).unwrap();
        assert_eq!(
            budget.remaining_bound(),
            ConnectorListingBound {
                entries: 3,
                total_name_bytes: 10,
                ..small()
            }
        );
        // Each table retains its namespace: 2 x (2 + 3) bytes.
        budget.admit_qualified_names("db", ["t01", "t02"]).unwrap();
        assert_eq!(budget.entries(), 3);
        assert_eq!(budget.total_name_bytes(), 12);
        let before = budget.clone();
        assert_exhausted(
            budget.admit_qualified_names("db", ["t"]).unwrap_err(),
            "total_name_bytes",
        );
        assert_eq!(budget, before);
        assert_exhausted(
            budget.admit_qualified_names("dbxx", []).unwrap_err(),
            "name_bytes",
        );
    }
}
