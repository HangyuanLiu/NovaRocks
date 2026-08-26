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

//! Split identity, scheduling weight, and node affinity.

use std::fmt::Debug;
use std::sync::Arc;

use crate::connector::{ConnectorError, ConnectorErrorKind};

/// The raw weight of one standard split, matching Trino's standard value.
pub const STANDARD_SPLIT_WEIGHT_RAW: u64 = 100;

/// Relative scheduling cost of one split.
///
/// Weight only influences assignment; it never changes what a split reads.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SplitWeight(u64);

impl SplitWeight {
    pub const STANDARD: Self = Self(STANDARD_SPLIT_WEIGHT_RAW);

    pub const fn raw_value(self) -> u64 {
        self.0
    }

    pub fn try_from_raw(raw: u64) -> Result<Self, ConnectorError> {
        if raw == 0 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector split weight must be positive",
            ));
        }
        if raw > STANDARD_SPLIT_WEIGHT_RAW * 1_000_000 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector split weight exceeds the hard limit",
            ));
        }
        Ok(Self(raw))
    }

    /// Convert a proportion of one standard split into a weight.
    ///
    /// The result is rounded up and clamped to a legal positive value, so a
    /// tiny split never becomes weightless.
    pub fn from_proportion(proportion: f64) -> Result<Self, ConnectorError> {
        if !proportion.is_finite() || proportion <= 0.0 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector split weight proportion must be finite and positive",
            ));
        }
        let scaled = (proportion * STANDARD_SPLIT_WEIGHT_RAW as f64).ceil();
        if scaled > (STANDARD_SPLIT_WEIGHT_RAW * 1_000_000) as f64 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector split weight proportion exceeds the hard limit",
            ));
        }
        Self::try_from_raw(scaled as u64)
    }
}

impl Default for SplitWeight {
    fn default() -> Self {
        Self::STANDARD
    }
}

/// A worker address a split prefers or requires.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HostAddress {
    host: Arc<str>,
    port: u16,
}

impl HostAddress {
    pub fn try_new(host: impl AsRef<str>, port: u16) -> Result<Self, ConnectorError> {
        let host = host.as_ref();
        if host.is_empty() || host.len() > 255 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector host address must be non-empty and bounded",
            ));
        }
        Ok(Self {
            host: Arc::from(host),
            port,
        })
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub const fn port(&self) -> u16 {
        self.port
    }
}

/// A connector-owned unit of remotely schedulable read work.
pub trait ConnectorSplit: Debug + Send + Sync + 'static {
    /// Whether any worker may run this split.
    fn is_remotely_accessible(&self) -> bool {
        true
    }

    /// Addresses this split must or prefers to run on.
    fn addresses(&self) -> &[HostAddress] {
        &[]
    }

    /// A stable key used to co-locate related splits; never an identity.
    fn affinity_key(&self) -> Option<&str> {
        None
    }

    fn split_weight(&self) -> SplitWeight {
        SplitWeight::STANDARD
    }

    fn retained_size_in_bytes(&self) -> u64;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_weight_rounds_up_and_stays_positive() {
        assert_eq!(
            SplitWeight::from_proportion(1.0).expect("legal"),
            SplitWeight::STANDARD
        );
        assert_eq!(
            SplitWeight::from_proportion(0.001)
                .expect("legal")
                .raw_value(),
            1
        );
        assert_eq!(
            SplitWeight::from_proportion(0.051)
                .expect("legal")
                .raw_value(),
            6
        );
    }

    #[test]
    fn illegal_weights_are_rejected() {
        assert!(SplitWeight::try_from_raw(0).is_err());
        assert!(SplitWeight::from_proportion(0.0).is_err());
        assert!(SplitWeight::from_proportion(-1.0).is_err());
        assert!(SplitWeight::from_proportion(f64::NAN).is_err());
        assert!(SplitWeight::from_proportion(f64::INFINITY).is_err());
    }

    #[test]
    fn host_addresses_are_bounded() {
        assert!(HostAddress::try_new("", 1).is_err());
        assert!(HostAddress::try_new("h", 9000).is_ok());
    }
}
