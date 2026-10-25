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

//! Caller-owned retention and an explicit port to its original byte budget.
//! Plain retention cannot authorize new allocation or mint a byte receipt.

use super::{ConnectorError, ConnectorErrorKind};
use novarocks_resource_contract::{ExistingOwnerRetentionLease, RetentionPinError};
use std::fmt;
use std::sync::Arc;

/// The original allocation author reserves before creating new backing. The
/// returned guard must keep that SAME receipt until its backing really exits.
/// It is not a new quota, a source-identity test, or a Provider authority.
pub trait ConnectorPayloadByteBudget: Send + Sync {
    fn reserve_bytes(&self, bytes: usize)
    -> Result<ConnectorPayloadRetentionGuard, ConnectorError>;
}

enum Retention {
    Shared(Arc<dyn Send + Sync>),
    Existing(ExistingOwnerRetentionLease),
    Budgeted(Arc<dyn ConnectorPayloadByteBudget>),
}

pub struct ConnectorPayloadRetentionGuard {
    holder: Retention,
}
impl ConnectorPayloadRetentionGuard {
    /// Bind only the caller's original byte author. Cloning this capability
    /// reserves no bytes; each allocation must request its own real receipt.
    pub fn from_byte_budget<T: ConnectorPayloadByteBudget + 'static>(budget: Arc<T>) -> Self {
        Self {
            holder: Retention::Budgeted(budget),
        }
    }
    pub(crate) fn is_byte_budget(&self) -> bool {
        matches!(&self.holder, Retention::Budgeted(_))
    }
    pub fn reserve_owned_bytes(&self, bytes: usize) -> Result<Self, ConnectorError> {
        match &self.holder {
            Retention::Budgeted(budget) => budget.reserve_bytes(bytes),
            Retention::Shared(_) | Retention::Existing(_) => Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "payload retention has no byte-budget authority",
            )),
        }
    }
    /// Retain an existing shared holder with its original owner semantics.
    pub fn from_shared<T: Send + Sync + 'static>(holder: Arc<T>) -> Self {
        Self {
            holder: Retention::Shared(holder),
        }
    }
    pub fn new<T: Send + Sync + 'static>(holder: T) -> Self {
        Self {
            holder: Retention::Shared(Arc::new(holder)),
        }
    }
    /// Move an original owner's existing pin; no new Arc allocation or pin.
    pub fn from_existing_lease(lease: ExistingOwnerRetentionLease) -> Self {
        Self {
            holder: Retention::Existing(lease),
        }
    }
    pub fn try_clone(&self) -> Result<Self, RetentionPinError> {
        let holder = match &self.holder {
            Retention::Shared(holder) => Retention::Shared(Arc::clone(holder)),
            Retention::Existing(lease) => Retention::Existing(lease.try_clone()?),
            Retention::Budgeted(budget) => Retention::Budgeted(Arc::clone(budget)),
        };
        Ok(Self { holder })
    }
}
// Transitional source-compatibility only. New Internal consumers must use the
// typed try_clone path. Checked failure changes no original count or source;
// this old infallible surface fails fast rather than fabricating another pin.
impl Clone for ConnectorPayloadRetentionGuard {
    fn clone(&self) -> Self {
        self.try_clone()
            .expect("original retention pin clone failed")
    }
}
// Shared holders remain opaque as before. The new existing-owner protocol
// separately requires RefUnwindSafe and publishes atomic pin transitions.
impl std::panic::RefUnwindSafe for ConnectorPayloadRetentionGuard {}
impl fmt::Debug for ConnectorPayloadRetentionGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ConnectorPayloadRetentionGuard")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_resource_contract::RetentionOwner;
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        sync::Mutex,
    };
    struct Owner {
        pins: Mutex<usize>,
        maximum: usize,
    }
    impl RetentionOwner for Owner {
        fn try_retain(&self, key: [u64; 2]) -> Result<(), RetentionPinError> {
            if key != [7, 3] {
                return Err(RetentionPinError::Conflict);
            }
            let mut pins = self.pins.lock().unwrap();
            if *pins >= self.maximum {
                return Err(RetentionPinError::Overflow);
            }
            *pins = pins.checked_add(1).ok_or(RetentionPinError::Overflow)?;
            Ok(())
        }
        fn release_pin(&self, key: [u64; 2]) {
            assert_eq!(key, [7, 3]);
            let mut pins = self.pins.lock().unwrap();
            *pins = pins.checked_sub(1).unwrap();
        }
        fn capacity_bytes(&self, key: [u64; 2]) -> Result<u64, RetentionPinError> {
            if key == [7, 3] {
                Ok(8)
            } else {
                Err(RetentionPinError::Conflict)
            }
        }
    }
    #[test]
    fn existing_variant_clones_the_same_owner_and_pins_without_an_adapter_arc() {
        let owner = Arc::new(Owner {
            pins: Mutex::new(1),
            maximum: 3,
        });
        let lease = ExistingOwnerRetentionLease::from_existing_pin(owner.clone(), [7, 3]);
        let guard = ConnectorPayloadRetentionGuard::from_existing_lease(lease);
        assert_eq!(Arc::strong_count(&owner), 2);
        let copy = guard.try_clone().unwrap();
        assert_eq!(Arc::strong_count(&owner), 3);
        assert_eq!(*owner.pins.lock().unwrap(), 2);
        drop(guard);
        assert_eq!(*owner.pins.lock().unwrap(), 1);
        drop(copy);
        assert_eq!(*owner.pins.lock().unwrap(), 0);
        assert_eq!(Arc::strong_count(&owner), 1);
    }
    #[test]
    fn typed_failure_and_old_clone_fail_fast_preserve_the_original_pin() {
        let owner = Arc::new(Owner {
            pins: Mutex::new(1),
            maximum: 1,
        });
        let guard = ConnectorPayloadRetentionGuard::from_existing_lease(
            ExistingOwnerRetentionLease::from_existing_pin(owner.clone(), [7, 3]),
        );
        assert!(matches!(
            guard.try_clone(),
            Err(RetentionPinError::Overflow)
        ));
        assert_eq!(*owner.pins.lock().unwrap(), 1);
        assert!(catch_unwind(AssertUnwindSafe(|| guard.clone())).is_err());
        assert_eq!(*owner.pins.lock().unwrap(), 1);
        assert_eq!(Arc::strong_count(&owner), 2);
        drop(guard);
        assert_eq!(*owner.pins.lock().unwrap(), 0);
    }
}
