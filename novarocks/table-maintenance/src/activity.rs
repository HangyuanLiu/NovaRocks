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

use std::collections::HashSet;
use std::hash::Hash;
use std::sync::{Arc, Mutex, Weak};

/// Process-local mutual exclusion for one application-maintenance target.
/// The generic key makes the business owner independent of a SQL/catalog
/// naming representation.
#[derive(Clone)]
pub struct TargetActivity<K> {
    active: Arc<Mutex<HashSet<K>>>,
}

impl<K> Default for TargetActivity<K> {
    fn default() -> Self {
        Self {
            active: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}

impl<K> TargetActivity<K>
where
    K: Clone + Eq + Hash,
{
    pub fn acquire(&self, target: K) -> Result<TargetActivityPermit<K>, TargetBusy> {
        let mut active = self.active.lock().map_err(|_| TargetBusy::Poisoned)?;
        if !active.insert(target.clone()) {
            return Err(TargetBusy::Busy);
        }
        Ok(TargetActivityPermit {
            _lease: Arc::new(ActivityLease {
                target,
                owner: Arc::downgrade(&self.active),
            }),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TargetBusy {
    Busy,
    Poisoned,
}

#[derive(Clone)]
pub struct TargetActivityPermit<K: Eq + Hash> {
    _lease: Arc<ActivityLease<K>>,
}

struct ActivityLease<K: Eq + Hash> {
    target: K,
    owner: Weak<Mutex<HashSet<K>>>,
}

impl<K> Drop for ActivityLease<K>
where
    K: Eq + Hash,
{
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade()
            && let Ok(mut active) = owner.lock()
        {
            active.remove(&self.target);
        }
    }
}

impl<K: Eq + Hash> std::fmt::Debug for TargetActivityPermit<K> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TargetActivityPermit")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::{TargetActivity, TargetBusy};

    #[test]
    fn one_target_has_one_owner_until_every_handle_drops() {
        let activity = TargetActivity::default();
        let first = activity.acquire("table-a").expect("first owner");
        assert!(matches!(activity.acquire("table-a"), Err(TargetBusy::Busy)));
        let child = first.clone();
        drop(first);
        assert!(matches!(activity.acquire("table-a"), Err(TargetBusy::Busy)));
        drop(child);
        activity.acquire("table-a").expect("released owner");
    }
}
