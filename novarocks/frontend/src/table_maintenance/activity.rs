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

//! Process-local mutual exclusion for table maintenance actions.
//!
//! The permit is intentionally not a distributed lease. It only proves that a
//! fresh action belongs to this frontend process, and a child rewrite can only
//! run through the exact permit its parent already owns.

use std::collections::HashSet;
use std::fmt;
use std::sync::{Arc, Mutex, Weak};

use crate::maintenance::MaintenanceTarget;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceActivityFamily {
    Optimize,
    Metadata,
    Rewrite,
    Cleanup,
}

impl fmt::Display for MaintenanceActivityFamily {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Optimize => "OPTIMIZE",
            Self::Metadata => "metadata maintenance",
            Self::Rewrite => "distributed rewrite",
            Self::Cleanup => "orphan cleanup",
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ActivityKey {
    catalog: String,
    namespace: String,
    table: String,
}

impl From<&MaintenanceTarget> for ActivityKey {
    fn from(target: &MaintenanceTarget) -> Self {
        Self {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
        }
    }
}

#[derive(Clone, Default)]
pub struct TableMaintenanceActivity {
    active: Arc<Mutex<HashSet<ActivityKey>>>,
}

impl fmt::Debug for TableMaintenanceActivity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TableMaintenanceActivity")
            .finish_non_exhaustive()
    }
}

impl TableMaintenanceActivity {
    pub fn acquire(
        &self,
        target: &MaintenanceTarget,
        family: MaintenanceActivityFamily,
    ) -> Result<MaintenanceActivityPermit, MaintenanceActivityBusy> {
        let key = ActivityKey::from(target);
        let mut active = self.active.lock().map_err(|_| MaintenanceActivityBusy {
            family,
            target: target.clone(),
            detail: "the process-local activity gate is poisoned".to_string(),
        })?;
        if !active.insert(key.clone()) {
            return Err(MaintenanceActivityBusy {
                family,
                target: target.clone(),
                detail: "another maintenance action is already active for this table in this frontend process"
                    .to_string(),
            });
        }
        Ok(MaintenanceActivityPermit {
            _lease: Arc::new(ActivityLease {
                key,
                owner: Arc::downgrade(&self.active),
            }),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaintenanceActivityBusy {
    family: MaintenanceActivityFamily,
    target: MaintenanceTarget,
    detail: String,
}

impl fmt::Display for MaintenanceActivityBusy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} is busy for {}.{}.{}: {}",
            self.family, self.target.catalog, self.target.namespace, self.target.table, self.detail
        )
    }
}

impl std::error::Error for MaintenanceActivityBusy {}

/// A drop-released proof that one exact process owns a table action.
///
/// It is deliberately non-constructible outside this module. A child action
/// receives a clone of the parent proof rather than re-acquiring its target.
#[derive(Clone)]
pub struct MaintenanceActivityPermit {
    _lease: Arc<ActivityLease>,
}

struct ActivityLease {
    key: ActivityKey,
    owner: Weak<Mutex<HashSet<ActivityKey>>>,
}

impl fmt::Debug for MaintenanceActivityPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("MaintenanceActivityPermit").finish()
    }
}

impl Drop for ActivityLease {
    fn drop(&mut self) {
        // This runs only after the final parent/child proof releases the
        // shared lease. A poisoned mutex means shutdown is already
        // inconsistent; never panic from Drop.
        if let Some(owner) = self.owner.upgrade() {
            if let Ok(mut active) = owner.lock() {
                active.remove(&self.key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MaintenanceActivityFamily, TableMaintenanceActivity};
    use crate::maintenance::MaintenanceTarget;

    fn target(table: &str) -> MaintenanceTarget {
        MaintenanceTarget {
            catalog: "iceberg".into(),
            namespace: "db".into(),
            table: table.into(),
        }
    }

    #[test]
    fn one_table_has_one_permit_but_distinct_tables_do_not_block_each_other() {
        let activity = TableMaintenanceActivity::default();
        let first = activity
            .acquire(&target("one"), MaintenanceActivityFamily::Optimize)
            .unwrap();
        assert!(
            activity
                .acquire(&target("one"), MaintenanceActivityFamily::Cleanup)
                .is_err()
        );
        let other = activity
            .acquire(&target("two"), MaintenanceActivityFamily::Cleanup)
            .unwrap();
        drop(first);
        drop(other);
        activity
            .acquire(&target("one"), MaintenanceActivityFamily::Metadata)
            .unwrap();
    }

    #[test]
    fn child_clone_releases_only_after_the_last_owner() {
        let activity = TableMaintenanceActivity::default();
        let parent = activity
            .acquire(&target("one"), MaintenanceActivityFamily::Optimize)
            .unwrap();
        let child = parent.clone();
        drop(parent);
        assert!(
            activity
                .acquire(&target("one"), MaintenanceActivityFamily::Rewrite)
                .is_err()
        );
        drop(child);
        activity
            .acquire(&target("one"), MaintenanceActivityFamily::Rewrite)
            .unwrap();
    }
}
