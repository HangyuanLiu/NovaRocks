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

//! Current-process MV target readiness and publication ownership.

use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TargetReadiness {
    Ready,
    Unavailable(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeAttempt<P> {
    pub publication_id: P,
}

pub struct ProcessRuntime<T, P> {
    inner: Mutex<BTreeMap<T, RuntimeEntry<P>>>,
}

impl<T, P> Default for ProcessRuntime<T, P> {
    fn default() -> Self {
        Self {
            inner: Mutex::new(BTreeMap::new()),
        }
    }
}

#[derive(Clone, Debug)]
struct RuntimeEntry<P> {
    readiness: TargetReadiness,
    active: Option<RuntimeAttempt<P>>,
}

impl<P> Default for RuntimeEntry<P> {
    fn default() -> Self {
        Self {
            readiness: TargetReadiness::Ready,
            active: None,
        }
    }
}

impl<T, P> ProcessRuntime<T, P>
where
    T: Clone + Ord,
    P: Copy + Eq,
{
    pub fn readiness(&self, target: &T) -> TargetReadiness {
        self.inner
            .lock()
            .expect("MV application runtime lock poisoned")
            .get(target)
            .map(|entry| entry.readiness.clone())
            .unwrap_or(TargetReadiness::Ready)
    }

    pub fn set_unavailable(&self, target: T, reason: String) {
        self.inner
            .lock()
            .expect("MV application runtime lock poisoned")
            .entry(target)
            .or_default()
            .readiness = TargetReadiness::Unavailable(reason);
    }

    pub fn set_ready(&self, target: T) {
        self.inner
            .lock()
            .expect("MV application runtime lock poisoned")
            .entry(target)
            .or_default()
            .readiness = TargetReadiness::Ready;
    }

    pub fn begin(&self, target: T, publication_id: P) -> bool {
        let mut entries = self
            .inner
            .lock()
            .expect("MV application runtime lock poisoned");
        let entry = entries.entry(target).or_default();
        if entry.active.is_some() {
            return false;
        }
        entry.active = Some(RuntimeAttempt { publication_id });
        true
    }

    pub fn finish(&self, target: &T, publication_id: P) {
        let mut entries = self
            .inner
            .lock()
            .expect("MV application runtime lock poisoned");
        if let Some(entry) = entries.get_mut(target)
            && entry
                .active
                .as_ref()
                .is_some_and(|active| active.publication_id == publication_id)
        {
            entry.active = None;
        }
    }

    pub fn has_active_publications(&self) -> bool {
        self.inner
            .lock()
            .expect("MV application runtime lock poisoned")
            .values()
            .any(|entry| entry.active.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::{ProcessRuntime, TargetReadiness};

    #[test]
    fn runtime_tracks_one_publication_and_readiness_per_target() {
        let runtime = ProcessRuntime::<String, u64>::default();
        let target = "ice.analytics.orders_mv".to_string();
        assert!(runtime.begin(target.clone(), 7));
        assert!(!runtime.begin(target.clone(), 8));
        runtime.set_unavailable(target.clone(), "accelerator projection failed".into());
        assert!(matches!(
            runtime.readiness(&target),
            TargetReadiness::Unavailable(_)
        ));
        runtime.finish(&target, 7);
        assert!(!runtime.has_active_publications());
        runtime.set_ready(target.clone());
        assert_eq!(runtime.readiness(&target), TargetReadiness::Ready);
    }
}
