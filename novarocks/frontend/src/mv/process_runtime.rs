// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements.  See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.  The ASF licenses this
// file to you under the Apache License, Version 2.0.

//! Process-local MV attempt state.
//!
//! No constructor accepts StateStore data: restart intentionally forgets active
//! work and the next scheduler pass begins a fresh publication attempt.

use std::collections::BTreeMap;
use std::sync::Mutex;

use novarocks_spi::connector::LakePublicationId;

use crate::mv::activity::CanonicalMvTarget;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MvTargetReadiness {
    Ready,
    Unavailable(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MvRuntimeAttempt {
    pub publication_id: LakePublicationId,
}

#[derive(Default)]
pub(crate) struct ProcessRuntime {
    inner: Mutex<BTreeMap<CanonicalMvTarget, RuntimeEntry>>,
}

#[derive(Clone, Debug)]
struct RuntimeEntry {
    readiness: MvTargetReadiness,
    active: Option<MvRuntimeAttempt>,
}

impl Default for RuntimeEntry {
    fn default() -> Self {
        Self {
            readiness: MvTargetReadiness::Ready,
            active: None,
        }
    }
}

impl ProcessRuntime {
    pub(crate) fn readiness(&self, target: &CanonicalMvTarget) -> MvTargetReadiness {
        self.inner
            .lock()
            .expect("MV runtime lock poisoned")
            .get(target)
            .map(|entry| entry.readiness.clone())
            .unwrap_or(MvTargetReadiness::Ready)
    }

    pub(crate) fn set_unavailable(&self, target: CanonicalMvTarget, reason: String) {
        self.inner
            .lock()
            .expect("MV runtime lock poisoned")
            .entry(target)
            .or_default()
            .readiness = MvTargetReadiness::Unavailable(reason);
    }

    pub(crate) fn set_ready(&self, target: CanonicalMvTarget) {
        self.inner
            .lock()
            .expect("MV runtime lock poisoned")
            .entry(target)
            .or_default()
            .readiness = MvTargetReadiness::Ready;
    }

    pub(crate) fn begin(
        &self,
        target: CanonicalMvTarget,
        publication_id: LakePublicationId,
    ) -> bool {
        let mut entries = self.inner.lock().expect("MV runtime lock poisoned");
        let entry = entries.entry(target).or_default();
        if entry.active.is_some() {
            return false;
        }
        entry.active = Some(MvRuntimeAttempt { publication_id });
        true
    }

    pub(crate) fn finish(&self, target: &CanonicalMvTarget, publication_id: LakePublicationId) {
        let mut entries = self.inner.lock().expect("MV runtime lock poisoned");
        if let Some(entry) = entries.get_mut(target)
            && entry
                .active
                .as_ref()
                .is_some_and(|active| active.publication_id == publication_id)
        {
            entry.active = None;
        }
    }
}
