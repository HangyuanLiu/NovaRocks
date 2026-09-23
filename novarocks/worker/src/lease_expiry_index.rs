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

//! Private, exact index of leases that can still expire in the registry.

use std::collections::BTreeSet;

use novarocks_execution_contract::task_execution::identity::QueryContextRef;

use crate::{InstalledLease, MonotonicInstant};

#[derive(Default)]
pub(super) struct LeaseExpiryIndex {
    by_deadline: BTreeSet<(MonotonicInstant, QueryContextRef)>,
}

impl LeaseExpiryIndex {
    pub(super) fn insert(&mut self, context: QueryContextRef, lease: InstalledLease) {
        let inserted = self.by_deadline.insert((lease.expires_at(), context));
        assert!(inserted, "one context has one indexed lease");
    }

    pub(super) fn replace(
        &mut self,
        context: QueryContextRef,
        previous: InstalledLease,
        renewed: InstalledLease,
    ) {
        let removed = self.by_deadline.remove(&(previous.expires_at(), context));
        assert!(removed, "renewed context must have an indexed lease");
        self.insert(context, renewed);
    }

    pub(super) fn remove(&mut self, context: QueryContextRef, lease: InstalledLease) -> bool {
        self.by_deadline.remove(&(lease.expires_at(), context))
    }

    pub(super) fn take_due(
        &mut self,
        now: MonotonicInstant,
    ) -> Option<(MonotonicInstant, QueryContextRef)> {
        let candidate = self.by_deadline.first().copied()?;
        if now < candidate.0 {
            return None;
        }
        self.by_deadline.pop_first()
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.by_deadline.len()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use novarocks_execution_contract::LeaseSequence;
    use novarocks_execution_contract::task_execution::identity::QueryContextRef;
    use novarocks_execution_contract::task_execution::lease::LeaseValidFor;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
    };

    use super::LeaseExpiryIndex;
    use crate::{InstalledLease, LeaseBounds, MonotonicInstant};

    fn at(seconds: u64) -> MonotonicInstant {
        MonotonicInstant::from_origin(Duration::from_secs(seconds))
    }

    fn context(number: i64) -> QueryContextRef {
        QueryContextRef::new(
            QueryExecutionId::new(
                QueryId::new(number, 1),
                AttemptId::new(1).expect("nonzero attempt"),
            )
            .expect("nonzero query"),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        )
    }

    fn lease(now: MonotonicInstant, seconds: u64) -> InstalledLease {
        InstalledLease::install_initial(
            LeaseValidFor::new(Duration::from_secs(seconds)).expect("valid lease"),
            LeaseBounds::DEFAULT,
            now,
        )
    }

    #[test]
    fn same_deadline_drains_every_context_without_visiting_later_entries() {
        let contexts = [context(1), context(2), context(3)];
        let mut index = LeaseExpiryIndex::default();
        index.insert(contexts[0], lease(at(0), 10));
        index.insert(contexts[1], lease(at(0), 10));
        index.insert(contexts[2], lease(at(0), 20));

        assert!(index.take_due(at(9)).is_none());
        let due: BTreeSet<_> = (0..2)
            .map(|_| index.take_due(at(10)).expect("same-time lease").1)
            .collect();
        assert_eq!(due, BTreeSet::from([contexts[0], contexts[1]]));
        assert!(index.take_due(at(10)).is_none());
        assert_eq!(index.len(), 1);
        assert_eq!(
            index.take_due(at(20)).map(|(_, context)| context),
            Some(contexts[2])
        );
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn renewals_replace_the_old_deadline_without_accumulating_entries() {
        let context = context(4);
        let mut index = LeaseExpiryIndex::default();
        let valid_for = LeaseValidFor::new(Duration::from_secs(10)).expect("valid lease");
        let mut installed = lease(at(0), 10);
        index.insert(context, installed);

        for sequence in 1..=512 {
            let renewed = installed.renew(
                LeaseSequence::new(sequence),
                valid_for,
                LeaseBounds::DEFAULT,
                at(sequence),
            );
            index.replace(context, installed, renewed);
            assert_eq!(index.len(), 1);
            installed = renewed;
        }

        assert!(index.take_due(at(10)).is_none());
        assert_eq!(
            index.take_due(installed.expires_at()),
            Some((installed.expires_at(), context))
        );
        assert!(index.take_due(at(600)).is_none());
    }
}
