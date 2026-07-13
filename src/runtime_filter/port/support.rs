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
use std::sync::Arc;
use std::time::Instant;

pub(crate) trait RuntimeFilterClock: Send + Sync {
    fn now(&self) -> Instant;
}

pub(crate) trait RuntimeFilterMemoryAccount: Send + Sync {
    fn consume(&self, bytes: usize);
    fn release(&self, bytes: usize);
}

struct MemoryLease {
    account: Arc<dyn RuntimeFilterMemoryAccount>,
    bytes: usize,
}

impl MemoryLease {
    fn new(account: Arc<dyn RuntimeFilterMemoryAccount>, bytes: usize) -> Self {
        if bytes != 0 {
            account.consume(bytes);
        }
        Self { account, bytes }
    }
}

impl Drop for MemoryLease {
    fn drop(&mut self) {
        if self.bytes != 0 {
            self.account.release(self.bytes);
        }
    }
}

pub(crate) struct TemporaryContributionLease(MemoryLease);

impl TemporaryContributionLease {
    pub(crate) fn new(account: Arc<dyn RuntimeFilterMemoryAccount>, bytes: usize) -> Self {
        Self(MemoryLease::new(account, bytes))
    }

    pub(crate) const fn bytes(&self) -> usize {
        self.0.bytes
    }
}

impl fmt::Debug for TemporaryContributionLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TemporaryContributionLease")
            .field("bytes", &self.bytes())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetainedReservationError {
    SizeOverflow,
    AccountMismatch,
}

impl fmt::Display for RetainedReservationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SizeOverflow => write!(formatter, "retained reservation size overflow"),
            Self::AccountMismatch => write!(formatter, "retained reservation account mismatch"),
        }
    }
}

impl std::error::Error for RetainedReservationError {}

#[derive(Default)]
pub(crate) struct RetainedMemoryReservation {
    account: Option<Arc<dyn RuntimeFilterMemoryAccount>>,
    bytes: usize,
}

impl RetainedMemoryReservation {
    pub(crate) const fn empty() -> Self {
        Self {
            account: None,
            bytes: 0,
        }
    }

    pub(crate) fn new(account: Arc<dyn RuntimeFilterMemoryAccount>, bytes: usize) -> Self {
        if bytes == 0 {
            return Self::empty();
        }
        account.consume(bytes);
        Self {
            account: Some(account),
            bytes,
        }
    }

    pub(crate) fn absorb(&mut self, mut incoming: Self) -> Result<(), RetainedReservationError> {
        if incoming.bytes == 0 {
            return Ok(());
        }
        if self.bytes == 0 {
            self.account = incoming.account.take();
            self.bytes = incoming.bytes;
            incoming.bytes = 0;
            return Ok(());
        }

        let account = self
            .account
            .as_ref()
            .expect("non-empty reservation account");
        let incoming_account = incoming
            .account
            .as_ref()
            .expect("non-empty incoming reservation account");
        if !Arc::ptr_eq(account, incoming_account) {
            return Err(RetainedReservationError::AccountMismatch);
        }
        let bytes = self
            .bytes
            .checked_add(incoming.bytes)
            .ok_or(RetainedReservationError::SizeOverflow)?;

        self.bytes = bytes;
        incoming.bytes = 0;
        incoming.account = None;
        Ok(())
    }

    pub(crate) const fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for RetainedMemoryReservation {
    fn drop(&mut self) {
        if self.bytes != 0 {
            self.account
                .as_ref()
                .expect("non-empty reservation account")
                .release(self.bytes);
        }
    }
}

impl fmt::Debug for RetainedMemoryReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedMemoryReservation")
            .field("bytes", &self.bytes)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::runtime_filter::model::contract::ChannelId;
    use crate::runtime_filter::port::value_domain::{
        LogicalSnapshot, MembershipValues, ReducedMembershipDomain,
    };

    #[derive(Default)]
    struct CountingMemoryAccount(AtomicUsize);

    impl RuntimeFilterMemoryAccount for CountingMemoryAccount {
        fn consume(&self, bytes: usize) {
            self.0.fetch_add(bytes, Ordering::SeqCst);
        }

        fn release(&self, bytes: usize) {
            self.0.fetch_sub(bytes, Ordering::SeqCst);
        }
    }

    #[test]
    fn temporary_and_retained_reservations_have_distinct_raii_ownership() {
        let account = Arc::new(CountingMemoryAccount::default());

        {
            let _temporary = TemporaryContributionLease::new(account.clone(), 11);
            assert_eq!(account.0.load(Ordering::SeqCst), 11);
        }
        assert_eq!(account.0.load(Ordering::SeqCst), 0);

        let mut retained = RetainedMemoryReservation::empty();
        retained
            .absorb(RetainedMemoryReservation::new(account.clone(), 13))
            .unwrap();
        retained
            .absorb(RetainedMemoryReservation::new(account.clone(), 17))
            .unwrap();
        assert_eq!(retained.bytes(), 30);
        assert_eq!(account.0.load(Ordering::SeqCst), 30);
        drop(retained);
        assert_eq!(account.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn logical_snapshot_arc_keeps_retained_bytes_until_the_last_owner_drops() {
        let account = Arc::new(CountingMemoryAccount::default());
        let snapshot = Arc::new(LogicalSnapshot::first(
            ChannelId::new(1),
            ReducedMembershipDomain::new(MembershipValues::int64([7]), false),
            RetainedMemoryReservation::new(account.clone(), 23),
        ));
        let last_owner = snapshot.clone();

        assert_eq!(account.0.load(Ordering::SeqCst), 23);
        drop(snapshot);
        assert_eq!(account.0.load(Ordering::SeqCst), 23);
        drop(last_owner);
        assert_eq!(account.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn retained_zero_reservations_are_empty_and_do_not_grow_an_account() {
        let account = Arc::new(CountingMemoryAccount::default());
        let mut retained = RetainedMemoryReservation::new(account.clone(), 0);

        retained
            .absorb(RetainedMemoryReservation::new(account.clone(), 0))
            .unwrap();

        assert_eq!(retained.bytes(), 0);
        assert_eq!(account.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn retained_absorb_overflow_keeps_self_and_releases_incoming() {
        let account = Arc::new(CountingMemoryAccount::default());
        let mut retained = RetainedMemoryReservation::new(account.clone(), usize::MAX);

        assert_eq!(
            retained.absorb(RetainedMemoryReservation::new(account.clone(), 1)),
            Err(RetainedReservationError::SizeOverflow)
        );
        assert_eq!(retained.bytes(), usize::MAX);
        assert_eq!(account.0.load(Ordering::SeqCst), usize::MAX);
        drop(retained);
        assert_eq!(account.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn retained_absorb_rejects_account_mismatch_without_leaking_incoming() {
        let left_account = Arc::new(CountingMemoryAccount::default());
        let right_account = Arc::new(CountingMemoryAccount::default());
        let mut retained = RetainedMemoryReservation::new(left_account.clone(), 11);

        assert_eq!(
            retained.absorb(RetainedMemoryReservation::new(right_account.clone(), 13)),
            Err(RetainedReservationError::AccountMismatch)
        );
        assert_eq!(retained.bytes(), 11);
        assert_eq!(left_account.0.load(Ordering::SeqCst), 11);
        assert_eq!(right_account.0.load(Ordering::SeqCst), 0);
        drop(retained);
        assert_eq!(left_account.0.load(Ordering::SeqCst), 0);
    }
}
