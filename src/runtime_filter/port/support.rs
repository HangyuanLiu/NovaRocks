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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MemoryAccountError {
    CapacityExceeded,
}

pub(crate) trait RuntimeFilterMemoryAccount: Send + Sync {
    fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError>;
    fn release(&self, bytes: usize);
}

struct MemoryLease {
    account: Arc<dyn RuntimeFilterMemoryAccount>,
    bytes: usize,
}

impl MemoryLease {
    fn try_new(
        account: Arc<dyn RuntimeFilterMemoryAccount>,
        bytes: usize,
    ) -> Result<Self, MemoryAccountError> {
        if bytes != 0 {
            account.try_consume(bytes)?;
        }
        Ok(Self { account, bytes })
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
    pub(crate) fn try_new(
        account: Arc<dyn RuntimeFilterMemoryAccount>,
        bytes: usize,
    ) -> Result<Self, MemoryAccountError> {
        MemoryLease::try_new(account, bytes).map(Self)
    }

    #[cfg(test)]
    pub(crate) fn new(account: Arc<dyn RuntimeFilterMemoryAccount>, bytes: usize) -> Self {
        Self::try_new(account, bytes).expect("test memory account accepts reservation")
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
    CapacityExceeded,
}

impl fmt::Display for RetainedReservationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SizeOverflow => write!(formatter, "retained reservation size overflow"),
            Self::AccountMismatch => write!(formatter, "retained reservation account mismatch"),
            Self::CapacityExceeded => {
                write!(formatter, "retained reservation account rejected bytes")
            }
        }
    }
}

impl std::error::Error for RetainedReservationError {}

#[derive(Default)]
pub(crate) struct RetainedMemoryReservation {
    account: Option<Arc<dyn RuntimeFilterMemoryAccount>>,
    bytes: usize,
}

pub(crate) struct RetainedReservationAbsorbFailure {
    error: RetainedReservationError,
    incoming: RetainedMemoryReservation,
}

impl RetainedReservationAbsorbFailure {
    #[cfg(test)]
    pub(crate) const fn error(&self) -> RetainedReservationError {
        self.error
    }

    pub(crate) fn into_parts(self) -> (RetainedReservationError, RetainedMemoryReservation) {
        (self.error, self.incoming)
    }
}

impl fmt::Debug for RetainedReservationAbsorbFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedReservationAbsorbFailure")
            .field("error", &self.error)
            .field("incoming_bytes", &self.incoming.bytes())
            .finish()
    }
}

impl RetainedMemoryReservation {
    pub(crate) const fn empty() -> Self {
        Self {
            account: None,
            bytes: 0,
        }
    }

    pub(crate) fn try_new(
        account: Arc<dyn RuntimeFilterMemoryAccount>,
        bytes: usize,
    ) -> Result<Self, RetainedReservationError> {
        if bytes == 0 {
            return Ok(Self::empty());
        }
        account
            .try_consume(bytes)
            .map_err(|_| RetainedReservationError::CapacityExceeded)?;
        Ok(Self {
            account: Some(account),
            bytes,
        })
    }

    #[cfg(test)]
    pub(crate) fn new(account: Arc<dyn RuntimeFilterMemoryAccount>, bytes: usize) -> Self {
        Self::try_new(account, bytes).expect("test memory account accepts reservation")
    }

    pub(crate) fn absorb(
        &mut self,
        mut incoming: Self,
    ) -> Result<(), RetainedReservationAbsorbFailure> {
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
            return Err(RetainedReservationAbsorbFailure {
                error: RetainedReservationError::AccountMismatch,
                incoming,
            });
        }
        let Some(bytes) = self.bytes.checked_add(incoming.bytes) else {
            return Err(RetainedReservationAbsorbFailure {
                error: RetainedReservationError::SizeOverflow,
                incoming,
            });
        };

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
        fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
            self.0.fetch_add(bytes, Ordering::SeqCst);
            Ok(())
        }

        fn release(&self, bytes: usize) {
            self.0.fetch_sub(bytes, Ordering::SeqCst);
        }
    }

    struct RejectingMemoryAccount;

    impl RuntimeFilterMemoryAccount for RejectingMemoryAccount {
        fn try_consume(&self, _bytes: usize) -> Result<(), MemoryAccountError> {
            Err(MemoryAccountError::CapacityExceeded)
        }

        fn release(&self, _bytes: usize) {}
    }

    #[test]
    fn temporary_and_retained_reservations_propagate_account_rejection() {
        let account = Arc::new(RejectingMemoryAccount);
        assert!(TemporaryContributionLease::try_new(account.clone(), 1).is_err());
        assert!(RetainedMemoryReservation::try_new(account, 1).is_err());
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

        let failure = retained
            .absorb(RetainedMemoryReservation::new(account.clone(), 1))
            .unwrap_err();
        assert_eq!(failure.error(), RetainedReservationError::SizeOverflow);
        drop(failure);
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

        let failure = retained
            .absorb(RetainedMemoryReservation::new(right_account.clone(), 13))
            .unwrap_err();
        assert_eq!(failure.error(), RetainedReservationError::AccountMismatch);
        drop(failure);
        assert_eq!(retained.bytes(), 11);
        assert_eq!(left_account.0.load(Ordering::SeqCst), 11);
        assert_eq!(right_account.0.load(Ordering::SeqCst), 0);
        drop(retained);
        assert_eq!(left_account.0.load(Ordering::SeqCst), 0);
    }
}
