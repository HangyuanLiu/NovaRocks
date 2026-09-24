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

//! Compiled into the memory crate's unit-test target under `cfg(loom)` so
//! the actual Account and Reservation atomics and the leaf slow mutex use
//! loom's synchronization types from the crate's dev-only dependency.

use crate::Reservation;
use crate::account::TopUpPolicy;
use crate::authority::{AuthorityConfig, MemoryAuthority};
use crate::ids::{AccountKind, ExternalRef};

fn setup() -> Reservation {
    let mut config = AuthorityConfig::new(8, 4, 4);
    config.top_up = TopUpPolicy::uniform(1);
    let authority = MemoryAuthority::new(config).unwrap();
    let sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    Reservation::new(&sponsor, ExternalRef::from_u128(2)).unwrap()
}

#[test]
fn grow_and_close_have_one_winner_for_idle_capacity() {
    loom::model(|| {
        let leaf = setup();
        leaf.try_grow(1).unwrap();
        let contender = leaf.clone();
        let grow = loom::thread::spawn(move || contender.try_grow(1).is_ok());
        let closer = leaf.clone();
        let close = loom::thread::spawn(move || {
            closer.close();
        });
        let grew = grow.join().unwrap();
        close.join().unwrap();
        let snapshot = leaf.snapshot();
        assert!(snapshot.live_bytes <= snapshot.committed_bytes);
        assert_eq!(snapshot.live_bytes, 1 + u64::from(grew));
        assert!(snapshot.closed);
        if grew {
            leaf.shrink(1);
        }
        leaf.shrink(1);
        assert_eq!(leaf.snapshot().committed_bytes, 0);
    });
}

#[test]
fn shrink_and_growth_preserve_live_within_committed_capacity() {
    loom::model(|| {
        let leaf = setup();
        leaf.try_grow(1).unwrap();
        let releasing = leaf.clone();
        let shrink = loom::thread::spawn(move || releasing.shrink(1));
        let growing = leaf.clone();
        let grow = loom::thread::spawn(move || growing.try_grow(1).is_ok());
        shrink.join().unwrap();
        let grew = grow.join().unwrap();
        let snapshot = leaf.snapshot();
        assert!(snapshot.live_bytes <= snapshot.committed_bytes);
        assert_eq!(snapshot.live_bytes, u64::from(grew));
        if grew {
            leaf.shrink(1);
        }
        leaf.trim();
        assert_eq!(leaf.snapshot().committed_bytes, 0);
    });
}

#[test]
fn concurrent_snapshot_never_reports_more_live_than_commitment() {
    loom::model(|| {
        let leaf = setup();
        let growing = leaf.clone();
        let grow = loom::thread::spawn(move || {
            growing.try_grow(1).unwrap();
            growing.shrink(1);
        });
        let observing = leaf.clone();
        let snapshot = loom::thread::spawn(move || {
            let snapshot = observing.snapshot();
            assert!(snapshot.live_bytes <= snapshot.committed_bytes);
            assert_eq!(
                snapshot.free_bytes + snapshot.live_bytes,
                snapshot.committed_bytes
            );
        });
        grow.join().unwrap();
        snapshot.join().unwrap();
        leaf.trim();
    });
}
