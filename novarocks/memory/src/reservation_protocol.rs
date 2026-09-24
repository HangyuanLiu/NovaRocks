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

//! The production leaf's atomic F/L operations, also included verbatim by
//! the protocol-only benchmark. Parent admission and account projection are
//! measured separately by the full Reservation candidate.

#[cfg(all(test, loom))]
use loom::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(all(test, loom)))]
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) fn take_free(free: &AtomicU64, amount: u64) -> bool {
    let mut current = free.load(Ordering::Acquire);
    loop {
        if current < amount {
            return false;
        }
        match free.compare_exchange_weak(
            current,
            current - amount,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return true,
            Err(observed) => current = observed,
        }
    }
}

pub(crate) fn add_live(live: &AtomicU64, amount: u64) -> u64 {
    live.fetch_add(amount, Ordering::AcqRel) + amount
}

pub(crate) fn release_live(live: &AtomicU64, free: &AtomicU64, amount: u64) -> u64 {
    live.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        current.checked_sub(amount)
    })
    .expect("reservation shrink exceeds retained bytes");
    free.fetch_add(amount, Ordering::AcqRel) + amount
}
