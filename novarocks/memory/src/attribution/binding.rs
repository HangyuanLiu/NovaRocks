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

//! Unsafe assembly boundary used by safe scopes and explicit owner helpers.
use super::tls;
use crate::lane::RecordRef;
/// Installs an already-held record from the global store, returning the outer binding.
/// # Safety
/// Caller retains that record's owner until restoring the returned binding,
/// including on unwind. The binding must stay on this thread and within one
/// synchronous step. An owned/model store reference must never be installed.
#[doc(hidden)]
pub unsafe fn install_ambient(reference: RecordRef) -> RecordRef {
    tls::replace(reference, false, false)
}
/// # Safety
/// Restore the exact binding returned by install_ambient, on the same thread,
/// before releasing the installed owner. Outer owners remain held too.
#[doc(hidden)]
pub unsafe fn restore_ambient(previous: RecordRef) {
    tls::replace(previous, false, true);
}
/// # Safety
/// Same lifetime/thread/global-store requirements as install_ambient. Explicit
/// owner installation takes precedence over the environment for new tokens.
#[doc(hidden)]
pub unsafe fn install_explicit(reference: RecordRef) -> RecordRef {
    tls::replace(reference, true, false)
}
/// # Safety
/// Restore the exact binding returned by install_explicit before its owner drops.
#[doc(hidden)]
pub unsafe fn restore_explicit(previous: RecordRef) {
    tls::replace(previous, true, true);
}
/// Flushes this thread's sole slot; safe because the slot owns its lifetime pin.
#[doc(hidden)]
pub fn flush_current() {
    tls::flush();
}
#[doc(hidden)]
pub fn pending_bytes() -> u64 {
    tls::pending_bytes()
}
/// Publishes explicit small-allocation facts, never authorizes allocation.
/// # Safety
/// Positive counts require a global-store owner retained through publication;
/// negative counts require genuine distinct outstanding allocations, not just
/// identities. Zero-count resizes retain their allocation lifetime. Deltas
/// describe facts exactly once and fit the signed 40-bit allocation count.
#[doc(hidden)]
pub unsafe fn publish_small(reference: RecordRef, bytes: i64, count: i64) {
    unsafe { tls::add(reference, 0, bytes, count) };
}

/// # Safety
/// Same held global-store owner/restoration contract as install_ambient.
pub(crate) unsafe fn try_install_ambient(reference: RecordRef) -> Option<RecordRef> {
    tls::try_replace(reference, false, false)
}
/// # Safety
/// Same publication lifetime contract as publish_small; only for the positive
/// small side of a tagged-to-small shrink, which creates no physical growth.
pub(crate) unsafe fn publish_small_transfer(reference: RecordRef, bytes: i64, count: i64) {
    unsafe { tls::add_transfer(reference, 0, bytes, count) };
}

/// # Safety
/// Same owner/restoration contract as install_explicit.
pub(crate) unsafe fn try_install_explicit(reference: RecordRef) -> Option<RecordRef> {
    tls::try_replace(reference, true, false)
}
