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

//! Each poll is one complete synchronous attribution step, including unwind.
use crate::lane::LaneHandle;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

pub struct AttributedFuture<F> {
    lane: LaneHandle,
    future: F,
}
/// Retains the same lane across polls and executor thread migration. Each poll
/// restores and flushes its thread's outer binding before returning Pending or
/// Ready. Dropping this wrapper holds no installed binding or pending slot.
pub fn attributed<F: Future>(lane: LaneHandle, future: F) -> AttributedFuture<F> {
    AttributedFuture { lane, future }
}
impl<F: Future> Future for AttributedFuture<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: future is structurally pinned; neither this implementation nor
        // a Drop implementation moves it. The lane is not projected as pinned.
        let this = unsafe { self.get_unchecked_mut() };
        this.lane.run(|| {
            // SAFETY: the structurally pinned field stays at the same address.
            unsafe { Pin::new_unchecked(&mut this.future) }.poll(cx)
        })
    }
}
