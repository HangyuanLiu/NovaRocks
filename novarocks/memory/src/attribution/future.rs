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

//! Construction, every poll, and in-place destruction share one owner policy.
// Design: ADR-0197 (docs/adr/ADR-0197-observation-owner-shards-and-coverage-shadow.md)
use crate::lane::LaneShards;
use std::{
    future::Future,
    mem::ManuallyDrop,
    pin::Pin,
    task::{Context, Poll},
};

pub struct AttributedFuture<F> {
    owner: LaneShards,
    future: ManuallyDrop<F>,
}
impl<F: Future> AttributedFuture<F> {
    pub fn new(owner: LaneShards, make: impl FnOnce() -> F) -> Self {
        let future = owner.run(make);
        Self {
            owner,
            future: ManuallyDrop::new(future),
        }
    }
}
impl<F: Future> Future for AttributedFuture<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: the future is structurally pinned. Neither poll nor Drop
        // moves it; projecting the owner does not pin the owner.
        let this = unsafe { self.get_unchecked_mut() };
        this.owner
            .run(|| unsafe { Pin::new_unchecked(&mut *this.future) }.poll(cx))
    }
}
impl<F> Drop for AttributedFuture<F> {
    fn drop(&mut self) {
        // SAFETY: the structurally pinned field is destroyed exactly once in
        // place, including when its destructor unwinds. ManuallyDrop prevents
        // automatic destruction from running it a second time.
        self.owner
            .run(|| unsafe { ManuallyDrop::drop(&mut self.future) });
    }
}
