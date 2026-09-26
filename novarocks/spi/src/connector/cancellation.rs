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

//! Attempt-local stop signals shared across connector and file operations.
//!
//! The owner can stop its own subtree. A view can only observe that decision;
//! neither Tokio's token nor a task registry becomes part of the connector
//! contract. Cancellation is a wakeup, not a typed failure cause or evidence
//! that started work has drained.

use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use tokio_util::sync::CancellationToken;

/// The only capability allowed to request a stop for one operation subtree.
#[derive(Clone, Default)]
pub struct ConnectorStopOwner {
    token: CancellationToken,
}

/// A read-only view of one operation's stop state.
#[derive(Clone)]
pub struct ConnectorStopView {
    inner: StopViewInner,
    /// Keeps a host-owned signal relay alive even when a file operation
    /// outlives the Connector request object that admitted it.
    lifetime: Option<Arc<dyn Any + Send + Sync>>,
}

#[derive(Clone)]
enum StopViewInner {
    Token(CancellationToken),
    Any(Arc<[ConnectorStopView]>),
}

impl ConnectorStopOwner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn view(&self) -> ConnectorStopView {
        ConnectorStopView {
            inner: StopViewInner::Token(self.token.clone()),
            lifetime: None,
        }
    }

    /// Derive a child that the caller may stop without stopping this owner.
    pub fn child(&self) -> Self {
        Self {
            token: self.token.child_token(),
        }
    }

    pub fn request_stop(&self) {
        self.token.cancel();
    }

    pub fn is_stopped(&self) -> bool {
        self.token.is_cancelled()
    }
}

impl ConnectorStopView {
    /// Combine independent, already-admitted stop authorities. Constructing a
    /// view installs no task or subscription; awaiting it registers with each
    /// source and observes a stop that happened before the await as well.
    pub fn any_of(first: Self, others: impl IntoIterator<Item = Self>) -> Self {
        let mut views = vec![first];
        views.extend(others);
        Self {
            inner: StopViewInner::Any(Arc::from(views)),
            lifetime: None,
        }
    }

    pub(crate) fn with_lifetime<T: Any + Send + Sync>(mut self, lifetime: Arc<T>) -> Self {
        self.lifetime = Some(lifetime);
        self
    }

    pub fn is_stopped(&self) -> bool {
        match &self.inner {
            StopViewInner::Token(token) => token.is_cancelled(),
            StopViewInner::Any(views) => views.iter().any(Self::is_stopped),
        }
    }

    /// This future is safe to create before or after a stop request and does
    /// not need a running Tokio executor merely to observe the flag.
    pub fn stopped(&self) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        match &self.inner {
            StopViewInner::Token(token) => {
                let token = token.clone();
                Box::pin(async move { token.cancelled().await })
            }
            StopViewInner::Any(views) => {
                let mut waits: Vec<_> = views.iter().map(Self::stopped).collect();
                Box::pin(async move {
                    std::future::poll_fn(move |context| {
                        for wait in &mut waits {
                            if wait.as_mut().poll(context).is_ready() {
                                return Poll::Ready(());
                            }
                        }
                        Poll::Pending
                    })
                    .await
                })
            }
        }
    }
}

impl std::fmt::Debug for ConnectorStopOwner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorStopOwner")
            .field("stopped", &self.is_stopped())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for ConnectorStopView {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorStopView")
            .field("stopped", &self.is_stopped())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn parent_stops_children_and_prior_stop_wakes_new_waiters() {
        let task = ConnectorStopOwner::new();
        let source = task.child();
        let operation = source.child();
        let first = operation.view().stopped();
        task.request_stop();
        first.await;
        operation.view().stopped().await;
        assert!(operation.is_stopped());
        assert!(source.is_stopped());
    }

    #[tokio::test]
    async fn child_stop_does_not_stop_parent_or_sibling() {
        let task = ConnectorStopOwner::new();
        let source = task.child();
        let sibling = task.child();
        let operation = source.child();
        let waiter_a = operation.view().stopped();
        let waiter_b = operation.view().stopped();
        source.request_stop();
        waiter_a.await;
        waiter_b.await;
        assert!(!task.is_stopped());
        assert!(!sibling.is_stopped());
    }

    #[tokio::test]
    async fn combined_view_wakes_from_either_authority_even_before_subscription() {
        let request = ConnectorStopOwner::new();
        let fence = ConnectorStopOwner::new();
        let combined = ConnectorStopView::any_of(request.view(), [fence.view()]);
        assert!(!combined.is_stopped());
        fence.request_stop();
        combined.stopped().await;
        assert!(combined.is_stopped());
        assert!(!request.is_stopped());

        let later = ConnectorStopView::any_of(request.view(), [ConnectorStopOwner::new().view()]);
        let waiter = later.stopped();
        request.request_stop();
        waiter.await;
    }
}
