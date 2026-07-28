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

use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

thread_local! {
    static CLIENT_DISCONNECT_SIGNAL: RefCell<Option<Arc<AtomicBool>>> = const { RefCell::new(None) };
}

struct ClientDisconnectSignalGuard {
    previous: Option<Arc<AtomicBool>>,
}

impl Drop for ClientDisconnectSignalGuard {
    fn drop(&mut self) {
        CLIENT_DISCONNECT_SIGNAL.with(|cell| {
            cell.replace(self.previous.take());
        });
    }
}

pub(crate) fn with_client_disconnect_signal<T>(
    signal: Arc<AtomicBool>,
    f: impl FnOnce() -> T,
) -> T {
    let _guard = CLIENT_DISCONNECT_SIGNAL.with(|cell| ClientDisconnectSignalGuard {
        previous: cell.replace(Some(signal)),
    });
    f()
}

pub(crate) fn current_client_disconnect_signal() -> Option<Arc<AtomicBool>> {
    CLIENT_DISCONNECT_SIGNAL.with(|cell| cell.borrow().clone())
}

pub(crate) fn client_disconnected() -> bool {
    CLIENT_DISCONNECT_SIGNAL.with(|cell| {
        cell.borrow()
            .as_ref()
            .is_some_and(|signal| signal.load(Ordering::SeqCst))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_client_disconnect_signal_restores_state_after_panic() {
        let signal = Arc::new(AtomicBool::new(true));
        let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            with_client_disconnect_signal(signal, || panic!("boom"));
        }));

        assert!(panic_result.is_err(), "closure should panic");
        assert!(
            !client_disconnected(),
            "disconnect signal must be restored after unwind"
        );
    }

    #[test]
    fn current_client_disconnect_signal_is_request_scoped_and_keeps_probe_alive() {
        let signal = Arc::new(AtomicBool::new(false));
        let captured = with_client_disconnect_signal(signal.clone(), || {
            let captured =
                current_client_disconnect_signal().expect("request-scoped signal is available");
            assert!(Arc::ptr_eq(&captured, &signal));
            captured
        });

        assert!(
            current_client_disconnect_signal().is_none(),
            "request scope must restore the previous signal"
        );
        signal.store(true, Ordering::SeqCst);
        assert!(
            captured.load(Ordering::SeqCst),
            "captured view must keep observing the request probe after scope exit"
        );
    }
}
