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
use std::future::Future;
use std::sync::{Arc, OnceLock};

use tokio::runtime::{Handle, Runtime};

use tracing::info;

/// Worker thread stack size for Tokio runtimes that run SQL workloads.
///
/// Tokio defaults to the platform thread stack (≈ 2 MiB), which is not enough
/// for our deeply-recursive analyzer / planner / fragment-builder walks.
/// Deeply-nested ASTs blow the stack and abort the whole process (we saw
/// this on `WITH RECURSIVE ... max_depth=10` Fibonacci and on multi-CTE
/// TPC-DS reports that nest INTERSECT / UNION ALL several levels deep).
///
/// 16 MiB is the value StarRocks BE / DuckDB / other comparable engines
/// converge on for the same reason.
pub const WORKER_STACK_SIZE_BYTES: usize = 16 * 1024 * 1024;

const DATA_RUNTIME_THREAD_NAME: &str = "novarocks-data-runtime";
static DATA_RUNTIME: OnceLock<Result<Arc<Runtime>, String>> = OnceLock::new();
static DATA_RUNTIME_SIZING: OnceLock<DataRuntimeSizing> = OnceLock::new();

/// Thread sizing for the process-wide data runtime.
///
/// The runtime itself is legitimately a process singleton — roughly fifty call
/// sites across connector, engine and MV code reach it from places that have no
/// composition-time value to receive. Its *sizing*, however, is configuration,
/// so the composition root installs it rather than having the runtime reach for
/// a config global at first use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DataRuntimeSizing {
    pub worker_threads: usize,
    pub max_blocking_threads: usize,
}

impl DataRuntimeSizing {
    /// Sizing used when no composition root installed one: unit tests and
    /// embedded engine users that never read a config file.
    pub fn machine_default() -> Self {
        Self {
            worker_threads: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
            max_blocking_threads: 64,
        }
    }

    fn normalized(self) -> Self {
        Self {
            worker_threads: self.worker_threads.max(1),
            max_blocking_threads: self.max_blocking_threads.max(1),
        }
    }
}

/// Installs the configured data-runtime sizing.
///
/// The composition root must call this before anything touches
/// [`data_runtime()`]. Installing the same sizing twice is accepted so that
/// `role=all-in-one`, where the frontend and backend roots share one config,
/// does not have to coordinate which of them installs.
///
/// Returns `Err` when the runtime was already built, because that means
/// something reached [`data_runtime()`] during startup and the configured
/// sizing was silently ignored. Startup should fail on that rather than run at
/// the fallback size while the operator believes their config took effect.
pub fn install_data_runtime_sizing(sizing: DataRuntimeSizing) -> Result<(), String> {
    let sizing = sizing.normalized();
    if let Err(_already_installed) = DATA_RUNTIME_SIZING.set(sizing) {
        let installed = DATA_RUNTIME_SIZING
            .get()
            .copied()
            .expect("sizing set failed, so one is installed");
        return if installed == sizing {
            Ok(())
        } else {
            Err(format!(
                "data runtime sizing already installed as {installed:?}; refusing to replace it with {sizing:?}"
            ))
        };
    }
    if DATA_RUNTIME.get().is_some() {
        return Err(format!(
            "data runtime was built before {sizing:?} was installed; \
             install_data_runtime_sizing must run before any data_runtime() use"
        ));
    }
    Ok(())
}

pub fn data_runtime() -> Result<&'static Arc<Runtime>, String> {
    match DATA_RUNTIME.get_or_init(|| {
        // Deliberately does not initialize `DATA_RUNTIME_SIZING`: leaving it
        // unset is what lets a late `install_data_runtime_sizing` report that
        // it lost the race instead of reporting a value conflict.
        let DataRuntimeSizing {
            worker_threads,
            max_blocking_threads,
        } = DATA_RUNTIME_SIZING
            .get()
            .copied()
            .unwrap_or_else(DataRuntimeSizing::machine_default)
            .normalized();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(worker_threads)
            .max_blocking_threads(max_blocking_threads)
            .thread_name(DATA_RUNTIME_THREAD_NAME)
            .thread_stack_size(WORKER_STACK_SIZE_BYTES)
            .build()
            .map_err(|e| format!("init data tokio runtime failed: {e}"))?;
        info!(
            worker_threads,
            max_blocking_threads,
            thread_name = DATA_RUNTIME_THREAD_NAME,
            "global data runtime initialized"
        );
        Ok(Arc::new(runtime))
    }) {
        Ok(runtime) => Ok(runtime),
        Err(err) => Err(err.clone()),
    }
}

pub fn data_runtime_handle() -> Result<Handle, String> {
    let runtime = data_runtime()?;
    Ok(runtime.handle().clone())
}

pub fn data_block_on<F>(future: F) -> Result<F::Output, String>
where
    F: Future,
{
    let runtime = data_runtime()?;
    if Handle::try_current().is_ok() {
        // Path A: Caller is on a thread that has a Tokio runtime handle (e.g. a
        // `task::spawn_blocking` closure inside the standalone server's request
        // handler).  `block_in_place` detects we are on a blocking thread (NOT a
        // worker thread) and simply runs the closure directly without suspending
        // any async task.  Using it instead of `Handle::block_on` lets us call
        // `runtime.block_on` on the *separate* data runtime safely.
        //
        // True async task callers (poll-context worker threads) must NOT reach this
        // path — `block_in_place` from a worker would still allow runtime.block_on
        // on the same runtime to deadlock. We rely on the convention that all DDL
        // is dispatched through `task::spawn_blocking`.
        return Ok(tokio::task::block_in_place(|| runtime.block_on(future)));
    }
    Ok(runtime.block_on(future))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn sizing_normalization_floors_both_thread_counts_at_one() {
        let normalized = DataRuntimeSizing {
            worker_threads: 0,
            max_blocking_threads: 0,
        }
        .normalized();
        assert_eq!(normalized.worker_threads, 1);
        assert_eq!(normalized.max_blocking_threads, 1);
    }

    #[test]
    fn machine_default_sizing_is_usable_without_a_config() {
        let sizing = DataRuntimeSizing::machine_default();
        assert!(sizing.worker_threads >= 1);
        assert_eq!(sizing.max_blocking_threads, 64);
    }

    #[test]
    fn data_runtime_is_singleton_across_threads() {
        let expected_ptr = Arc::as_ptr(data_runtime().expect("get data runtime")) as usize;
        let handles = (0..16)
            .map(|_| {
                thread::spawn(move || {
                    for _ in 0..64 {
                        let ptr = Arc::as_ptr(data_runtime().expect("get data runtime")) as usize;
                        assert_eq!(ptr, expected_ptr);
                    }
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().expect("join runtime singleton checker");
        }
    }

    #[test]
    fn data_block_on_runs_outside_runtime() {
        let value = data_block_on(async { 7_i32 }).expect("run with data runtime");
        assert_eq!(value, 7);
    }

    #[test]
    fn data_block_on_via_block_in_place_from_runtime_context() {
        // Exercises the `Handle::try_current().is_ok()` branch — specifically the
        // case where `runtime.block_on` drives the outer async context, which
        // `block_in_place` treats as a blocking thread regardless of whether the
        // call site is a literal `spawn_blocking` closure or a `block_on` call.
        // In production DDL all paths go through `task::spawn_blocking`, but the
        // behaviour of `block_in_place` is identical in both cases.
        let runtime = data_runtime().expect("get data runtime");
        let value = runtime.block_on(async {
            data_block_on(async { 1_u8 }).expect("block_in_place succeeds from runtime context")
        });
        assert_eq!(value, 1_u8);
    }
}
