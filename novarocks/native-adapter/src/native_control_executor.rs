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

//! A small bounded blocking executor for Native control mutations.

use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::Instant;

use tonic::Status;

type ControlJob = Box<dyn FnOnce() + Send + 'static>;

pub struct NativeControlExecutor {
    sender: Mutex<Option<mpsc::SyncSender<ControlJob>>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

impl NativeControlExecutor {
    pub fn start(worker_threads: usize, queued_jobs: usize) -> Result<Arc<Self>, String> {
        if worker_threads == 0 || queued_jobs == 0 {
            return Err("native control executor requires nonzero workers and queue".to_owned());
        }
        let (sender, receiver) = mpsc::sync_channel::<ControlJob>(queued_jobs);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::with_capacity(worker_threads);
        for index in 0..worker_threads {
            let receiver = Arc::clone(&receiver);
            match std::thread::Builder::new()
                .name(format!("native-control-{index}"))
                .spawn(move || {
                    loop {
                        let job = receiver
                            .lock()
                            .expect("native control queue poisoned")
                            .recv();
                        let Ok(job) = job else { break };
                        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(job)).is_err() {
                            tracing::error!("native control job panicked");
                        }
                    }
                }) {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    drop(sender);
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(format!("spawn native control worker: {error}"));
                }
            }
        }
        Ok(Arc::new(Self {
            sender: Mutex::new(Some(sender)),
            workers: Mutex::new(workers),
        }))
    }

    pub async fn execute<T, F>(&self, work: F) -> Result<T, Status>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, Status> + Send + 'static,
    {
        let (answer_tx, answer_rx) = tokio::sync::oneshot::channel();
        let queued_at = Instant::now();
        self.sender
            .lock()
            .expect("native control sender poisoned")
            .as_ref()
            .ok_or_else(|| Status::unavailable("native control executor stopped"))?
            .try_send(Box::new(move || {
                crate::backend_metrics::native_control_queue_wait("started", queued_at.elapsed());
                let _ = answer_tx.send(work());
            }))
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => {
                    Status::resource_exhausted("native control execution capacity exhausted")
                }
                mpsc::TrySendError::Disconnected(_) => {
                    Status::unavailable("native control executor stopped")
                }
            })?;
        answer_rx
            .await
            .map_err(|_| Status::internal("native control job panicked"))?
    }
}

impl Drop for NativeControlExecutor {
    fn drop(&mut self) {
        self.sender
            .lock()
            .expect("native control sender poisoned")
            .take();
        for worker in self
            .workers
            .lock()
            .expect("native control workers poisoned")
            .drain(..)
        {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bounded_executor_runs_control_after_ordinary_pool_is_irrelevant() {
        let executor = NativeControlExecutor::start(1, 1).unwrap();
        assert_eq!(executor.execute(|| Ok::<_, Status>(7)).await.unwrap(), 7);
    }
}
