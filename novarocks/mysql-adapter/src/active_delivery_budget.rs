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

//! Response-local accounting for finite adapter delivery work, including IO.

use std::{future::Future, io, time::Duration};
use tokio::time::Instant;

pub(crate) const ACTIVE_DELIVERY_WORK_BUDGET: Duration = Duration::from_secs(30);

pub(crate) struct ActiveDeliveryWorkBudget {
    remaining: Duration,
}

impl ActiveDeliveryWorkBudget {
    pub(crate) fn new() -> Self {
        Self {
            remaining: ACTIVE_DELIVERY_WORK_BUDGET,
        }
    }

    fn phase(&mut self) -> io::Result<ActivePhase<'_>> {
        if self.remaining.is_zero() {
            return Err(crate::relay_result_writer::timeout_error());
        }
        let started = Instant::now();
        let deadline = started + self.remaining;
        Ok(ActivePhase {
            budget: self,
            started,
            deadline,
        })
    }

    /// The guard starts when this future is first polled, and also debits when
    /// an enclosing cancellation select drops the actual borrowed IO future.
    pub(crate) async fn run<T>(
        &mut self,
        future: impl Future<Output = io::Result<T>>,
    ) -> io::Result<T> {
        let phase = self.phase()?;
        let outcome = tokio::time::timeout_at(phase.deadline, future).await;
        // A finite synchronous poll may cross the deadline without yielding.
        // Refuse subsequent publication even when timeout's inner poll was ready.
        let expired = Instant::now() >= phase.deadline;
        drop(phase);
        if let Ok(Err(error)) = outcome {
            return Err(error);
        }
        if expired {
            return Err(crate::relay_result_writer::timeout_error());
        }
        outcome.map_err(|_| crate::relay_result_writer::timeout_error())?
    }

    pub(crate) fn run_sync<T>(&mut self, work: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
        let phase = self.phase()?;
        let outcome = work();
        let expired = Instant::now() >= phase.deadline;
        drop(phase);
        if let Err(error) = outcome {
            return Err(error);
        }
        if expired {
            Err(crate::relay_result_writer::timeout_error())
        } else {
            outcome
        }
    }
}

struct ActivePhase<'a> {
    budget: &'a mut ActiveDeliveryWorkBudget,
    started: Instant,
    deadline: Instant,
}

impl Drop for ActivePhase<'_> {
    fn drop(&mut self) {
        self.budget.remaining = self.budget.remaining.saturating_sub(self.started.elapsed());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::pin;

    #[tokio::test(start_paused = true)]
    async fn cumulative_phases_exclude_producer_wait_and_reject_twenty_plus_eleven() {
        let mut budget = ActiveDeliveryWorkBudget::new();
        budget
            .run(async {
                tokio::time::sleep(Duration::from_secs(20)).await;
                Ok(())
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(35)).await;
        assert_eq!(budget.remaining, Duration::from_secs(10));
        let error = budget
            .run(async {
                tokio::time::sleep(Duration::from_secs(11)).await;
                Ok(())
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(budget.remaining.is_zero());
        let polled = std::cell::Cell::new(false);
        assert!(
            budget
                .run(async {
                    polled.set(true);
                    Ok(())
                })
                .await
                .is_err()
        );
        assert!(!polled.get());
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_drop_debits_the_active_phase_without_charging_the_gap() {
        let mut budget = ActiveDeliveryWorkBudget::new();
        {
            let phase = budget.run(std::future::pending::<io::Result<()>>());
            let mut phase = pin!(phase);
            tokio::select! {
                _ = &mut phase => panic!("phase unexpectedly finished"),
                _ = tokio::time::sleep(Duration::from_secs(7)) => {}
            }
        }
        assert_eq!(budget.remaining, Duration::from_secs(23));
        tokio::time::sleep(Duration::from_secs(35)).await;
        assert_eq!(budget.remaining, Duration::from_secs(23));
    }

    #[tokio::test(start_paused = true)]
    async fn small_successful_phases_never_renew_the_response_budget() {
        let mut budget = ActiveDeliveryWorkBudget::new();
        for _ in 0..29 {
            budget
                .run(async {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    Ok(())
                })
                .await
                .unwrap();
        }
        assert!(
            budget
                .run(async {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    Ok(())
                })
                .await
                .is_err()
        );
        assert!(budget.remaining.is_zero());
        assert!(budget.run_sync(|| Ok(())).is_err());
    }
}
