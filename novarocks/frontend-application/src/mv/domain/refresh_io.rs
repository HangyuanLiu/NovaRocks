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

use std::sync::{Mutex, MutexGuard, OnceLock};

use novarocks_spi::connector::ConnectorRequestContext;
use novarocks_types::naming::TableIdentity;

// This inspector is pure over the sealed metadata loaded by its caller.
pub(crate) fn observe_loaded_refresh_base(
    storage_observation: &dyn novarocks_spi::connector::MvStorageObservationPort,
    lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
    metadata: &novarocks_spi::connector::ConnectorTableMetadata,
    table_ref: &TableIdentity,
    connector_context: &ConnectorRequestContext,
) -> Result<
    crate::mv::domain::storage_observation::MvRefreshBaseObservation,
    novarocks_spi::connector::ConnectorError,
> {
    let observation = crate::mv::domain::storage_observation::observe_refresh_base(
        storage_observation,
        lease,
        metadata,
        connector_context.clone(),
    )?;
    if observation.table() != &metadata.identity {
        return Err(novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::CorruptData,
            format!(
                "MV refresh base observation identity does not match loaded metadata for {}",
                table_ref.fqn()
            ),
        ));
    }
    Ok(observation)
}

pub(crate) fn acquire_mv_refresh_lock() -> Result<MutexGuard<'static, ()>, String> {
    static MV_REFRESH_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    lock_mv_refresh_mutex(MV_REFRESH_LOCK.get_or_init(|| Mutex::new(())))
}

fn lock_mv_refresh_mutex(lock: &Mutex<()>) -> Result<MutexGuard<'_, ()>, String> {
    lock.lock()
        .map_err(|_| "materialized view refresh lock poisoned".to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    #[test]
    fn lock_mv_refresh_mutex_reports_poisoned_lock() {
        let lock: &'static Mutex<()> = Box::leak(Box::new(Mutex::new(())));
        static PANIC_HOOK_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let _hook_guard = PANIC_HOOK_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("panic hook lock");
        let old_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let poison_result = std::panic::catch_unwind(|| {
            let _guard = lock.lock().expect("lock");
            panic!("poison test lock");
        });
        std::panic::set_hook(old_hook);
        assert!(poison_result.is_err());

        let err = super::lock_mv_refresh_mutex(lock).expect_err("poisoned lock should fail");
        assert!(err.contains("poisoned"), "err={err}");
    }
}
