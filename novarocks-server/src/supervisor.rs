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

use anyhow::anyhow;
use tokio::sync::watch;
type RoleTaskResult = anyhow::Result<()>;

enum PrimaryRole {
    Frontend(RoleTaskResult),
    Backend(RoleTaskResult),
    Shutdown,
}

fn role_outcome(role: &str, result: RoleTaskResult) -> Result<(), String> {
    match result {
        Ok(()) => Ok(()),
        Err(error) => Err(format!("{role} role failed: {error:#}")),
    }
}

fn unexpected_exit(role: &str, result: Result<(), String>) -> String {
    match result {
        Ok(()) => format!("{role} role stopped unexpectedly"),
        Err(error) => error,
    }
}

/// Run the normal frontend and backend applications as one supervised process.
///
/// The first role failure (or Ctrl-C) is the sole shutdown authority.  The
/// sibling receives the shared stop signal and is awaited before the result is
/// returned, so all-in-one has the same role-local cleanup path as a
/// multi-process deployment.
pub async fn supervise_all_in_one<Frontend, Backend, Shutdown>(
    frontend: Frontend,
    backend: Backend,
    stop_tx: watch::Sender<bool>,
    shutdown: Shutdown,
) -> anyhow::Result<()>
where
    Frontend: Future<Output = anyhow::Result<()>>,
    Backend: Future<Output = anyhow::Result<()>>,
    Shutdown: Future<Output = ()>,
{
    tokio::pin!(frontend);
    tokio::pin!(backend);
    let primary = tokio::select! {
        result = &mut frontend => PrimaryRole::Frontend(result),
        result = &mut backend => PrimaryRole::Backend(result),
        _ = shutdown => PrimaryRole::Shutdown,
    };
    let _ = stop_tx.send(true);

    match primary {
        PrimaryRole::Frontend(result) => {
            let primary = unexpected_exit("frontend", role_outcome("frontend", result));
            match role_outcome("backend cleanup", backend.await) {
                Ok(()) => Err(anyhow!(primary)),
                Err(cleanup) => Err(anyhow!("{primary}; cleanup: {cleanup}")),
            }
        }
        PrimaryRole::Backend(result) => {
            let primary = unexpected_exit("backend", role_outcome("backend", result));
            match role_outcome("frontend cleanup", frontend.await) {
                Ok(()) => Err(anyhow!(primary)),
                Err(cleanup) => Err(anyhow!("{primary}; cleanup: {cleanup}")),
            }
        }
        PrimaryRole::Shutdown => {
            let frontend = role_outcome("frontend cleanup", frontend.await);
            let backend = role_outcome("backend cleanup", backend.await);
            match (frontend, backend) {
                (Ok(()), Ok(())) => Ok(()),
                (Err(frontend), Ok(())) => Err(anyhow!("{frontend}")),
                (Ok(()), Err(backend)) => Err(anyhow!("{backend}")),
                (Err(frontend), Err(backend)) => Err(anyhow!("{frontend}; cleanup: {backend}")),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future;

    use super::supervise_all_in_one;

    #[test]
    fn frontend_failure_stops_backend_before_returning() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
            let (stopped_tx, stopped_rx) = tokio::sync::oneshot::channel();
            let result = supervise_all_in_one(
                async { Err(anyhow::anyhow!("frontend listener failed")) },
                async move {
                    while !*stop_rx.borrow() {
                        stop_rx.changed().await.expect("supervisor owns sender");
                    }
                    stopped_tx.send(()).expect("record backend stop");
                    Ok(())
                },
                stop_tx,
                future::pending(),
            )
            .await;

            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("frontend listener failed")
            );
            stopped_rx
                .await
                .expect("backend must observe stop before return");
        });
    }
}
