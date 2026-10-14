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

//! Finite direct-RPC identities declared before any Native role starts.

use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::test_sessions::{
    ClusterTestSessionPlan, RoleLaunchReference, RoleSlot,
};
use novarocks_failpoint::{session::DomainGrant, target_binding::RoleProcessReceipt};
use novarocks_proto_models::{common, novarocks as proto};
use novarocks_types::{BackendProcessId, FrontendProcessId};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Mutex,
    time::Instant,
};

#[derive(Clone)]
pub(super) struct DeclaredQuery {
    pub query_id: common::UniqueId,
    pub frontend: FrontendProcessId,
}
impl DeclaredQuery {
    fn fresh() -> Self {
        let frontend = FrontendProcessId::new_v7();
        let bytes = frontend.to_bytes();
        Self {
            query_id: common::UniqueId {
                hi: i64::from_be_bytes(bytes[..8].try_into().expect("identity width")),
                lo: i64::from_be_bytes(bytes[8..].try_into().expect("identity width")),
            },
            frontend,
        }
    }
    pub fn context(&self, backend: BackendProcessId) -> proto::QueryContextRef {
        proto::QueryContextRef {
            query_execution_id: Some(proto::QueryExecutionId {
                query_id: Some(self.query_id.clone()),
                attempt_id: 1,
            }),
            frontend_process_id: Some(proto::FrontendProcessId {
                value: self.frontend.to_bytes().to_vec(),
            }),
            backend_process_id: Some(proto::BackendProcessId {
                value: backend.to_bytes().to_vec(),
            }),
        }
    }
}
#[derive(Clone)]
pub(super) struct DirectHold {
    pub backend_index: usize,
    pub query_index: usize,
    pub task_id: Option<u32>,
    pub token: String,
}
#[derive(Clone)]
pub(super) struct DirectIntent {
    pub queries: Vec<DeclaredQuery>,
    pub holds: Vec<DirectHold>,
    pub deadline: Instant,
    references: BTreeMap<usize, RoleLaunchReference>,
}
#[derive(Default)]
pub(super) struct DirectIntentOwner(Mutex<Option<DirectIntent>>);
impl DirectIntentOwner {
    pub fn freeze(
        &self,
        action: &str,
        query_count: usize,
        targets: &[(usize, usize, Option<u32>)],
        shared_backend_token: bool,
        cluster_size: usize,
        ordinary: &Path,
        controlled: Option<&Path>,
        deadline: Instant,
    ) -> Result<ClusterTestSessionPlan> {
        let mut owner = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("Native intent owner poisoned"))?;
        ensure!(owner.is_none(), "Native direct intent already frozen");
        ensure!(
            matches!(
                action,
                "ingress-hold" | "registry-hold" | "preparation-hold"
            ),
            "unsupported Native direct action"
        );
        ensure!(
            !targets.is_empty() && targets.len() <= 64,
            "Native direct grant count outside finite bound"
        );
        let controlled =
            controlled.context("Native holds require an explicit --test-control-binary")?;
        let mut plan = ClusterTestSessionPlan::new(deadline)?;
        let fe = plan.reserve_launch(RoleSlot::Frontend)?;
        plan.authorize_ordinary_launch(&fe, ordinary.to_path_buf())?;
        ensure!(
            query_count > 0 && query_count <= 64,
            "Native direct query count outside finite bound"
        );
        let queries = (0..query_count)
            .map(|_| DeclaredQuery::fresh())
            .collect::<Vec<_>>();
        let mut references = BTreeMap::new();
        let mut backend_tokens = BTreeMap::new();
        let mut holds = vec![];
        let mut targets_seen = BTreeSet::new();
        let mut grants: BTreeMap<usize, Vec<DomainGrant>> = BTreeMap::new();
        for (grant_index, &(index, query_index, task_id)) in targets.iter().enumerate() {
            ensure!(
                targets_seen.insert((index, query_index, task_id)),
                "duplicate Native direct exact target"
            );
            ensure!(
                index < cluster_size,
                "Native direct target backend outside topology"
            );
            let query = queries
                .get(query_index)
                .context("Native direct target query outside frozen set")?;
            ensure!(
                task_id.is_some() == (action == "preparation-hold"),
                "Native direct target has wrong Task specificity"
            );
            let token = if shared_backend_token {
                if let Some(token) = backend_tokens.get(&index) {
                    String::clone(token)
                } else {
                    let token = plan.fresh_domain_secret()?;
                    backend_tokens.insert(index, token.clone());
                    token
                }
            } else {
                plan.fresh_domain_secret()?
            };
            let mut parameters = json!({"action":action,"token":token,"backend_index":index,
                "query_id_hi":query.query_id.hi,"query_id_lo":query.query_id.lo,"attempt_id":1});
            if let Some(task_id) = task_id {
                parameters["stage_id"] = json!(1);
                parameters["task_id"] = json!(task_id);
            } else {
                parameters["frontend_process_id"] = json!(query.frontend.to_string());
            }
            grants.entry(index).or_default().push(DomainGrant {
                domain: "native".into(),
                grant_id: format!("direct-{grant_index}"),
                parameters,
            });
            holds.push(DirectHold {
                backend_index: index,
                query_index,
                task_id,
                token,
            });
        }
        for index in 0..cluster_size {
            let reference = plan.reserve_launch(RoleSlot::Backend(index))?;
            if let Some(grants) = grants.remove(&index) {
                plan.authorize_launch(
                    &reference,
                    grants,
                    vec![("novarocks-native-adapter".into(), "test-controls".into())],
                )?;
                plan.set_launch_binary(&reference, controlled.to_path_buf())?;
                references.insert(index, reference);
            } else {
                plan.authorize_ordinary_launch(&reference, ordinary.to_path_buf())?;
            }
        }
        plan.validate_selected_intents(cluster_size)?;
        *owner = Some(DirectIntent {
            queries,
            holds,
            deadline,
            references,
        });
        Ok(plan)
    }
    pub fn get(&self) -> Result<DirectIntent> {
        self.0
            .lock()
            .map_err(|_| anyhow::anyhow!("Native intent owner poisoned"))?
            .clone()
            .context("Native direct intent was not frozen before launch")
    }
}
impl DirectIntent {
    pub fn verify_backend(&self, index: usize, actual: BackendProcessId) -> Result<()> {
        ensure!(
            Instant::now() < self.deadline,
            "Native original deadline expired"
        );
        let reference = self
            .references
            .get(&index)
            .context("backend has no finite Native declaration")?;
        let receipt = RoleProcessReceipt::read_private(&reference.process_receipt)?;
        ensure!(
            receipt.session_id == reference.session_id,
            "Native role receipt session mismatch"
        );
        ensure!(
            receipt.backend_process()? == actual,
            "Native direct target is not the authenticated declared process"
        );
        Ok(())
    }
    pub fn token(&self, index: usize) -> Result<&str> {
        self.holds
            .iter()
            .find(|hold| hold.backend_index == index)
            .map(|hold| hold.token.as_str())
            .context("backend has no frozen Native hold")
    }
}

/// A bounded marker read shares the original absolute clock across every partial read.
pub(super) fn read_marker(
    stream: &mut std::os::unix::net::UnixStream,
    deadline: Instant,
) -> Result<String> {
    use std::io::Read;
    stream.set_nonblocking(true)?;
    let mut bytes = vec![];
    loop {
        ensure!(
            Instant::now() < deadline,
            "Native marker original deadline expired"
        );
        let mut buffer = [0u8; 512];
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => {
                ensure!(
                    bytes.len() + count <= 4096,
                    "Native marker exceeds bounded frame"
                );
                bytes.extend_from_slice(&buffer[..count]);
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::yield_now()
            }
            Err(error) => return Err(error.into()),
        }
    }
    ensure!(
        Instant::now() < deadline,
        "Native marker original deadline expired"
    );
    stream.set_nonblocking(false)?;
    String::from_utf8(bytes).context("Native marker is not UTF-8")
}
pub(super) fn release_hold(
    stream: &mut std::os::unix::net::UnixStream,
    deadline: Instant,
) -> Result<()> {
    use std::io::Write;
    stream.set_write_timeout(Some(
        deadline
            .checked_duration_since(Instant::now())
            .context("Native hold original deadline expired")?,
    ))?;
    stream.write_all(b"R")?;
    ensure!(
        Instant::now() < deadline,
        "Native hold original deadline expired"
    );
    Ok(())
}

/// Failure cleanup releases by EOF even when the original action clock has elapsed.
pub(super) fn close_hold(stream: &mut std::os::unix::net::UnixStream) {
    use std::io::Write;
    let _ = stream.set_nonblocking(true);
    let _ = stream.write_all(b"R");
    let _ = stream.shutdown(std::net::Shutdown::Both);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn saturation_contexts_have_distinct_complete_identities() {
        let queries = (0..8).map(|_| DeclaredQuery::fresh()).collect::<Vec<_>>();
        let backend = BackendProcessId::new_v7();
        for (index, query) in queries.iter().enumerate() {
            let context = query.context(backend);
            assert_eq!(context.query_execution_id.as_ref().unwrap().attempt_id, 1);
            for other in &queries[..index] {
                assert_ne!(query.query_id, other.query_id);
                assert_ne!(query.frontend, other.frontend);
                assert_ne!(context, other.context(backend));
            }
        }
    }
    #[test]
    fn expired_marker_rejects_already_buffered_data() {
        use std::io::Write;
        let (mut reader, mut writer) = std::os::unix::net::UnixStream::pair().unwrap();
        writer.write_all(b"NIH1 late\n").unwrap();
        writer.shutdown(std::net::Shutdown::Write).unwrap();
        assert!(read_marker(&mut reader, Instant::now()).is_err());
    }
    #[test]
    fn finite_intent_freezes_once_and_refuses_a_replacement_backend() {
        let binary = std::env::current_exe().unwrap();
        let owner = DirectIntentOwner::default();
        let deadline = Instant::now() + std::time::Duration::from_secs(10);
        let targets = (0..8).map(|query| (0, query, None)).collect::<Vec<_>>();
        let plan = owner
            .freeze(
                "ingress-hold",
                8,
                &targets,
                false,
                3,
                &binary,
                Some(&binary),
                deadline,
            )
            .unwrap();
        let intent = owner.get().unwrap();
        assert_eq!(intent.holds.len(), 8);
        assert_eq!(intent.deadline, plan.deadline());
        assert!(
            owner
                .freeze(
                    "ingress-hold",
                    8,
                    &targets,
                    false,
                    3,
                    &binary,
                    Some(&binary),
                    deadline
                )
                .is_err()
        );
        let reference = &intent.references[&0];
        let process = BackendProcessId::new_v7();
        RoleProcessReceipt {
            schema_version: 1,
            session_id: reference.session_id.clone(),
            launch_id: "test-launch".into(),
            role: novarocks_failpoint::session::SessionRole::Be,
            process_id: process.to_string(),
        }
        .write_private(&reference.process_receipt)
        .unwrap();
        assert!(intent.verify_backend(0, process).is_ok());
        assert!(
            intent
                .verify_backend(0, BackendProcessId::new_v7())
                .is_err()
        );
    }
    #[test]
    fn duplicate_tasks_are_refused_before_launch() {
        let binary = std::env::current_exe().unwrap();
        let owner = DirectIntentOwner::default();
        assert!(
            owner
                .freeze(
                    "preparation-hold",
                    1,
                    &[(0, 0, Some(1)), (0, 0, Some(1))],
                    true,
                    3,
                    &binary,
                    Some(&binary),
                    Instant::now() + std::time::Duration::from_secs(10)
                )
                .is_err()
        );
        assert!(owner.get().is_err());
    }
}
