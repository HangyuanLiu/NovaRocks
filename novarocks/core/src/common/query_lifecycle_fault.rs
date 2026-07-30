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

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::query_execution::lifecycle::QueryExecutionId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryLifecycleFaultKind {
    InitAckDrop,
    StageAckDrop,
    StartAckDrop,
    StartAckSuppress,
    HeartbeatStop,
    HeartbeatStopAfterStage,
    RestartAfterInitAck,
}

impl QueryLifecycleFaultKind {
    pub const fn file_stem(self) -> &'static str {
        match self {
            Self::InitAckDrop => "init-ack-drop",
            Self::StageAckDrop => "stage-ack-drop",
            Self::StartAckDrop => "start-ack-drop",
            Self::StartAckSuppress => "start-ack-suppress",
            Self::HeartbeatStop => "heartbeat-stop",
            Self::HeartbeatStopAfterStage => "heartbeat-stop-after-stage",
            Self::RestartAfterInitAck => "restart-after-init-ack",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryLifecycleFaultScope {
    pub token: String,
    pub execution_id: QueryExecutionId,
    pub backend_index: usize,
    pub backend_id: u64,
    pub start_epoch: u64,
}

/// Runner-owned one-shot failure for a local Stage build. Unlike ACK faults,
/// the runner does not preselect a backend: the first non-empty batch that
/// contains the requested local ordinal claims it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagePrepareFailure {
    pub token: String,
    pub ordinal: usize,
}

pub fn arm_path(root: &Path, backend_index: usize, kind: QueryLifecycleFaultKind) -> PathBuf {
    root.join(format!("be-{backend_index}.{}.arm", kind.file_stem()))
}

pub fn trigger_path(root: &Path, backend_index: usize, kind: QueryLifecycleFaultKind) -> PathBuf {
    root.join(format!("be-{backend_index}.{}.trigger", kind.file_stem()))
}

pub fn claim_stage_prepare_failure(
    root: &Path,
    available_fragments: usize,
) -> Result<Option<StagePrepareFailure>, String> {
    let path = root.join("stage-prepare-fail.trigger");
    let contents = match fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("read {}: {error}", path.display())),
    };
    let fields = parse_fields(&contents)?;
    let failure = StagePrepareFailure {
        token: required_token(&fields)?,
        ordinal: required_usize(&fields, "ordinal")?,
    };
    if failure.ordinal == 0 {
        return Err("stage prepare fault ordinal must be at least one".to_string());
    }
    if failure.ordinal > available_fragments {
        return Ok(None);
    }
    match fs::remove_file(&path) {
        Ok(()) => Ok(Some(failure)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(format!("consume {}: {error}", path.display())),
    }
}

pub fn bind_armed_fault(
    root: &Path,
    kind: QueryLifecycleFaultKind,
    execution_id: QueryExecutionId,
    backend_index: usize,
    backend_id: u64,
    start_epoch: u64,
) -> Result<Option<QueryLifecycleFaultScope>, String> {
    let arm = arm_path(root, backend_index, kind);
    let contents = match fs::read_to_string(&arm) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("read {}: {error}", arm.display())),
    };
    let fields = parse_fields(&contents)?;
    let token = required_token(&fields)?;
    let armed_backend_index = required_usize(&fields, "backend_index")?;
    if armed_backend_index != backend_index {
        return Err(format!(
            "fault arm backend_index {armed_backend_index} does not match path backend {backend_index}"
        ));
    }
    let scope = QueryLifecycleFaultScope {
        token,
        execution_id,
        backend_index,
        backend_id,
        start_epoch,
    };
    let trigger = trigger_path(root, backend_index, kind);
    publish_new(&trigger, serialize_scope(&scope).as_bytes())?;
    fs::remove_file(&arm).map_err(|error| format!("consume {}: {error}", arm.display()))?;
    Ok(Some(scope))
}

pub fn claim_matching_fault(
    root: &Path,
    kind: QueryLifecycleFaultKind,
    execution_id: QueryExecutionId,
    backend_index: usize,
    backend_id: u64,
    start_epoch: u64,
) -> Result<Option<QueryLifecycleFaultScope>, String> {
    let trigger = trigger_path(root, backend_index, kind);
    let contents = match fs::read_to_string(&trigger) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("read {}: {error}", trigger.display())),
    };
    let scope = parse_scope(&contents)?;
    if scope.execution_id != execution_id
        || scope.backend_index != backend_index
        || scope.backend_id != backend_id
        || scope.start_epoch != start_epoch
    {
        return Ok(None);
    }
    match fs::remove_file(&trigger) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("consume {}: {error}", trigger.display())),
    }
    Ok(Some(scope))
}

/// Reads a matching runner-owned fault without consuming it.
///
/// `StartAckSuppress` remains armed for the full execution attempt so both
/// the first Start RPC and its one idempotent retry have an unknown outcome.
/// That forces the frontend through the partial-start global Abort path. The
/// SQL runner owns cleanup of the fault directory after the step.
pub fn observe_matching_fault(
    root: &Path,
    kind: QueryLifecycleFaultKind,
    execution_id: QueryExecutionId,
    backend_index: usize,
    backend_id: u64,
    start_epoch: u64,
) -> Result<Option<QueryLifecycleFaultScope>, String> {
    let trigger = trigger_path(root, backend_index, kind);
    let contents = match fs::read_to_string(&trigger) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("read {}: {error}", trigger.display())),
    };
    let scope = parse_scope(&contents)?;
    if scope.execution_id != execution_id
        || scope.backend_index != backend_index
        || scope.backend_id != backend_id
        || scope.start_epoch != start_epoch
    {
        return Ok(None);
    }
    Ok(Some(scope))
}

fn serialize_scope(scope: &QueryLifecycleFaultScope) -> String {
    format!(
        "token={}\nexecution_hi={}\nexecution_lo={}\nattempt={}\nbackend_index={}\nbackend_id={}\nstart_epoch={}\n",
        scope.token,
        scope.execution_id.query_id().high(),
        scope.execution_id.query_id().low(),
        scope.execution_id.attempt_id().get(),
        scope.backend_index,
        scope.backend_id,
        scope.start_epoch
    )
}

fn parse_scope(contents: &str) -> Result<QueryLifecycleFaultScope, String> {
    let fields = parse_fields(contents)?;
    let query_id = crate::query_execution::contract::QueryId::new(
        required_i64(&fields, "execution_hi")?,
        required_i64(&fields, "execution_lo")?,
    );
    let attempt =
        crate::query_execution::lifecycle::AttemptId::new(required_u64(&fields, "attempt")?)
            .map_err(|error| error.to_string())?;
    let execution_id =
        QueryExecutionId::new(query_id, attempt).map_err(|error| error.to_string())?;
    Ok(QueryLifecycleFaultScope {
        token: required_token(&fields)?,
        execution_id,
        backend_index: required_usize(&fields, "backend_index")?,
        backend_id: required_u64(&fields, "backend_id")?,
        start_epoch: required_u64(&fields, "start_epoch")?,
    })
}

fn parse_fields(contents: &str) -> Result<BTreeMap<&str, &str>, String> {
    let mut fields = BTreeMap::new();
    for line in contents.lines() {
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| format!("malformed fault field {line:?}"))?;
        if key.is_empty() || value.is_empty() || fields.insert(key, value).is_some() {
            return Err(format!("invalid or duplicate fault field {key:?}"));
        }
    }
    Ok(fields)
}

fn required_token(fields: &BTreeMap<&str, &str>) -> Result<String, String> {
    let token = fields
        .get("token")
        .ok_or_else(|| "fault scope missing token".to_string())?;
    if !token
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err("fault scope token is invalid".to_string());
    }
    Ok((*token).to_string())
}

fn required_u64(fields: &BTreeMap<&str, &str>, key: &str) -> Result<u64, String> {
    fields
        .get(key)
        .ok_or_else(|| format!("fault scope missing {key}"))?
        .parse::<u64>()
        .map_err(|error| format!("invalid fault scope {key}: {error}"))
}

fn required_i64(fields: &BTreeMap<&str, &str>, key: &str) -> Result<i64, String> {
    fields
        .get(key)
        .ok_or_else(|| format!("fault scope missing {key}"))?
        .parse::<i64>()
        .map_err(|error| format!("invalid fault scope {key}: {error}"))
}

fn required_usize(fields: &BTreeMap<&str, &str>, key: &str) -> Result<usize, String> {
    fields
        .get(key)
        .ok_or_else(|| format!("fault scope missing {key}"))?
        .parse::<usize>()
        .map_err(|error| format!("invalid fault scope {key}: {error}"))
}

fn publish_new(path: &Path, contents: &[u8]) -> Result<(), String> {
    static NEXT_STAGING: AtomicU64 = AtomicU64::new(1);
    let staging = path.with_extension(format!(
        "binding-{}-{}",
        std::process::id(),
        NEXT_STAGING.fetch_add(1, Ordering::Relaxed)
    ));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging)
        .map_err(|error| format!("create {}: {error}", staging.display()))?;
    file.write_all(contents)
        .map_err(|error| format!("write {}: {error}", staging.display()))?;
    drop(file);
    if let Err(error) = fs::hard_link(&staging, path) {
        let _ = fs::remove_file(&staging);
        return Err(format!("publish {}: {error}", path.display()));
    }
    let _ = fs::remove_file(staging);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_execution::lifecycle::AttemptId;

    fn execution_id(lo: i64) -> QueryExecutionId {
        QueryExecutionId::new(
            crate::query_execution::contract::QueryId::new(7, lo),
            AttemptId::new(1).expect("attempt"),
        )
        .expect("execution id")
    }

    #[test]
    fn scoped_fault_only_consumes_exact_execution_and_backend_identity() {
        let root = std::env::temp_dir().join(format!(
            "novarocks-lifecycle-scope-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("create scope root");
        let arm = arm_path(&root, 1, QueryLifecycleFaultKind::InitAckDrop);
        fs::write(&arm, "token=abc-123\nbackend_index=1\n").expect("write arm");
        let expected = bind_armed_fault(
            &root,
            QueryLifecycleFaultKind::InitAckDrop,
            execution_id(9),
            1,
            17,
            23,
        )
        .expect("bind")
        .expect("armed");

        assert!(
            claim_matching_fault(
                &root,
                QueryLifecycleFaultKind::InitAckDrop,
                execution_id(10),
                1,
                17,
                23,
            )
            .expect("mismatch does not error")
            .is_none()
        );
        assert!(trigger_path(&root, 1, QueryLifecycleFaultKind::InitAckDrop).exists());
        assert_eq!(
            claim_matching_fault(
                &root,
                QueryLifecycleFaultKind::InitAckDrop,
                execution_id(9),
                1,
                17,
                23,
            )
            .expect("claim"),
            Some(expected)
        );
        assert!(!trigger_path(&root, 1, QueryLifecycleFaultKind::InitAckDrop).exists());
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn observed_fault_remains_armed_for_the_start_retry() {
        let root = std::env::temp_dir().join(format!(
            "novarocks-lifecycle-observe-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("create scope root");
        let arm = arm_path(&root, 1, QueryLifecycleFaultKind::StartAckSuppress);
        fs::write(&arm, "token=retry-ack\nbackend_index=1\n").expect("write arm");
        let expected = bind_armed_fault(
            &root,
            QueryLifecycleFaultKind::StartAckSuppress,
            execution_id(9),
            1,
            17,
            23,
        )
        .expect("bind")
        .expect("armed");

        for _ in 0..2 {
            assert_eq!(
                observe_matching_fault(
                    &root,
                    QueryLifecycleFaultKind::StartAckSuppress,
                    execution_id(9),
                    1,
                    17,
                    23,
                )
                .expect("observe"),
                Some(expected.clone())
            );
        }
        assert!(trigger_path(&root, 1, QueryLifecycleFaultKind::StartAckSuppress).exists());
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn stage_prepare_fault_waits_for_a_batch_with_the_requested_ordinal() {
        let root = std::env::temp_dir().join(format!(
            "novarocks-stage-prepare-fault-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("create scope root");
        let trigger = root.join("stage-prepare-fail.trigger");
        fs::write(&trigger, "token=stage-ordinal\nordinal=2\n").expect("write trigger");

        assert!(
            claim_stage_prepare_failure(&root, 0)
                .expect("empty batch check")
                .is_none()
        );
        assert!(trigger.exists());
        assert!(
            claim_stage_prepare_failure(&root, 1)
                .expect("short batch check")
                .is_none()
        );
        assert!(trigger.exists());
        assert_eq!(
            claim_stage_prepare_failure(&root, 2)
                .expect("eligible batch check")
                .expect("eligible batch claims fault")
                .ordinal,
            2
        );
        assert!(!trigger.exists());
        fs::remove_dir_all(root).expect("cleanup");
    }
}
