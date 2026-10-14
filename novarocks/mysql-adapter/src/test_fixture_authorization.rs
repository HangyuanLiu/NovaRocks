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

use novarocks_failpoint::scope::{ActionClaim, ControlCancellation, GrantHandle, ProcessBinding};
use novarocks_types::FrontendProcessId;
use serde::Deserialize;
use std::{io, path::PathBuf};

/// MySQL owns mutual exclusion before either fixture creates transport resources.
pub struct MysqlFixtureAuthorization {
    action: String,
    path: PathBuf,
    nonce: [u8; 16],
    grant: GrantHandle,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Parameters {
    action: String,
    socket: PathBuf,
    nonce_hex: String,
}
impl MysqlFixtureAuthorization {
    pub const DOMAIN: &'static str = "mysql-fixture";
    pub fn from_grants(grants: Vec<GrantHandle>) -> io::Result<Option<Self>> {
        if grants.is_empty() {
            return Ok(None);
        }
        if grants.iter().any(|grant| grant.domain() != Self::DOMAIN) {
            return Err(invalid("MySQL fixture received a foreign domain grant"));
        }
        if grants.len() != 1 {
            return Err(invalid(
                "exact MySQL and Closing pressure grants cannot coexist",
            ));
        }
        let grant = grants.into_iter().next().expect("one grant");
        let (action, path, nonce) = parse_parameters(grant.parameters())?;
        if !matches!(grant.cancellation().process(), ProcessBinding::Frontend(_)) {
            return Err(invalid("MySQL fixture requires an FE process"));
        }
        Ok(Some(Self {
            action,
            path,
            nonce,
            grant,
        }))
    }
    pub fn is_exact_write(&self) -> bool {
        self.action == "exact-write"
    }
    pub fn is_closing_pressure(&self) -> bool {
        self.action == "closing-pressure"
    }
    pub(crate) fn claim(
        self,
        action: &str,
        frontend: FrontendProcessId,
    ) -> io::Result<(PathBuf, [u8; 16], ControlCancellation, ActionClaim)> {
        if self.action != action {
            return Err(invalid("MySQL fixture grant action mismatch"));
        }
        let cancellation = self.grant.cancellation();
        let claim = self.grant.claim_once(ProcessBinding::Frontend(frontend))?;
        Ok((self.path, self.nonce, cancellation, claim))
    }
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn parse_parameters(parameters: &serde_json::Value) -> io::Result<(String, PathBuf, [u8; 16])> {
    let parameters: Parameters = serde_json::from_value(parameters.clone())
        .map_err(|_| invalid("MySQL fixture grant is invalid"))?;
    let supported = match parameters.action.as_str() {
        "exact-write" => cfg!(feature = "mem-1-m07-exact-mysql-write"),
        "closing-pressure" => cfg!(feature = "mem-1-m07-closing-pressure"),
        _ => false,
    };
    if !supported {
        return Err(invalid("MySQL fixture action is not compiled"));
    }
    if !parameters.socket.is_absolute() || parameters.socket.as_os_str().len() > 90 {
        return Err(invalid("MySQL fixture socket path is invalid"));
    }
    let text = parameters.nonce_hex.as_bytes();
    if text.len() != 32
        || !text
            .iter()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
    {
        return Err(invalid(
            "MySQL fixture nonce must be canonical lowercase hex",
        ));
    }
    let mut nonce = [0; 16];
    for (out, pair) in nonce.iter_mut().zip(text.chunks_exact(2)) {
        let digit = |b: u8| {
            if b.is_ascii_digit() {
                b - b'0'
            } else {
                b - b'a' + 10
            }
        };
        *out = digit(pair[0]) * 16 + digit(pair[1]);
    }
    if nonce == [0; 16] {
        return Err(invalid("MySQL fixture nonce must be nonzero"));
    }
    Ok((parameters.action, parameters.socket, nonce))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compiled_fixture_without_grant_cannot_activate() {
        assert!(
            MysqlFixtureAuthorization::from_grants(Vec::new())
                .unwrap()
                .is_none()
        );
    }
    #[test]
    fn nonce_and_unknown_action_refused_without_echoing_input() {
        for nonce in [
            "",
            "00000000000000000000000000000000",
            "0123456789ABCDEF0123456789abcdef",
            "é123456789abcdef0123456789abcdef",
        ] {
            let parameters = serde_json::json!({"action":"exact-write","socket":"/tmp/owned.sock","nonce_hex":nonce});
            let error = parse_parameters(&parameters).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert!(!error.to_string().contains(nonce) || nonce.is_empty());
        }
        let parameters = serde_json::json!({"action":"unknown","socket":"/tmp/owned.sock","nonce_hex":"0123456789abcdef0123456789abcdef"});
        assert!(parse_parameters(&parameters).is_err());
    }
    #[test]
    fn complete_owner_parameters_required() {
        let action = if cfg!(feature = "mem-1-m07-exact-mysql-write") {
            "exact-write"
        } else if cfg!(feature = "mem-1-m07-closing-pressure") {
            "closing-pressure"
        } else {
            return;
        };
        let parameters = serde_json::json!({"action":action,"socket":"/tmp/owned.sock","nonce_hex":"0123456789abcdef0123456789abcdef"});
        let (_, _, nonce) = parse_parameters(&parameters).unwrap();
        assert_eq!(
            nonce,
            [
                1, 35, 69, 103, 137, 171, 205, 239, 1, 35, 69, 103, 137, 171, 205, 239
            ]
        );
        let mut unknown = parameters.clone();
        unknown["extra"] = serde_json::json!(true);
        assert!(parse_parameters(&unknown).is_err());
        let mut relative = parameters;
        relative["socket"] = serde_json::json!("relative.sock");
        assert!(parse_parameters(&relative).is_err());
    }
}

#[cfg(all(test, unix))]
mod session_tests {
    use super::*;
    use novarocks_failpoint::liveness::{PreparedRoleSession, RunnerSession};
    use novarocks_failpoint::session::{
        DECLARATION_VERSION, DomainGrant, SessionDeclaration, SessionRole,
    };
    use std::os::unix::fs::DirBuilderExt;
    #[test]
    fn conflict_precedes_transport_bind_and_closed_launch_cannot_claim() {
        let action = if cfg!(feature = "mem-1-m07-exact-mysql-write") {
            "exact-write"
        } else if cfg!(feature = "mem-1-m07-closing-pressure") {
            "closing-pressure"
        } else {
            return;
        };
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let root = PathBuf::from(format!(
            "/tmp/g2mysql-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&root)
            .unwrap();
        let fixture_path = root.join("fixture.sock");
        let mut declaration = SessionDeclaration {
            schema_version: DECLARATION_VERSION,
            session_id: "mysql-session".into(),
            launch_id: "mysql-launch".into(),
            role: SessionRole::Fe,
            lifetime_millis: 10_000,
            liveness_socket: root.join("live.sock"),
            handshake_secret: "abcdefghijklmnopqrstuvwxyz0123456789".into(),
            domain_grants: vec![DomainGrant {
                domain: MysqlFixtureAuthorization::DOMAIN.into(),
                grant_id: "fixture".into(),
                parameters: serde_json::json!({"action":action,"socket":fixture_path,"nonce_hex":"0123456789abcdef0123456789abcdef"}),
            }],
        };
        let mut foreign = declaration.domain_grants[0].clone();
        foreign.domain = "foreign-domain".into();
        foreign.grant_id = "foreign-grant".into();
        declaration.domain_grants.push(foreign);
        let path = root.join("declaration.json");
        declaration.write_private(&path).unwrap();
        let prepared = PreparedRoleSession::read_and_claim(&path, SessionRole::Fe).unwrap();
        let mut runner = RunnerSession::start(declaration).unwrap();
        let frontend = FrontendProcessId::new_v7();
        let mut role = prepared.bind(ProcessBinding::Frontend(frontend)).unwrap();
        assert!(
            MysqlFixtureAuthorization::from_grants(role.scope().grants_for("foreign-domain"))
                .is_err()
        );
        let grants = role.scope().grants_for(MysqlFixtureAuthorization::DOMAIN);
        let mut duplicate = grants.clone();
        duplicate.extend(grants.clone());
        assert!(MysqlFixtureAuthorization::from_grants(duplicate).is_err());
        assert!(!fixture_path.exists());
        let authorization = MysqlFixtureAuthorization::from_grants(grants)
            .unwrap()
            .unwrap();
        role.close_and_join().unwrap();
        assert!(authorization.claim(action, frontend).is_err());
        assert!(!fixture_path.exists());
        runner.end_and_join().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

/// Shared only by the original fixture and its exact kernel, not a registry.
pub(crate) struct FixtureActionObservation(std::sync::Mutex<(ActionClaim, bool)>);
impl FixtureActionObservation {
    pub(crate) fn new(claim: ActionClaim) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self(std::sync::Mutex::new((claim, false))))
    }
    pub(crate) fn cancellation(&self) -> ControlCancellation {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .0
            .cancellation()
    }
    pub(crate) fn record_hit(&self) -> io::Result<()> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| invalid("MySQL fixture action observation lock failed"))?;
        if !state.1 {
            state.0.record_hit()?;
            state.1 = true;
        }
        Ok(())
    }
}
