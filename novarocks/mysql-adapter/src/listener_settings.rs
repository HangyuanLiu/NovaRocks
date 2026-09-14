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

//! MySQL listener settings that are independent of role composition.

use std::net::{Ipv4Addr, SocketAddr};

pub const DEFAULT_MYSQL_USER: &str = "root";
const DEFAULT_MYSQL_PORT: u16 = 9030;

/// Fully resolved MySQL listener settings.
///
/// Role composition resolves these settings before opening the protocol
/// listener. The protocol server receives an already-ready session factory;
/// it neither reads configuration nor opens an application host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedMysqlListenerSettings {
    bind_addr: SocketAddr,
    user: String,
}

impl ResolvedMysqlListenerSettings {
    pub fn new(bind_addr: SocketAddr, user: impl Into<String>) -> Self {
        Self {
            bind_addr,
            user: user.into(),
        }
    }

    pub fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    pub fn user(&self) -> &str {
        &self.user
    }

    /// Transfers the resolved listener address and authenticated principal to
    /// the protocol listener without exposing mutable settings fields.
    pub fn into_parts(self) -> (SocketAddr, String) {
        (self.bind_addr, self.user)
    }
}

/// Resolves protocol listener settings from already-loaded configuration.
pub fn resolve_mysql_listener_settings(
    configured_port: Option<u16>,
    configured_user: Option<&str>,
    port_override: Option<u16>,
) -> Result<ResolvedMysqlListenerSettings, String> {
    let mysql_port = port_override
        .or(configured_port)
        .unwrap_or(DEFAULT_MYSQL_PORT);
    let user = configured_user.unwrap_or(DEFAULT_MYSQL_USER);
    if user != DEFAULT_MYSQL_USER {
        return Err(format!(
            "standalone server only supports user `{DEFAULT_MYSQL_USER}`, got `{user}`"
        ));
    }
    Ok(ResolvedMysqlListenerSettings::new(
        SocketAddr::from((Ipv4Addr::LOCALHOST, mysql_port)),
        user,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_default_loopback_listener_for_default_user() {
        let settings = resolve_mysql_listener_settings(None, None, None).expect("defaults");
        assert_eq!(
            settings.bind_addr(),
            SocketAddr::from((Ipv4Addr::LOCALHOST, 9030))
        );
        assert_eq!(settings.user(), DEFAULT_MYSQL_USER);
    }

    #[test]
    fn rejects_an_unsupported_configured_user() {
        let error = resolve_mysql_listener_settings(None, Some("alice"), None)
            .expect_err("unsupported user must fail closed");
        assert_eq!(
            error,
            "standalone server only supports user `root`, got `alice`"
        );
    }
}
