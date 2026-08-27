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

//! Server-owned construction of immutable Native trust startup facts.

use std::{fs, sync::Arc};

use anyhow::{Context, Result, bail};
use novarocks_native_trust::{
    AutomaticTlsMaterial, DeploymentId, NativeCallerSubject, NativeTlsMaterial,
    NativeTransportMode, NativeTrust, PemTransportMaterial, ValidatedSharedSecret,
};
use novarocks_types::{ClusterRole, NativeEndpoint};

use crate::{
    app_config::{NativeTrustConfig, NovaRocksConfig},
    network,
};

const MAX_PEM_FILE_BYTES: u64 = 1024 * 1024;

#[derive(Clone)]
pub enum NativeTrustTransport {
    Plaintext,
    Automatic(AutomaticTlsMaterial),
    Pem(NativeTlsMaterial),
}

impl NativeTrustTransport {
    pub fn mode(&self) -> NativeTransportMode {
        match self {
            Self::Plaintext => NativeTransportMode::Disabled,
            Self::Automatic(_) => NativeTransportMode::Automatic,
            Self::Pem(_) => NativeTransportMode::Pem,
        }
    }
}

#[derive(Clone)]
pub struct NativeTrustSnapshot {
    advertised_endpoint: NativeEndpoint,
    trust: Arc<NativeTrust>,
    transport: NativeTrustTransport,
}

impl NativeTrustSnapshot {
    pub fn advertised_endpoint(&self) -> &NativeEndpoint {
        &self.advertised_endpoint
    }

    pub fn trust(&self) -> &Arc<NativeTrust> {
        &self.trust
    }

    pub fn transport(&self) -> &NativeTrustTransport {
        &self.transport
    }
}

pub fn build_role_native_trust_snapshot(
    role: ClusterRole,
    config: &NovaRocksConfig,
) -> Result<NativeTrustSnapshot> {
    let source = config
        .native_trust
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("missing required [native_trust] configuration"))?;
    let advertised_endpoint = network::standalone_native_advertise_endpoint(
        &config.server.host,
        &config.server.priority_networks,
        &config.cluster.advertise_host,
        config.cluster.advertise_port,
        config.server.grpc_port,
    )
    .map_err(anyhow::Error::msg)
    .context("resolve native advertised endpoint")?;
    let deployment_id = DeploymentId::parse(source.deployment_id.clone())
        .map_err(anyhow::Error::msg)
        .context("validate native trust deployment id")?;
    let shared_secret = ValidatedSharedSecret::new(source.shared_secret.clone())
        .map_err(anyhow::Error::msg)
        .context("validate native trust shared secret")?;
    let role_name = match role {
        ClusterRole::Fe => "fe",
        ClusterRole::Be => "be",
    };
    let subject = NativeCallerSubject::parse(format!("{role_name}@{advertised_endpoint}"))
        .map_err(anyhow::Error::msg)
        .context("construct native caller subject")?;
    let trust = Arc::new(NativeTrust::new(
        deployment_id,
        shared_secret,
        subject,
        source.transport.mode,
    ));
    let transport = transport_from_source(source, trust.as_ref(), &advertised_endpoint)?;
    Ok(NativeTrustSnapshot {
        advertised_endpoint,
        trust,
        transport,
    })
}

pub fn ensure_all_in_one_trust_homogeneous(
    fe: &NovaRocksConfig,
    be: &NovaRocksConfig,
) -> Result<()> {
    let fe = fe
        .native_trust
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("FE missing [native_trust]"))?;
    let be = be
        .native_trust
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("BE missing [native_trust]"))?;
    if fe.deployment_id != be.deployment_id
        || fe.shared_secret != be.shared_secret
        || fe.transport.mode != be.transport.mode
    {
        bail!(
            "all-in-one native trust configuration mismatch: deployment identity, shared secret, and transport mode must match"
        );
    }
    Ok(())
}

fn transport_from_source(
    source: &NativeTrustConfig,
    trust: &NativeTrust,
    endpoint: &NativeEndpoint,
) -> Result<NativeTrustTransport> {
    match source.transport.mode {
        NativeTransportMode::Disabled => Ok(NativeTrustTransport::Plaintext),
        NativeTransportMode::Automatic => {
            AutomaticTlsMaterial::for_endpoint(trust.clone(), endpoint.clone())
                .map(NativeTrustTransport::Automatic)
                .map_err(anyhow::Error::msg)
                .context("construct automatic native TLS material")
        }
        NativeTransportMode::Pem => {
            let certificate_chain = read_pem(
                source.transport.certificate_chain_path.as_deref(),
                "certificate chain",
            )?;
            let private_key =
                read_pem(source.transport.private_key_path.as_deref(), "private key")?;
            let trust_roots =
                read_pem(source.transport.trust_roots_path.as_deref(), "trust roots")?;
            PemTransportMaterial::new(certificate_chain, private_key, trust_roots)
                .and_then(|material| material.tls_material())
                .map(NativeTrustTransport::Pem)
                .map_err(anyhow::Error::msg)
                .context("parse native TLS PEM material")
        }
    }
}

fn read_pem(path: Option<&std::path::Path>, label: &str) -> Result<Vec<u8>> {
    let path = path.ok_or_else(|| anyhow::anyhow!("missing native TLS {label} path"))?;
    let metadata = fs::metadata(path)
        .with_context(|| format!("read native TLS {label} metadata: {}", path.display()))?;
    if metadata.len() > MAX_PEM_FILE_BYTES {
        bail!("native TLS {label} file exceeds {MAX_PEM_FILE_BYTES} byte limit");
    }
    fs::read(path).with_context(|| format!("read native TLS {label}: {}", path.display()))
}
