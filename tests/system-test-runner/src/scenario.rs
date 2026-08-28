use anyhow::{Result, bail};
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, CrossProcessServerHandle,
    NativeTrustFixture, ServerHandle,
};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub trait Scenario: Send + Sync {
    fn name(&self) -> &'static str;

    /// External-fixture scenarios remain discoverable and runnable by exact
    /// selector, but do not turn the normal no-Docker system baseline into a
    /// Docker requirement.
    fn is_explicit_stage(&self) -> bool {
        false
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        CrossProcessChildEnvironment::default()
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        Ok(ScenarioLaunchConfig {
            child_environment: self.child_environment(),
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()>;
}

#[derive(Debug, Clone, Default)]
pub struct ScenarioLaunchConfig {
    pub child_environment: CrossProcessChildEnvironment,
    pub config_overlay: CrossProcessConfigOverlay,
    pub native_trust_fixture: NativeTrustFixture,
}

pub struct ScenarioContext {
    name: &'static str,
    handle: CrossProcessServerHandle,
    scenario_root: PathBuf,
    deadline: Instant,
    actions: Vec<String>,
    binary: PathBuf,
    base_config_path: PathBuf,
    cluster_size: usize,
    startup_timeout: Duration,
}

impl ScenarioContext {
    pub fn new(
        name: &'static str,
        handle: CrossProcessServerHandle,
        scenario_root: PathBuf,
        timeout: Duration,
        binary: PathBuf,
        base_config_path: PathBuf,
        cluster_size: usize,
    ) -> Self {
        Self {
            name,
            handle,
            scenario_root,
            deadline: Instant::now() + timeout,
            actions: Vec::new(),
            binary,
            base_config_path,
            cluster_size,
            startup_timeout: timeout,
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn handle(&mut self) -> &mut CrossProcessServerHandle {
        &mut self.handle
    }

    pub fn mysql_port(&self) -> u16 {
        self.handle.runtime().fe_mysql_port
    }

    pub fn mysql_user(&self) -> &str {
        self.handle.mysql_user()
    }

    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn remaining(&self, operation: &str) -> Result<Duration> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            bail!("scenario {} timed out before {operation}", self.name);
        }
        Ok(remaining)
    }

    pub fn action(&mut self, action: impl Into<String>) {
        self.actions.push(action.into());
    }

    pub fn actions(&self) -> &[String] {
        &self.actions
    }

    pub fn runtime_dir(&self) -> &Path {
        self.handle.runtime_dir()
    }

    pub fn scenario_root(&self) -> &Path {
        &self.scenario_root
    }

    pub fn diagnostics(&self) -> String {
        self.handle.diagnostics()
    }

    pub fn retain_artifacts(&mut self) {
        self.handle.retain_runtime_artifacts();
    }

    pub fn shutdown(&mut self) -> Result<()> {
        ServerHandle::shutdown(&mut self.handle)
    }

    /// Launch a peer native cluster for a focused multi-cluster scenario.
    /// The peer shares the runner binary and base configuration, while the
    /// caller owns an isolated runtime directory and its explicit overlay.
    pub fn launch_peer_cluster(
        &self,
        name: &str,
        launch_config: ScenarioLaunchConfig,
    ) -> Result<CrossProcessServerHandle> {
        let runtime_root = self.scenario_root.join(name);
        CrossProcessServerHandle::launch(novarocks_cluster_harness::CrossProcessClusterOptions {
            binary: self.binary.clone(),
            base_config_path: self.base_config_path.clone(),
            runtime_root,
            cluster_size: self.cluster_size,
            query_lifecycle_faults_enabled: true,
            cleanup_faults_enabled: true,
            startup_timeout: self.startup_timeout,
            child_environment: launch_config.child_environment,
            config_overlay: launch_config.config_overlay,
            native_trust_fixture: launch_config.native_trust_fixture,
        })
    }
}
