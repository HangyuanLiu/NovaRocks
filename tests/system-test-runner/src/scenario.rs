use anyhow::{Result, bail};
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, CrossProcessServerHandle, ServerHandle,
};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub trait Scenario: Send + Sync {
    fn name(&self) -> &'static str;

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
    pub initial_backend_seeds: Option<Vec<usize>>,
}

pub struct ScenarioContext {
    name: &'static str,
    handle: CrossProcessServerHandle,
    scenario_root: PathBuf,
    deadline: Instant,
    actions: Vec<String>,
}

impl ScenarioContext {
    pub fn new(
        name: &'static str,
        handle: CrossProcessServerHandle,
        scenario_root: PathBuf,
        timeout: Duration,
    ) -> Self {
        Self {
            name,
            handle,
            scenario_root,
            deadline: Instant::now() + timeout,
            actions: Vec::new(),
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
}
