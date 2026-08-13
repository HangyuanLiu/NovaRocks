use crate::cli::Cli;
use anyhow::{Context, Result, bail};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct RunnerConfig {
    pub binary: PathBuf,
    pub base_config_path: PathBuf,
    pub artifact_root: PathBuf,
    pub cluster_size: usize,
    pub timeout: Duration,
}

impl RunnerConfig {
    pub fn from_cli(cli: &Cli) -> Result<Self> {
        let binary = cli
            .binary
            .clone()
            .context("--binary is required when running scenarios")?;
        if !binary.is_file() {
            bail!("--binary path does not name a file: {}", binary.display());
        }
        let base_config_path = cli
            .config
            .clone()
            .context("--config is required when running scenarios")?;
        if !base_config_path.is_file() {
            bail!(
                "--config path does not name a file: {}",
                base_config_path.display()
            );
        }
        let artifact_root = cli
            .artifact_root
            .clone()
            .context("--artifact-root is required when running scenarios")?;
        Ok(Self {
            binary,
            base_config_path,
            artifact_root,
            cluster_size: cli.cluster_size,
            timeout: Duration::from_secs(cli.timeout_secs),
        })
    }
}
