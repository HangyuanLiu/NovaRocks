use anyhow::{Result, bail};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cli {
    pub list: bool,
    pub only: Vec<String>,
    pub binary: Option<PathBuf>,
    pub config: Option<PathBuf>,
    pub artifact_root: Option<PathBuf>,
    pub cluster_size: usize,
    pub timeout_secs: u64,
}

impl Cli {
    pub fn parse_env() -> Result<Self> {
        Self::parse(std::env::args().skip(1))
    }

    pub fn parse(arguments: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut cli = Self {
            list: false,
            only: Vec::new(),
            binary: None,
            config: None,
            artifact_root: None,
            cluster_size: 3,
            timeout_secs: 300,
        };
        let mut arguments = arguments.into_iter();
        while let Some(argument) = arguments.next() {
            let mut value = |flag: &str| {
                arguments
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("{flag} requires a value"))
            };
            match argument.as_str() {
                "--list" => cli.list = true,
                "--only" => cli.only.push(value("--only")?),
                "--binary" => cli.binary = Some(PathBuf::from(value("--binary")?)),
                "--config" => cli.config = Some(PathBuf::from(value("--config")?)),
                "--artifact-root" => {
                    cli.artifact_root = Some(PathBuf::from(value("--artifact-root")?));
                }
                "--cluster-size" => {
                    cli.cluster_size = value("--cluster-size")?.parse().map_err(|_| {
                        anyhow::anyhow!("--cluster-size must be a positive integer")
                    })?;
                }
                "--timeout-secs" => {
                    cli.timeout_secs = value("--timeout-secs")?.parse().map_err(|_| {
                        anyhow::anyhow!("--timeout-secs must be a positive integer")
                    })?;
                }
                "--help" | "-h" => bail!(Self::usage()),
                _ => bail!("unknown option {argument}\n{}", Self::usage()),
            }
        }
        if cli.cluster_size == 0 {
            bail!("--cluster-size must be >= 1");
        }
        if cli.timeout_secs == 0 {
            bail!("--timeout-secs must be >= 1");
        }
        Ok(cli)
    }

    pub const fn usage() -> &'static str {
        "usage: novarocks-system-tests [--list] [--only <exact-name>]... \\\n+         [--binary <path> --config <path> --artifact-root <path>] \\\n+         [--cluster-size <N>] [--timeout-secs <N>]"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_three_backends() {
        let cli = Cli::parse(Vec::new()).expect("parse defaults");
        assert_eq!(cli.cluster_size, 3);
        assert_eq!(cli.timeout_secs, 300);
    }

    #[test]
    fn only_is_repeatable() {
        let cli = Cli::parse(vec![
            "--only".to_string(),
            "query-lifecycle/mysql-disconnect".to_string(),
            "--only".to_string(),
            "connector/generation-replacement".to_string(),
        ])
        .expect("parse repeated selectors");
        assert_eq!(cli.only.len(), 2);
    }
}
