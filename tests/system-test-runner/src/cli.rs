use anyhow::{Result, bail};
use novarocks_cluster_harness::LaunchProfile;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cli {
    pub list: bool,
    pub list_default: bool,
    pub only: Vec<String>,
    pub binary: Option<PathBuf>,
    pub compatible_binary: Option<PathBuf>,
    pub other_island_binary: Option<PathBuf>,
    pub config: Option<PathBuf>,
    pub artifact_root: Option<PathBuf>,
    pub cluster_size: usize,
    pub timeout_secs: u64,
    pub launch_profile: LaunchProfile,
    pub uea1_workload_manifest: Option<PathBuf>,
    /// Exact private binding for the optional small HMS correctness preflight.
    pub hms_classification_binding: Option<PathBuf>,
    /// Frozen provenance admission for the explicit original exact MySQL matrix.
    pub exact_mysql_execution_binding: Option<PathBuf>,
    /// Independent new frozen input and neutral-feature admission.
    pub held_native_execution_binding: Option<PathBuf>,
    /// Original stdin/stdout source fences for an independent live collector.
    pub held_live_collector_fences: bool,
}
impl Cli {
    pub fn parse_env() -> Result<Self> {
        Self::parse(std::env::args().skip(1))
    }

    pub fn parse(arguments: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut cli = Self {
            list: false,
            list_default: false,
            only: Vec::new(),
            binary: None,
            compatible_binary: None,
            other_island_binary: None,
            config: None,
            artifact_root: None,
            cluster_size: 3,
            timeout_secs: 300,
            launch_profile: LaunchProfile::FaultScenario,
            uea1_workload_manifest: None,
            hms_classification_binding: None,
            exact_mysql_execution_binding: None,
            held_native_execution_binding: None,
            held_live_collector_fences: false,
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
                "--list-default" => cli.list_default = true,
                "--only" => cli.only.push(value("--only")?),
                "--binary" => cli.binary = Some(PathBuf::from(value("--binary")?)),
                "--compatible-binary" => {
                    cli.compatible_binary = Some(PathBuf::from(value("--compatible-binary")?));
                }
                "--other-island-binary" => {
                    cli.other_island_binary = Some(PathBuf::from(value("--other-island-binary")?));
                }
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
                "--launch-profile" => {
                    cli.launch_profile = value("--launch-profile")?
                        .parse()
                        .map_err(anyhow::Error::msg)?;
                }
                "--exact-mysql-execution-binding" => {
                    if cli.exact_mysql_execution_binding.is_some() {
                        bail!("--exact-mysql-execution-binding must appear exactly once");
                    }
                    cli.exact_mysql_execution_binding =
                        Some(PathBuf::from(value("--exact-mysql-execution-binding")?));
                }
                "--held-native-execution-binding" => {
                    if cli.held_native_execution_binding.is_some() {
                        bail!("--held-native-execution-binding must appear exactly once");
                    }
                    cli.held_native_execution_binding =
                        Some(PathBuf::from(value("--held-native-execution-binding")?));
                }
                "--held-live-collector-fences-v1" => {
                    if cli.held_live_collector_fences {
                        bail!("--held-live-collector-fences-v1 must appear exactly once");
                    }
                    cli.held_live_collector_fences = true;
                }
                "--hms-classification-binding" => {
                    if cli.hms_classification_binding.is_some() {
                        bail!("--hms-classification-binding must appear exactly once");
                    }
                    cli.hms_classification_binding =
                        Some(PathBuf::from(value("--hms-classification-binding")?));
                }
                "--uea1-workload-manifest" => {
                    cli.uea1_workload_manifest =
                        Some(PathBuf::from(value("--uea1-workload-manifest")?));
                }
                "--help" | "-h" => bail!(Self::usage()),
                _ => bail!("unknown option {argument}\n{}", Self::usage()),
            }
        }
        if cli.held_live_collector_fences && cli.held_native_execution_binding.is_none() {
            bail!("held live source fences require explicit held Native admission");
        }
        if cli.held_native_execution_binding.is_some()
            && (cli.list
                || cli.list_default
                || cli.compatible_binary.is_some()
                || cli.other_island_binary.is_some()
                || cli.uea1_workload_manifest.is_some()
                || cli.exact_mysql_execution_binding.is_some()
                || cli.hms_classification_binding.is_some()
                || cli.launch_profile != LaunchProfile::FaultScenario
                || cli.cluster_size != 3
                || !(cli.only.is_empty()
                    || (cli.only.len() == 1
                        && cli.only[0] == "result-delivery/held-response-late-ack")))
        {
            bail!("held native admission is one exclusive fault-scenario 1FE+3BE case");
        }
        if cli.hms_classification_binding.is_some()
            && (cli.list
                || cli.list_default
                || !cli.only.is_empty()
                || cli.compatible_binary.is_some()
                || cli.other_island_binary.is_some()
                || cli.uea1_workload_manifest.is_some()
                || cli.exact_mysql_execution_binding.is_some())
        {
            bail!("--hms-classification-binding is an exclusive preflight run mode");
        }
        if cli.exact_mysql_execution_binding.is_some()
            && (cli.list
                || cli.list_default
                || cli.compatible_binary.is_some()
                || cli.other_island_binary.is_some()
                || cli.uea1_workload_manifest.is_some()
                || cli.launch_profile != LaunchProfile::FaultScenario
                || cli.cluster_size != 3)
        {
            bail!(
                "--exact-mysql-execution-binding requires explicit fault-scenario 1FE+3BE without alternate modes"
            );
        }
        if cli.list && cli.list_default {
            bail!("--list and --list-default are mutually exclusive");
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
        concat!(
            "usage: novarocks-system-tests [--list | --list-default] [--only <exact-name>]... ",
            "[--binary <path> [--compatible-binary <path>] ",
            "[--other-island-binary <path>] --config <path> ",
            "--artifact-root <path>] [--cluster-size <N>] [--timeout-secs <N>] ",
            "[--launch-profile <fault-scenario|performance>] ",
            "[--uea1-workload-manifest <path>] [--hms-classification-binding <path>] [--exact-mysql-execution-binding <path>] [--held-native-execution-binding <path>] [--held-live-collector-fences-v1]"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_fences_require_unique_exclusive_held_admission_in_both_orders() {
        let fence = "--held-live-collector-fences-v1";
        assert!(Cli::parse([fence.to_owned()]).is_err());
        for args in [
            vec![fence, "--held-native-execution-binding", "actual.json"],
            vec!["--held-native-execution-binding", "actual.json", fence],
        ] {
            assert!(
                Cli::parse(args.iter().map(|s| (*s).to_owned()))
                    .unwrap()
                    .held_live_collector_fences
            );
            for extra in [
                vec![fence],
                vec!["--exact-mysql-execution-binding", "old.json"],
                vec!["--list"],
                vec!["--cluster-size", "1"],
            ] {
                let mut changed = args.clone();
                changed.extend(extra.clone());
                assert!(Cli::parse(changed.iter().map(|s| (*s).to_owned())).is_err());
                let mut changed = extra;
                changed.extend(args.clone());
                assert!(Cli::parse(changed.iter().map(|s| (*s).to_owned())).is_err());
            }
        }
    }

    #[test]
    fn held_execution_binding_is_unique_exclusive_in_both_flag_orders() {
        let base = vec![
            "--held-native-execution-binding".to_owned(),
            "held.json".to_owned(),
        ];
        assert!(Cli::parse(base.clone()).is_ok());
        let mut selected = base.clone();
        selected.extend([
            "--only".into(),
            "result-delivery/held-response-late-ack".into(),
        ]);
        assert!(Cli::parse(selected).is_ok());
        for extra in [
            vec!["--held-native-execution-binding", "duplicate"],
            vec!["--exact-mysql-execution-binding", "old"],
            vec!["--hms-classification-binding", "hms"],
            vec!["--only", "other"],
            vec!["--cluster-size", "1"],
            vec!["--list"],
            vec!["--launch-profile", "performance"],
            vec!["--uea1-workload-manifest", "perf"],
            vec![
                "--only",
                "result-delivery/held-response-late-ack",
                "--only",
                "result-delivery/held-response-late-ack",
            ],
        ] {
            let extra: Vec<String> = extra.into_iter().map(str::to_owned).collect();
            let mut args = base.clone();
            args.extend(extra.clone());
            assert!(Cli::parse(args).is_err());
            let mut args = extra;
            args.extend(base.clone());
            assert!(Cli::parse(args).is_err());
        }
    }

    #[test]
    fn exact_execution_binding_is_explicit_unique_and_topology_fixed() {
        let flag = "--exact-mysql-execution-binding".to_string();
        let args = vec![
            flag.clone(),
            "frozen.json".to_string(),
            "--only".into(),
            "exact-native-resident-cut-1".into(),
        ];
        let cli = Cli::parse(args).unwrap();
        assert_eq!(
            cli.exact_mysql_execution_binding,
            Some(PathBuf::from("frozen.json"))
        );
        for extra in [
            vec![flag.clone(), "duplicate.json".into()],
            vec!["--list".into()],
            vec!["--cluster-size".into(), "1".into()],
            vec!["--launch-profile".into(), "performance".into()],
            vec!["--hms-classification-binding".into(), "hms.json".into()],
            vec!["--uea1-workload-manifest".into(), "perf.json".into()],
        ] {
            let mut args = vec![flag.clone(), "frozen.json".to_string()];
            args.extend(extra);
            assert!(Cli::parse(args).is_err());
        }
    }

    #[test]
    fn defaults_to_three_backends() {
        let cli = Cli::parse(Vec::new()).expect("parse defaults");
        assert_eq!(cli.cluster_size, 3);
        assert_eq!(cli.timeout_secs, 300);
        assert_eq!(cli.launch_profile, LaunchProfile::FaultScenario);
    }

    #[test]
    fn parses_distinct_registry_list_modes() {
        let all = Cli::parse(vec!["--list".to_string()]).expect("list all");
        assert!(all.list && !all.list_default);
        let defaults = Cli::parse(vec!["--list-default".to_string()]).expect("list defaults");
        assert!(!defaults.list && defaults.list_default);
        assert!(Cli::parse(vec!["--list".to_string(), "--list-default".to_string()]).is_err());
    }

    #[test]
    fn only_is_repeatable() {
        let cli = Cli::parse(vec![
            "--only".to_string(),
            "query-lifecycle/mysql-disconnect".to_string(),
            "--only".to_string(),
            "connector/catalog-version-drain".to_string(),
        ])
        .expect("parse repeated selectors");
        assert_eq!(cli.only.len(), 2);
    }

    #[test]
    fn parses_optional_compatibility_island_binaries() {
        let cli = Cli::parse(vec![
            "--compatible-binary".to_string(),
            "/tmp/compatible".to_string(),
            "--other-island-binary".to_string(),
            "/tmp/other-island".to_string(),
        ])
        .expect("parse optional island binaries");
        assert_eq!(
            cli.compatible_binary,
            Some(PathBuf::from("/tmp/compatible"))
        );
        assert_eq!(
            cli.other_island_binary,
            Some(PathBuf::from("/tmp/other-island"))
        );
    }

    #[test]
    fn parses_performance_profile_and_manifest() {
        let cli = Cli::parse(vec![
            "--launch-profile".to_string(),
            "performance".to_string(),
            "--uea1-workload-manifest".to_string(),
            "/tmp/workloads.json".to_string(),
        ])
        .expect("parse performance inputs");
        assert_eq!(cli.launch_profile, LaunchProfile::Performance);
        assert_eq!(
            cli.uea1_workload_manifest,
            Some(PathBuf::from("/tmp/workloads.json"))
        );
    }
}

#[cfg(test)]
mod hms_preflight_cli_tests {
    use super::*;

    fn parse(values: &[&str]) -> Result<Cli> {
        Cli::parse(values.iter().map(|value| (*value).to_string()))
    }

    #[test]
    fn hms_binding_is_exclusive_and_cannot_be_repeated() {
        assert!(parse(&["--hms-classification-binding", "/private/input.json"]).is_ok());
        assert!(
            parse(&[
                "--hms-classification-binding",
                "/private/input.json",
                "--hms-classification-binding",
                "/other.json"
            ])
            .is_err()
        );
        for option in [
            "--only",
            "--compatible-binary",
            "--other-island-binary",
            "--uea1-workload-manifest",
        ] {
            assert!(
                parse(&[
                    "--hms-classification-binding",
                    "/private/input.json",
                    option,
                    "value"
                ])
                .is_err()
            );
        }
        assert!(
            parse(&[
                "--hms-classification-binding",
                "/private/input.json",
                "--list"
            ])
            .is_err()
        );
    }
}
