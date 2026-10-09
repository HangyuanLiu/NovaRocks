use crate::cli::Cli;
use crate::config::RunnerConfig;
use crate::scenario::{
    Scenario, ScenarioContext, ScenarioEvidenceOutcome, resolve_backend_binaries, resolve_binary,
};
use crate::scenarios;
use anyhow::{Context, Result, bail};
use novarocks_cluster_harness::{CrossProcessClusterOptions, CrossProcessServerHandle};
use std::fs;

pub fn run(cli: Cli) -> Result<()> {
    let scenarios = scenarios::all();
    if cli.list || cli.list_default {
        for scenario in list_scenarios(&scenarios, cli.list_default)? {
            println!("{}", scenario.name());
        }
        return Ok(());
    }
    if cli.list_default {
        for scenario in select(&scenarios, &[])? {
            println!("{}", scenario.name());
        }
        return Ok(());
    }
    let config = RunnerConfig::from_cli(&cli)?;
    if let Some(binding) = &cli.hms_classification_binding {
        if config.cluster_size != 3
            || config.launch_profile != novarocks_cluster_harness::LaunchProfile::FaultScenario
        {
            bail!("HMS classification preflight requires fault-scenario 1FE+3BE");
        }
        let scenario =
            scenarios::hms_classification_preflight::HmsClassificationPreflight::from_binding(
                binding,
                &config.base_config_path,
            )?;
        return run_one(&scenario, &config);
    }
    let selected = select(&scenarios, &cli.only)?;
    if selected.is_empty() {
        bail!("no system scenarios are registered");
    }
    for scenario in &selected {
        scenario.validate_runner_inputs(
            config.launch_profile,
            config.uea1_workload_manifest.as_deref(),
        )?;
    }
    for scenario in selected {
        run_one(scenario, &config)?;
    }
    Ok(())
}

fn list_scenarios(
    scenarios: &[Box<dyn Scenario>],
    defaults_only: bool,
) -> Result<Vec<&dyn Scenario>> {
    if defaults_only {
        select(scenarios, &[])
    } else {
        Ok(scenarios.iter().map(|scenario| scenario.as_ref()).collect())
    }
}

fn select<'a>(
    scenarios: &'a [Box<dyn Scenario>],
    only: &[String],
) -> Result<Vec<&'a dyn Scenario>> {
    if only.is_empty() {
        return Ok(scenarios
            .iter()
            .filter(|scenario| !scenario.is_explicit_stage())
            .map(|scenario| scenario.as_ref())
            .collect());
    }
    let mut selected = Vec::with_capacity(only.len());
    for requested in only {
        let scenario = scenarios
            .iter()
            .find(|scenario| scenario.name() == requested)
            .map(|scenario| scenario.as_ref())
            .with_context(|| format!("unknown system scenario {requested}"))?;
        selected.push(scenario);
    }
    Ok(selected)
}

fn run_one(scenario: &dyn Scenario, config: &RunnerConfig) -> Result<()> {
    let scenario_root = config.artifact_root.join(scenario.name().replace('/', "-"));
    fs::create_dir_all(&scenario_root)
        .with_context(|| format!("create scenario artifact root {}", scenario_root.display()))?;
    let launch_config = match scenario.launch_config(&scenario_root) {
        Ok(config) => config,
        Err(error) => {
            return match scenario.teardown() {
                Ok(()) => Err(error).with_context(|| {
                    format!("prepare launch configuration for {}", scenario.name())
                }),
                Err(teardown) => Err(anyhow::anyhow!(
                    "prepare launch configuration for {} failed: {error:#}; fixture teardown failed: {teardown:#}",
                    scenario.name()
                )),
            };
        }
    };
    // The explicit FE-child fixture pair is the existing activation contract.
    // Capture only once before launch; serial startup consumes this original budget.
    let exact_mysql_clock = match crate::scenario::ExactMysqlPrelaunchClock::capture(
        &launch_config.child_environment,
    ) {
        Ok(clock) => clock,
        Err(error) => {
            return finish_exact_mysql_scenario(Some(error), Ok(()), scenario.teardown());
        }
    };
    let uea1_preparation_diagnostic_secret = launch_config
        .child_environment
        .fe
        .get("NOVAROCKS_PREPARATION_DIAGNOSTIC_SECRET")
        .cloned();
    let preparation = (|| -> Result<_> {
        if exact_mysql_clock.is_some() {
            crate::scenario::ExactMysqlPrelaunchClock::validate_launch(
                config.launch_profile,
                config.cluster_size,
            )?;
        }
        let fe_binary = resolve_binary(
            launch_config.binary_layout.frontend,
            config.compatible_binary.as_ref(),
            config.other_island_binary.as_ref(),
        )?;
        let be_binaries = resolve_backend_binaries(
            &launch_config.binary_layout.backends,
            &config.binary,
            config.compatible_binary.as_ref(),
            config.other_island_binary.as_ref(),
            config.cluster_size,
        )?;
        Ok((fe_binary, be_binaries))
    })();
    let (fe_binary, be_binaries) = match preparation {
        Ok(binaries) => binaries,
        Err(error) if exact_mysql_clock.is_some() => {
            return finish_exact_mysql_scenario(Some(error), Ok(()), scenario.teardown());
        }
        Err(error) => return Err(error),
    };
    let cluster_options = CrossProcessClusterOptions {
        binary: config.binary.clone(),
        fe_binary,
        be_binaries,
        expected_eligible_backend_count: launch_config.expected_eligible_backend_count,
        base_config_path: config.base_config_path.clone(),
        // Reports and the retained scenario evidence live at `scenario_root`.
        // The harness may remove only this disposable child directory after a
        // successful scenario, while a failure keeps it for log inspection.
        runtime_root: scenario_root.join("runtime"),
        cluster_size: config.cluster_size,
        launch_profile: config.launch_profile,
        startup_timeout: config.timeout,
        child_environment: launch_config.child_environment,
        config_overlay: launch_config.config_overlay,
        native_trust_fixture: launch_config.native_trust_fixture,
    };
    let handle = match launch_config.native_root_reply_fault {
        Some(root_fault) => CrossProcessServerHandle::launch_with_native_root_reply_fault(
            cluster_options,
            launch_config.native_fault_proxies,
            root_fault,
        ),
        None => CrossProcessServerHandle::launch_with_native_fault_proxies(
            cluster_options,
            launch_config.native_fault_proxies,
        ),
    }
    .with_context(|| format!("launch system scenario {}", scenario.name()));
    let handle = match handle {
        Ok(handle) => handle,
        Err(error) => {
            if exact_mysql_clock.is_some() {
                return finish_exact_mysql_scenario(Some(error), Ok(()), scenario.teardown());
            }
            return match scenario.teardown() {
                Ok(()) => Err(error),
                Err(teardown) => Err(anyhow::anyhow!(
                    "{error:#}; fixture teardown failed: {teardown:#}"
                )),
            };
        }
    };
    let mut context = ScenarioContext::new(
        scenario.name(),
        handle,
        scenario_root,
        config.timeout,
        config.binary.clone(),
        config.compatible_binary.clone(),
        config.other_island_binary.clone(),
        config.base_config_path.clone(),
        config.cluster_size,
        config.launch_profile,
        config.uea1_workload_manifest.clone(),
        uea1_preparation_diagnostic_secret,
        exact_mysql_clock,
    );
    context.action("cluster launched and topology barrier passed");
    let result = scenario.run(&mut context);
    if let Err(error) = &result {
        context.retain_artifacts();
        let evidence_path = match context.write_evidence(ScenarioEvidenceOutcome::Failed) {
            Ok(path) => path.display().to_string(),
            Err(_) if exact_mysql_clock.is_some() => {
                "unavailable (exact fixture evidence write failed)".to_string()
            }
            Err(evidence_error) => format!("unavailable ({evidence_error:#})"),
        };
        eprintln!(
            "scenario={} failed; actions={:?}; runtime_dir={}; evidence={}; diagnostics={}",
            context.name(),
            context.actions(),
            context.runtime_dir().display(),
            evidence_path,
            context.diagnostics()
        );
        let launch_profile = match config.launch_profile {
            novarocks_cluster_harness::LaunchProfile::FaultScenario => "fault-scenario",
            novarocks_cluster_harness::LaunchProfile::Performance => "performance",
        };
        let manifest = config
            .uea1_workload_manifest
            .as_ref()
            .map(|path| format!(" --uea1-workload-manifest {}", path.display()))
            .unwrap_or_default();
        eprintln!(
            "rerun: novarocks-system-tests --only {} --binary {} --config {} --artifact-root {} --cluster-size {} --timeout-secs {} --launch-profile {}{}",
            context.name(),
            config.binary.display(),
            config.base_config_path.display(),
            config.artifact_root.display(),
            config.cluster_size,
            config.timeout.as_secs(),
            launch_profile,
            manifest,
        );
        let cluster_cleanup = if exact_mysql_clock.is_some() {
            context.shutdown_exact_mysql_fixture()
        } else {
            context.shutdown()
        };
        let fixture_cleanup = scenario.teardown();
        if exact_mysql_clock.is_some() {
            return finish_exact_mysql_scenario(
                Some(
                    result
                        .err()
                        .expect("failed scenario retains its original error"),
                ),
                cluster_cleanup,
                fixture_cleanup,
            );
        }
        return match (cluster_cleanup, fixture_cleanup) {
            (Ok(()), Ok(())) => Err(anyhow::anyhow!(
                "scenario {} failed: {error:#}",
                context.name()
            )),
            (Err(cluster), Ok(())) => Err(anyhow::anyhow!(
                "scenario {} failed: {error:#}; cluster cleanup failed: {cluster:#}",
                context.name()
            )),
            (Ok(()), Err(fixture)) => Err(anyhow::anyhow!(
                "scenario {} failed: {error:#}; fixture teardown failed: {fixture:#}",
                context.name()
            )),
            (Err(cluster), Err(fixture)) => Err(anyhow::anyhow!(
                "scenario {} failed: {error:#}; cluster cleanup failed: {cluster:#}; fixture teardown failed: {fixture:#}",
                context.name()
            )),
        };
    }
    context.action("scenario assertions passed");
    let cluster_cleanup = if exact_mysql_clock.is_some() {
        context.shutdown_exact_mysql_fixture()
    } else {
        context.shutdown()
    }
    .with_context(|| format!("cleanup system scenario {}", context.name()));
    let fixture_cleanup = scenario.teardown();
    if exact_mysql_clock.is_some() {
        finish_exact_mysql_scenario(None, cluster_cleanup, fixture_cleanup)?;
    } else {
        match (cluster_cleanup, fixture_cleanup) {
            (Ok(()), Ok(())) => {}
            (Err(cluster), Ok(())) => return Err(cluster),
            (Ok(()), Err(fixture)) => {
                return Err(fixture)
                    .with_context(|| format!("teardown fixture for {}", scenario.name()));
            }
            (Err(cluster), Err(fixture)) => {
                return Err(anyhow::anyhow!(
                    "{cluster:#}; fixture teardown failed: {fixture:#}"
                ));
            }
        }
    }
    context.action("cluster and fixture cleanup passed");
    let evidence_path = context
        .write_evidence(ScenarioEvidenceOutcome::Passed)
        .with_context(|| format!("write passing scenario evidence for {}", context.name()))?;
    println!(
        "scenario={} PASS evidence={}",
        scenario.name(),
        evidence_path.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_listing_preserves_default_selection_and_explicit_opt_in() {
        let scenarios = crate::scenarios::all();
        let all = list_scenarios(&scenarios, false).expect("list all scenarios");
        let defaults = list_scenarios(&scenarios, true).expect("list default scenarios");
        let names = |selected: &[&dyn Scenario]| {
            selected
                .iter()
                .map(|scenario| scenario.name())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            names(&defaults),
            names(&select(&scenarios, &[]).expect("default selection"))
        );
        assert_eq!(
            names(&all),
            scenarios
                .iter()
                .map(|scenario| scenario.name())
                .collect::<Vec<_>>()
        );
        assert!(!defaults.is_empty());
        assert!(
            defaults
                .iter()
                .all(|scenario| !scenario.is_explicit_stage())
        );
        let explicit = all
            .iter()
            .filter(|scenario| scenario.is_explicit_stage())
            .collect::<Vec<_>>();
        assert!(!explicit.is_empty());
        assert_eq!(all.len(), defaults.len() + explicit.len());
        for scenario in explicit {
            assert!(!names(&defaults).contains(&scenario.name()));
            let selected =
                select(&scenarios, &[scenario.name().to_string()]).expect("explicit opt-in");
            assert_eq!(names(&selected), vec![scenario.name()]);
        }
    }

    #[test]
    fn empty_registry_rejects_unknown_selector() {
        assert!(select(&[], &["missing".to_string()]).is_err());
    }

    #[test]
    fn default_selection_excludes_external_fixture_scenarios() {
        let scenarios = crate::scenarios::all();
        let selected = select(&scenarios, &[]).expect("select default system baseline");
        assert!(selected.iter().all(|scenario| {
            scenario.name() != "frontend-lifecycle/blue-green-session-cutover"
                && scenario.name() != "query-concurrency/uea4a1-b0-performance"
        }));
        assert!(
            select(
                &scenarios,
                &["frontend-lifecycle/blue-green-session-cutover".to_string()]
            )
            .expect("select explicit blue/green scenario")
            .iter()
            .any(|scenario| scenario.name() == "frontend-lifecycle/blue-green-session-cutover")
        );
    }

    #[test]
    fn default_selection_accepts_the_default_launch_inputs() {
        let scenarios = crate::scenarios::all();
        for scenario in select(&scenarios, &[]).expect("select default system baseline") {
            scenario
                .validate_runner_inputs(
                    novarocks_cluster_harness::LaunchProfile::FaultScenario,
                    None,
                )
                .unwrap_or_else(|error| panic!("default scenario {}: {error:#}", scenario.name()));
        }
    }

    #[test]
    fn performance_preflight_rejects_missing_profile_and_manifest() {
        let scenarios = crate::scenarios::all();
        let selected = select(
            &scenarios,
            &["performance/uea1-short-concurrent".to_string()],
        )
        .expect("select performance scenario");
        let scenario = selected[0];
        assert!(
            scenario
                .validate_runner_inputs(
                    novarocks_cluster_harness::LaunchProfile::FaultScenario,
                    None
                )
                .is_err()
        );
        assert!(
            scenario
                .validate_runner_inputs(novarocks_cluster_harness::LaunchProfile::Performance, None)
                .is_err()
        );
    }

    #[test]
    fn startup_baseline_requires_the_performance_profile_before_launch() {
        let scenarios = crate::scenarios::all();
        assert!(
            select(&scenarios, &[])
                .expect("select default scenarios")
                .iter()
                .all(|scenario| scenario.name() != "task-execution/uea5d-startup-baseline"),
            "the 1,000-query formal baseline must remain an explicit stage"
        );
        for name in [
            "task-execution/startup-baseline",
            "task-execution/uea5d-startup-baseline",
        ] {
            let selected =
                select(&scenarios, &[name.to_string()]).expect("select startup baseline scenario");
            let scenario = selected[0];
            assert!(
                scenario
                    .validate_runner_inputs(
                        novarocks_cluster_harness::LaunchProfile::FaultScenario,
                        None
                    )
                    .is_err()
            );
            scenario
                .validate_runner_inputs(novarocks_cluster_harness::LaunchProfile::Performance, None)
                .expect("performance profile is accepted before startup");
        }
    }
}

// Preserve the actual primary and cleanup sources for this explicit fixture only.
struct ExactMysqlScenarioFailure {
    primary: Option<anyhow::Error>,
    cluster: Option<anyhow::Error>,
    fixture: Option<anyhow::Error>,
}
impl std::fmt::Display for ExactMysqlScenarioFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "exact MySQL scenario failed: primary={} role_cleanup={} fixture_cleanup={}",
            self.primary.is_some(),
            self.cluster.is_some(),
            self.fixture.is_some()
        )
    }
}
impl std::fmt::Debug for ExactMysqlScenarioFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for ExactMysqlScenarioFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.primary
            .as_ref()
            .or(self.cluster.as_ref())
            .or(self.fixture.as_ref())
            .map(|error| error.as_ref())
    }
}
fn finish_exact_mysql_scenario(
    primary: Option<anyhow::Error>,
    cluster: Result<()>,
    fixture: Result<()>,
) -> Result<()> {
    let cluster = cluster.err();
    let fixture = fixture.err();
    if primary.is_none() && cluster.is_none() && fixture.is_none() {
        Ok(())
    } else {
        Err(ExactMysqlScenarioFailure {
            primary,
            cluster,
            fixture,
        }
        .into())
    }
}
#[cfg(all(test, unix))]
mod exact_mysql_cleanup_tests {
    use super::*;
    #[test]
    fn exact_cleanup_keeps_owned_primary_and_secondary_error_sources() {
        let primary = std::fs::File::open("/dev/null/no-original-file").unwrap_err();
        let error = finish_exact_mysql_scenario(
            Some(primary.into()),
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe).into()),
            Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied).into()),
        )
        .unwrap_err();
        let owned = error.downcast_ref::<ExactMysqlScenarioFailure>().unwrap();
        assert!(
            owned
                .primary
                .as_ref()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .is_some()
        );
        assert_eq!(
            owned
                .cluster
                .as_ref()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .kind(),
            std::io::ErrorKind::BrokenPipe
        );
        assert!(owned.fixture.is_some());
        assert!(finish_exact_mysql_scenario(None, Ok(()), Ok(())).is_ok());
    }
}

/// Exact fixture error sources can contain raw child output. Preserve them for
/// typed inspection while terminal presentation uses only the bounded verdict.
pub(crate) fn exact_mysql_failure_presentation(error: &anyhow::Error) -> Option<String> {
    error
        .chain()
        .find_map(|source| source.downcast_ref::<ExactMysqlScenarioFailure>())
        .map(|failure| failure.to_string())
}

#[cfg(test)]
mod exact_mysql_presentation_tests {
    use super::*;
    #[test]
    fn terminal_presentation_does_not_expand_retained_raw_source_canary() {
        let canary = "0123456789abcdef0123456789abcdef";
        let original = std::io::Error::other(format!("actual child tail: {canary}"));
        let error = finish_exact_mysql_scenario(Some(original.into()), Ok(()), Ok(())).unwrap_err();
        // The original source remains inspectable; the main terminal path uses
        // this exact bounded formatter instead of anyhow's expanded chain.
        assert!(format!("{error:#}").contains(canary));
        let shown =
            exact_mysql_failure_presentation(&error.context("outer scenario context")).unwrap();
        assert!(shown.contains("primary=true"));
        assert!(!shown.contains(canary));
        assert!(exact_mysql_failure_presentation(&anyhow::anyhow!("ordinary")).is_none());
    }
}

#[cfg(test)]
mod exact_mysql_prelaunch_teardown_tests {
    use super::*;
    use crate::scenario::{ScenarioBinary, ScenarioLaunchConfig};
    use novarocks_cluster_harness::{CrossProcessChildEnvironment, LaunchProfile};
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    struct Fixture {
        frontend: ScenarioBinary,
        backends: Vec<ScenarioBinary>,
        teardowns: AtomicUsize,
    }
    impl Scenario for Fixture {
        fn name(&self) -> &'static str {
            "component/exact-prelaunch-refusal"
        }
        fn launch_config(&self, _: &Path) -> Result<ScenarioLaunchConfig> {
            let mut environment = CrossProcessChildEnvironment::default();
            environment.fe.insert(
                "NOVAROCKS_MEM_1_M07_MYSQL_WRITE_SOCKET".into(),
                "unused-private-path".into(),
            );
            environment.fe.insert(
                "NOVAROCKS_MEM_1_M07_MYSQL_WRITE_NONCE_HEX".into(),
                "synthetic-only".into(),
            );
            let mut config = ScenarioLaunchConfig::default();
            config.child_environment = environment;
            config.binary_layout.frontend = self.frontend;
            config.binary_layout.backends = self.backends.clone();
            Ok(config)
        }
        fn run(&self, _: &mut ScenarioContext) -> Result<()> {
            panic!("prelaunch refusal must never launch a role")
        }
        fn teardown(&self) -> Result<()> {
            self.teardowns.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    #[test]
    fn exact_prelaunch_topology_and_both_binary_resolution_errors_teardown_once() {
        let root =
            std::env::temp_dir().join(format!("novarocks-exact-prelaunch-{}", std::process::id()));
        let mut config = RunnerConfig {
            binary: root.join("deliberately-no-binary"),
            compatible_binary: None,
            other_island_binary: None,
            base_config_path: root.join("deliberately-no-config"),
            artifact_root: root.clone(),
            cluster_size: 3,
            timeout: Duration::from_secs(20),
            launch_profile: LaunchProfile::FaultScenario,
            uea1_workload_manifest: None,
        };
        let mut observations = Vec::new();
        for (profile, count, frontend, backends) in [
            (
                LaunchProfile::FaultScenario,
                2,
                ScenarioBinary::Primary,
                vec![],
            ),
            (
                LaunchProfile::Performance,
                3,
                ScenarioBinary::Primary,
                vec![],
            ),
            (
                LaunchProfile::FaultScenario,
                3,
                ScenarioBinary::Compatible,
                vec![],
            ),
            (
                LaunchProfile::FaultScenario,
                3,
                ScenarioBinary::Primary,
                vec![ScenarioBinary::OtherIsland; 3],
            ),
        ] {
            config.launch_profile = profile;
            config.cluster_size = count;
            let fixture = Fixture {
                frontend,
                backends,
                teardowns: AtomicUsize::new(0),
            };
            observations.push((
                run_one(&fixture, &config),
                fixture.teardowns.load(Ordering::SeqCst),
            ));
        }
        let cleanup = std::fs::remove_dir_all(&root);
        assert!(cleanup.is_ok());
        for (verdict, teardowns) in observations {
            let error = verdict.expect_err("invalid launch input cannot start roles");
            let failure = error.downcast_ref::<ExactMysqlScenarioFailure>().unwrap();
            assert!(
                failure.primary.is_some() && failure.cluster.is_none() && failure.fixture.is_none()
            );
            assert_eq!(teardowns, 1);
        }
    }
}
