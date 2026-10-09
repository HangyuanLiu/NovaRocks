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
    let exact =
        cli.exact_mysql_execution_binding.is_some() || cli.held_native_execution_binding.is_some();
    let result = run_dispatch(cli);
    if exact {
        finish_exact_mysql_scenario(result.err(), Ok(()), Ok(()))
    } else {
        result
    }
}

fn run_dispatch(cli: Cli) -> Result<()> {
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
    if let Some(binding) = &config.held_native_execution_binding {
        #[cfg(unix)]
        {
            use crate::exact_native_admission::held_native_admission;
            anyhow::ensure!(
                config.cluster_size == 3
                    && config.launch_profile
                        == novarocks_cluster_harness::LaunchProfile::FaultScenario,
                "held native admission requires fault-scenario 1FE+3BE"
            );
            let admitted =
                held_native_admission::admit(binding, &config.binary, &config.base_config_path)?;
            let scene = scenarios::exact_mysql_native_driver::held_late_ack_from_admitted(
                admitted.for_scene(),
            )?;
            anyhow::ensure!(
                cli.only.is_empty() || (cli.only.len() == 1 && cli.only[0] == scene.name()),
                "held native selector differs from admitted case"
            );
            let primary = run_one(scene.as_ref(), &config);
            // Re-admission is preparation only, never a renewed scene20/protocol5.
            // All role cleanup has already settled through original run_one ownership.
            let postrun =
                held_native_admission::admit(binding, &config.binary, &config.base_config_path)
                    .and_then(|after| {
                        anyhow::ensure!(
                            after == admitted,
                            "held admission changed during original scene"
                        );
                        Ok(())
                    });
            return finish_exact_mysql_scenario(primary.err(), postrun, Ok(()));
        }
        #[cfg(not(unix))]
        bail!("held native admission requires Unix original role ownership");
    }
    if let Some(binding) = &config.exact_mysql_execution_binding {
        #[cfg(unix)]
        {
            if config.cluster_size != 3
                || config.launch_profile != novarocks_cluster_harness::LaunchProfile::FaultScenario
            {
                bail!("exact MySQL execution requires fault-scenario 1FE+3BE");
            }
            let admitted = crate::exact_native_admission::admit(
                binding,
                &config.binary,
                &config.base_config_path,
            )?;
            let exact =
                scenarios::exact_mysql_native_driver::scenarios_from_admitted(admitted.clone())?;
            let selected = select_exact(&exact, &cli.only)?;
            for scenario in selected {
                let current = crate::exact_native_admission::admit(
                    binding,
                    &config.binary,
                    &config.base_config_path,
                )?;
                anyhow::ensure!(
                    current == admitted,
                    "original admission changed before exact scene launch"
                );
                run_one(scenario, &config)?;
                // Final source/binary drift is an admission failure even when
                // the original scene already settled successfully. This new
                // preparation check cannot renew its original 20-second clock.
                let after = crate::exact_native_admission::admit(
                    binding,
                    &config.binary,
                    &config.base_config_path,
                )?;
                anyhow::ensure!(
                    after == admitted,
                    "original admission changed during exact scene"
                );
            }
            return Ok(());
        }
        #[cfg(not(unix))]
        bail!("exact MySQL execution requires Unix original fixture ownership");
    }
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

#[cfg(unix)]
fn select_exact<'a>(
    scenarios: &'a [Box<dyn Scenario>],
    only: &[String],
) -> Result<Vec<&'a dyn Scenario>> {
    if only.is_empty() {
        return Ok(scenarios.iter().map(|scenario| scenario.as_ref()).collect());
    }
    let unique: std::collections::BTreeSet<_> = only.iter().collect();
    anyhow::ensure!(
        only.len() <= scenarios.len() && unique.len() == only.len(),
        "exact matrix selectors must be unique original cases"
    );
    select(scenarios, only)
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
            return finish_exact_mysql_scenario(Some(error), Ok(()), scenario.teardown());
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
    let root_observation_clock = match scenario.root_observation_deadline() {
        Ok(clock) => clock,
        Err(error) => return finish_exact_mysql_scenario(Some(error), Ok(()), scenario.teardown()),
    };
    if exact_mysql_clock.is_some() && root_observation_clock.is_some() {
        return finish_exact_mysql_scenario(
            Some(anyhow::anyhow!(
                "neutral root source cannot activate an exact MySQL gate owner"
            )),
            Ok(()),
            scenario.teardown(),
        );
    }
    let bounded_original = exact_mysql_clock.is_some() || root_observation_clock.is_some();
    let uea1_preparation_diagnostic_secret = launch_config
        .child_environment
        .fe
        .get("NOVAROCKS_PREPARATION_DIAGNOSTIC_SECRET")
        .cloned();
    let preparation = (|| -> Result<_> {
        if bounded_original {
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
        Err(error) if bounded_original => {
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
    let handle = if let Some(clock) = exact_mysql_clock {
        if launch_config.native_root_reply_fault.is_some()
            || !launch_config
                .native_fault_proxies
                .backend_retained_byte_limits
                .is_empty()
        {
            finish_exact_mysql_scenario(
                Some(anyhow::anyhow!(
                    "exact MySQL prelaunch config freeze requires direct original roles"
                )),
                Ok(()),
                scenario.teardown(),
            )?;
            unreachable!();
        }
        CrossProcessServerHandle::launch_with_exact_mysql_prelaunch_check(
            cluster_options,
            &|artifact| {
                clock.remaining("original prepared config freeze")?;
                scenario.freeze_prepared_exact_config(
                    artifact,
                    &scenario_root,
                    clock.deadline(),
                )?;
                clock.remaining("original prepared config freeze completion")?;
                Ok(())
            },
        )
    } else if let Some(deadline) = root_observation_clock {
        if launch_config.native_root_reply_fault.is_some()
            || !launch_config
                .native_fault_proxies
                .backend_retained_byte_limits
                .is_empty()
        {
            return finish_exact_mysql_scenario(
                Some(anyhow::anyhow!(
                    "neutral original source requires direct original roles"
                )),
                Ok(()),
                scenario.teardown(),
            );
        }
        CrossProcessServerHandle::launch_with_exact_mysql_prelaunch_check(
            cluster_options,
            &|artifact| {
                anyhow::ensure!(
                    std::time::Instant::now() < deadline,
                    "neutral original prelaunch clock expired"
                );
                scenario.freeze_prepared_exact_config(artifact, &scenario_root, deadline)?;
                anyhow::ensure!(
                    std::time::Instant::now() < deadline,
                    "neutral original prepared freeze was late"
                );
                Ok(())
            },
        )
    } else {
        match launch_config.native_root_reply_fault {
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
    }
    .with_context(|| format!("launch system scenario {}", scenario.name()));
    let handle = match handle {
        Ok(handle) => handle,
        Err(error) => {
            if bounded_original {
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
            Err(_) if bounded_original => {
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
        let exact_binding = config
            .exact_mysql_execution_binding
            .as_ref()
            .map(|path| format!(" --exact-mysql-execution-binding {}", path.display()))
            .unwrap_or_default();
        eprintln!(
            "rerun: novarocks-system-tests --only {} --binary {} --config {} --artifact-root {} --cluster-size {} --timeout-secs {} --launch-profile {}{}{}",
            context.name(),
            config.binary.display(),
            config.base_config_path.display(),
            config.artifact_root.display(),
            config.cluster_size,
            config.timeout.as_secs(),
            launch_profile,
            manifest,
            exact_binding,
        );
        let cluster_cleanup = if exact_mysql_clock.is_some() {
            context.shutdown_exact_mysql_fixture()
        } else {
            context.shutdown()
        };
        let fixture_cleanup = scenario.teardown();
        if bounded_original {
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
    if bounded_original {
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
    if let Some(clock) = exact_mysql_clock {
        clock.remaining("settled original role and final evidence completion")?;
    }
    if let Some(deadline) = root_observation_clock {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "neutral original role cleanup and final evidence exceeded prelaunch clock"
        );
    }
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
    #[cfg(unix)]
    #[test]
    fn original_binding_parse_error_remains_owned_but_terminal_is_finite() -> Result<()> {
        let root = tempfile::tempdir()?;
        let binding = root.path().join("binding.json");
        let base = root.path().join("base.toml");
        let canary = "0123456789abcdef0123456789abcdef";
        fs::write(&binding, format!("{{\"{canary}\":true}}"))?;
        fs::write(&base, "[server]\nhost='127.0.0.1'\n")?;
        let cli = Cli::parse([
            "--binary".into(),
            std::env::current_exe()?.display().to_string(),
            "--config".into(),
            base.display().to_string(),
            "--artifact-root".into(),
            root.path().join("artifacts").display().to_string(),
            "--launch-profile".into(),
            "fault-scenario".into(),
            "--cluster-size".into(),
            "3".into(),
            "--exact-mysql-execution-binding".into(),
            binding.display().to_string(),
        ])?;
        let result = run(cli);
        root.close()?;
        let error = result.unwrap_err();
        assert!(error.chain().any(|source| source.is::<serde_json::Error>()));
        assert!(format!("{error:#}").contains(canary));
        assert!(
            !exact_mysql_failure_presentation(&error)
                .unwrap()
                .contains(canary)
        );
        Ok(())
    }
    #[cfg(unix)]
    #[test]
    fn explicit_exact_selection_includes_all_and_refuses_duplicate_or_foreign_cases() {
        let scenarios = crate::scenarios::all();
        assert_eq!(
            select_exact(&scenarios, &[]).unwrap().len(),
            scenarios.len()
        );
        let name = scenarios[0].name().to_owned();
        assert_eq!(select_exact(&scenarios, &[name.clone()]).unwrap().len(), 1);
        assert!(select_exact(&scenarios, &[name.clone(), name]).is_err());
        assert!(select_exact(&scenarios, &["foreign-original-case".into()]).is_err());
    }
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
            exact_mysql_execution_binding: None,
            held_native_execution_binding: None,
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

#[cfg(test)]
mod neutral_root_prelaunch_tests {
    use super::*;
    use crate::scenario::{ScenarioBinary, ScenarioLaunchConfig};
    use novarocks_cluster_harness::LaunchProfile;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    #[derive(Clone, Copy)]
    enum Mode {
        LaunchFailure,
        ClockFailure,
        BothClocks,
        Neutral,
    }
    #[derive(Debug)]
    struct OriginalSource(Arc<u8>);
    impl std::fmt::Display for OriginalSource {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("original prelaunch component source")
        }
    }
    impl std::error::Error for OriginalSource {}
    struct Fixture {
        mode: Mode,
        clock: Mutex<Option<Instant>>,
        primary: Arc<u8>,
        secondary: Arc<u8>,
        teardowns: AtomicUsize,
    }
    impl Fixture {
        fn new(mode: Mode) -> Self {
            Self {
                mode,
                clock: Mutex::new(None),
                primary: Arc::new(7),
                secondary: Arc::new(9),
                teardowns: AtomicUsize::new(0),
            }
        }
        fn error(source: &Arc<u8>) -> anyhow::Error {
            std::io::Error::other(OriginalSource(source.clone())).into()
        }
    }
    impl Scenario for Fixture {
        fn name(&self) -> &'static str {
            "component/neutral-prelaunch-refusal"
        }
        fn launch_config(&self, _: &std::path::Path) -> Result<ScenarioLaunchConfig> {
            if matches!(self.mode, Mode::LaunchFailure) {
                return Err(Self::error(&self.primary));
            }
            let mut clock = self.clock.lock().unwrap();
            assert!(clock.is_none());
            *clock = Some(Instant::now() + Duration::from_secs(20));
            let mut config = ScenarioLaunchConfig::default();
            // An absent compatible binary makes any missed early guard fail at
            // a different branch without ever starting a role.
            config.binary_layout.frontend = ScenarioBinary::Compatible;
            if matches!(self.mode, Mode::BothClocks) {
                config.child_environment.fe.insert(
                    "NOVAROCKS_MEM_1_M07_MYSQL_WRITE_SOCKET".into(),
                    "unused-private-path".into(),
                );
                config.child_environment.fe.insert(
                    "NOVAROCKS_MEM_1_M07_MYSQL_WRITE_NONCE_HEX".into(),
                    "synthetic-only".into(),
                );
            }
            Ok(config)
        }
        fn root_observation_deadline(&self) -> Result<Option<Instant>> {
            if matches!(self.mode, Mode::ClockFailure) {
                return Err(Self::error(&self.primary));
            }
            Ok(*self.clock.lock().unwrap())
        }
        fn run(&self, _: &mut ScenarioContext) -> Result<()> {
            panic!("prelaunch refusal must never enter a role-backed scenario");
        }
        fn teardown(&self) -> Result<()> {
            self.teardowns.fetch_add(1, Ordering::SeqCst);
            if matches!(self.mode, Mode::LaunchFailure | Mode::ClockFailure) {
                Err(Self::error(&self.secondary))
            } else {
                Ok(())
            }
        }
    }
    fn configuration(root: &std::path::Path) -> RunnerConfig {
        RunnerConfig {
            binary: root.join("no-binary"),
            compatible_binary: None,
            other_island_binary: None,
            base_config_path: root.join("no-config"),
            artifact_root: root.into(),
            cluster_size: 3,
            timeout: Duration::from_secs(20),
            launch_profile: LaunchProfile::FaultScenario,
            uea1_workload_manifest: None,
            exact_mysql_execution_binding: None,
            held_native_execution_binding: None,
        }
    }
    fn assert_source(error: &anyhow::Error, expected: &Arc<u8>) {
        let io = error.downcast_ref::<std::io::Error>().unwrap();
        let source = io
            .get_ref()
            .unwrap()
            .downcast_ref::<OriginalSource>()
            .unwrap();
        assert!(Arc::ptr_eq(&source.0, expected));
    }
    #[test]
    fn actual_launch_and_clock_errors_retain_both_original_sources_and_teardown_once() {
        let root =
            std::env::temp_dir().join(format!("novarocks-neutral-source-{}", std::process::id()));
        for mode in [Mode::LaunchFailure, Mode::ClockFailure] {
            let fixture = Fixture::new(mode);
            let outcome = run_one(&fixture, &configuration(&root));
            let cleanup = std::fs::remove_dir_all(&root);
            assert!(cleanup.is_ok());
            let error = outcome.unwrap_err();
            let failure = error.downcast_ref::<ExactMysqlScenarioFailure>().unwrap();
            assert_source(failure.primary.as_ref().unwrap(), &fixture.primary);
            assert_source(failure.fixture.as_ref().unwrap(), &fixture.secondary);
            assert!(failure.cluster.is_none());
            assert_eq!(fixture.teardowns.load(Ordering::SeqCst), 1);
        }
    }
    #[test]
    fn conflicting_neutral_and_exact_clocks_refuse_before_binary_resolution() {
        let root =
            std::env::temp_dir().join(format!("novarocks-neutral-conflict-{}", std::process::id()));
        let fixture = Fixture::new(Mode::BothClocks);
        let outcome = run_one(&fixture, &configuration(&root));
        let cleanup = std::fs::remove_dir_all(&root);
        assert!(cleanup.is_ok());
        let error = outcome.unwrap_err();
        let failure = error.downcast_ref::<ExactMysqlScenarioFailure>().unwrap();
        assert!(
            failure
                .primary
                .as_ref()
                .unwrap()
                .to_string()
                .contains("neutral root source cannot activate")
        );
        assert!(failure.cluster.is_none() && failure.fixture.is_none());
        assert_eq!(fixture.teardowns.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn neutral_topology_and_binary_refusals_keep_one_original_teardown() {
        let root =
            std::env::temp_dir().join(format!("novarocks-neutral-topology-{}", std::process::id()));
        for (profile, count) in [
            (LaunchProfile::FaultScenario, 2),
            (LaunchProfile::Performance, 3),
            (LaunchProfile::FaultScenario, 3),
        ] {
            let fixture = Fixture::new(Mode::Neutral);
            let mut config = configuration(&root);
            config.launch_profile = profile;
            config.cluster_size = count;
            let outcome = run_one(&fixture, &config);
            let cleanup = std::fs::remove_dir_all(&root);
            assert!(cleanup.is_ok());
            let error = outcome.unwrap_err();
            let failure = error.downcast_ref::<ExactMysqlScenarioFailure>().unwrap();
            assert!(
                failure.primary.is_some() && failure.cluster.is_none() && failure.fixture.is_none()
            );
            assert_eq!(fixture.teardowns.load(Ordering::SeqCst), 1);
        }
    }
}
