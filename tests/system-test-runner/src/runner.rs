use crate::cli::Cli;
use crate::config::RunnerConfig;
use crate::scenario::{Scenario, ScenarioContext};
use crate::scenarios;
use anyhow::{Context, Result, bail};
use novarocks_cluster_harness::{CrossProcessClusterOptions, CrossProcessServerHandle};
use std::fs;

pub fn run(cli: Cli) -> Result<()> {
    let scenarios = scenarios::all();
    if cli.list {
        for scenario in &scenarios {
            println!("{}", scenario.name());
        }
        return Ok(());
    }
    let config = RunnerConfig::from_cli(&cli)?;
    let selected = select(&scenarios, &cli.only)?;
    if selected.is_empty() {
        bail!("no system scenarios are registered");
    }
    for scenario in selected {
        run_one(scenario, &config)?;
    }
    Ok(())
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
    let launch_config = scenario
        .launch_config(&scenario_root)
        .with_context(|| format!("prepare launch configuration for {}", scenario.name()))?;
    let handle = CrossProcessServerHandle::launch(CrossProcessClusterOptions {
        binary: config.binary.clone(),
        base_config_path: config.base_config_path.clone(),
        runtime_root: scenario_root.clone(),
        cluster_size: config.cluster_size,
        query_lifecycle_faults_enabled: true,
        cleanup_faults_enabled: true,
        startup_timeout: config.timeout,
        child_environment: launch_config.child_environment,
        config_overlay: launch_config.config_overlay,
        native_trust_fixture: launch_config.native_trust_fixture,
    })
    .with_context(|| format!("launch system scenario {}", scenario.name()))?;
    let mut context = ScenarioContext::new(
        scenario.name(),
        handle,
        scenario_root,
        config.timeout,
        config.binary.clone(),
        config.base_config_path.clone(),
        config.cluster_size,
    );
    context.action("cluster launched and topology barrier passed");
    let result = scenario.run(&mut context);
    if let Err(error) = &result {
        context.retain_artifacts();
        eprintln!(
            "scenario={} failed; actions={:?}; runtime_dir={}; diagnostics={}",
            context.name(),
            context.actions(),
            context.runtime_dir().display(),
            context.diagnostics()
        );
        eprintln!(
            "rerun: novarocks-system-tests --only {} --binary {} --config {} --artifact-root {} --cluster-size {} --timeout-secs {}",
            context.name(),
            config.binary.display(),
            config.base_config_path.display(),
            config.artifact_root.display(),
            config.cluster_size,
            config.timeout.as_secs(),
        );
        let cleanup = context.shutdown();
        return match cleanup {
            Ok(()) => Err(anyhow::anyhow!(
                "scenario {} failed: {error:#}",
                context.name()
            )),
            Err(cleanup) => Err(anyhow::anyhow!(
                "scenario {} failed: {error:#}; cleanup failed: {cleanup:#}",
                context.name()
            )),
        };
    }
    context.action("scenario assertions passed");
    context
        .shutdown()
        .with_context(|| format!("cleanup system scenario {}", context.name()))?;
    println!("scenario={} PASS", scenario.name());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
