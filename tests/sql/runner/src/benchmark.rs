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

use crate::config::{TestLane, build_suite_configs, resolve_repo_root};
use anyhow::{Result, bail};
use clap::{ArgAction, Parser};

#[derive(Debug, Parser)]
#[command(
    name = "novarocks-sql-benchmark",
    about = "Run release SQL benchmarks from tests/sql/benchmarks/"
)]
struct BenchmarkCli {
    /// Print the deterministic benchmark workload names and exit.
    #[arg(long, action = ArgAction::SetTrue)]
    list_suites: bool,
}

pub fn run() -> Result<()> {
    let cli = BenchmarkCli::parse();
    let base_dir = resolve_repo_root()?;
    let workloads = build_suite_configs(&base_dir, TestLane::Benchmark)?;
    if workloads.is_empty() {
        bail!("no benchmark workload directories found under tests/sql/benchmarks");
    }
    if cli.list_suites {
        for name in workloads.keys() {
            println!("{name}");
        }
        return Ok(());
    }
    bail!(
        "benchmark execution protocol is not available yet; use --list-suites or complete TST-11 T05"
    )
}

#[cfg(test)]
mod tests {
    use super::BenchmarkCli;
    use clap::CommandFactory;

    #[test]
    fn benchmark_help_mentions_only_the_benchmark_root() {
        let help = BenchmarkCli::command().render_long_help().to_string();
        assert!(help.contains("tests/sql/benchmarks/"));
        assert!(!help.contains("tests/sql/correctness/"));
    }
}
