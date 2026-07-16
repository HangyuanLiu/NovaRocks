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

use std::path::PathBuf;

use anyhow::{Result, bail};
use novarocks_codegen_audit::{audit_repo, run_self_tests};

fn main() -> Result<()> {
    let mut arguments = std::env::args().skip(1);
    let mode = arguments.next().unwrap_or_else(|| "audit".to_string());
    let repo = arguments
        .next()
        .map(PathBuf::from)
        .unwrap_or(std::env::current_dir()?);
    if arguments.next().is_some() {
        bail!("usage: novarocks-codegen-audit [audit|self-test] [repo-root]");
    }

    match mode.as_str() {
        "audit" => {
            let violations = audit_repo(&repo)?;
            if !violations.is_empty() {
                for violation in violations {
                    eprintln!("ERROR: {violation}");
                }
                std::process::exit(1);
            }
            println!("plan IR codegen boundary audit passed");
        }
        "self-test" => {
            run_self_tests()?;
            println!("plan IR codegen boundary audit self-tests passed");
        }
        _ => bail!("unsupported mode: {mode}"),
    }
    Ok(())
}
