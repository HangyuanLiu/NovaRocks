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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Control client for the real REST service's post-requirements commit hook.

use anyhow::{Context, Result, bail, ensure};
use reqwest::blocking::Client;
use serde_json::Value;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub(crate) struct Control {
    uri: String,
    client: Client,
}

pub(crate) struct Hold {
    control: Control,
    arm_id: String,
    table: String,
    released: bool,
}

impl Control {
    pub(crate) fn new(uri: String) -> Result<Self> {
        Ok(Self {
            uri,
            client: Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(5))
                .build()?,
        })
    }

    pub(crate) fn arm(&self, table: &str) -> Result<Hold> {
        let (namespace, table_name) = table
            .split_once('.')
            .context("publication service hold requires namespace.table")?;
        ensure!(
            !namespace.is_empty()
                && !table_name.is_empty()
                && table
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_'))
                && !table_name.contains('.'),
            "publication service hold requires an exact namespace.table"
        );
        let response = self
            .client
            .post(format!("{}/arm", self.uri))
            .query(&[("table", table)])
            .send()?
            .error_for_status()
            .context("arm REST service publication hold")?;
        let body: Value = response.json()?;
        let arm_id = body["arm_id"]
            .as_str()
            .context("REST service hold response lacks arm_id")?
            .to_string();
        Ok(Hold {
            control: self.clone(),
            arm_id,
            table: table.to_string(),
            released: false,
        })
    }
}

impl Hold {
    pub(crate) fn wait_until_held(&self, deadline: Instant) -> Result<()> {
        loop {
            let response = self
                .control
                .client
                .get(format!("{}/status", self.control.uri))
                .query(&[("arm_id", &self.arm_id)])
                .send()?
                .error_for_status()?;
            let body: Value = response.json()?;
            match body["phase"].as_str() {
                Some("held") => return Ok(()),
                Some("armed") if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                other => bail!("REST service publication hold was not reached: {other:?}"),
            }
        }
    }

    pub(crate) fn release(&mut self) -> Result<()> {
        if self.released {
            return Ok(());
        }
        self.control
            .client
            .post(format!("{}/release", self.control.uri))
            .query(&[("arm_id", &self.arm_id)])
            .send()?
            .error_for_status()
            .context("release REST service publication hold")?;
        self.released = true;
        Ok(())
    }

    pub(crate) fn verify_original_commit_conflicted(&self) -> Result<String> {
        let trace = self
            .control
            .client
            .get(format!("{}/trace", self.control.uri))
            .send()?
            .error_for_status()?
            .text()?;
        let events = trace
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let held = events
            .iter()
            .filter(|event| event["arm_id"] == self.arm_id)
            .collect::<Vec<_>>();
        let names = held
            .iter()
            .filter_map(|event| event["event"].as_str())
            .collect::<Vec<_>>();
        let expected = [
            "hold-armed",
            "requirements-passed-before-persistent-commit",
            "hold-reached",
            "hold-released",
            "delegate-commit-start",
            "delegate-commit-conflict",
        ];
        ensure!(
            names == expected,
            "REST service held commit trace differs: {names:?}"
        );
        let base = held[2]["base_metadata"]
            .as_str()
            .context("hold base missing")?;
        let updated = held[2]["updated_metadata"]
            .as_str()
            .context("hold updated metadata missing")?;
        ensure!(
            held.iter().all(|event| event["table"] == self.table)
                && held[4]["base_metadata"] == base
                && held[4]["updated_metadata"] == updated
                && held[5]["base_metadata"] == base
                && held[5]["updated_metadata"] == updated,
            "REST service changed the held commit identity"
        );
        let hold_sequence = held[2]["sequence"]
            .as_u64()
            .context("hold sequence missing")?;
        let conflict_sequence = held[5]["sequence"]
            .as_u64()
            .context("conflict sequence missing")?;
        let thread = held[5]["thread"]
            .as_str()
            .context("conflict thread missing")?;
        ensure!(
            events
                .iter()
                .filter(|event| {
                    event["event"] == "delegate-commit-success"
                        && event["table"] == self.table
                        && event["arm_id"] == ""
                        && event["sequence"]
                            .as_u64()
                            .is_some_and(|n| n > hold_sequence && n < conflict_sequence)
                })
                .count()
                == 1,
            "REST service did not commit exactly one competitor during the hold"
        );
        ensure!(
            events.iter().any(|event| {
                event["event"] == "refresh"
                    && event["table"] == self.table
                    && event["thread"] == thread
                    && event["sequence"]
                        .as_u64()
                        .is_some_and(|n| n > conflict_sequence)
            }),
            "REST service did not refresh after the JDBC conflict"
        );
        ensure!(
            !events.iter().any(|event| {
                event["event"] == "delegate-commit-start"
                    && event["table"] == self.table
                    && event["thread"] == thread
                    && event["sequence"]
                        .as_u64()
                        .is_some_and(|n| n > conflict_sequence)
            }),
            "REST service delegated the stale request a second time"
        );
        Ok(format!(
            "arm={} table={} base={base} updated={updated}",
            self.arm_id, self.table
        ))
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.release();
        }
    }
}
