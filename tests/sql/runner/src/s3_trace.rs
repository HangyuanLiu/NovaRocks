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

//! Runner-owned, test-only S3 request trace for MV scale measurements.

use anyhow::{Context, Result, bail, ensure};
use novarocks_cluster_harness::isolated_iceberg_rest::IsolatedIcebergRestFixture;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const ALIAS: &str = "uea7trace";
const HOST_ENV: &str = "MC_HOST_uea7trace";

pub(crate) struct S3TraceHandle {
    artifact: PathBuf,
    alias_url: String,
    child: Option<Child>,
    next_barrier: u64,
}

impl S3TraceHandle {
    pub(crate) fn start(fixture: &IsolatedIcebergRestFixture, artifact: &Path) -> Result<Self> {
        let identity = fixture.static_s3_identity();
        // The isolated fixture generates URL-unreserved credentials. Keep them
        // out of process arguments, logs, and trace artifacts.
        let url_unreserved = |byte: u8| byte.is_ascii_alphanumeric() || b"-._~".contains(&byte);
        ensure!(
            !identity.access_key_id.is_empty()
                && !identity.secret_access_key.is_empty()
                && identity.access_key_id.bytes().all(url_unreserved)
                && identity.secret_access_key.bytes().all(url_unreserved),
            "isolated S3 trace credentials cannot form a safe mc environment alias"
        );
        let endpoint = &fixture.endpoints().minio_endpoint;
        let host = endpoint
            .strip_prefix("http://")
            .context("isolated MinIO must use HTTP")?;
        let alias_url = format!(
            "http://{}:{}@{}",
            identity.access_key_id, identity.secret_access_key, host
        );
        if let Some(parent) = artifact.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create S3 trace directory {}", parent.display()))?;
        }
        let stdout = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(artifact)
            .with_context(|| format!("create new S3 trace artifact {}", artifact.display()))?;
        let stderr_path = PathBuf::from(format!("{}.stderr", artifact.display()));
        let stderr = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&stderr_path)
            .with_context(|| format!("create new S3 trace stderr {}", stderr_path.display()))?;
        let child = Command::new("mc")
            .args(["admin", "trace", "--json", ALIAS])
            .env(HOST_ENV, &alias_url)
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .context("start MinIO mc S3 trace")?;
        let mut handle = Self {
            artifact: artifact.to_path_buf(),
            alias_url,
            child: Some(child),
            next_barrier: 0,
        };
        handle
            .barrier()
            .context("wait for MinIO S3 trace readiness")?;
        Ok(handle)
    }

    pub(crate) fn finish(&mut self) -> Result<()> {
        let barrier = self
            .barrier()
            .context("drain MinIO S3 trace before fixture shutdown");
        let stopped = self.stop();
        match (barrier, stopped) {
            (Ok(()), Ok(())) => self.capture_object_sizes(),
            (Err(error), Ok(())) => Err(error),
            (Ok(()), Err(error)) => Err(error),
            (Err(barrier), Err(stop)) => Err(anyhow::anyhow!(
                "S3 trace barrier failed: {barrier:#}; trace stop failed: {stop:#}"
            )),
        }
    }

    pub(crate) fn object_sizes_artifact(&self) -> PathBuf {
        PathBuf::from(format!("{}.objects.jsonl", self.artifact.display()))
    }

    fn capture_object_sizes(&self) -> Result<()> {
        // List only after the trace stops. These S3 reads must not enter any
        // measured publication window or the raw request trace.
        let artifact = self.object_sizes_artifact();
        let output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&artifact)
            .with_context(|| {
                format!("create new S3 object-size artifact {}", artifact.display())
            })?;
        let status = Command::new("mc")
            .args(["ls", "--recursive", "--json", &format!("{ALIAS}/warehouse")])
            .env(HOST_ENV, &self.alias_url)
            .stdin(Stdio::null())
            .stdout(Stdio::from(output))
            .stderr(Stdio::null())
            .status()
            .context("list isolated MinIO object sizes")?;
        ensure!(
            status.success(),
            "isolated MinIO object-size listing failed"
        );
        ensure!(
            fs::metadata(&artifact)?.len() > 0,
            "isolated MinIO object-size listing was empty"
        );
        Ok(())
    }

    fn barrier(&mut self) -> Result<()> {
        let token = format!(
            "uea7-trace-barrier-{}-{}",
            std::process::id(),
            self.next_barrier
        );
        self.next_barrier += 1;
        let target = format!("{ALIAS}/warehouse/{token}");
        let start_offset = fs::metadata(&self.artifact)
            .with_context(|| format!("stat S3 trace {}", self.artifact.display()))?
            .len();
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut next_request = Instant::now();
        loop {
            if Instant::now() >= next_request {
                let _ = Command::new("mc")
                    .args(["stat", &target])
                    .env(HOST_ENV, &self.alias_url)
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .context("send MinIO S3 trace barrier request")?;
                next_request = Instant::now() + Duration::from_millis(500);
            }
            let mut trace = fs::File::open(&self.artifact)
                .with_context(|| format!("read S3 trace {}", self.artifact.display()))?;
            trace.seek(SeekFrom::Start(start_offset))?;
            let mut contents = Vec::new();
            trace.read_to_end(&mut contents)?;
            if let Some(last_newline) = contents.iter().rposition(|byte| *byte == b'\n') {
                let complete = &contents[..=last_newline];
                if complete
                    .windows(token.len())
                    .any(|window| window == token.as_bytes())
                {
                    return Ok(());
                }
            }
            if self
                .child
                .as_mut()
                .expect("live S3 trace child")
                .try_wait()
                .context("poll MinIO S3 trace")?
                .is_some()
            {
                bail!("MinIO S3 trace exited before its barrier was observed");
            }
            if Instant::now() >= deadline {
                bail!("MinIO S3 trace did not observe its barrier within 10 seconds");
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn stop(&mut self) -> Result<()> {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            child.wait().context("reap MinIO S3 trace process")?;
        }
        Ok(())
    }
}

impl Drop for S3TraceHandle {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
