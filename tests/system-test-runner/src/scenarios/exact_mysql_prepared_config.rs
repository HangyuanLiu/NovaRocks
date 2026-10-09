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

//! Exact original prepared-config artifact, frozen before any original role spawn.
use anyhow::{Result, ensure};
use novarocks_cluster_harness::EffectiveLaunchConfigEvidence;
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Instant;

const ARTIFACT_LIMIT: usize = 131_072;
pub(super) struct OriginalPreparedConfig {
    file: File,
    path: PathBuf,
    dev: u64,
    ino: u64,
    length: u64,
    sha256: [u8; 32],
    original_deadline: Instant,
}
impl OriginalPreparedConfig {
    pub(super) fn freeze(
        artifact: &EffectiveLaunchConfigEvidence,
        root: &Path,
        deadline: Instant,
    ) -> Result<Self> {
        super::check(deadline)?;
        let bytes = artifact.artifact_bytes();
        ensure!(
            !bytes.is_empty() && bytes.len() <= ARTIFACT_LIMIT,
            "original prepared artifact exceeds fixed bound"
        );
        let sha256: [u8; 32] = Sha256::digest(bytes).into();
        ensure!(
            sha256 == super::hash32(artifact.artifact_sha256())?
                && artifact.semantics_sha256() == artifact.artifact_sha256(),
            "original prepared config projection digest differs"
        );
        let path = root.join("exact-native-effective-launch-config.json");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        file.set_permissions(fs::Permissions::from_mode(0o400))?;
        let metadata = file.metadata()?;
        let value = Self {
            file,
            path,
            dev: metadata.dev(),
            ino: metadata.ino(),
            length: bytes.len() as u64,
            sha256,
            original_deadline: deadline,
        };
        value.verify_original_file()?;
        Ok(value)
    }
    pub(super) fn verify(&self, artifact: &EffectiveLaunchConfigEvidence) -> Result<()> {
        ensure!(
            super::hash32(artifact.artifact_sha256())? == self.sha256
                && artifact.artifact_bytes().len() as u64 == self.length
                && <[u8; 32]>::from(Sha256::digest(artifact.artifact_bytes())) == self.sha256,
            "launched config differs from original prelaunch artifact"
        );
        self.verify_original_file()
    }
    pub(super) fn verify_original_file(&self) -> Result<()> {
        super::check(self.original_deadline)?;
        let check_identity = |metadata: fs::Metadata| -> Result<()> {
            ensure!(
                metadata.is_file()
                    && metadata.dev() == self.dev
                    && metadata.ino() == self.ino
                    && metadata.len() == self.length
                    && metadata.mode() & 0o777 == 0o400,
                "original prelaunch config artifact was replaced or changed"
            );
            Ok(())
        };
        check_identity(fs::symlink_metadata(&self.path)?)?;
        check_identity(self.file.metadata()?)?;
        let mut scratch = [0u8; 4096];
        let mut at = 0u64;
        let mut hash = Sha256::new();
        while at < self.length {
            let limit = (self.length - at).min(scratch.len() as u64) as usize;
            let count = self.file.read_at(&mut scratch[..limit], at)?;
            ensure!(count != 0, "original prelaunch config artifact ended early");
            hash.update(&scratch[..count]);
            at += count as u64;
            super::check(self.original_deadline)?;
        }
        ensure!(
            <[u8; 32]>::from(hash.finalize()) == self.sha256,
            "original prelaunch config artifact bytes changed"
        );
        check_identity(fs::symlink_metadata(&self.path)?)?;
        check_identity(self.file.metadata()?)?;
        super::check(self.original_deadline)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    fn fixture(root: &Path) -> OriginalPreparedConfig {
        let path = root.join("prepared-original.json");
        let bytes = b"original prepared facts";
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .unwrap();
        file.write_all(bytes).unwrap();
        file.sync_all().unwrap();
        file.set_permissions(fs::Permissions::from_mode(0o400))
            .unwrap();
        let metadata = file.metadata().unwrap();
        OriginalPreparedConfig {
            file,
            path,
            dev: metadata.dev(),
            ino: metadata.ino(),
            length: bytes.len() as u64,
            sha256: Sha256::digest(bytes).into(),
            original_deadline: Instant::now() + Duration::from_secs(2),
        }
    }
    #[test]
    fn original_prepared_file_rechecks_identity_digest_and_original_clock() {
        let root = tempfile::tempdir().unwrap();
        let mut original = fixture(root.path());
        original.verify_original_file().unwrap();
        original.original_deadline = Instant::now();
        assert!(original.verify_original_file().is_err());
    }
    #[test]
    fn same_inode_same_length_body_change_is_refused() {
        let root = tempfile::tempdir().unwrap();
        let original = fixture(root.path());
        original.file.write_at(b"changed!", 0).unwrap();
        assert!(original.verify_original_file().is_err());
    }
    #[test]
    fn replaced_path_does_not_borrow_the_still_open_original_file() {
        let root = tempfile::tempdir().unwrap();
        let original = fixture(root.path());
        let old = root.path().join("old");
        fs::rename(&original.path, &old).unwrap();
        fs::copy(old, &original.path).unwrap();
        assert!(original.verify_original_file().is_err());
    }
    #[test]
    fn symlink_to_the_original_inode_cannot_satisfy_path_ownership() {
        let root = tempfile::tempdir().unwrap();
        let original = fixture(root.path());
        let old = root.path().join("old");
        fs::rename(&original.path, &old).unwrap();
        std::os::unix::fs::symlink(old, &original.path).unwrap();
        assert!(original.verify_original_file().is_err());
    }
}
