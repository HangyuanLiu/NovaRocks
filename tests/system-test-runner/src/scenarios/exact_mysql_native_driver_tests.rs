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

use super::*;
use std::error::Error as _;

fn admitted_shape() -> AdmittedExactNativeRun {
    AdmittedExactNativeRun {
        clean_revision: "0".repeat(40),
        source_tree_sha256: "1".repeat(64),
        server_binary_sha256: "2".repeat(64),
        server_build_identity: "host-only-shape-not-native".into(),
        runner_binary_sha256: "3".repeat(64),
        base_config_sha256: "4".repeat(64),
        frozen_execution_binding_sha256: "5".repeat(64),
        large_input_sha256: LARGE_INPUT_SHA.into(),
        tiny_input_sha256: TINY_INPUT_SHA.into(),
    }
}
fn root(data: u64, bytes: u64, running: u64, exited: u64, end: u64) -> BTreeMap<String, u64> {
    [
        ("channels", 1),
        ("ends_acknowledged", 0),
        ("sealed", 0),
        ("data_positions", data),
        ("payload_bytes", bytes),
        ("producers_running", running),
        ("producers_exited", exited),
        ("ends_published", end),
        ("segments", data),
        ("terminal_task_records", exited),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_string(), value))
    .collect()
}
#[test]
fn original_ten_inputs_and_independent_prefix_pins_stay_closed() {
    assert_eq!(CASES.len(), 10);
    let mut names = std::collections::BTreeSet::new();
    for case in CASES {
        assert!(names.insert(case.name));
        assert_eq!(
            hash32(case.sql_sha).unwrap(),
            <[u8; 32]>::from(Sha256::digest(case.sql.as_bytes()))
        );
        assert_eq!(
            hash32(case.prefix_sha).unwrap(),
            case.input.prefix_hash().unwrap()
        );
        assert_eq!(case.input.expect_complete_tail, case.input.columns == 1);
    }
    assert_eq!(
        CASES[..3]
            .iter()
            .map(|case| case.input.cut)
            .collect::<Vec<_>>(),
        [S - 1, S, S + 1]
    );
    assert_eq!(CASES[3].input.columns, 17);
    assert_eq!(
        CASES[4..]
            .iter()
            .map(|case| case.input.cut)
            .collect::<Vec<_>>(),
        [1, 2, 3, 4, 5, 6]
    );
}
#[test]
fn reserved_stop_is_inside_fixed_sixteen_even_at_all_loop_limits() {
    assert_eq!(
        1 + HELD_SNAPSHOTS + RESUME_SNAPSHOTS + FINAL_EXIT_SNAPSHOTS + 1,
        16
    );
    assert_eq!(CONTROL_REPLY_SLOTS, 16);
    assert_eq!(IDLE_SAMPLES, 51);
}
#[test]
fn admitted_shape_cannot_change_immutable_inputs_or_replace_pin_with_empty() {
    let mut run = admitted_shape();
    assert!(run.validate_shape().is_ok());
    run.large_input_sha256 = "6".repeat(64);
    assert!(run.validate_shape().is_err());
    run = admitted_shape();
    run.server_binary_sha256.clear();
    assert!(run.validate_shape().is_err());
    // Shape success is deliberately not a source/binary/freeze admission test.
}
#[test]
fn tiny_qualifies_one_original_data_and_never_synthetic_w2() {
    let input = CASES[4].input;
    assert!(qualifies_root(input, &root(1, 8, 0, 1, 1)).unwrap());
    assert!(!qualifies_root(input, &root(2, 8, 0, 1, 1)).unwrap());
    assert!(!qualifies_root(input, &root(1, 8, 1, 0, 0)).unwrap());
}
#[test]
fn large_resident_and_missing_tail_require_distinct_original_census() {
    assert!(qualifies_root(CASES[0].input, &root(2, S + 8, 0, 1, 1)).unwrap());
    assert!(!qualifies_root(CASES[0].input, &root(1, S, 0, 1, 1)).unwrap());
    assert!(qualifies_root(CASES[3].input, &root(2, 2 * S, 1, 0, 0)).unwrap());
    assert!(!qualifies_root(CASES[3].input, &root(2, 2 * S, 0, 1, 1)).unwrap());
}
#[test]
fn same_socket_health_schema_structure_and_more_results_are_checked() {
    let mut payload = Vec::new();
    for value in [b"def".as_slice(), b"", b"", b"", b"total", b"total"] {
        payload.push(value.len() as u8);
        payload.extend_from_slice(value);
    }
    let fixed = payload.len();
    payload.extend_from_slice(&[12, 63, 0, 20, 0, 0, 0, 8, 0, 0, 0, 0, 0]);
    health_column(&payload).unwrap();
    payload[fixed + 7] = 253;
    assert!(health_column(&payload).is_err());
    payload[fixed + 7] = 8;
    payload.push(0);
    assert!(health_column(&payload).is_err());
    assert!(health_eof(&[0xfe, 0, 0, 2, 0]).is_ok());
    assert!(health_eof(&[0xfe, 0, 0, 10, 0]).is_err());
    assert!(health_eof(&[0xfe]).is_err());
}
#[test]
fn saving_finite_error_facts_never_calls_untrusted_formatter() {
    struct BrokenFormatter;
    impl std::fmt::Debug for BrokenFormatter {
        fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            panic!("unexpected source Debug")
        }
    }
    impl std::fmt::Display for BrokenFormatter {
        fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            panic!("unexpected source Display")
        }
    }
    impl std::error::Error for BrokenFormatter {}
    let original = anyhow::Error::new(BrokenFormatter);
    assert_eq!(
        safe_error_fact(&original)["class"],
        "OtherActualSourceRetained"
    );
    assert!(original.downcast_ref::<BrokenFormatter>().is_some());
}
#[test]
fn first_and_secondary_original_io_sources_remain_owned() {
    let mut slot = Some(anyhow::Error::new(std::io::Error::from_raw_os_error(5)));
    retain_secondary(&mut slot, std::io::Error::from_raw_os_error(13).into());
    let aggregate = slot
        .as_ref()
        .unwrap()
        .downcast_ref::<SecondaryFailure>()
        .unwrap();
    assert_eq!(
        aggregate
            .primary
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .raw_os_error(),
        Some(5)
    );
    assert_eq!(
        aggregate
            .secondary
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .raw_os_error(),
        Some(13)
    );
    let failure = SceneFailure {
        errors: [slot, None, None, None, None],
    };
    assert!(failure.source().is_some());
}
#[test]
fn private_environment_is_fe_only_and_never_reuses_nonce_as_locator() -> Result<()> {
    let mut guard = HostPrivateOwnerGuard::create()?;
    let observed = (|| -> Result<_> {
        let environment = guard.owner.environment();
        let mode = fs::symlink_metadata(&guard.owner.parent)?.mode() & 0o777;
        let fe_only = environment.be.is_empty() && environment.be_by_index.is_empty();
        let nonce_length = environment.fe[NONCE_ENV].len();
        let independent_locator = !environment.fe[SOCKET_ENV].contains(&environment.fe[NONCE_ENV]);
        Ok((
            environment.fe.len(),
            fe_only,
            nonce_length,
            independent_locator,
            mode,
        ))
    })();
    let cleanup = guard.cleanup();
    let (fields, fe_only, nonce_length, independent_locator, mode) =
        host_component_settle(observed, cleanup)?;
    // All result assertions occur after the original exact owner cleanup.
    assert_eq!(fields, 2);
    assert!(fe_only);
    assert_eq!(nonce_length, 32);
    assert!(independent_locator);
    assert_eq!(mode, 0o700);
    Ok(())
}

#[test]
fn receipt_crossing_original_deadline_fails_after_writer_returns() {
    let deadline = Instant::now() + Duration::from_millis(1);
    let result = settle_original_receipt(deadline, |before| {
        assert!(before);
        std::thread::sleep(Duration::from_millis(5));
        Ok(())
    });
    assert!(result.is_err());
}
#[test]
fn expired_receipt_retains_actual_write_source_and_never_admits_success() {
    let result = settle_original_receipt(Instant::now(), |before| {
        assert!(!before);
        Err(std::io::Error::from_raw_os_error(5).into())
    });
    let error = result.unwrap_err();
    let aggregate = error.downcast_ref::<SecondaryFailure>().unwrap();
    let inner = aggregate
        .primary
        .downcast_ref::<SecondaryFailure>()
        .unwrap();
    assert_eq!(
        inner
            .secondary
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .raw_os_error(),
        Some(5)
    );
}
#[test]
fn private_cleanup_refuses_missing_identity_replacement_and_nonempty_parent() -> Result<()> {
    let mut guard = HostPrivateOwnerGuard::create()?;
    let observed = (|| -> Result<_> {
        let identity = guard.owner.identity.take();
        let missing_identity_refused = guard.owner.remove_empty_parent().is_err();
        guard.owner.identity = identity;
        guard.create_synthetic_file()?;
        let nonempty_refused = guard.owner.remove_empty_parent().is_err();
        let original_file_present = guard.original_file_present()?;
        Ok((
            missing_identity_refused,
            nonempty_refused,
            original_file_present,
        ))
    })();
    // The synthetic file has its own exact dev/ino pin and is removed before
    // the parent, before any assertions or unexpected-error propagation.
    let cleanup = guard.cleanup();
    let (missing_identity_refused, nonempty_refused, original_file_present) =
        host_component_settle(observed, cleanup)?;
    let missing_parent_refused = guard.owner.remove_empty_parent().is_err();
    assert!(missing_identity_refused);
    assert!(nonempty_refused);
    assert!(original_file_present);
    assert!(missing_parent_refused);
    Ok(())
}

// Host-only fixture safety. This does not change production Drop behavior and
// never stands in for a native listener, watcher, session, child or role join.
struct HostPrivateOwnerGuard {
    owner: PrivateControlOwner,
    synthetic_file: Option<(PathBuf, u64, u64)>,
    active: bool,
}
impl HostPrivateOwnerGuard {
    fn create() -> Result<Self> {
        let mut guard = Self {
            owner: PrivateControlOwner::reserve()?,
            synthetic_file: None,
            active: true,
        };
        if let Err(primary) = guard.owner.initialize() {
            let cleanup = guard.cleanup();
            return host_component_settle(Err(primary), cleanup);
        }
        Ok(guard)
    }
    fn recheck_parent(&self) -> Result<()> {
        let (dev, ino) = self
            .owner
            .identity
            .context("host original parent identity unavailable")?;
        let metadata = fs::symlink_metadata(&self.owner.parent)?;
        ensure!(
            metadata.is_dir()
                && metadata.dev() == dev
                && metadata.ino() == ino
                && metadata.mode() & 0o777 == 0o700,
            "host original parent identity changed"
        );
        Ok(())
    }
    fn create_synthetic_file(&mut self) -> Result<()> {
        ensure!(
            self.synthetic_file.is_none(),
            "host synthetic file already exists"
        );
        self.recheck_parent()?;
        let path = self.owner.parent.join("foreign");
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let metadata = file.metadata()?;
        ensure!(metadata.is_file(), "host synthetic file is not regular");
        self.synthetic_file = Some((path, metadata.dev(), metadata.ino()));
        file.write_all(b"host component")?;
        Ok(())
    }
    fn original_file_present(&self) -> Result<bool> {
        self.recheck_parent()?;
        let (path, dev, ino) = self
            .synthetic_file
            .as_ref()
            .context("host synthetic file pin absent")?;
        let metadata = fs::symlink_metadata(path)?;
        Ok(metadata.is_file() && metadata.dev() == *dev && metadata.ino() == *ino)
    }
    fn cleanup(&mut self) -> Result<()> {
        if !self.active {
            return Ok(());
        }
        if let Some((path, _, _)) = &self.synthetic_file {
            ensure!(
                self.original_file_present()?,
                "host synthetic file identity changed; retain it"
            );
            fs::remove_file(path)?;
            self.synthetic_file = None;
        }
        // No recursive removal, socket unlink, unknown file deletion or missing
        // parent waiver: delegate to the original exact empty-parent check.
        self.owner.remove_empty_parent()?;
        self.active = false;
        Ok(())
    }
}
impl Drop for HostPrivateOwnerGuard {
    fn drop(&mut self) {
        // Only a fallback for this host fixture; an error retains unknown paths.
        let _ = self.cleanup();
    }
}
fn host_component_settle<T>(primary: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(_), Err(cleanup)) => Err(cleanup),
        (Err(primary), Err(secondary)) => Err(SecondaryFailure { primary, secondary }.into()),
    }
}

#[test]
fn host_guard_fallback_settles_owned_synthetic_file_and_empty_parent() -> Result<()> {
    let mut guard = HostPrivateOwnerGuard::create()?;
    let parent = guard.owner.parent.clone();
    if let Err(primary) = guard.create_synthetic_file() {
        let cleanup = guard.cleanup();
        return host_component_settle(Err(primary), cleanup);
    }
    drop(guard);
    // Actual filesystem observation after this host-only fallback, no native proof.
    let missing = matches!(fs::symlink_metadata(parent), Err(error) if error.kind() == std::io::ErrorKind::NotFound);
    assert!(missing);
    Ok(())
}

#[test]
fn host_component_cleanup_preserves_both_original_sources() {
    let error = host_component_settle::<()>(
        Err(std::io::Error::from_raw_os_error(5).into()),
        Err(std::io::Error::from_raw_os_error(13).into()),
    )
    .unwrap_err();
    let aggregate = error.downcast_ref::<SecondaryFailure>().unwrap();
    assert_eq!(
        aggregate
            .primary
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .raw_os_error(),
        Some(5)
    );
    assert_eq!(
        aggregate
            .secondary
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .raw_os_error(),
        Some(13)
    );
}
