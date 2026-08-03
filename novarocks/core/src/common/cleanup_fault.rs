// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.  The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Runner-owned, frontend-only cleanup fault tokens.
//!
//! This deliberately has no relationship to query-lifecycle faults: orphan
//! cleanup never creates a query execution, fragment, or backend worker.  A
//! token is a single file published by the SQL runner and claimed with an
//! atomic rename, so one failure point can be exercised across an FE restart
//! without retaining mutable process-local state.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupFaultKind {
    DeleteFailed,
    DropDeleteResponse,
    ReceiptWriteFailed,
    CheckpointFailed,
    KillFeAfterDelete,
}

impl CleanupFaultKind {
    pub const fn file_stem(self) -> &'static str {
        match self {
            Self::DeleteFailed => "delete-failed",
            Self::DropDeleteResponse => "drop-delete-response",
            Self::ReceiptWriteFailed => "receipt-write-failed",
            Self::CheckpointFailed => "checkpoint-failed",
            Self::KillFeAfterDelete => "kill-fe-after-delete",
        }
    }
}

pub fn trigger_path(root: &Path, kind: CleanupFaultKind) -> PathBuf {
    root.join(format!("{}.trigger", kind.file_stem()))
}

/// Claim a token once.  Missing tokens are normal; malformed/unreadable
/// runner state is surfaced rather than silently changing destructive tests.
pub fn claim(root: &Path, kind: CleanupFaultKind) -> Result<bool, String> {
    let trigger = trigger_path(root, kind);
    let sequence = NEXT_CLAIM.fetch_add(1, Ordering::Relaxed);
    let claimed = root.join(format!(
        ".{}.claimed-{}-{}",
        kind.file_stem(),
        std::process::id(),
        sequence
    ));
    match fs::rename(&trigger, &claimed) {
        Ok(()) => {
            fs::remove_file(&claimed).map_err(|error| {
                format!(
                    "remove claimed cleanup fault {}: {error}",
                    claimed.display()
                )
            })?;
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!(
            "claim cleanup fault {}: {error}",
            trigger.display()
        )),
    }
}

pub fn claim_configured(kind: CleanupFaultKind) -> Result<bool, String> {
    // Read the runner-owned process environment rather than relying on the
    // application-config singleton.  FE startup validates that this value and
    // `debug.cleanup_fault_dir` are identical, while this keeps the hook
    // usable in the provider's synchronous callback without a second config
    // initialization path.
    let Some(root) = std::env::var_os("NOVAROCKS_SQL_TEST_CLEANUP_FAULT_DIR") else {
        return Ok(false);
    };
    claim(Path::new(&root), kind)
}

static NEXT_CLAIM: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claims_a_token_once_and_removes_it() {
        let root = std::env::temp_dir().join(format!(
            "novarocks-cleanup-fault-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("create root");
        let trigger = trigger_path(&root, CleanupFaultKind::DropDeleteResponse);
        fs::write(&trigger, "token=test\n").expect("write token");
        assert!(claim(&root, CleanupFaultKind::DropDeleteResponse).expect("claim"));
        assert!(!claim(&root, CleanupFaultKind::DropDeleteResponse).expect("second claim"));
        assert!(!trigger.exists());
        fs::remove_dir_all(root).expect("cleanup root");
    }
}
