use crate::cluster::ServerHandle;
use crate::types::QueryMeta;
use anyhow::{Result, bail};
use std::thread::sleep;
use std::time::Duration;

pub(crate) fn has_fault(meta: &QueryMeta) -> bool {
    meta.kill_be_index.is_some()
        || meta.network_partition_be.is_some()
        || meta.heartbeat_delay_ms.is_some()
        || meta.restart_be_delay_ms.is_some()
}

pub(crate) fn apply_pre_query(meta: &QueryMeta, server: &mut dyn ServerHandle) -> Result<()> {
    if let Some(index) = meta.network_partition_be {
        bail!(
            "network_partition_be is unsupported by the SQL test runner in Task 7.1 (index={index})"
        );
    }

    if meta.restart_be_delay_ms.is_some() && meta.kill_be_index.is_none() {
        bail!("restart_be_delay_ms requires kill_be_index so the runner knows which BE to restart");
    }

    if has_fault(meta) && !server.supports_fault_injection() {
        bail!(
            "fault injection directives require a mutable cross-process server handle; current server mode does not support fault injection"
        );
    }

    if let Some(index) = meta.kill_be_index {
        server.kill_be(index)?;
        if let Some(delay_ms) = meta.restart_be_delay_ms {
            sleep(Duration::from_millis(delay_ms));
            server.restart_be(index)?;
        }
    }

    if let Some(delay_ms) = meta.heartbeat_delay_ms {
        sleep(Duration::from_millis(delay_ms));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct RecordingServerHandle {
        events: Vec<String>,
    }

    impl ServerHandle for RecordingServerHandle {
        fn target_host(&self) -> Option<&str> {
            None
        }

        fn target_port(&self) -> Option<u16> {
            None
        }

        fn supports_fault_injection(&self) -> bool {
            true
        }

        fn kill_be(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("kill:{index}"));
            Ok(())
        }

        fn restart_be(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("restart:{index}"));
            Ok(())
        }
    }

    #[test]
    fn has_fault_detects_any_fault_directive() {
        assert!(!has_fault(&QueryMeta::default()));
        assert!(has_fault(&QueryMeta {
            heartbeat_delay_ms: Some(0),
            ..QueryMeta::default()
        }));
    }

    #[test]
    fn restart_delay_without_kill_is_rejected() {
        let meta = QueryMeta {
            restart_be_delay_ms: Some(0),
            ..QueryMeta::default()
        };
        let mut server = RecordingServerHandle::default();

        let err = apply_pre_query(&meta, &mut server).expect_err("restart without kill");

        assert!(
            err.to_string()
                .contains("restart_be_delay_ms requires kill_be_index"),
            "unexpected error: {err}"
        );
        assert!(server.events.is_empty());
    }

    #[test]
    fn unsupported_server_mode_rejects_fault_directives() {
        struct UnsupportedServerHandle;

        impl ServerHandle for UnsupportedServerHandle {
            fn target_host(&self) -> Option<&str> {
                None
            }

            fn target_port(&self) -> Option<u16> {
                None
            }
        }

        let meta = QueryMeta {
            heartbeat_delay_ms: Some(0),
            ..QueryMeta::default()
        };
        let mut server = UnsupportedServerHandle;

        let err = apply_pre_query(&meta, &mut server).expect_err("unsupported server mode");

        assert!(
            err.to_string()
                .contains("require a mutable cross-process server handle"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn network_partition_is_explicitly_unsupported() {
        let meta = QueryMeta {
            network_partition_be: Some(1),
            ..QueryMeta::default()
        };
        let mut server = RecordingServerHandle::default();

        let err = apply_pre_query(&meta, &mut server).expect_err("unsupported partition");

        assert!(
            err.to_string()
                .contains("network_partition_be is unsupported"),
            "unexpected error: {err}"
        );
        assert!(server.events.is_empty());
    }

    #[test]
    fn kill_and_restart_target_same_be() {
        let meta = QueryMeta {
            kill_be_index: Some(2),
            restart_be_delay_ms: Some(0),
            ..QueryMeta::default()
        };
        let mut server = RecordingServerHandle::default();

        apply_pre_query(&meta, &mut server).expect("apply fault");

        assert_eq!(server.events, vec!["kill:2", "restart:2"]);
    }
}
