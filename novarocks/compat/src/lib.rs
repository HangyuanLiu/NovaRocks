mod application;
mod backend_service;
mod brpc;
mod control;
mod disk_report;
pub mod fragment;
mod frontend_rpc;
mod heartbeat_service;
mod internal_rpc_client;
mod lake_agent_tasks;
mod lake_ffi;
mod lake_storage;
mod listeners;
mod load;
mod protocol;
mod report;
mod schema_fe_tables;
mod schema_frontend;
mod schema_loads;
mod schema_provider;
mod schema_tracking_logs;
mod sink_frontend;
mod starlet_metadata;
mod storage_wire;
mod thrift_debug;

pub use application::{
    CompatApplicationError, CompatApplicationErrorKind, CompatApplicationHost, CompatServerConfig,
    run_compat_server_until_shutdown,
};
