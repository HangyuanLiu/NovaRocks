//! Frontend-owned native transport.
//!
//! Generated Tonic stubs are deliberately private: Frontend implements Core's
//! carrier-neutral ports but does not re-export a role-neutral transport API.

pub(crate) mod codec;
pub(crate) mod report_server;
pub(crate) mod transport;

pub(crate) mod generated {
    include!(concat!(env!("OUT_DIR"), "/novarocks.rs"));
}
