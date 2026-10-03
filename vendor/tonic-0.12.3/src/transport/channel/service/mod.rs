mod add_origin;
use self::add_origin::AddOrigin;

mod user_agent;
use self::user_agent::UserAgent;

mod reconnect;
use self::reconnect::Reconnect;

mod connection;
pub(super) use self::connection::Connection;

mod discover;
pub(super) use self::discover::DynamicServiceStream;

mod io;
use self::io::BoxedIo;

mod connector;
pub(crate) use self::connector::Connector;

mod executor;
pub(super) use self::executor::{Executor, SharedExec};

#[cfg(feature = "tls")]
mod tls;
#[cfg(feature = "tls")]
pub(super) use self::tls::TlsConnector;

mod attempt_connector;
pub(crate) use self::attempt_connector::AttemptTimeoutConnector;

mod connection_driver;
mod request_task_executor;
mod request_task_pool;
pub use self::request_task_pool::OriginalHttp2RequestTaskPool;
pub use connection_driver::{
    http2_protocol_task_allocation_capacity_bound,
    http2_split_client_task_allocation_capacity_bounds, OriginalConnectionDriver,
    OriginalHttp2ProtocolTask,
};
