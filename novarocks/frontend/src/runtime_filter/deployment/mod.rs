//! Frontend-owned runtime-filter deployment safety checks.
//!
//! The deployment compiler supplies its proposed participant installs here
//! before they are encoded for the lifecycle barrier.  Core deliberately does
//! not interpret this policy or wait graph.

mod channel_projection;
mod liveness;
mod role_graph;
mod routing;

pub(crate) use channel_projection::{
    ChannelInstallInput, ChannelProjectionError, ChannelRouteFact, ConsumerInstallInput,
    OutboundMaterializationOwner, ProducerInstallInput, project_channel_installs,
};
pub(crate) use liveness::{RuntimeFilterWaitEdge, RuntimeFilterWaitGraph};
pub(crate) use role_graph::{
    RoutingAvailability, RoutingBindingPlacement, RoutingChannelInput,
    RoutingProducerInstancePlacement,
};
pub(crate) use routing::compile_routing;
