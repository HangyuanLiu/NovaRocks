use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InternalRpcTransport {
    Grpc,
    #[cfg(feature = "compat")]
    BrpcCompat,
}

const INTERNAL_RPC_TRANSPORT_DEFAULT: u8 = 0;
const INTERNAL_RPC_TRANSPORT_GRPC: u8 = 1;
#[cfg(all(test, feature = "compat"))]
const INTERNAL_RPC_TRANSPORT_BRPC_COMPAT: u8 = 2;

static INTERNAL_RPC_TRANSPORT_OVERRIDE: AtomicU8 = AtomicU8::new(INTERNAL_RPC_TRANSPORT_DEFAULT);

fn default_internal_rpc_transport_for_current_build() -> InternalRpcTransport {
    #[cfg(all(feature = "compat", not(test)))]
    {
        InternalRpcTransport::BrpcCompat
    }
    #[cfg(not(all(feature = "compat", not(test))))]
    {
        InternalRpcTransport::Grpc
    }
}

pub(crate) fn internal_rpc_transport_for_current_process() -> InternalRpcTransport {
    match INTERNAL_RPC_TRANSPORT_OVERRIDE.load(Ordering::Acquire) {
        INTERNAL_RPC_TRANSPORT_GRPC => InternalRpcTransport::Grpc,
        #[cfg(all(test, feature = "compat"))]
        INTERNAL_RPC_TRANSPORT_BRPC_COMPAT => InternalRpcTransport::BrpcCompat,
        _ => default_internal_rpc_transport_for_current_build(),
    }
}

pub(crate) fn use_grpc_internal_rpc_transport() {
    INTERNAL_RPC_TRANSPORT_OVERRIDE.store(INTERNAL_RPC_TRANSPORT_GRPC, Ordering::Release);
}

#[cfg(test)]
pub(crate) struct InternalRpcTransportOverrideGuard {
    previous: u8,
}

#[cfg(test)]
impl Drop for InternalRpcTransportOverrideGuard {
    fn drop(&mut self) {
        INTERNAL_RPC_TRANSPORT_OVERRIDE.store(self.previous, Ordering::Release);
    }
}

#[cfg(all(test, feature = "compat"))]
pub(crate) fn use_brpc_compat_internal_rpc_transport_for_test() -> InternalRpcTransportOverrideGuard
{
    let previous =
        INTERNAL_RPC_TRANSPORT_OVERRIDE.swap(INTERNAL_RPC_TRANSPORT_BRPC_COMPAT, Ordering::AcqRel);
    InternalRpcTransportOverrideGuard { previous }
}
