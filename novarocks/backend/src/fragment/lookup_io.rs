use std::sync::Arc;

use novarocks_execution::runtime::fragment::io::{
    FragmentIoError, FragmentIoErrorKind, FragmentIoOperation, FragmentLookupClient, LookupBatch,
    LookupColumn, LookupRequest,
};

use crate::BackendDataRuntime;
use crate::rpc::client::BackendRpcClient;

pub(crate) fn grpc_fragment_lookup_client(
    runtime: BackendDataRuntime,
) -> Arc<dyn FragmentLookupClient> {
    Arc::new(GrpcFragmentLookupClient { runtime })
}

struct GrpcFragmentLookupClient {
    runtime: BackendDataRuntime,
}

impl FragmentLookupClient for GrpcFragmentLookupClient {
    fn lookup(&self, request: LookupRequest) -> Result<LookupBatch, FragmentIoError> {
        let endpoint = request.target().endpoint().ok_or_else(|| {
            lookup_error(
                FragmentIoErrorKind::InvalidResponse,
                format!(
                    "lookup target {} has no endpoint",
                    request.target().backend_id()
                ),
            )
        })?;
        let port = u16::try_from(endpoint.port()).map_err(|error| {
            lookup_error(
                FragmentIoErrorKind::InvalidResponse,
                format!("invalid gRPC lookup port: {error}"),
            )
        })?;
        let request = remote_request(&request)?;
        let response = BackendRpcClient::new_host_port(
            self.runtime.clone(),
            endpoint.host().to_string(),
            port,
        )
        .and_then(|client| client.lookup(request))
        .map_err(|error| lookup_error(FragmentIoErrorKind::Unavailable, error))?;
        decode_response(response)
    }
}

fn remote_request(
    request: &LookupRequest,
) -> Result<novarocks_proto_models::filter::LookupRequest, FragmentIoError> {
    let mut output = novarocks_proto_models::filter::LookupRequest {
        query_id: Some(novarocks_proto_models::common::UniqueId {
            hi: request.query_id().high(),
            lo: request.query_id().low(),
        }),
        lookup_node_id: request.lookup_node_id(),
        request_tuple_id: request.tuple_id(),
        request_columns: Vec::with_capacity(request.columns().len()),
    };
    for column in request.columns() {
        let data = crate::runtime::lookup::encode_column_ipc(column.values())
            .map_err(|error| lookup_error(FragmentIoErrorKind::Internal, error))?;
        output
            .request_columns
            .push(novarocks_proto_models::filter::Column {
                slot_id: column.slot_id().as_u32() as i32,
                data_size: data.len() as i64,
                data,
            });
    }
    Ok(output)
}

fn decode_response(
    response: novarocks_proto_models::filter::LookupResponse,
) -> Result<LookupBatch, FragmentIoError> {
    if let Some(status) = response.status.as_ref()
        && status.code != 0
    {
        return Err(lookup_error(
            FragmentIoErrorKind::RemoteRejected,
            format!("lookup failed: {}", status.message),
        ));
    }
    let mut columns = Vec::with_capacity(response.columns.len());
    for column in response.columns {
        if column.data.is_empty() {
            return Err(lookup_error(
                FragmentIoErrorKind::InvalidResponse,
                "lookup response column missing data",
            ));
        }
        let slot_id = novarocks_types::SlotId::try_from(column.slot_id).map_err(|error| {
            lookup_error(FragmentIoErrorKind::InvalidResponse, error.to_string())
        })?;
        let values = crate::runtime::lookup::decode_column_ipc(&column.data)
            .map_err(|error| lookup_error(FragmentIoErrorKind::InvalidResponse, error))?;
        columns.push(LookupColumn::new(slot_id, values));
    }
    Ok(LookupBatch::new(columns))
}

fn lookup_error(kind: FragmentIoErrorKind, message: impl Into<String>) -> FragmentIoError {
    FragmentIoError::new(FragmentIoOperation::Lookup, kind, message)
}
