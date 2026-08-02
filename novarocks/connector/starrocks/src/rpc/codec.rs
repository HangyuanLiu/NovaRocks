use std::fmt;
use std::sync::Arc;

use arrow::datatypes::DataType;
use bytes::Bytes;
use novarocks_spi::connector::ConnectorError;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::codec::{Base64Bytes, CODEC_VERSION, decode_v1, encode_v1};
use crate::direct::DirectOuterFacts;
use crate::domain::{
    StarRocksRpcOpaquePayload, StarRocksRpcTransport, StarRocksSelectedStrategy, invalid,
};

use super::StarRocksRemoteEndpoint;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StarRocksRpcOutputBinding {
    pub output_index: Option<usize>,
    pub remote_slot_id: i32,
    pub name: Arc<str>,
    pub data_type: DataType,
    pub nullable: bool,
    pub is_const: bool,
    pub row_marker: bool,
}

#[derive(Clone)]
pub struct StarRocksRpcSplit {
    transport: StarRocksRpcTransport,
    endpoint: StarRocksRemoteEndpoint,
    token: Bytes,
    outputs: Vec<StarRocksRpcOutputBinding>,
}

impl StarRocksRpcSplit {
    pub fn try_new(
        transport: StarRocksRpcTransport,
        endpoint: StarRocksRemoteEndpoint,
        token: Bytes,
        outputs: Vec<StarRocksRpcOutputBinding>,
    ) -> Result<Self, ConnectorError> {
        if token.is_empty()
            || std::str::from_utf8(&token).is_err()
            || outputs.is_empty()
            || outputs.iter().any(|output| {
                output.name.is_empty() || output.row_marker == output.output_index.is_some()
            })
        {
            return Err(invalid(
                "StarRocks RPC split requires a token and output mapping",
            ));
        }
        let remote_slots = outputs
            .iter()
            .map(|output| output.remote_slot_id)
            .collect::<std::collections::BTreeSet<_>>();
        if remote_slots.len() != outputs.len() {
            return Err(invalid("StarRocks RPC split has duplicate remote slots"));
        }
        Ok(Self {
            transport,
            endpoint,
            token,
            outputs,
        })
    }
    pub fn transport(&self) -> StarRocksRpcTransport {
        self.transport
    }
    pub fn endpoint(&self) -> &StarRocksRemoteEndpoint {
        &self.endpoint
    }
    pub fn outputs(&self) -> &[StarRocksRpcOutputBinding] {
        &self.outputs
    }
    pub(crate) fn token(&self) -> &Bytes {
        &self.token
    }
}

impl fmt::Debug for StarRocksRpcSplit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StarRocksRpcSplit")
            .field("transport", &self.transport)
            .field("endpoint", &self.endpoint)
            .field("outputs", &self.outputs)
            .field("token_len", &self.token.len())
            .finish()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RpcSplitPayload {
    version: u16,
    owner: String,
    incarnation: Base64Bytes,
    attempt: Uuid,
    freeze: Base64Bytes,
    strategy: StarRocksSelectedStrategy,
    schema_version: Base64Bytes,
    data_version: Base64Bytes,
    output_schema_digest: Base64Bytes,
    endpoint_host: String,
    endpoint_port: u16,
    token: Base64Bytes,
    outputs: Vec<RpcOutput>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RpcOutput {
    output_index: Option<usize>,
    remote_slot_id: i32,
    name: String,
    data_type: String,
    nullable: bool,
    is_const: bool,
    row_marker: bool,
}

pub(crate) fn encode_rpc_split(
    facts: &DirectOuterFacts,
    split: &StarRocksRpcSplit,
    max_bytes: usize,
) -> Result<StarRocksRpcOpaquePayload, ConnectorError> {
    let StarRocksSelectedStrategy::Rpc { transport } = facts.strategy else {
        return Err(invalid("cannot encode an RPC split for a direct strategy"));
    };
    if split.transport != transport {
        return Err(invalid(
            "RPC split transport does not match frozen strategy",
        ));
    }
    let payload = RpcSplitPayload {
        version: CODEC_VERSION,
        owner: facts.owner.to_string(),
        incarnation: Base64Bytes(Bytes::copy_from_slice(&facts.incarnation)),
        attempt: facts.attempt,
        freeze: Base64Bytes(Bytes::copy_from_slice(&facts.freeze.0)),
        strategy: facts.strategy,
        schema_version: Base64Bytes(facts.schema_version.clone()),
        data_version: Base64Bytes(facts.data_version.clone()),
        output_schema_digest: Base64Bytes(Bytes::copy_from_slice(&facts.output_schema_digest)),
        endpoint_host: split.endpoint.host().to_string(),
        endpoint_port: split.endpoint.port(),
        token: Base64Bytes(split.token.clone()),
        outputs: split
            .outputs
            .iter()
            .map(|binding| RpcOutput {
                output_index: binding.output_index,
                remote_slot_id: binding.remote_slot_id,
                name: binding.name.to_string(),
                data_type: data_type_name(&binding.data_type),
                nullable: binding.nullable,
                is_const: binding.is_const,
                row_marker: binding.row_marker,
            })
            .collect(),
    };
    StarRocksRpcOpaquePayload::new(encode_v1(&payload, "RPC split", max_bytes)?)
}

pub(crate) fn decode_rpc_split(
    bytes: &Bytes,
    facts: &DirectOuterFacts,
) -> Result<StarRocksRpcSplit, ConnectorError> {
    let payload: RpcSplitPayload = decode_v1(bytes, "RPC split")?;
    if payload.version != CODEC_VERSION
        || payload.owner.as_str() != facts.owner.as_ref()
        || payload.incarnation.0.as_ref() != facts.incarnation
        || payload.attempt != facts.attempt
        || payload.freeze.0.as_ref() != facts.freeze.0
        || payload.strategy != facts.strategy
        || payload.schema_version.0 != facts.schema_version
        || payload.data_version.0 != facts.data_version
        || payload.output_schema_digest.0.as_ref() != facts.output_schema_digest
    {
        return Err(invalid("RPC split does not match outer frozen facts"));
    }
    if payload.token.0.is_empty() {
        return Err(invalid("StarRocks RPC split token must not be empty"));
    }
    let StarRocksSelectedStrategy::Rpc { transport } = payload.strategy else {
        return Err(invalid("RPC split is not tagged as RPC"));
    };
    let endpoint = StarRocksRemoteEndpoint::try_new(payload.endpoint_host, payload.endpoint_port)?;
    let mut seen = std::collections::BTreeSet::new();
    let outputs = payload
        .outputs
        .into_iter()
        .map(|output| {
            if output.name.is_empty()
                || !seen.insert(output.remote_slot_id)
                || output.row_marker == output.output_index.is_some()
            {
                return Err(invalid("invalid StarRocks RPC output mapping"));
            }
            Ok(StarRocksRpcOutputBinding {
                output_index: output.output_index,
                remote_slot_id: output.remote_slot_id,
                name: Arc::from(output.name),
                data_type: parse_data_type(&output.data_type)?,
                nullable: output.nullable,
                is_const: output.is_const,
                row_marker: output.row_marker,
            })
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    StarRocksRpcSplit::try_new(transport, endpoint, payload.token.0, outputs)
}

fn data_type_name(value: &DataType) -> String {
    format!("{value:?}")
}
fn parse_data_type(value: &str) -> Result<DataType, ConnectorError> {
    match value {
        "Boolean" => Ok(DataType::Boolean),
        "Int8" => Ok(DataType::Int8),
        "Int16" => Ok(DataType::Int16),
        "Int32" => Ok(DataType::Int32),
        "Int64" => Ok(DataType::Int64),
        "Float32" => Ok(DataType::Float32),
        "Float64" => Ok(DataType::Float64),
        "Utf8" => Ok(DataType::Utf8),
        "Binary" => Ok(DataType::Binary),
        _ => Err(crate::domain::unsupported(
            "unsupported StarRocks RPC output type",
        )),
    }
}
