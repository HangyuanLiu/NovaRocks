// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF License, Version 2.0.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::DataType;
use chrono::{Datelike, NaiveDate, NaiveDateTime};

use crate::thrift::frontend_service::{self, TFrontendServiceSyncClient};
use crate::thrift::status_code;
use crate::thrift::{exprs, types};
use novarocks::common::config;
use novarocks::connector::starrocks::ports::{
    AutoIncrementRange, AutomaticPartitionEntry, AutomaticPartitionIndex, AutomaticPartitionKey,
    AutomaticPartitionNode, AutomaticPartitionRequest, AutomaticPartitionResult,
    AutomaticPartitionTablet, SinkFrontendAddress, SinkFrontendProvider,
};

const CREATE_PARTITION_TRANSPORT_RETRIES: usize = 4;
const UNIX_EPOCH_DAY_OFFSET: i32 = 719_163;

pub(crate) fn sink_frontend_provider() -> Arc<dyn SinkFrontendProvider> {
    Arc::new(CompatSinkFrontendProvider)
}

struct CompatSinkFrontendProvider;

impl SinkFrontendProvider for CompatSinkFrontendProvider {
    fn create_automatic_partitions(
        &self,
        request: &AutomaticPartitionRequest,
    ) -> Result<AutomaticPartitionResult, String> {
        create_automatic_partitions(
            &request.frontend,
            request.db_id,
            request.table_id,
            request.txn_id,
            request.is_temp,
            request.partition_values.clone(),
        )
    }

    fn allocate_auto_increment_range(
        &self,
        frontend: &SinkFrontendAddress,
        table_id: i64,
        rows: usize,
    ) -> Result<AutoIncrementRange, String> {
        allocate_auto_increment_interval(frontend, table_id, rows)
    }
}

fn to_thrift_address(addr: &SinkFrontendAddress) -> types::TNetworkAddress {
    types::TNetworkAddress::new(addr.host.clone(), addr.port)
}

fn with_control_client<T, F>(
    fe_addr: &SinkFrontendAddress,
    transport_retries: usize,
    f: F,
) -> Result<T, String>
where
    F: Clone + FnOnce(&mut dyn TFrontendServiceSyncClient) -> Result<T, String>,
{
    crate::frontend_rpc::with_control_client(&to_thrift_address(fe_addr), transport_retries, f)
}

pub(crate) fn create_automatic_partitions(
    fe_addr: &SinkFrontendAddress,
    db_id: i64,
    table_id: i64,
    txn_id: i64,
    is_temp: bool,
    partition_values: Vec<Vec<String>>,
) -> Result<AutomaticPartitionResult, String> {
    if db_id <= 0 {
        return Err(format!(
            "invalid db_id for automatic partition create: {db_id}"
        ));
    }
    if table_id <= 0 {
        return Err(format!(
            "invalid table_id for automatic partition create: {table_id}"
        ));
    }
    if txn_id <= 0 {
        return Err(format!(
            "invalid txn_id for automatic partition create: {txn_id}"
        ));
    }
    if partition_values.is_empty() {
        return Err("automatic partition values cannot be empty".to_string());
    }

    let request = frontend_service::TCreatePartitionRequest::new(
        Some(txn_id),
        Some(db_id),
        Some(table_id),
        Some(partition_values),
        Some(is_temp),
        None::<i32>,
    );
    let retry_interval = Duration::from_millis(config::fe_rpc_retry_interval_ms().clamp(1, 5_000));
    let deadline = Instant::now() + Duration::from_millis(config::fe_rpc_timeout_ms().max(1));
    let mut service_unavailable_attempts = 0usize;
    loop {
        let response =
            with_control_client(fe_addr, CREATE_PARTITION_TRANSPORT_RETRIES, |client| {
                client
                    .create_partition(request.clone())
                    .map_err(|e| format!("createPartition RPC failed: {e}"))
            })?;
        let status = response
            .status
            .as_ref()
            .ok_or_else(|| "createPartition response missing status".to_string())?;
        if status.status_code == status_code::TStatusCode::OK {
            return create_partition_result_from_wire(response);
        }
        if status.status_code == status_code::TStatusCode::SERVICE_UNAVAILABLE
            && Instant::now() < deadline
        {
            service_unavailable_attempts += 1;
            if service_unavailable_attempts > 1 {
                std::thread::sleep(retry_interval);
            }
            continue;
        }
        let detail = status
            .error_msgs
            .as_ref()
            .map(|v| v.join("; "))
            .unwrap_or_default();
        return Err(format!(
            "createPartition failed: status={:?}, error={detail}",
            status.status_code
        ));
    }
}

fn create_partition_result_from_wire(
    response: frontend_service::TCreatePartitionResult,
) -> Result<AutomaticPartitionResult, String> {
    let partitions = response
        .partitions
        .unwrap_or_default()
        .into_iter()
        .map(|part| {
            Ok(AutomaticPartitionEntry {
                partition_id: part.id,
                is_shadow: part.is_shadow_partition.unwrap_or(false),
                indexes: part
                    .indexes
                    .into_iter()
                    .map(|idx| AutomaticPartitionIndex {
                        index_id: idx.index_id,
                        tablet_ids: idx.tablet_ids,
                    })
                    .collect(),
                start_key: partition_boundary_key_from_wire(
                    part.start_keys.as_deref(),
                    part.start_key.as_ref(),
                )?,
                end_key: partition_boundary_key_from_wire(
                    part.end_keys.as_deref(),
                    part.end_key.as_ref(),
                )?,
                in_keys: partition_in_keys_from_wire(part.in_keys.as_deref())?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let tablets = response
        .tablets
        .unwrap_or_default()
        .into_iter()
        .map(|tablet| AutomaticPartitionTablet {
            tablet_id: tablet.tablet_id,
            node_ids: tablet.node_ids,
        })
        .collect();
    let nodes = response
        .nodes
        .unwrap_or_default()
        .into_iter()
        .map(|node| {
            let option = i32::try_from(node.option).map_err(|_| {
                format!(
                    "createPartition returned node option out of range: {}",
                    node.option
                )
            })?;
            Ok(AutomaticPartitionNode {
                id: node.id,
                option,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(AutomaticPartitionResult {
        partitions,
        tablets,
        nodes,
    })
}

fn partition_boundary_key_from_wire(
    key_nodes: Option<&[exprs::TExprNode]>,
    legacy_node: Option<&exprs::TExprNode>,
) -> Result<Option<Vec<AutomaticPartitionKey>>, String> {
    let Some(nodes) = key_nodes.or_else(|| legacy_node.map(std::slice::from_ref)) else {
        return Ok(None);
    };
    if nodes.is_empty() {
        return Ok(None);
    }
    parse_partition_key_nodes(nodes).map(Some)
}

fn partition_in_keys_from_wire(
    in_keys: Option<&[Vec<exprs::TExprNode>]>,
) -> Result<Vec<Vec<AutomaticPartitionKey>>, String> {
    let Some(in_keys) = in_keys else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(in_keys.len());
    for key in in_keys {
        out.push(parse_partition_key_nodes(key)?);
    }
    Ok(out)
}

fn parse_partition_key_nodes(
    nodes: &[exprs::TExprNode],
) -> Result<Vec<AutomaticPartitionKey>, String> {
    let mut out = Vec::with_capacity(nodes.len());
    for node in nodes {
        out.push(parse_partition_key_node(node)?);
    }
    Ok(out)
}

fn parse_partition_key_node(node: &exprs::TExprNode) -> Result<AutomaticPartitionKey, String> {
    match node.node_type {
        t if t == exprs::TExprNodeType::NULL_LITERAL => Ok(AutomaticPartitionKey::Null),
        t if t == exprs::TExprNodeType::BOOL_LITERAL => {
            let value = node
                .bool_literal
                .as_ref()
                .ok_or_else(|| "BOOL_LITERAL missing bool_literal payload".to_string())?
                .value;
            Ok(AutomaticPartitionKey::Bool(value))
        }
        t if t == exprs::TExprNodeType::INT_LITERAL => {
            let value = node
                .int_literal
                .as_ref()
                .ok_or_else(|| "INT_LITERAL missing int_literal payload".to_string())?
                .value as i128;
            Ok(AutomaticPartitionKey::Int(value))
        }
        t if t == exprs::TExprNodeType::LARGE_INT_LITERAL => {
            let value = node
                .large_int_literal
                .as_ref()
                .ok_or_else(|| "LARGE_INT_LITERAL missing payload".to_string())?
                .value
                .trim()
                .parse::<i128>()
                .map_err(|_| "LARGE_INT_LITERAL parse failed".to_string())?;
            Ok(AutomaticPartitionKey::Int(value))
        }
        t if t == exprs::TExprNodeType::DECIMAL_LITERAL => {
            let text = node
                .decimal_literal
                .as_ref()
                .ok_or_else(|| "DECIMAL_LITERAL missing decimal_literal payload".to_string())?
                .value
                .clone();
            let DataType::Decimal128(precision, scale) = literal_arrow_type(&node.type_)
                .ok_or_else(|| {
                    "DECIMAL_LITERAL missing or unsupported type descriptor".to_string()
                })?
            else {
                return Err("DECIMAL_LITERAL type descriptor is not decimal".to_string());
            };
            let value = parse_decimal_literal_value(&text, precision, scale)?;
            Ok(AutomaticPartitionKey::Decimal { value, scale })
        }
        t if t == exprs::TExprNodeType::STRING_LITERAL
            || t == exprs::TExprNodeType::DATE_LITERAL =>
        {
            let value = if t == exprs::TExprNodeType::STRING_LITERAL {
                node.string_literal
                    .as_ref()
                    .ok_or_else(|| "STRING_LITERAL missing string_literal payload".to_string())?
                    .value
                    .clone()
            } else {
                node.date_literal
                    .as_ref()
                    .ok_or_else(|| "DATE_LITERAL missing date_literal payload".to_string())?
                    .value
                    .clone()
            };
            match literal_arrow_type(&node.type_) {
                Some(DataType::Date32) => Ok(AutomaticPartitionKey::Date32(
                    parse_date_literal_days(&value)?,
                )),
                Some(DataType::Timestamp(_, _)) | Some(DataType::Time64(_)) => Ok(
                    AutomaticPartitionKey::TimestampMicros(parse_datetime_literal_micros(&value)?),
                ),
                Some(DataType::Binary) => Ok(AutomaticPartitionKey::Binary(value.into_bytes())),
                _ => Ok(AutomaticPartitionKey::Utf8(value)),
            }
        }
        t if t == exprs::TExprNodeType::BINARY_LITERAL => {
            let value = node
                .binary_literal
                .as_ref()
                .ok_or_else(|| "BINARY_LITERAL missing payload".to_string())?
                .value
                .clone();
            Ok(AutomaticPartitionKey::Binary(value))
        }
        t if t == exprs::TExprNodeType::FLOAT_LITERAL => {
            let _ = node
                .float_literal
                .as_ref()
                .ok_or_else(|| "FLOAT_LITERAL missing float_literal payload".to_string())?;
            Err("unsupported partition key literal node type: FLOAT_LITERAL".to_string())
        }
        other => Err(format!(
            "unsupported partition key literal node type: {:?}",
            other
        )),
    }
}

fn literal_arrow_type(desc: &types::TTypeDesc) -> Option<DataType> {
    let scalar = desc.types.as_ref()?.first()?.scalar_type.as_ref()?;
    match scalar.type_ {
        primitive if primitive == types::TPrimitiveType::DATE => Some(DataType::Date32),
        primitive
            if primitive == types::TPrimitiveType::DATETIME
                || primitive == types::TPrimitiveType::TIME =>
        {
            Some(DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                None,
            ))
        }
        primitive
            if primitive == types::TPrimitiveType::BINARY
                || primitive == types::TPrimitiveType::VARBINARY =>
        {
            Some(DataType::Binary)
        }
        primitive
            if primitive == types::TPrimitiveType::DECIMAL
                || primitive == types::TPrimitiveType::DECIMAL32
                || primitive == types::TPrimitiveType::DECIMAL64
                || primitive == types::TPrimitiveType::DECIMAL128 =>
        {
            Some(DataType::Decimal128(
                u8::try_from(scalar.precision?).ok()?,
                i8::try_from(scalar.scale?).ok()?,
            ))
        }
        primitive if primitive == types::TPrimitiveType::DECIMALV2 => {
            Some(DataType::Decimal128(27, 9))
        }
        _ => None,
    }
}

fn parse_date_literal_days(value: &str) -> Result<i32, String> {
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return Ok(date.num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET);
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return Ok(dt.date().num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET);
    }
    Err(format!("invalid DATE literal '{value}'"))
}

fn parse_datetime_literal_micros(value: &str) -> Result<i64, String> {
    if let Ok(dt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(dt.and_utc().timestamp_micros());
    }
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        let dt = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| format!("invalid DATETIME literal '{value}'"))?;
        return Ok(dt.and_utc().timestamp_micros());
    }
    Err(format!("invalid DATETIME literal '{value}'"))
}

fn parse_decimal_literal_value(value: &str, precision: u8, scale: i8) -> Result<i128, String> {
    if scale < 0 {
        return Err(format!("invalid decimal scale: {scale}"));
    }
    let mut s = value.trim();
    if s.is_empty() {
        return Err("empty DECIMAL literal".to_string());
    }

    let mut sign: i128 = 1;
    if let Some(rest) = s.strip_prefix('-') {
        sign = -1;
        s = rest;
    } else if let Some(rest) = s.strip_prefix('+') {
        s = rest;
    }
    if s.is_empty() {
        return Err("empty DECIMAL literal".to_string());
    }

    let mut iter = s.split('.');
    let int_part_raw = iter.next().unwrap_or("");
    let frac_part = iter.next().unwrap_or("");
    if iter.next().is_some() {
        return Err(format!("invalid DECIMAL literal '{value}'"));
    }
    if int_part_raw.is_empty() && frac_part.is_empty() {
        return Err(format!("invalid DECIMAL literal '{value}'"));
    }

    let int_part = if int_part_raw.is_empty() {
        "0"
    } else {
        int_part_raw
    };
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(format!("invalid DECIMAL literal '{value}'"));
    }

    let scale_usize = scale as usize;
    if frac_part.len() > scale_usize {
        return Err(format!(
            "DECIMAL literal '{}' exceeds scale {}",
            value, scale_usize
        ));
    }

    let mut digits = String::with_capacity(int_part.len() + scale_usize);
    digits.push_str(int_part);
    digits.push_str(frac_part);
    for _ in 0..(scale_usize - frac_part.len()) {
        digits.push('0');
    }

    let digits_trim = digits.trim_start_matches('0');
    let digits_final = if digits_trim.is_empty() {
        "0"
    } else {
        digits_trim
    };
    if digits_final.len() > precision as usize {
        return Err(format!(
            "DECIMAL literal '{}' exceeds precision {}",
            value, precision
        ));
    }

    let unsigned = digits_final
        .parse::<i128>()
        .map_err(|_| format!("failed to parse DECIMAL literal '{value}'"))?;
    Ok(unsigned.saturating_mul(sign))
}

pub(crate) fn allocate_auto_increment_interval(
    fe_addr: &SinkFrontendAddress,
    table_id: i64,
    rows: usize,
) -> Result<AutoIncrementRange, String> {
    if table_id <= 0 {
        return Err(format!(
            "invalid table_id for auto increment allocation: {table_id}"
        ));
    }
    if rows == 0 {
        return Err("auto increment allocation rows cannot be zero".to_string());
    }
    let rows_i64 = i64::try_from(rows)
        .map_err(|_| format!("auto increment allocation rows overflow: {rows}"))?;
    let request = frontend_service::TAllocateAutoIncrementIdParam {
        table_id: Some(table_id),
        rows: Some(rows_i64),
    };
    let response = with_control_client(fe_addr, 1, |client| {
        client
            .alloc_auto_increment_id(request)
            .map_err(|e| format!("alloc_auto_increment_id RPC failed: {e}"))
    })?;
    auto_increment_interval_from_wire(response)
}

fn auto_increment_interval_from_wire(
    response: frontend_service::TAllocateAutoIncrementIdResult,
) -> Result<AutoIncrementRange, String> {
    let status = response
        .status
        .as_ref()
        .ok_or_else(|| "alloc_auto_increment_id response missing status".to_string())?;
    if status.status_code != status_code::TStatusCode::OK {
        let detail = status
            .error_msgs
            .as_ref()
            .map(|v| v.join("; "))
            .unwrap_or_default();
        return Err(format!(
            "alloc_auto_increment_id failed: status={:?}, error={}",
            status.status_code, detail
        ));
    }
    let start = response
        .auto_increment_id
        .ok_or_else(|| "alloc_auto_increment_id response missing auto_increment_id".to_string())?;
    let allocated_rows = response
        .allocated_rows
        .ok_or_else(|| "alloc_auto_increment_id response missing allocated_rows".to_string())?;
    if allocated_rows <= 0 {
        return Err(format!(
            "alloc_auto_increment_id returned invalid allocated_rows={allocated_rows}"
        ));
    }
    let end = start.checked_add(allocated_rows).ok_or_else(|| {
        format!(
            "auto increment interval overflow: start={} allocated_rows={}",
            start, allocated_rows
        )
    })?;
    Ok(AutoIncrementRange { start, end })
}
