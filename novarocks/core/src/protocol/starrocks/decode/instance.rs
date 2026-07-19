// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroUsize;

use crate::common::types::UniqueId;
use crate::exec::fragment::program::{FragmentNodeId, ScanAssignmentKind, ScanSourceContract};
use crate::protocol::common::error::FieldPath;
use crate::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
use crate::runtime::fragment::instance::ScanAssignments;
use crate::runtime::fragment::instance::{BackendNum, FragmentInstanceId};
use crate::runtime::query_context::QueryId;
use crate::runtime::query_options::QueryOptions;
use crate::runtime::runtime_filter_params::RuntimeFilterParams;
use crate::runtime::scan_range::{
    BrokerFileFormat, BrokerFileScanRange, DatacacheOptions, DeletionVectorDescriptor, FileFormat,
    FileScanRange, IcebergDeleteFile, IcebergFileContent, IcebergFileFormat, ScanRangeParams,
};
use crate::thrift::{descriptors, internal_service, plan_nodes, types};

use super::{
    StarRocksFragmentDecodeError, decode_query_options, decode_runtime_endpoint,
    decode_runtime_filter_params,
};

pub(crate) struct DecodedStarRocksInstanceParts {
    pub(crate) query_id: QueryId,
    pub(crate) fragment_instance_id: FragmentInstanceId,
    pub(crate) backend_num: BackendNum,
    pub(crate) query_options: QueryOptions,
    pub(crate) pipeline_dop: NonZeroUsize,
    pub(crate) scan_ranges: BTreeMap<i32, Vec<internal_service::TScanRangeParams>>,
    pub(crate) per_exchange_sender_counts: BTreeMap<i32, i32>,
    pub(crate) batch_exchange_sender_counts: HashMap<i32, usize>,
    pub(crate) runtime_filter_params: RuntimeFilterParams,
    pub(crate) report_endpoint: Option<RuntimeEndpoint>,
    pub(crate) destinations: Vec<FragmentDestination>,
    pub(crate) sender_id: Option<i32>,
    pub(crate) typed_result_sink: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct LakeScanProgramFacts {
    pub(crate) db_name: Option<String>,
    pub(crate) table_name: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct LakeMetaScanRangeFact {
    pub(crate) tablet_id: i64,
    pub(crate) version: i64,
    pub(crate) row_count: Option<i64>,
    pub(crate) partition_id: Option<i64>,
    pub(crate) db_name: Option<String>,
    pub(crate) table_name: Option<String>,
    pub(crate) empty: bool,
    pub(crate) has_more: bool,
}

pub(crate) fn decode_lake_meta_scan_range_facts(
    nodes: &[plan_nodes::TPlanNode],
    raw_ranges: &BTreeMap<i32, Vec<internal_service::TScanRangeParams>>,
    path: FieldPath,
) -> Result<BTreeMap<i32, Vec<LakeMetaScanRangeFact>>, StarRocksFragmentDecodeError> {
    let mut output = BTreeMap::new();
    for node in nodes
        .iter()
        .filter(|node| node.node_type == plan_nodes::TPlanNodeType::LAKE_META_SCAN_NODE)
    {
        let ranges = raw_ranges.get(&node.node_id).ok_or_else(|| {
            StarRocksFragmentDecodeError::missing(
                path.clone().map_key(node.node_id.to_string()),
                "LAKE_META_SCAN_NODE requires per-node ranges",
            )
        })?;
        let mut decoded = Vec::with_capacity(ranges.len());
        for (index, params) in ranges.iter().enumerate() {
            if params.empty.unwrap_or(false) {
                decoded.push(LakeMetaScanRangeFact {
                    tablet_id: 0,
                    version: 0,
                    row_count: None,
                    partition_id: None,
                    db_name: None,
                    table_name: None,
                    empty: true,
                    has_more: params.has_more.unwrap_or(false),
                });
                continue;
            }
            let internal = params
                .scan_range
                .internal_scan_range
                .as_ref()
                .ok_or_else(|| {
                    StarRocksFragmentDecodeError::missing(
                        path.clone()
                            .map_key(node.node_id.to_string())
                            .index(index)
                            .field("scan_range")
                            .field("internal_scan_range"),
                        "LAKE_META_SCAN_NODE requires internal_scan_range",
                    )
                })?;
            let version = internal.version.parse::<i64>().map_err(|error| {
                StarRocksFragmentDecodeError::invalid_value(
                    path.clone()
                        .map_key(node.node_id.to_string())
                        .index(index)
                        .field("scan_range")
                        .field("internal_scan_range")
                        .field("version"),
                    format!("invalid tablet version {:?}: {error}", internal.version),
                )
            })?;
            decoded.push(LakeMetaScanRangeFact {
                tablet_id: internal.tablet_id,
                version,
                row_count: internal.row_count,
                partition_id: internal.partition_id,
                db_name: (!internal.db_name.trim().is_empty())
                    .then(|| internal.db_name.trim().to_string()),
                table_name: internal
                    .table_name
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string),
                empty: false,
                has_more: params.has_more.unwrap_or(false),
            });
        }
        output.insert(node.node_id, decoded);
    }
    Ok(output)
}

pub(crate) fn decode_lake_scan_program_facts(
    nodes: &[plan_nodes::TPlanNode],
    raw_ranges: &BTreeMap<i32, Vec<internal_service::TScanRangeParams>>,
    path: FieldPath,
) -> Result<BTreeMap<i32, LakeScanProgramFacts>, StarRocksFragmentDecodeError> {
    let mut output = BTreeMap::new();
    for node in nodes
        .iter()
        .filter(|node| node.node_type == plan_nodes::TPlanNodeType::LAKE_SCAN_NODE)
    {
        let mut facts = LakeScanProgramFacts::default();
        for (index, params) in raw_ranges
            .get(&node.node_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
            .iter()
            .enumerate()
        {
            if params.empty.unwrap_or(false) {
                continue;
            }
            let internal = params
                .scan_range
                .internal_scan_range
                .as_ref()
                .ok_or_else(|| {
                    StarRocksFragmentDecodeError::missing(
                        path.clone()
                            .map_key(node.node_id.to_string())
                            .index(index)
                            .field("scan_range")
                            .field("internal_scan_range"),
                        "LAKE_SCAN_NODE requires internal_scan_range",
                    )
                })?;
            let fill_data_cache = internal.fill_data_cache.unwrap_or(true);
            let skip_page_cache = internal.skip_page_cache.unwrap_or(false);
            let skip_disk_cache = internal.skip_disk_cache.unwrap_or(false);
            if !fill_data_cache || skip_page_cache || skip_disk_cache {
                return Err(StarRocksFragmentDecodeError::unsupported(
                    path.clone()
                        .map_key(node.node_id.to_string())
                        .index(index)
                        .field("scan_range")
                        .field("internal_scan_range"),
                    "internal-table cache controls are not supported",
                ));
            }
            if facts.db_name.is_none() {
                facts.db_name = (!internal.db_name.trim().is_empty())
                    .then(|| internal.db_name.trim().to_string());
            }
            if facts.table_name.is_none() {
                facts.table_name = internal
                    .table_name
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string);
            }
        }
        output.insert(node.node_id, facts);
    }
    Ok(output)
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct StarRocksDecodeFacts {
    stream_load_paths: BTreeMap<UniqueId, String>,
    path_rewrite: Option<StarRocksPathRewriteFacts>,
    datacache_available: bool,
    jdbc: Option<StarRocksJdbcFacts>,
    object_store_defaults: StarRocksObjectStoreDefaults,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct StarRocksObjectStoreDefaults {
    retry_max_times: Option<usize>,
    retry_min_delay_ms: Option<u64>,
    retry_max_delay_ms: Option<u64>,
    timeout_ms: Option<u64>,
    io_timeout_ms: Option<u64>,
}

impl StarRocksObjectStoreDefaults {
    pub(crate) fn new(
        retry_max_times: Option<usize>,
        retry_min_delay_ms: Option<u64>,
        retry_max_delay_ms: Option<u64>,
        timeout_ms: Option<u64>,
        io_timeout_ms: Option<u64>,
    ) -> Self {
        Self {
            retry_max_times,
            retry_min_delay_ms,
            retry_max_delay_ms,
            timeout_ms,
            io_timeout_ms,
        }
    }

    pub(crate) fn apply_to(&self, config: &mut crate::fs::object_store::ObjectStoreConfig) {
        config.retry_max_times = config.retry_max_times.or(self.retry_max_times);
        config.retry_min_delay_ms = config.retry_min_delay_ms.or(self.retry_min_delay_ms);
        config.retry_max_delay_ms = config.retry_max_delay_ms.or(self.retry_max_delay_ms);
        config.timeout_ms = config.timeout_ms.or(self.timeout_ms);
        config.io_timeout_ms = config.io_timeout_ms.or(self.io_timeout_ms);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StarRocksJdbcFacts {
    url: String,
    user: Option<String>,
    password: Option<String>,
    default_db: Option<String>,
}

impl StarRocksJdbcFacts {
    pub(crate) fn new(
        url: String,
        user: Option<String>,
        password: Option<String>,
        default_db: Option<String>,
    ) -> Self {
        Self {
            url,
            user,
            password,
            default_db,
        }
    }

    pub(crate) fn connection(&self) -> (String, Option<String>, Option<String>) {
        (self.url.clone(), self.user.clone(), self.password.clone())
    }

    pub(crate) fn default_db(&self) -> Option<&str> {
        self.default_db.as_deref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StarRocksPathRewriteFacts {
    from_prefix: String,
    to_prefix: String,
}

impl StarRocksPathRewriteFacts {
    pub(crate) fn new(from_prefix: String, to_prefix: String) -> Self {
        Self {
            from_prefix,
            to_prefix,
        }
    }

    pub(crate) fn from_prefix(&self) -> &str {
        &self.from_prefix
    }

    pub(crate) fn to_prefix(&self) -> &str {
        &self.to_prefix
    }
}

impl StarRocksDecodeFacts {
    pub(crate) fn new(
        stream_load_paths: BTreeMap<UniqueId, String>,
        path_rewrite: Option<StarRocksPathRewriteFacts>,
        datacache_available: bool,
        jdbc: Option<StarRocksJdbcFacts>,
        object_store_defaults: StarRocksObjectStoreDefaults,
    ) -> Self {
        Self {
            stream_load_paths,
            path_rewrite,
            datacache_available,
            jdbc,
            object_store_defaults,
        }
    }

    pub(crate) fn stream_load_path(&self, id: UniqueId) -> Option<&str> {
        self.stream_load_paths.get(&id).map(String::as_str)
    }

    pub(crate) fn path_rewrite(&self) -> Option<&StarRocksPathRewriteFacts> {
        self.path_rewrite.as_ref()
    }

    pub(crate) const fn datacache_available(&self) -> bool {
        self.datacache_available
    }

    pub(crate) fn jdbc(&self) -> Option<&StarRocksJdbcFacts> {
        self.jdbc.as_ref()
    }

    pub(crate) fn object_store_defaults(&self) -> &StarRocksObjectStoreDefaults {
        &self.object_store_defaults
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_instance_parts(
    params: &internal_service::TPlanFragmentExecParams,
    query_options: Option<&internal_service::TQueryOptions>,
    coord: Option<&types::TNetworkAddress>,
    backend_num: Option<i32>,
    pipeline_dop: i32,
    batch_exchange_sender_counts: &HashMap<i32, usize>,
    typed_result_sink: bool,
    _facts: &StarRocksDecodeFacts,
    root_path: FieldPath,
) -> Result<DecodedStarRocksInstanceParts, StarRocksFragmentDecodeError> {
    let params_path = root_path.clone().field("params");
    let pipeline_dop = usize::try_from(pipeline_dop)
        .ok()
        .and_then(NonZeroUsize::new)
        .ok_or_else(|| {
            StarRocksFragmentDecodeError::out_of_range(
                root_path
                    .clone()
                    .field("query_options")
                    .field("pipeline_dop"),
                format!("pipeline_dop must be positive, got {pipeline_dop}"),
            )
        })?;
    let backend_num = backend_num.unwrap_or(0);
    let backend_num =
        BackendNum::try_new(backend_num).map_err(StarRocksFragmentDecodeError::Binding)?;
    let query_options = decode_query_options(query_options)?;
    let runtime_filter_params = params
        .runtime_filter_params
        .as_ref()
        .map(|value| {
            decode_runtime_filter_params(value, params_path.clone().field("runtime_filter_params"))
        })
        .transpose()?
        .unwrap_or_default();
    let report_endpoint = coord
        .map(|value| decode_runtime_endpoint(value, root_path.clone().field("coord")))
        .transpose()?;
    Ok(DecodedStarRocksInstanceParts {
        query_id: QueryId {
            hi: params.query_id.hi,
            lo: params.query_id.lo,
        },
        fragment_instance_id: FragmentInstanceId::new(UniqueId {
            hi: params.fragment_instance_id.hi,
            lo: params.fragment_instance_id.lo,
        }),
        backend_num,
        query_options,
        pipeline_dop,
        scan_ranges: params.per_node_scan_ranges.clone(),
        per_exchange_sender_counts: params.per_exch_num_senders.clone(),
        batch_exchange_sender_counts: batch_exchange_sender_counts.clone(),
        runtime_filter_params,
        report_endpoint,
        destinations: params
            .destinations
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .enumerate()
            .map(|(index, destination)| {
                super::decode_fragment_destination(
                    destination,
                    params_path.clone().field("destinations").index(index),
                )
            })
            .collect::<Result<Vec<_>, _>>()?,
        sender_id: params.sender_id,
        typed_result_sink,
    })
}

pub(crate) fn decode_scan_contracts_and_assignments(
    nodes: &[plan_nodes::TPlanNode],
    raw_ranges: &BTreeMap<i32, Vec<internal_service::TScanRangeParams>>,
    facts: &StarRocksDecodeFacts,
    path: FieldPath,
) -> Result<
    (
        BTreeMap<FragmentNodeId, ScanSourceContract>,
        ScanAssignments,
    ),
    StarRocksFragmentDecodeError,
> {
    let mut kinds = BTreeMap::new();
    let mut known_node_ids = BTreeMap::new();
    let mut schema_requirements = BTreeMap::new();
    for (index, node) in nodes.iter().enumerate() {
        known_node_ids.insert(node.node_id, index);
        let kind = match node.node_type {
            plan_nodes::TPlanNodeType::FILE_SCAN_NODE => Some(ScanAssignmentKind::BrokerFile),
            plan_nodes::TPlanNodeType::HDFS_SCAN_NODE => Some(ScanAssignmentKind::File),
            plan_nodes::TPlanNodeType::LAKE_SCAN_NODE => Some(ScanAssignmentKind::StarRocksTablet),
            plan_nodes::TPlanNodeType::SCHEMA_SCAN_NODE => {
                super::node::supported_schema_scan_requires_ranges(node).map(|required| {
                    schema_requirements.insert(FragmentNodeId::new(node.node_id), required);
                    ScanAssignmentKind::SchemaSelection
                })
            }
            _ => None,
        };
        if let Some(kind) = kind {
            let id = FragmentNodeId::new(node.node_id);
            if kinds.insert(id, kind).is_some() {
                return Err(StarRocksFragmentDecodeError::invalid_value(
                    FieldPath::root("exec_plan_fragment")
                        .field("fragment")
                        .field("plan")
                        .field("nodes")
                        .index(index)
                        .field("node_id"),
                    format!("duplicate scan node id {}", node.node_id),
                ));
            }
        }
    }
    for node_id in raw_ranges.keys() {
        if !known_node_ids.contains_key(node_id) {
            return Err(StarRocksFragmentDecodeError::invalid_value(
                path.clone().map_key(node_id.to_string()),
                format!("scan ranges assigned to unknown scan node {node_id}"),
            ));
        }
    }
    let contracts = kinds
        .iter()
        .map(|(id, kind)| (*id, ScanSourceContract::new(*kind)))
        .collect::<BTreeMap<_, _>>();
    let mut assignments = BTreeMap::new();
    for (id, kind) in kinds {
        let ranges = raw_ranges.get(&id.get()).map(Vec::as_slice).unwrap_or(&[]);
        if kind == ScanAssignmentKind::SchemaSelection {
            if schema_requirements.get(&id).copied().unwrap_or(false)
                && !raw_ranges.contains_key(&id.get())
            {
                return Err(StarRocksFragmentDecodeError::missing(
                    path.clone().map_key(id.get().to_string()),
                    "schema scan requires a per-node selection assignment",
                ));
            }
            if ranges.iter().any(|range| range.has_more.unwrap_or(false)) {
                return Err(StarRocksFragmentDecodeError::unsupported(
                    path.clone().map_key(id.get().to_string()),
                    "incremental schema-scan selections are not supported",
                ));
            }
            let selected =
                ranges.is_empty() || ranges.iter().any(|range| !range.empty.unwrap_or(false));
            assignments.insert(
                id,
                (kind, vec![ScanRangeParams::schema_selection(selected)]),
            );
            continue;
        }
        let mut decoded = Vec::new();
        for (index, params) in ranges.iter().enumerate() {
            if params.empty.unwrap_or(false) {
                continue;
            }
            decoded.extend(decode_scan_range_params(
                kind,
                params,
                facts,
                path.clone().map_key(id.get().to_string()).index(index),
            )?);
        }
        assignments.insert(id, (kind, decoded));
    }
    let assignments =
        ScanAssignments::try_new(assignments).map_err(StarRocksFragmentDecodeError::Binding)?;
    Ok((contracts, assignments))
}

fn decode_scan_range_params(
    kind: ScanAssignmentKind,
    params: &internal_service::TScanRangeParams,
    facts: &StarRocksDecodeFacts,
    path: FieldPath,
) -> Result<Vec<ScanRangeParams>, StarRocksFragmentDecodeError> {
    let decoded = match kind {
        ScanAssignmentKind::BrokerFile => {
            let broker = params
                .scan_range
                .broker_scan_range
                .as_ref()
                .ok_or_else(|| {
                    StarRocksFragmentDecodeError::missing(
                        path.clone().field("scan_range").field("broker_scan_range"),
                        "FILE_SCAN_NODE assignment requires broker_scan_range",
                    )
                })?;
            broker
                .ranges
                .iter()
                .enumerate()
                .map(|(range_index, range)| {
                    let range_path = path
                        .clone()
                        .field("scan_range")
                        .field("broker_scan_range")
                        .field("ranges")
                        .index(range_index);
                    let path_value = if range.file_type == types::TFileType::FILE_LOCAL {
                        range.path.clone()
                    } else if range.file_type == types::TFileType::FILE_STREAM {
                        let load_id = range.load_id.as_ref().ok_or_else(|| {
                            StarRocksFragmentDecodeError::missing(
                                range_path.clone().field("load_id"),
                                "FILE_STREAM range requires load_id",
                            )
                        })?;
                        facts
                            .stream_load_path(UniqueId {
                                hi: load_id.hi,
                                lo: load_id.lo,
                            })
                            .ok_or_else(|| {
                                StarRocksFragmentDecodeError::missing(
                                    range_path.clone().field("load_id"),
                                    "FILE_STREAM load_id has no immutable path fact",
                                )
                            })?
                            .to_string()
                    } else {
                        return Err(StarRocksFragmentDecodeError::unsupported(
                            range_path.clone().field("file_type"),
                            format!("unsupported broker file type {:?}", range.file_type),
                        ));
                    };
                    let format =
                        if range.format_type == plan_nodes::TFileFormatType::FORMAT_CSV_PLAIN {
                            BrokerFileFormat::Csv
                        } else if range.format_type == plan_nodes::TFileFormatType::FORMAT_JSON {
                            BrokerFileFormat::Json
                        } else {
                            return Err(StarRocksFragmentDecodeError::unsupported(
                                range_path.clone().field("format_type"),
                                format!("unsupported broker file format {:?}", range.format_type),
                            ));
                        };
                    let mut decoded = ScanRangeParams::broker_file(BrokerFileScanRange {
                        path: path_value,
                        file_size: range.file_size.unwrap_or_default(),
                        offset: range.start_offset,
                        length: range.size,
                        format,
                        strip_outer_array: range.strip_outer_array.unwrap_or(false),
                        jsonpaths: range.jsonpaths.clone(),
                    });
                    decoded.volume_id = params.volume_id;
                    decoded.empty = params.empty;
                    decoded.has_more = params.has_more;
                    Ok(decoded)
                })
                .collect::<Result<Vec<_>, _>>()?
        }
        ScanAssignmentKind::File => {
            let hdfs = params.scan_range.hdfs_scan_range.as_ref().ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    path.clone().field("scan_range").field("hdfs_scan_range"),
                    "HDFS_SCAN_NODE assignment requires hdfs_scan_range",
                )
            })?;
            vec![ScanRangeParams::file(decode_hdfs_scan_range(
                hdfs,
                facts,
                path.clone(),
            )?)]
        }
        ScanAssignmentKind::StarRocksTablet => {
            let internal = params
                .scan_range
                .internal_scan_range
                .as_ref()
                .ok_or_else(|| {
                    StarRocksFragmentDecodeError::missing(
                        path.clone()
                            .field("scan_range")
                            .field("internal_scan_range"),
                        "LAKE_SCAN_NODE assignment requires internal_scan_range",
                    )
                })?;
            let partition_id = internal.partition_id.ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    path.clone()
                        .field("scan_range")
                        .field("internal_scan_range")
                        .field("partition_id"),
                    "LAKE_SCAN_NODE assignment requires partition_id",
                )
            })?;
            let version = internal.version.parse::<i64>().map_err(|error| {
                StarRocksFragmentDecodeError::invalid_value(
                    path.clone()
                        .field("scan_range")
                        .field("internal_scan_range")
                        .field("version"),
                    format!("invalid tablet version {:?}: {error}", internal.version),
                )
            })?;
            vec![
                ScanRangeParams::starrocks_tablet(internal.tablet_id, partition_id, version)
                    .map_err(|detail| {
                        StarRocksFragmentDecodeError::invalid_value(path.clone(), detail)
                    })?,
            ]
        }
        ScanAssignmentKind::SchemaSelection => unreachable!("schema selection is decoded per node"),
    };
    Ok(decoded
        .into_iter()
        .map(|mut range| {
            range.volume_id = params.volume_id;
            range.empty = params.empty;
            range.has_more = params.has_more;
            range
        })
        .collect())
}

fn decode_hdfs_scan_range(
    src: &plan_nodes::THdfsScanRange,
    facts: &StarRocksDecodeFacts,
    path: FieldPath,
) -> Result<FileScanRange, StarRocksFragmentDecodeError> {
    let file_format = match src.file_format.as_ref() {
        Some(value) if *value == descriptors::THdfsFileFormat::PARQUET => FileFormat::Parquet,
        Some(value) if *value == descriptors::THdfsFileFormat::ORC => FileFormat::Orc,
        Some(value) => {
            return Err(StarRocksFragmentDecodeError::unsupported(
                path.clone()
                    .field("scan_range")
                    .field("hdfs_scan_range")
                    .field("file_format"),
                format!("unsupported HDFS file format {value:?}"),
            ));
        }
        None => {
            return Err(StarRocksFragmentDecodeError::missing(
                path.clone()
                    .field("scan_range")
                    .field("hdfs_scan_range")
                    .field("file_format"),
                "HDFS scan range requires file_format",
            ));
        }
    };
    let mut full_path = src.full_path.clone();
    if let (Some(value), Some(rewrite)) = (full_path.as_mut(), facts.path_rewrite()) {
        let from = rewrite.from_prefix().trim();
        let to = rewrite.to_prefix().trim();
        if from.is_empty() || to.is_empty() {
            return Err(StarRocksFragmentDecodeError::invalid_value(
                path.clone()
                    .field("scan_range")
                    .field("hdfs_scan_range")
                    .field("full_path"),
                "path rewrite facts require non-empty prefixes",
            ));
        }
        if value.starts_with(from) {
            *value = format!("{to}{}", &value[from.len()..]);
        }
    }
    let delete_files = src
        .delete_files
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .enumerate()
        .map(|(index, file)| {
            let file_content = match file.file_content {
                Some(types::TIcebergFileContent::POSITION_DELETES) => {
                    IcebergFileContent::PositionDeletes
                }
                Some(types::TIcebergFileContent::EQUALITY_DELETES) => {
                    IcebergFileContent::EqualityDeletes
                }
                value => {
                    return Err(StarRocksFragmentDecodeError::unsupported(
                        path.clone()
                            .field("scan_range")
                            .field("hdfs_scan_range")
                            .field("delete_files")
                            .index(index)
                            .field("file_content"),
                        format!("unsupported Iceberg delete-file content {value:?}"),
                    ));
                }
            };
            Ok(IcebergDeleteFile {
                full_path: file.full_path.clone(),
                file_format: IcebergFileFormat::Parquet,
                file_content,
                length: file.length,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let deletion_vector_descriptor =
        src.deletion_vector_descriptor
            .as_ref()
            .map(|value| DeletionVectorDescriptor {
                storage_type: value.storage_type.clone(),
                path_or_inline_dv: value.path_or_inline_dv.clone(),
                offset: value.offset,
                size_in_bytes: value.size_in_bytes,
                cardinality: value.cardinality,
            });
    Ok(FileScanRange {
        file_format,
        full_path,
        relative_path: src.relative_path.clone(),
        table_id: src.table_id,
        offset: src.offset.unwrap_or_default(),
        length: src.length.unwrap_or_default(),
        file_length: src.file_length.unwrap_or_default(),
        delete_files,
        deletion_vector_descriptor,
        first_row_id: src.first_row_id,
        data_sequence_number: src.data_sequence_number,
        modification_time: src.modification_time,
        datacache_options: src
            .datacache_options
            .as_ref()
            .map(|value| DatacacheOptions {
                enable_populate_datacache: value.enable_populate_datacache,
                priority: value.priority,
            }),
        candidate_node: src.candidate_node.clone(),
        included_positions: src.included_positions.clone().unwrap_or_default(),
        serialized_split: src.serialized_split.clone(),
        use_iceberg_jni_metadata_reader: src.use_iceberg_jni_metadata_reader.unwrap_or(false),
        ivm_change_op: None,
        file_pruning_min_max_values: None,
    })
}
