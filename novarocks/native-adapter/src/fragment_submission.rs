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

//! Native fragment envelope and static execution-contract projections.
//!
//! This module validates only protobuf shape and projects immutable wire
//! values into Execution contracts.  It has no Backend context lookup, task
//! state, Connector I/O, or fragment lifecycle authority.

use std::collections::BTreeMap;

use novarocks_execution::exec::fragment::program::{
    FragmentNodeId, ScanAssignmentKind, ScanSourceContract,
};
use novarocks_execution::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
use novarocks_execution::runtime::fragment::FragmentSinkAssignment;
use novarocks_proto_codec::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::{novarocks as proto, plan};
use novarocks_types::UniqueId;

pub fn require_root(
    fragment: &plan::PlanFragment,
) -> Result<&plan::DistributedNode, ProtocolError> {
    fragment.root.as_ref().ok_or_else(|| {
        error(
            FieldPath::root("plan_fragment").field("root"),
            ProtocolErrorKind::MissingField,
            "native PlanFragment requires root",
        )
    })
}

pub fn require_sink(fragment: &plan::PlanFragment) -> Result<&plan::DataSink, ProtocolError> {
    fragment.sink.as_ref().ok_or_else(|| {
        error(
            FieldPath::root("plan_fragment").field("sink"),
            ProtocolErrorKind::MissingField,
            "native PlanFragment requires sink",
        )
    })
}

pub fn decode_scan_source_contracts(
    root: &plan::DistributedNode,
    path: FieldPath,
) -> Result<BTreeMap<FragmentNodeId, ScanSourceContract>, ProtocolError> {
    let mut assignments = BTreeMap::new();
    visit_scan_contracts(root, path, &mut assignments)?;
    Ok(assignments)
}

fn visit_scan_contracts(
    node: &plan::DistributedNode,
    path: FieldPath,
    assignments: &mut BTreeMap<FragmentNodeId, ScanSourceContract>,
) -> Result<(), ProtocolError> {
    if let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_ref()
        && let Some(plan::plan_node::Kind::Scan(scan)) = physical.kind.as_ref()
    {
        let scan_path = path
            .clone()
            .field("payload")
            .field("physical")
            .field("scan");
        let table = scan.table.as_ref().ok_or_else(|| {
            error(
                scan_path.clone().field("table"),
                ProtocolErrorKind::MissingField,
                format!("native ScanNode node_id={} requires table", node.node_id),
            )
        })?;
        let source = table.source.as_ref().ok_or_else(|| {
            error(
                scan_path.clone().field("table").field("source"),
                ProtocolErrorKind::MissingField,
                format!("native ScanNode node_id={} requires source", node.node_id),
            )
        })?;
        let source = source.kind.as_ref().ok_or_else(|| {
            error(
                scan_path
                    .clone()
                    .field("table")
                    .field("source")
                    .field("kind"),
                ProtocolErrorKind::MissingField,
                format!(
                    "native ScanNode node_id={} requires source kind",
                    node.node_id
                ),
            )
        })?;
        let _ = source;
        if assignments
            .insert(
                FragmentNodeId::new(node.node_id),
                ScanSourceContract::new(ScanAssignmentKind::File),
            )
            .is_some()
        {
            return Err(error(
                path.clone().field("node_id"),
                ProtocolErrorKind::InconsistentFields,
                format!("native plan has duplicate scan node_id={}", node.node_id),
            ));
        }
    }
    for (index, child) in node.children.iter().enumerate() {
        visit_scan_contracts(
            child,
            path.clone().field("children").index(index),
            assignments,
        )?;
    }
    Ok(())
}

pub fn decode_fragment_sink_assignment(
    sink: &plan::DataSink,
    instance: &proto::InstanceParams,
) -> Result<FragmentSinkAssignment, ProtocolError> {
    let path = FieldPath::root("plan_fragment").field("sink");
    let kind = sink.kind.as_ref().ok_or_else(|| {
        error(
            path.clone().field("kind"),
            ProtocolErrorKind::MissingField,
            "native PlanFragment sink requires kind",
        )
    })?;
    match kind {
        plan::data_sink::Kind::DataStream(_) => Ok(FragmentSinkAssignment::StreamDestinations {
            destinations: decode_instance_destinations(&instance.destinations)?,
            sender_id: None,
        }),
        plan::data_sink::Kind::MultiCastDataStream(grouped) => {
            let groups = grouped
                .destinations
                .iter()
                .enumerate()
                .map(|(index, group)| {
                    decode_stream_destination_list(
                        group,
                        path.clone()
                            .field("multi_cast_data_stream")
                            .field("destinations")
                            .index(index),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(FragmentSinkAssignment::DestinationGroups {
                groups,
                sender_id: None,
            })
        }
        plan::data_sink::Kind::ChangeStreamRouter(router) => {
            let groups = router
                .routes
                .iter()
                .enumerate()
                .map(|(index, branch)| {
                    let group_path = path
                        .clone()
                        .field("change_stream_router")
                        .field("routes")
                        .index(index)
                        .field("destinations");
                    let group = branch.destinations.as_ref().ok_or_else(|| {
                        error(
                            group_path.clone(),
                            ProtocolErrorKind::MissingField,
                            "native change-stream branch requires destinations",
                        )
                    })?;
                    decode_stream_destination_list(group, group_path)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(FragmentSinkAssignment::DestinationGroups {
                groups,
                sender_id: None,
            })
        }
        plan::data_sink::Kind::Result(_) | plan::data_sink::Kind::Noop(_) => {
            if instance.destinations.is_empty() {
                Ok(FragmentSinkAssignment::None)
            } else {
                Ok(FragmentSinkAssignment::StreamDestinations {
                    destinations: decode_instance_destinations(&instance.destinations)?,
                    sender_id: None,
                })
            }
        }
    }
}

fn decode_instance_destinations(
    src: &[proto::Destination],
) -> Result<Vec<FragmentDestination>, ProtocolError> {
    src.iter()
        .enumerate()
        .map(|(index, destination)| {
            let path = FieldPath::root("instance_params")
                .field("destinations")
                .index(index);
            let finst_id = destination.finst_id.as_ref().ok_or_else(|| {
                error(
                    path.clone().field("finst_id"),
                    ProtocolErrorKind::MissingField,
                    "native Destination requires finst_id",
                )
            })?;
            let source_finst_id = destination.source_finst_id.as_ref().ok_or_else(|| {
                error(
                    path.clone().field("source_finst_id"),
                    ProtocolErrorKind::MissingField,
                    "native Destination requires source_finst_id",
                )
            })?;
            FragmentDestination::new(
                UniqueId::new(finst_id.hi, finst_id.lo),
                RuntimeEndpoint::parse(&destination.endpoint).map_err(|detail| {
                    error(
                        path.clone().field("endpoint"),
                        ProtocolErrorKind::InvalidValue,
                        detail,
                    )
                })?,
                UniqueId::new(source_finst_id.hi, source_finst_id.lo),
                destination.sender_ordinal,
                destination.sender_count,
            )
            .map_err(|detail| error(path, ProtocolErrorKind::InvalidValue, detail))
        })
        .collect()
}

fn decode_stream_destination_list(
    group: &plan::StreamDestinationList,
    path: FieldPath,
) -> Result<Vec<FragmentDestination>, ProtocolError> {
    group
        .destinations
        .iter()
        .enumerate()
        .map(|(index, destination)| {
            let destination_path = path.clone().field("destinations").index(index);
            let finst_id = destination.finst_id.as_ref().ok_or_else(|| {
                error(
                    destination_path.clone().field("finst_id"),
                    ProtocolErrorKind::MissingField,
                    "native stream destination requires finst_id",
                )
            })?;
            let source_finst_id = destination.source_finst_id.as_ref().ok_or_else(|| {
                error(
                    destination_path.clone().field("source_finst_id"),
                    ProtocolErrorKind::MissingField,
                    "native stream destination requires source_finst_id",
                )
            })?;
            FragmentDestination::new(
                UniqueId::new(finst_id.hi, finst_id.lo),
                RuntimeEndpoint::parse(&destination.endpoint).map_err(|detail| {
                    error(
                        destination_path.clone().field("endpoint"),
                        ProtocolErrorKind::InvalidValue,
                        detail,
                    )
                })?,
                UniqueId::new(source_finst_id.hi, source_finst_id.lo),
                destination.sender_ordinal,
                destination.sender_count,
            )
            .map_err(|detail| error(destination_path, ProtocolErrorKind::InvalidValue, detail))
        })
        .collect()
}

fn error(path: FieldPath, kind: ProtocolErrorKind, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, kind, detail)
}

#[cfg(test)]
mod tests {
    use super::{
        decode_fragment_sink_assignment, decode_scan_source_contracts, require_root, require_sink,
    };
    use novarocks_execution::exec::fragment::program::{FragmentNodeId, ScanAssignmentKind};
    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_models::{novarocks as proto, plan};

    #[test]
    fn preserves_missing_fragment_envelope_errors() {
        let root = require_root(&plan::PlanFragment::default()).expect_err("root is required");
        assert_eq!(
            root.to_string(),
            "native protocol error at plan_fragment.root (missing field): native PlanFragment requires root"
        );
        let sink = require_sink(&plan::PlanFragment::default()).expect_err("sink is required");
        assert_eq!(
            sink.to_string(),
            "native protocol error at plan_fragment.sink (missing field): native PlanFragment requires sink"
        );
    }

    #[expect(
        clippy::needless_update,
        reason = "The fixture keeps explicit defaulted proto fields for wire-contract readability."
    )]
    #[test]
    fn classifies_typed_connector_read_as_file_assignment() {
        let root = plan::DistributedNode {
            node_id: 17,
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                kind: Some(plan::plan_node::Kind::Scan(plan::ScanNode {
                    table: Some(plan::TableDef {
                        source: Some(plan::ScanSource {
                            kind: Some(plan::scan_source::Kind::TypedConnectorRead(
                                Default::default(),
                            )),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
                ..Default::default()
            })),
            ..Default::default()
        };
        let contracts =
            decode_scan_source_contracts(&root, FieldPath::root("plan_fragment").field("root"))
                .expect("decode scan contract");
        assert_eq!(
            contracts
                .get(&FragmentNodeId::new(17))
                .map(|contract| contract.assignment_kind()),
            Some(ScanAssignmentKind::File)
        );
    }

    #[test]
    fn preserves_missing_scan_source_path() {
        let root = plan::DistributedNode {
            node_id: 17,
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                kind: Some(plan::plan_node::Kind::Scan(plan::ScanNode {
                    table: Some(plan::TableDef::default()),
                    ..Default::default()
                })),
                ..Default::default()
            })),
            ..Default::default()
        };
        let error =
            decode_scan_source_contracts(&root, FieldPath::root("plan_fragment").field("root"))
                .expect_err("missing source must fail");
        assert_eq!(
            error.to_string(),
            "native protocol error at plan_fragment.root.payload.physical.scan.table.source (missing field): native ScanNode node_id=17 requires source"
        );
    }

    #[test]
    fn stream_destination_missing_id_preserves_error_text() {
        let error = decode_fragment_sink_assignment(
            &plan::DataSink {
                kind: Some(plan::data_sink::Kind::DataStream(
                    plan::DataStreamSink::default(),
                )),
            },
            &proto::InstanceParams {
                destinations: vec![proto::Destination::default()],
                ..Default::default()
            },
        )
        .expect_err("destination id is required");
        assert_eq!(
            error.to_string(),
            "native protocol error at instance_params.destinations[0].finst_id (missing field): native Destination requires finst_id"
        );
    }

    #[test]
    fn multicast_destination_missing_id_preserves_error_text() {
        let error = decode_fragment_sink_assignment(
            &plan::DataSink {
                kind: Some(plan::data_sink::Kind::MultiCastDataStream(
                    plan::MultiCastDataStreamSink {
                        destinations: vec![plan::StreamDestinationList {
                            destinations: vec![plan::StreamDestination::default()],
                        }],
                        ..Default::default()
                    },
                )),
            },
            &proto::InstanceParams::default(),
        )
        .expect_err("stream destination id is required");
        assert_eq!(
            error.to_string(),
            "native protocol error at plan_fragment.sink.multi_cast_data_stream.destinations[0].destinations[0].finst_id (missing field): native stream destination requires finst_id"
        );
    }
}
