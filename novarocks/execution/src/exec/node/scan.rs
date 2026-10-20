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
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use futures::future::BoxFuture;

use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::ExprId;
use crate::runtime::profile::RuntimeProfile;
use novarocks_spi::connector::read_stack::ConnectorPollBudget;

/// A scan's single output stream, owned by the one driver that polls it.
///
/// `Poll::Pending` registers the context's waker and is never end of
/// stream; neither is an empty chunk. `Ready(None)` means no more chunks.
/// After an error the driver polls the stream no more.
pub trait ScanChunkStream:
    Stream<Item = crate::runtime::fragment::ExecutionResult<Chunk>> + Send
{
    /// Ends delivery and asks every operation of the scan to stop, before
    /// returning. The future resolves once they all exited, with the first
    /// real error; it only observes, so dropping it changes nothing.
    fn close(self: Pin<Box<Self>>) -> BoxFuture<'static, Result<(), String>>;
}

/// A scan output stream owned by its driver.
pub type ScanOutputStream = Pin<Box<dyn ScanChunkStream>>;

/// Hands a scan's single output stream to the driver that runs it.
pub trait ScanStreamSource: Send + Sync {
    /// Hands the stream over, polled with `budget` in every driver turn and
    /// reporting into the scan operator's own `profile`. A scan has one
    /// stream: a second claim is refused, never a second reader.
    fn claim(
        &self,
        budget: ConnectorPollBudget,
        profile: Option<RuntimeProfile>,
        construction: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) -> crate::runtime::fragment::ExecutionResult<ScanOutputStream>;
}

/// One bound scan: the stream its driver polls, and the hooks through which
/// the pipeline tells the scan about its consumer.
pub trait ScanOp: Send + Sync {
    /// The scan's one output stream. The pipeline runs a single driver that
    /// polls it and hands its chunks to the target degree of parallelism.
    fn stream_source(&self) -> Arc<dyn ScanStreamSource>;

    /// Starts terminal cleanup of the scan's reads without waiting for them:
    /// the scan's stream observes their exit when it is closed.
    fn terminate(&self) -> Result<(), String> {
        Ok(())
    }

    /// Reports whether the scan's consumer is full and holds back more reader
    /// work. The callback must not wait: output can stay queued while the
    /// downstream pipeline is blocked.
    fn on_output_backpressure(&self, _paused: bool) {}

    /// Reports one nonempty chunk actually taken by the scan's consumer.
    /// Producing a chunk is not progress at this boundary.
    fn on_nonempty_chunk_consumed(&self) {}

    fn profile_name(&self) -> Option<String> {
        None
    }
}

/// Instance-decoded, proto-free connector ranges handed to [`ScanSource::bind`].
///
/// The wire -> connector-range conversion lives in the decoders; this enum is
/// that conversion's already-enriched output. Keeping it proto-free lets the
/// wire-free connector layer materialize a per-instance [`ScanOp`] from static
/// config plus these ranges.
///
/// Every variant is consumed at execution time: the decoders route the
/// enriched ranges into the instance's `ScanAssignment`, and
/// `materialize_scan_bindings` replays them through `ScanSource::bind` to
/// produce the per-instance `ScanOp`.
#[derive(Clone, Debug)]
pub enum BoundScanRanges {
    /// No frozen range: a typed scan's work arrives as splits at runtime,
    /// and a system relation is read without any.
    None,
    /// Schema scans carry only the per-instance assignment gate.
    SchemaSelection { should_scan: bool },
}

/// Static, proto-free description of a scan source that materializes a
/// per-instance [`ScanOp`] from [`BoundScanRanges`].
///
/// `bind` performs only "connector-ranges + static config -> op"; all wire
/// decoding happens earlier in the decoders. This keeps the connector layer
/// uniform and free of proto/thrift types. Later KRN-1 phases store an
/// Task-owned `Arc<dyn ScanSource>` values bind their exact assignment at execution time.
pub trait ScanSource: Send + Sync {
    fn bind(&self, ranges: BoundScanRanges) -> Result<Arc<dyn ScanOp>, String>;

    fn profile_name(&self) -> Option<String> {
        None
    }

    /// Rebuild this source around the fragment's runtime-filter consumer
    /// contracts.
    ///
    /// The contracts are decoded after the source is lowered, so a source that
    /// can consult a live filter learns of them here rather than at
    /// construction. Returning `None` keeps the source exactly as it was built,
    /// which is what every source that cannot consult a filter does.
    fn with_runtime_filter_contracts(
        &self,
        contracts: &[crate::exec::node::runtime_filter::RuntimeFilterConsumerBinding],
    ) -> Result<Option<Arc<dyn ScanSource>>, String> {
        let _ = contracts;
        Ok(None)
    }
}

// Compile-time object-safety assertion for `ScanSource`.
#[cfg(test)]
const _: fn(&dyn ScanSource) = |_scan_source: &dyn ScanSource| {};

/// A stream source for tests whose scan is built but never run: claiming it fails.
#[cfg(test)]
pub(crate) struct UnusedScanStream;

#[cfg(test)]
impl ScanStreamSource for UnusedScanStream {
    fn claim(
        &self,
        _budget: ConnectorPollBudget,
        _profile: Option<RuntimeProfile>,
        _construction: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) -> crate::runtime::fragment::ExecutionResult<ScanOutputStream> {
        Err("this test scan is never run".to_string().into())
    }
}
