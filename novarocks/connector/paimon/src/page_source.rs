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

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use futures::Stream;
use futures::future::BoxFuture;
use novarocks_spi::connector::read_stack::{
    BudgetConsume, ConnectorPageSource, ConnectorPageStream, ConnectorPollBudget,
    ConnectorSourceOperations, PageSourceMetrics, SourcePage,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorResourceReservation};

use crate::reader::{PaimonBatchReader, PaimonBatchStream, PaimonReadBatch};
use crate::resources::PaimonExecutionResources;

/// Page-source lifecycle for one already-merged Paimon SDK stream.
pub struct PaimonPageSource {
    reader: Option<Box<dyn PaimonBatchReader>>,
    resources: PaimonExecutionResources,
    remaining_rows: Option<u64>,
    finished: bool,
    metrics: PageSourceMetrics,
}

impl PaimonPageSource {
    pub fn new(
        reader: Box<dyn PaimonBatchReader>,
        resources: PaimonExecutionResources,
        row_limit: Option<u64>,
    ) -> Self {
        Self {
            reader: Some(reader),
            resources,
            remaining_rows: row_limit,
            finished: row_limit == Some(0),
            metrics: PageSourceMetrics::default(),
        }
    }

    fn close_once(&mut self) -> Result<(), ConnectorError> {
        self.finished = true;
        match self.reader.take() {
            Some(mut reader) => reader.close(),
            None => Ok(()),
        }
    }

    fn fail(&mut self, error: ConnectorError) -> ConnectorError {
        match self.close_once() {
            Ok(()) => error,
            Err(close_error) => error.with_cleanup_context(close_error.to_string()),
        }
    }

    fn page_from_batch(&mut self, batch: PaimonReadBatch) -> Result<SourcePage, ConnectorError> {
        let (batch, output_reservation) = batch.into_parts();
        let available = batch.num_rows();
        let rows = match self.remaining_rows {
            Some(remaining) => usize::try_from(remaining.min(available as u64)).map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "Paimon output row limit does not fit in memory",
                )
            })?,
            None => available,
        };
        if let Some(remaining) = &mut self.remaining_rows {
            *remaining -= rows as u64;
        }
        let page = page_from_rows(&self.resources, batch, output_reservation, rows)?;
        self.metrics.completed_positions =
            self.metrics.completed_positions.saturating_add(rows as u64);
        self.metrics.file.batches_delivered = self.metrics.file.batches_delivered.saturating_add(1);
        if self.remaining_rows == Some(0) {
            self.close_once()?;
        }
        Ok(page)
    }
}

/// One page of the first `rows` rows of `batch`, charged to the output
/// reservation the SDK handed over with it, or to a new one.
fn page_from_rows(
    resources: &PaimonExecutionResources,
    batch: RecordBatch,
    output_reservation: Option<ConnectorResourceReservation>,
    rows: usize,
) -> Result<SourcePage, ConnectorError> {
    let columns: Vec<ArrayRef> = if rows == batch.num_rows() {
        batch.columns().to_vec()
    } else {
        batch
            .columns()
            .iter()
            .map(|column| column.slice(0, rows))
            .collect()
    };
    let page = if columns.is_empty() {
        SourcePage::zero_channel(rows)
    } else {
        let retained_bytes = columns
            .iter()
            .map(|column| column.get_array_memory_size() as u64)
            .fold(0_u64, u64::saturating_add);
        let reservation = match output_reservation {
            Some(reservation) => reservation,
            None => resources.reserve_output(retained_bytes.max(1))?,
        };
        let output = resources.transfer_output(reservation, retained_bytes)?;
        SourcePage::try_new_accounted(rows, columns, output)?
    };
    resources.checkpoint()?;
    Ok(page)
}

impl ConnectorPageSource for PaimonPageSource {
    fn next_source_page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
        if self.finished {
            // A zero limit owns a reader but must still close it before return.
            self.close_once()?;
            return Ok(None);
        }
        if let Err(error) = self.resources.checkpoint() {
            return Err(self.fail(error));
        }
        loop {
            let started = Instant::now();
            let next = match self.reader.as_mut() {
                Some(reader) => reader.next_batch(),
                None => return Ok(None),
            };
            self.metrics.read_time_nanos = self
                .metrics
                .read_time_nanos
                .saturating_add(started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
            match next {
                Ok(Some(batch)) => {
                    self.metrics.file.rows_decoded = self
                        .metrics
                        .file
                        .rows_decoded
                        .saturating_add(batch.num_rows() as u64);
                    if let Err(error) = self.resources.checkpoint() {
                        return Err(self.fail(error));
                    }
                    if batch.num_rows() == 0 {
                        if let Err(error) = self.resources.checkpoint() {
                            return Err(self.fail(error));
                        }
                        continue;
                    }
                    return self
                        .page_from_batch(batch)
                        .map(Some)
                        .map_err(|error| self.fail(error));
                }
                Ok(None) => {
                    self.close_once()?;
                    return Ok(None);
                }
                Err(error) => return Err(self.fail(error)),
            }
        }
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn metrics(&self) -> PageSourceMetrics {
        self.metrics
    }

    fn memory_usage_bytes(&self) -> u64 {
        0
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.close_once()
    }
}

impl Drop for PaimonPageSource {
    fn drop(&mut self) {
        let _ = self.close_once();
    }
}

/// One Paimon split read as a page stream the host driver polls.
///
/// Creating it reads nothing: the first poll loads the split's exact schema
/// and opens the SDK stream, awaited through the split's own operations.
/// Each SDK batch, empty or not, spends one unit of the host's turn budget,
/// and the SDK spends more at its own cooperation points. Closing it drops
/// the SDK stream, seals the split's operations and returns a future that
/// observes their exit.
pub struct PaimonPageStream {
    state: StreamState,
    resources: PaimonExecutionResources,
    budget: ConnectorPollBudget,
    /// The unit a delivered or skipped batch owes before the next is taken.
    spending: Option<BudgetConsume>,
    /// The split's own child of the task source; `None` without a range
    /// service.
    operations: Option<ConnectorSourceOperations>,
    metrics: PageSourceMetrics,
}

enum StreamState {
    Opening(BoxFuture<'static, Result<Box<dyn PaimonBatchStream>, ConnectorError>>),
    Reading(Box<dyn PaimonBatchStream>),
    Ended,
}

impl PaimonPageStream {
    pub fn new(
        opening: BoxFuture<'static, Result<Box<dyn PaimonBatchStream>, ConnectorError>>,
        resources: PaimonExecutionResources,
        budget: &ConnectorPollBudget,
        operations: Option<ConnectorSourceOperations>,
    ) -> Self {
        Self {
            state: StreamState::Opening(opening),
            resources,
            budget: budget.clone(),
            spending: None,
            operations,
            metrics: PageSourceMetrics::default(),
        }
    }

    /// Ends the stream on `error`, dropping the SDK stream; what it still
    /// holds exits on its own and is observed by `close`.
    fn fail(&mut self, error: ConnectorError) -> Poll<Option<Result<SourcePage, ConnectorError>>> {
        let error = match std::mem::replace(&mut self.state, StreamState::Ended) {
            StreamState::Reading(mut reader) => match reader.close() {
                Ok(()) => error,
                Err(close_error) => error.with_cleanup_context(close_error.to_string()),
            },
            StreamState::Opening(_) | StreamState::Ended => error,
        };
        Poll::Ready(Some(Err(error)))
    }
}

impl Stream for PaimonPageStream {
    type Item = Result<SourcePage, ConnectorError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            let reader = match &mut this.state {
                StreamState::Opening(opening) => match opening.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(reader)) => {
                        this.state = StreamState::Reading(reader);
                        continue;
                    }
                    Poll::Ready(Err(error)) => return this.fail(error),
                },
                StreamState::Reading(reader) => reader,
                StreamState::Ended => return Poll::Ready(None),
            };
            if let Some(spending) = this.spending.as_mut() {
                if Pin::new(spending).poll(cx).is_pending() {
                    return Poll::Pending;
                }
                this.spending = None;
            }
            if let Err(error) = this.resources.checkpoint() {
                return this.fail(error);
            }
            let started = Instant::now();
            let next = reader.poll_next_batch(cx);
            this.metrics.read_time_nanos = this
                .metrics
                .read_time_nanos
                .saturating_add(started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
            match next {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(Some(batch))) => {
                    this.spending = Some(this.budget.consume(1));
                    this.metrics.file.rows_decoded = this
                        .metrics
                        .file
                        .rows_decoded
                        .saturating_add(batch.num_rows() as u64);
                    if let Err(error) = this.resources.checkpoint() {
                        return this.fail(error);
                    }
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let rows = batch.num_rows();
                    let (batch, output_reservation) = batch.into_parts();
                    return match page_from_rows(&this.resources, batch, output_reservation, rows) {
                        Ok(page) => {
                            this.metrics.completed_positions =
                                this.metrics.completed_positions.saturating_add(rows as u64);
                            this.metrics.file.batches_delivered =
                                this.metrics.file.batches_delivered.saturating_add(1);
                            Poll::Ready(Some(Ok(page)))
                        }
                        Err(error) => this.fail(error),
                    };
                }
                Poll::Ready(Ok(None)) => {
                    let closed = match std::mem::replace(&mut this.state, StreamState::Ended) {
                        StreamState::Reading(mut reader) => reader.close(),
                        StreamState::Opening(_) | StreamState::Ended => Ok(()),
                    };
                    return match closed {
                        Ok(()) => Poll::Ready(None),
                        Err(error) => Poll::Ready(Some(Err(error))),
                    };
                }
                Poll::Ready(Err(error)) => return this.fail(error),
            }
        }
    }
}

impl ConnectorPageStream for PaimonPageStream {
    fn metrics(&self) -> PageSourceMetrics {
        self.metrics
    }

    fn memory_usage_bytes(&self) -> u64 {
        // Every retained byte is charged to the admitted ledger by its owner.
        0
    }

    fn close(self: Pin<Box<Self>>) -> BoxFuture<'static, Result<(), ConnectorError>> {
        let this = *Pin::into_inner(self);
        let closed = match this.state {
            StreamState::Reading(mut reader) => reader.close(),
            // Dropping the opening stops its reads; their exit is observed
            // through the sealed operations.
            StreamState::Opening(_) | StreamState::Ended => Ok(()),
        };
        let exited = this.operations.map(|operations| {
            operations.seal();
            operations.exited()
        });
        Box::pin(async move {
            let exited = match exited {
                Some(exited) => exited.await,
                None => Ok(()),
            };
            closed?;
            exited
        })
    }
}
