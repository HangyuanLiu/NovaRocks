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

//! Carrier-free root read boundary. Every decoded reply retains the complete
//! pre-admitted window; logical ACK, read seal and timeouts do not free it.

use super::QueryExecutionError;
use novarocks_execution_contract::{
    TaskIdentity,
    root_lifetime::RootReadSealed,
    root_result::{RootReadOutcome, RootResultEnd, RootResultRead, RootResultReply},
};
use novarocks_workload_control::{ResultWindowAlias, ResultWindowClass, WorkError};
use std::{future::Future, num::NonZeroU64, pin::Pin};

/// Native adapters implement the wire call, while the application retains
/// one exact read owner and serializes both fetch and optional ACK-only calls.
/// The physical guard follows the body/codec/connection until actual exit.
pub trait BoundedRootReadPort: Send + Sync + 'static {
    /// A failure carries its recovery class and topology requirement exactly
    /// as the transport classified it.
    fn read(
        &self,
        request: RootResultRead,
        physical_guard: ResultWindowAlias,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<RetainedRootReply, crate::coordination::RootResultFetchFailure>,
                > + Send
                + 'static,
        >,
    >;
    /// Stop future fetch/replay dispatch before lifecycle joins and Release.
    /// This is local read closure, not a Worker compound close operation.
    fn seal(&self, sealed: RootReadSealed) -> Result<(), QueryExecutionError>;
}

/// A bounded reply cannot expose an independently clonable Bytes backing.
/// Native decoding transfers the response and its actual physical guard here;
/// protocol adapters receive only borrowed payload slices with exact sequence.
pub struct RetainedRootReply {
    reply: RootResultReply,
    physical_guard: ResultWindowAlias,
}
impl RetainedRootReply {
    pub fn try_new(
        reply: RootResultReply,
        physical_guard: ResultWindowAlias,
        simultaneously_live_backing_bytes: u64,
    ) -> Result<Self, WorkError> {
        let valid_class = match reply.kind {
            novarocks_result_contract::RootOutputKind::ClientRows => matches!(
                physical_guard.class(),
                ResultWindowClass::Client | ResultWindowClass::Local
            ),
            novarocks_result_contract::RootOutputKind::InternalFacts(_) => {
                physical_guard.class() == ResultWindowClass::Internal
            }
            novarocks_result_contract::RootOutputKind::CountOnly => {
                physical_guard.class() != ResultWindowClass::Closing
            }
        };
        let covers_visible_body = match &reply.outcome {
            RootReadOutcome::Data(data) => {
                simultaneously_live_backing_bytes >= data.body().len() as u64
            }
            _ => true,
        };
        let checked = if !valid_class || reply.validate().is_err() || !covers_visible_body {
            Err(WorkError::Conflict)
        } else {
            physical_guard.check_backing_total(simultaneously_live_backing_bytes)
        };
        if let Err(error) = checked {
            drop(reply);
            drop(physical_guard);
            return Err(error);
        }
        Ok(Self {
            reply,
            physical_guard,
        })
    }
    pub fn root_task(&self) -> TaskIdentity {
        self.reply.root_task
    }
    /// The decoded reply, for frontier validation.
    pub fn reply(&self) -> &RootResultReply {
        &self.reply
    }
    pub fn accepted_consumed(&self) -> u64 {
        self.reply.accepted_consumed
    }
    pub fn profile(&self) -> novarocks_result_contract::RootProfileId {
        self.reply.profile
    }
    pub fn kind(&self) -> novarocks_result_contract::RootOutputKind {
        self.reply.kind
    }
    pub fn outcome(&self) -> RootReplyView<'_> {
        match &self.reply.outcome {
            RootReadOutcome::AckOnly => RootReplyView::AckOnly,
            RootReadOutcome::NotReady => RootReplyView::NotReady,
            RootReadOutcome::Retired => RootReplyView::Retired,
            RootReadOutcome::AwaitTerminalControl => RootReplyView::AwaitTerminalControl,
            RootReadOutcome::End(end) => RootReplyView::End(*end),
            RootReadOutcome::Data(data) => RootReplyView::Data {
                sequence: data.sequence(),
                body: data.body().as_ref(),
                end_after_data: data.end_after_data(),
            },
        }
    }
    pub fn retain_physical_guard(&self) -> ResultWindowAlias {
        self.physical_guard.clone()
    }
}
#[derive(Clone, Copy, Debug)]
pub enum RootReplyView<'a> {
    AckOnly,
    NotReady,
    Retired,
    AwaitTerminalControl,
    Data {
        sequence: NonZeroU64,
        body: &'a [u8],
        end_after_data: Option<RootResultEnd>,
    },
    End(RootResultEnd),
}
