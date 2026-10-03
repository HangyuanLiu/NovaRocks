//! Opt-in concrete-future dispatch without narrowing the ordinary enum executor.
use super::{ClientRequestAdmission, ClientRequestTaskKind, Executor, Read, Write};
use crate::body::Body;
use crate::client::dispatch::SendWhen;
use crate::proto::h2::client::{ConnTask, H2ClientFuture, PipeMap};
use crate::proto::h2::upgrade::UpgradedSendStreamTask;
use std::{error::Error, io};

/// Allocation facts for the three distinct actual client task futures.
/// These facts alone neither reserve capacity nor fund any dispatch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SplitClientTaskAllocationBounds {
    /// Internal connection task.
    pub connection: usize,
    /// Request body pipe task.
    pub pipe: usize,
    /// Response/callback task, including this adapter's actual executor type.
    pub send: usize,
}

/// An explicit client executor that dispatches each enum variant's real future
/// through `Typed`. Ordinary builders retain their original executor and bounds.
/// The caller must install independently prepared original positions in Typed
/// before claiming that any dispatch is originally funded. Upgrade dispatch
/// retains Legacy; original modes covering only these three futures must refuse
/// CONNECT before allocating upgrade state.
#[derive(Clone, Debug)]
pub struct SplitClientExecutor<Legacy, Typed> {
    legacy: Legacy,
    typed: Typed,
}

impl<L, P> SplitClientExecutor<L, P> {
    /// Construct an opt-in adapter without allocating or preparing a task.
    pub fn new(legacy: L, typed: P) -> Self {
        Self { legacy, typed }
    }

    /// Query the same concrete futures dispatched below. No IO, future, task,
    /// channel, or temporary allocation model is constructed.
    pub fn allocation_capacity_bounds<B, T>() -> io::Result<SplitClientTaskAllocationBounds>
    where
        B: Body + 'static,
        B::Error: Into<Box<dyn Error + Send + Sync>>,
        T: Read + Write + Unpin,
        L: Executor<UpgradedSendStreamTask<B::Data>>,
        P: Executor<ConnTask<T, B>> + Executor<PipeMap<B>> + Executor<SendWhen<B, Self>>,
    {
        Ok(SplitClientTaskAllocationBounds {
            connection: <P as Executor<ConnTask<T, B>>>::task_allocation_capacity_bound()?,
            pipe: <P as Executor<PipeMap<B>>>::task_allocation_capacity_bound()?,
            send: <P as Executor<SendWhen<B, Self>>>::task_allocation_capacity_bound()?,
        })
    }
}

impl<L, P, D> Executor<UpgradedSendStreamTask<D>> for SplitClientExecutor<L, P>
where
    L: Executor<UpgradedSendStreamTask<D>>,
{
    fn execute(&self, future: UpgradedSendStreamTask<D>) {
        self.legacy.execute(future);
    }
}

impl<L, P, B, T> Executor<H2ClientFuture<B, T, Self>> for SplitClientExecutor<L, P>
where
    L: Clone + Executor<UpgradedSendStreamTask<B::Data>>,
    P: Clone + Executor<ConnTask<T, B>> + Executor<PipeMap<B>> + Executor<SendWhen<B, Self>>,
    B: Body + 'static,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
    T: Read + Write + Unpin,
{
    fn task_allocation_capacity_bound() -> io::Result<usize> {
        <P as Executor<ConnTask<T, B>>>::task_allocation_capacity_bound()
    }

    fn try_take_prepared_task(&mut self) -> io::Result<Option<Self>> {
        let prepared = <P as Executor<ConnTask<T, B>>>::try_take_prepared_task(&mut self.typed)?;
        Ok(prepared.map(|typed| Self::new(self.legacy.clone(), typed)))
    }

    fn supports_client_request_task_lease() -> bool {
        <P as Executor<PipeMap<B>>>::supports_client_request_task_lease()
            && <P as Executor<SendWhen<B, Self>>>::supports_client_request_task_lease()
    }

    fn client_request_admission(&self) -> io::Result<Option<ClientRequestAdmission>> {
        let admission = <P as Executor<ConnTask<T, B>>>::client_request_admission(&self.typed)?;
        if admission.is_some()
            && (!<P as Executor<PipeMap<B>>>::supports_client_request_task_lease()
                || !<P as Executor<SendWhen<B, Self>>>::supports_client_request_task_lease())
        {
            return Err(io::ErrorKind::Unsupported.into());
        }
        Ok(admission)
    }

    fn try_execute(&self, future: H2ClientFuture<B, T, Self>) -> io::Result<()> {
        // Move each concrete future before pinning/allocating a TaskCell. There
        // is no enum TaskCell, extra erasure Box, or ordinary-spawn fallback here.
        match future {
            H2ClientFuture::Task { task } => self.typed.try_execute(task),
            H2ClientFuture::Pipe { pipe, lease } => match lease {
                Some(lease) => <P as Executor<PipeMap<B>>>::with_client_request_task_lease(
                    &self.typed,
                    ClientRequestTaskKind::Pipe,
                    lease,
                )?
                .try_execute(pipe),
                None => self.typed.try_execute(pipe),
            },
            H2ClientFuture::Send { send_when, lease } => match lease {
                Some(lease) => <P as Executor<SendWhen<B, Self>>>::with_client_request_task_lease(
                    &self.typed,
                    ClientRequestTaskKind::Send,
                    lease,
                )?
                .try_execute(send_when),
                None => self.typed.try_execute(send_when),
            },
        }
    }

    fn execute(&self, future: H2ClientFuture<B, T, Self>) {
        self.try_execute(future)
            .expect("split HTTP/2 task dispatch refused");
    }
}
