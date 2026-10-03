//! Actual original dispatch for the opt-in split client executor.
use super::{
    connection_driver::OriginalHttp2ProtocolTask, request_task_pool::OriginalHttp2RequestTaskPool,
    SharedExec,
};
use hyper::rt::{
    ClientRequestAdmission, ClientRequestTaskKind, ClientRequestTaskLease, Executor,
    SplitClientExecutor,
};
use std::{future::Future, io};

pub(super) type OriginalSplitClientExec =
    SplitClientExecutor<SharedExec, OriginalRequestTaskExecutor>;

#[derive(Clone, Debug)]
pub(super) struct OriginalRequestTaskExecutor {
    admission: ClientRequestAdmission,
    protocol: Option<OriginalHttp2ProtocolTask>,
    prepared_protocol: bool,
    lease: Option<(ClientRequestTaskKind, ClientRequestTaskLease)>,
}
impl OriginalRequestTaskExecutor {
    pub(super) fn new<I>(
        pool: OriginalHttp2RequestTaskPool,
        protocol: OriginalHttp2ProtocolTask,
    ) -> io::Result<Self>
    where
        I: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
    {
        let bounds = OriginalSplitClientExec::allocation_capacity_bounds::<
            crate::body::BoxBody,
            super::io::OwnedConnectionIo<I>,
        >()?;
        let admission = pool.admission(bounds.pipe, bounds.send)?;
        Ok(Self {
            admission,
            protocol: Some(protocol),
            prepared_protocol: false,
            lease: None,
        })
    }
}
impl<F: Future<Output = ()> + Send + 'static> Executor<F> for OriginalRequestTaskExecutor {
    fn task_allocation_capacity_bound() -> io::Result<usize> {
        tokio::runtime::Handle::task_allocation_capacity_bound::<F>()
    }
    fn client_request_admission(&self) -> io::Result<Option<ClientRequestAdmission>> {
        Ok(Some(self.admission.clone()))
    }
    fn supports_client_request_task_lease() -> bool {
        true
    }
    fn with_client_request_task_lease(
        &self,
        kind: ClientRequestTaskKind,
        lease: ClientRequestTaskLease,
    ) -> io::Result<Self> {
        if tokio::runtime::Handle::task_allocation_capacity_bound::<F>()?
            > lease.task_allocation_capacity_bound(kind)?
        {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        Ok(Self {
            admission: self.admission.clone(),
            protocol: None,
            prepared_protocol: false,
            lease: Some((kind, lease)),
        })
    }
    fn try_take_prepared_task(&mut self) -> io::Result<Option<Self>> {
        let Some(protocol) = self.protocol.take() else {
            return Ok(None);
        };
        protocol.prepare::<F>()?;
        Ok(Some(Self {
            admission: self.admission.clone(),
            protocol: Some(protocol),
            prepared_protocol: true,
            lease: None,
        }))
    }
    fn try_execute(&self, future: F) -> io::Result<()> {
        if let Some((kind, lease)) = &self.lease {
            let runtime =
                tokio::runtime::Handle::try_current().map_err(|_| io::ErrorKind::NotConnected)?;
            let actual = tokio::runtime::Handle::task_allocation_capacity_bound::<F>()?;
            // Elect before allocating the separate TaskCell owner wrapper.
            lease.elect_dispatch(*kind, actual)?;
            let owner = lease.clone().into_task_owner();
            // No handle is stored in the grant/pool, so no task backlink exists.
            drop(runtime.spawn_with_task_owner(future, owner)?);
            Ok(())
        } else if self.prepared_protocol {
            self.protocol
                .as_ref()
                .ok_or(io::ErrorKind::InvalidInput)?
                .spawn(future)
        } else {
            // A missing original request pair cannot degrade to legacy spawn.
            Err(io::ErrorKind::InvalidInput.into())
        }
    }
    fn execute(&self, future: F) {
        self.try_execute(future)
            .expect("original split HTTP/2 task dispatch refused");
    }
}
