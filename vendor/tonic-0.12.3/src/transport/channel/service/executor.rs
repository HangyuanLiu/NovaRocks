use crate::transport::channel::BoxFuture;
use hyper_util::rt::TokioExecutor;
use std::{future::Future, sync::Arc};

pub(crate) use hyper::rt::Executor;

#[derive(Clone)]
pub(crate) struct SharedExec {
    protocol: Option<super::connection_driver::OriginalHttp2ProtocolTask>,
    prepared_protocol: bool,
    inner: Arc<dyn Executor<BoxFuture<'static, ()>> + Send + Sync + 'static>,
}

impl SharedExec {
    pub(crate) fn new<E>(exec: E) -> Self
    where
        E: Executor<BoxFuture<'static, ()>> + Send + Sync + 'static,
    {
        Self {
            protocol: None,
            prepared_protocol: false,
            inner: Arc::new(exec),
        }
    }

    pub(crate) fn with_protocol_task(
        mut self,
        task: super::connection_driver::OriginalHttp2ProtocolTask,
    ) -> Self {
        self.protocol = Some(task);
        self.prepared_protocol = false;
        self
    }

    pub(crate) fn tokio() -> Self {
        Self::new(TokioExecutor::new())
    }
}

impl<F> Executor<F> for SharedExec
where
    F: Future<Output = ()> + Send + 'static,
{
    fn task_allocation_capacity_bound() -> std::io::Result<usize> {
        // A fact for future original dispatch, not a receipt for the legacy
        // type-erased Box path used by execute when no capability is installed.
        tokio::runtime::Handle::task_allocation_capacity_bound::<F>()
    }

    fn try_take_prepared_task(&mut self) -> std::io::Result<Option<Self>> {
        let Some(task) = self.protocol.take() else {
            return Ok(None);
        };
        task.prepare::<F>()?;
        let mut prepared = self.clone();
        prepared.protocol = Some(task);
        prepared.prepared_protocol = true;
        Ok(Some(prepared))
    }

    fn try_execute(&self, fut: F) -> std::io::Result<()> {
        if self.prepared_protocol {
            self.protocol
                .as_ref()
                .ok_or(std::io::ErrorKind::InvalidInput)?
                .spawn(fut)
        } else {
            self.inner.execute(Box::pin(fut));
            Ok(())
        }
    }

    fn execute(&self, fut: F) {
        // A prepared path cannot silently degrade to ordinary unowned spawn.
        self.try_execute(fut)
            .expect("original HTTP/2 task dispatch refused");
    }
}
