use std::sync::Arc;

use novarocks_execution::exec::chunk::Chunk;
use novarocks_execution::runtime::fragment::io::{
    FragmentIoError, FragmentResultSession, FragmentResultWriter, ResultAbort, ResultWriteSpec,
};

#[cfg(test)]
pub(crate) fn discard_result_writer() -> Arc<dyn FragmentResultWriter> {
    Arc::new(DiscardResultWriter)
}

#[cfg(test)]
pub(crate) fn discard_result_session() -> Arc<dyn FragmentResultSession> {
    Arc::new(DiscardResultSession)
}

#[cfg(test)]
struct DiscardResultWriter;

#[cfg(test)]
impl FragmentResultWriter for DiscardResultWriter {
    fn open(
        &self,
        _spec: ResultWriteSpec,
    ) -> Result<Arc<dyn FragmentResultSession>, FragmentIoError> {
        Ok(Arc::new(DiscardResultSession))
    }
}

#[cfg(test)]
struct DiscardResultSession;

#[cfg(test)]
impl FragmentResultSession for DiscardResultSession {
    fn write(&self, _chunk: Chunk) -> Result<(), FragmentIoError> {
        Ok(())
    }

    fn finish(&self) -> Result<(), FragmentIoError> {
        Ok(())
    }

    fn abort(&self, _reason: ResultAbort) {}
}
