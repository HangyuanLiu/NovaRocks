use std::sync::Arc;

use novarocks_types::PrimitiveType;

use crate::common::ids::SlotId;
use crate::common::types::UniqueId;
use crate::common::util::FieldRenderSchema;
use crate::exec::chunk::Chunk;

use super::FragmentIoError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultPresentation {
    MysqlText,
    HttpJson,
    Statistic,
}

#[derive(Clone, Debug)]
pub struct ResultProjection {
    slot_id: SlotId,
    primitive: PrimitiveType,
    field_schema: FieldRenderSchema,
}

impl ResultProjection {
    pub(crate) fn new(
        slot_id: SlotId,
        primitive: PrimitiveType,
        field_schema: FieldRenderSchema,
    ) -> Self {
        Self {
            slot_id,
            primitive,
            field_schema,
        }
    }

    pub const fn slot_id(&self) -> SlotId {
        self.slot_id
    }

    pub const fn primitive(&self) -> PrimitiveType {
        self.primitive
    }

    pub(crate) fn field_schema(&self) -> &FieldRenderSchema {
        &self.field_schema
    }
}

#[derive(Clone, Debug)]
pub struct ResultWriteSpec {
    fragment_instance_id: UniqueId,
    presentation: ResultPresentation,
    projections: Option<Vec<ResultProjection>>,
    typed: bool,
}

impl ResultWriteSpec {
    pub(crate) fn new(
        fragment_instance_id: UniqueId,
        presentation: ResultPresentation,
        projections: Option<Vec<ResultProjection>>,
        typed: bool,
    ) -> Self {
        Self {
            fragment_instance_id,
            presentation,
            projections,
            typed,
        }
    }

    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    pub const fn presentation(&self) -> ResultPresentation {
        self.presentation
    }

    pub fn projections(&self) -> Option<&[ResultProjection]> {
        self.projections.as_deref()
    }

    pub const fn is_typed(&self) -> bool {
        self.typed
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultAbort {
    PrepareRollback,
    NeverStarted,
    Failed,
    Cancelled,
}

pub trait FragmentResultWriter: Send + Sync + 'static {
    fn open(
        &self,
        spec: ResultWriteSpec,
    ) -> Result<Arc<dyn FragmentResultSession>, FragmentIoError>;
}

pub trait FragmentResultSession: Send + Sync + 'static {
    fn write(&self, chunk: Chunk) -> Result<(), FragmentIoError>;
    fn finish(&self) -> Result<(), FragmentIoError>;
    fn abort(&self, reason: ResultAbort);
}
