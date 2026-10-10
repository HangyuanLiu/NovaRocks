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

//! Cumulative allocation requests of the borrowed flat pool emitter. The
//! caller checks the locked compiler/library source contract and admission.
//! These layout requests cover coexistence conservatively; they are neither
//! allocator measurements nor an Account/grant. Payload goes directly into
//! the final stream, without a body Vec, Array::to_data, or general IPC writer.

use crate::{ipc_schema_v2::SchemaPreflight, physical_type_v2::TypeCodecError};
use arrow::{datatypes::DataType, ipc::KeyValue};
use flatbuffers::WIPOffset;
use novarocks_type_contract::CompileCheckpoints;
use std::alloc::Layout;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Requests {
    pub bytes: usize,
    pub count: usize,
}

// FlatBuffers 25.9.23 builder.rs FieldLoc has these two fields. Under the
// caller's locked Rust source contract its allocation layout is size 8,
// alignment 4. This is an allocation implementation detail, not a type grammar.
#[allow(dead_code)]
struct FieldLocation {
    offset: u32,
    slot: u16,
}

/// Layout of the sole locked FlatBuffers private FieldLoc source mirror.
pub(crate) fn field_location_layout() -> Layout {
    Layout::new::<FieldLocation>()
}

type TypeWalkerEntry<'a> = (&'a DataType, usize);
type MetadataOffset = WIPOffset<KeyValue<'static>>;

fn invalid() -> TypeCodecError {
    TypeCodecError::InvalidShape("flat writer allocation request is not representable")
}

impl Requests {
    fn request<T>(
        &mut self,
        capacity: usize,
        mut work: Option<&mut CompileCheckpoints<'_>>,
    ) -> Result<(), TypeCodecError> {
        let policy = crate::ipc_schema_v2::owner_admission::Policy(work.is_none());
        let layout = policy.numeric(Layout::array::<T>(capacity).map_err(|_| invalid()));
        if let Some(work) = work.as_deref_mut() {
            work.step()?;
        }
        let layout = layout?;
        if layout.size() == 0 {
            return Ok(());
        }
        let next = policy.numeric(
            self.bytes
                .checked_add(layout.size())
                .zip(self.count.checked_add(1))
                .ok_or_else(invalid),
        );
        if let Some(work) = work {
            work.step()?;
        }
        let (bytes, count) = next?;
        self.bytes = bytes;
        self.count = count;
        Ok(())
    }

    fn vtable_requests(
        &mut self,
        tables: usize,
        mut work: Option<&mut CompileCheckpoints<'_>>,
    ) -> Result<(), TypeCodecError> {
        if tables == 0 {
            return Ok(());
        }
        // Audited Rust 1.98.1 RawVec<u32>: the first push reserves four elements;
        // subsequent pushes double the capacity. Count every request, not
        // only the final backing, even if some vtables deduplicate.
        let mut capacity = 4usize;
        loop {
            self.request::<u32>(capacity, work.as_deref_mut())?;
            if capacity >= tables {
                break;
            }
            let next = crate::ipc_schema_v2::owner_admission::Policy(work.is_none())
                .numeric(capacity.checked_mul(2).ok_or_else(invalid));
            if let Some(work) = work.as_deref_mut() {
                work.step()?;
            }
            capacity = next?;
        }
        Ok(())
    }
}

/// First two flat `vec![(root, 1)]` type-walker requests. This is available
/// before the schema helper begins; it does not scan source metadata or infer
/// admission from a live handle. The complete model below counts four walks.
pub(super) fn prefix_request_bytes() -> usize {
    2 * Layout::new::<TypeWalkerEntry<'_>>().size()
}

pub(super) fn preflight(
    schema: &SchemaPreflight,
    batch_backing: usize,
    stream_capacity: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Requests, TypeCodecError> {
    preflight_core(schema, batch_backing, stream_capacity, Some(work))
}

pub(super) fn preflight_core(
    schema: &SchemaPreflight,
    batch_backing: usize,
    stream_capacity: usize,
    mut work: Option<&mut CompileCheckpoints<'_>>,
) -> Result<Requests, TypeCodecError> {
    let result = (|| {
        let field_location = Layout::new::<FieldLocation>();
        let matches_source = field_location.size() == 8 && field_location.align() == 4;
        if let Some(work) = work.as_deref_mut() {
            work.step()?;
        }
        if !matches_source {
            return Err(TypeCodecError::InvalidShape(
                "flat writer FieldLoc allocation source layout changed",
            ));
        }
        let mut requests = Requests { bytes: 0, count: 0 };
        // Schema primary backing and its finished metadata copy coexist.
        // The actual finished copy is no larger than the admitted backing.
        requests.request::<u8>(schema.backing, work.as_deref_mut())?;
        requests.request::<u8>(schema.backing, work.as_deref_mut())?;
        requests.request::<u8>(batch_backing, work.as_deref_mut())?;
        requests.request::<u8>(stream_capacity, work.as_deref_mut())?;

        // The resource helper performs two flat walks and the exact schema
        // encoder performs two more. The list macro has exact capacity one.
        for _ in 0..4 {
            requests.request::<TypeWalkerEntry<'_>>(1, work.as_deref_mut())?;
        }
        // emit_field reserves these exact metadata capacities. An empty Vec
        // makes no request, and the lexical-sort and offsets Vecs coexist.
        requests.request::<(&str, &str)>(schema.metadata_entries, work.as_deref_mut())?;
        requests.request::<MetadataOffset>(schema.metadata_entries, work.as_deref_mut())?;

        // Schema tables can insert up to seven slots: first FieldLoc capacity
        // four, then eight. Count both requests. All actual candidate schema
        // vtables are bounded by the existing schema author's table count.
        requests.request::<FieldLocation>(4, work.as_deref_mut())?;
        requests.request::<FieldLocation>(8, work.as_deref_mut())?;
        requests.vtable_requests(schema.tables, work.as_deref_mut())?;

        // The uncompressed batch Message/RecordBatch each insert at most
        // four slots. Two candidate vtables fit the first u32 capacity four.
        requests.request::<FieldLocation>(4, work.as_deref_mut())?;
        requests.request::<u32>(4, work.as_deref_mut())?;
        Ok(requests)
    })();
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    // This borrowed owner retains the same pending work and failure latch.
    // An ordinary layout/overflow error must also observe its completed tail.
    if let Some(work) = work {
        work.flush()?;
    }
    result
}

/// Batch/output requests shared by flat and recursive writers. Schema requests
/// belong to their sole schema allocation author and are not counted here.
pub(crate) fn batch_and_stream_requests(
    batch_backing: usize,
    stream_capacity: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Requests, TypeCodecError> {
    batch_and_stream_requests_core(batch_backing, stream_capacity, Some(work))
}

pub(crate) fn batch_and_stream_requests_core(
    batch_backing: usize,
    stream_capacity: usize,
    mut work: Option<&mut CompileCheckpoints<'_>>,
) -> Result<Requests, TypeCodecError> {
    let field_location = Layout::new::<FieldLocation>();
    let matches_source = field_location.size() == 8 && field_location.align() == 4;
    if let Some(work) = work.as_deref_mut() {
        work.step()?;
    }
    if !matches_source {
        return Err(TypeCodecError::InvalidShape(
            "flat writer FieldLoc allocation source layout changed",
        ));
    }
    let mut requests = Requests { bytes: 0, count: 0 };
    requests.request::<u8>(batch_backing, work.as_deref_mut())?;
    requests.request::<FieldLocation>(4, work.as_deref_mut())?;
    requests.request::<u32>(4, work.as_deref_mut())?;
    requests.request::<u8>(stream_capacity, work)?;
    Ok(requests)
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
    use std::sync::Mutex;

    #[derive(Default)]
    struct OriginalControl {
        calls: Mutex<Vec<(CompilePhase, u32)>>,
        refusal: Option<CompileControlError>,
    }
    impl PureCompileControl for OriginalControl {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            self.calls.lock().unwrap().push((phase, units));
            if units != 0
                && let Some(error) = self.refusal
            {
                return Err(error);
            }
            Ok(())
        }
    }
    fn schema(entries: usize) -> SchemaPreflight {
        SchemaPreflight {
            backing: 512,
            tables: entries + 4,
            metadata_entries: entries,
            string_bytes: 0,
            field_occurrences: 1,
            type_occurrences: 1,
            metadata_nonempty_fields: usize::from(entries != 0),
            child_offset_items: 0,
            child_offset_requests: 0,
            union_id_items: 0,
            union_id_requests: 0,
        }
    }

    #[test]
    fn own_emitter_empty_metadata_counts_all_cumulative_requests() {
        let control = OriginalControl::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let requests = preflight(&schema(0), 192, 2048, &mut work).unwrap();
        let walks = 4 * Layout::new::<TypeWalkerEntry<'_>>().size();
        // Four primary/copy requests, four walkers, schema FieldLoc 4+8,
        // schema vtables 4, batch FieldLoc 4, and batch vtables 4.
        assert_eq!(requests.count, 13);
        assert_eq!(requests.bytes, 512 * 2 + 192 + 2048 + walks + 160);
        assert_eq!(prefix_request_bytes(), walks / 2);
        assert!(control.calls.lock().unwrap().last().unwrap().1 > 0);
    }

    #[test]
    fn metadata_containers_and_vtable_growth_count_every_request() {
        let control = OriginalControl::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let requests = preflight(&schema(5), 192, 2048, &mut work).unwrap();
        let walks = 4 * Layout::new::<TypeWalkerEntry<'_>>().size();
        let metadata =
            5 * (Layout::new::<(&str, &str)>().size() + Layout::new::<MetadataOffset>().size());
        // Nine schema tables require capacities 4,8,16, not capacity 9.
        let auxiliary = (4 + 8 + 4) * 8 + (4 + 8 + 16 + 4) * 4;
        assert_eq!(requests.count, 17);
        assert_eq!(
            requests.bytes,
            512 * 2 + 192 + 2048 + walks + metadata + auxiliary
        );
    }

    #[test]
    fn vtable_geometric_boundaries_have_no_empty_allocation() {
        for (tables, expected_bytes, expected_count) in [
            (0, 0, 0),
            (1, 16, 1),
            (4, 16, 1),
            (5, 48, 2),
            (8, 48, 2),
            (9, 112, 3),
        ] {
            let control = OriginalControl::default();
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
            let mut requests = Requests { bytes: 0, count: 0 };
            requests.vtable_requests(tables, Some(&mut work)).unwrap();
            assert_eq!(requests.bytes, expected_bytes);
            assert_eq!(requests.count, expected_count);
        }
    }

    #[test]
    fn individual_layout_and_cumulative_overflow_are_rejected() {
        let control = OriginalControl::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let mut source = schema(0);
        source.backing = usize::MAX;
        assert!(matches!(
            preflight(&source, 1, 1, &mut work),
            Err(TypeCodecError::InvalidShape(_))
        ));
        assert!(control.calls.lock().unwrap().last().unwrap().1 > 0);

        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        source.backing = isize::MAX as usize;
        assert!(matches!(
            preflight(&source, 1, 1, &mut work),
            Err(TypeCodecError::InvalidShape(_))
        ));
    }

    #[test]
    fn ordinary_failure_tail_keeps_original_typed_control_primary() {
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = OriginalControl {
                refusal: Some(error),
                ..Default::default()
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
            let mut source = schema(0);
            source.backing = usize::MAX;
            assert!(matches!(preflight(&source, 1, 1, &mut work),
                Err(TypeCodecError::Control(cause)) if cause == error));
            let first = control.calls.lock().unwrap().clone();
            assert_eq!(first.len(), 2);
            assert_eq!(first[0], (CompilePhase::Encode, 0));
            assert!(first[1].1 > 0);
            assert!(matches!(preflight(&schema(0), 192, 2048, &mut work),
                Err(TypeCodecError::Control(cause)) if cause == error));
            assert_eq!(*control.calls.lock().unwrap(), first);
        }
    }
}
