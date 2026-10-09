// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not //! Fixed copy-only facts from the production close_relay freeze.
//! This module owns no IO, body, root read, lease, task or cancellation capability.

use novarocks_query_application::api::{
    ResidentRootSegment, ResidentSegmentScalars, RootDataScalars, RootSegmentDelivery,
};
use novarocks_result_contract::ValidatedClientBody;
use opensrv_mysql::FramingCursor;
use std::io;

#[derive(Clone, Copy, Debug)]
pub(crate) struct ClientBodyScalars {
    pub body_bytes: u64,
    pub before_remaining: u32,
    pub before_completed_rows: u64,
    pub after_remaining: u32,
    pub after_completed_rows: u64,
}
impl ClientBodyScalars {
    fn copy(body: &ValidatedClientBody<'_>) -> Self {
        Self {
            body_bytes: body.body().len() as u64,
            before_remaining: body.before().remaining(),
            before_completed_rows: body.before().completed_rows(),
            after_remaining: body.after().remaining(),
            after_completed_rows: body.after().completed_rows(),
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum CurrentBodySource {
    None,
    FrozenDelivering,
    FrozenReady,
    OriginalDeliveryFallback,
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct OriginalFreezeScalars {
    pub had_resident_window: bool,
    // Exactly the array returned by the ONE original freeze: [delivering, ready].
    pub slots: [Option<ResidentSegmentScalars>; 2],
    pub fallback_delivery: Option<RootDataScalars>,
    pub fallback_delivery_rows: Option<u64>,
    pub current_source: CurrentBodySource,
    pub framing: FramingCursor,
    pub buffered_row_bytes: u64,
    pub current: Option<ClientBodyScalars>,
    pub next: Option<ClientBodyScalars>,
    // These are lengths of the existing production-selected slices, never body aliases.
    pub tail_complete: bool,
    pub tail_parts: u8,
    pub tail_part_bytes: [u64; 2],
    pub tail_selected_bytes: u64,
}
impl OriginalFreezeScalars {
    pub(crate) fn capture(
        had_resident_window: bool,
        original_items: &[Option<ResidentRootSegment>; 2],
        original_delivery: Option<&RootSegmentDelivery>,
        framing: FramingCursor,
        buffered_row_bytes: usize,
        current: Option<&ValidatedClientBody<'_>>,
        next: Option<&ValidatedClientBody<'_>>,
        original_tail: Option<&[&[u8]]>,
    ) -> io::Result<Self> {
        let slots = [
            original_items[0]
                .as_ref()
                .and_then(ResidentRootSegment::fixed_scalars),
            original_items[1]
                .as_ref()
                .and_then(ResidentRootSegment::fixed_scalars),
        ];
        // Projection absence is not silently reclassified as an empty original slot.
        for index in 0..2 {
            if original_items[index].is_some() != slots[index].is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "original resident projection is not Data",
                ));
            }
        }
        let current_source = if current.is_none() {
            CurrentBodySource::None
        } else if slots[0].is_some_and(|slot| slot.has_validated_client_rows) {
            CurrentBodySource::FrozenDelivering
        } else if slots[1].is_some_and(|slot| slot.has_validated_client_rows) {
            CurrentBodySource::FrozenReady
        } else {
            CurrentBodySource::OriginalDeliveryFallback
        };
        let mut tail_part_bytes = [0; 2];
        let mut tail_selected_bytes = 0u64;
        if let Some(parts) = original_tail {
            if parts.len() > 2 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "original resident tail exceeds two slices",
                ));
            }
            for (index, part) in parts.iter().enumerate() {
                tail_part_bytes[index] = part.len() as u64;
                tail_selected_bytes = tail_selected_bytes
                    .checked_add(tail_part_bytes[index])
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "resident tail length overflow")
                    })?;
            }
        }
        Ok(Self {
            had_resident_window,
            slots,
            fallback_delivery: original_delivery.and_then(RootSegmentDelivery::data_scalars),
            fallback_delivery_rows: original_delivery.map(RootSegmentDelivery::rows),
            current_source,
            framing,
            buffered_row_bytes: buffered_row_bytes as u64,
            current: current.map(ClientBodyScalars::copy),
            next: next.map(ClientBodyScalars::copy),
            tail_complete: original_tail.is_some(),
            tail_parts: original_tail.map_or(0, |parts| parts.len() as u8),
            tail_part_bytes,
            tail_selected_bytes,
        })
    }
}
