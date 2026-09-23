// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorDocumentDiscoveryCompleteness, ConnectorDocumentDiscoveryIncompleteReason,
    ConnectorDocumentDiscoveryItem, ConnectorDocumentDiscoveryPage,
    ConnectorDocumentDiscoveryRequest, ConnectorError, ConnectorErrorKind, ConnectorTableIdentity,
};

use super::create::IcebergDocumentStorage;

const CURSOR_MAGIC: &[u8; 8] = b"NRDISC01";
const CURSOR_FIXED_BYTES: usize = CURSOR_MAGIC.len() + 8 + 8 + 4 + 4;

#[derive(Debug)]
struct DiscoveryWorkBudget {
    remaining_pages: usize,
    remaining_table_loads: usize,
    seen_tokens: HashSet<[u8; 32]>,
}

impl DiscoveryWorkBudget {
    fn initial(request: &ConnectorDocumentDiscoveryRequest) -> Self {
        Self {
            remaining_pages: request.remaining_items(),
            remaining_table_loads: request.remaining_items(),
            seen_tokens: HashSet::new(),
        }
    }

    fn consume_page(&mut self) -> Result<(), ConnectorError> {
        self.remaining_pages = self.remaining_pages.checked_sub(1).ok_or_else(|| {
            exhausted("Iceberg document discovery exhausted its underlying page budget")
        })?;
        Ok(())
    }

    fn consume_table_load(&mut self) -> Result<(), ConnectorError> {
        self.remaining_table_loads =
            self.remaining_table_loads.checked_sub(1).ok_or_else(|| {
                exhausted("Iceberg document discovery exhausted its table-load budget")
            })?;
        Ok(())
    }

    fn accept_next_token(
        &mut self,
        token: &str,
        max_token_bytes: usize,
    ) -> Result<(), ConnectorError> {
        if token.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg catalog returned an empty discovery cursor",
            ));
        }
        if token.len() > max_token_bytes {
            return Err(exhausted(
                "Iceberg catalog discovery cursor exceeds the caller handle budget",
            ));
        }
        let digest = token_digest(token);
        if !self.seen_tokens.insert(digest) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg catalog returned a cyclic discovery cursor",
            ));
        }
        Ok(())
    }

    fn exhausted(&self) -> bool {
        self.remaining_pages == 0 || self.remaining_table_loads == 0
    }
}

pub(crate) fn discover(
    storage: &IcebergDocumentStorage,
    request: &ConnectorDocumentDiscoveryRequest,
) -> Result<ConnectorDocumentDiscoveryPage, ConnectorError> {
    super::io::check_context(request.context())?;
    let namespace = request.namespace().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "Iceberg document discovery currently requires an exact namespace scope",
        )
    })?;
    let (mut cursor, mut work) = match request.cursor() {
        Some(cursor) => decode_cursor(cursor, request)?,
        None => (None, DiscoveryWorkBudget::initial(request)),
    };
    let namespace_name = crate::catalog::CatalogNamespaceName::new(namespace.clone());
    let page_size = request.page_size();
    loop {
        super::io::check_context(request.context())?;
        work.consume_page()?;
        let catalog = Arc::clone(storage.runtime().novarocks_catalog());
        let page_namespace = namespace_name.clone();
        let page_cursor = cursor.clone();
        let page = storage
            .runtime()
            .resources()
            .catalog_runtime()
            .block_on(async move {
                catalog
                    .list_tables_page(page_namespace, page_cursor, page_size)
                    .await
            })
            .map_err(|error| {
                ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    format!("list Iceberg document candidates runtime: {error}"),
                )
            })??;
        if page.tables.len() > page_size || page.tables.len() > work.remaining_table_loads {
            return Err(exhausted(
                "Iceberg catalog page exceeds the remaining discovery work budget",
            ));
        }
        let mut items = Vec::with_capacity(page.tables.len());
        for table_name in page.tables {
            super::io::check_context(request.context())?;
            work.consume_table_load()?;
            let physical = storage
                .runtime()
                .load_table_classified_for_request(
                    &table_name.namespace,
                    &table_name.name,
                    request.context(),
                )
                .map_err(|(kind, message)| ConnectorError::new(kind, message))?;
            let table = physical.into_table();
            let marker = match super::observation::managed_marker(table.metadata()) {
                Ok(marker) => marker,
                Err(error) if error.kind() == ConnectorErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            items.push(ConnectorDocumentDiscoveryItem::try_new(
                ConnectorTableIdentity {
                    instance_id: storage.descriptor().instance_id.clone(),
                    namespace: table_name.namespace,
                    table: table_name.name,
                },
                super::observation::table_object_id(table.metadata())?,
                super::observation::committed_version(&table)?,
                marker,
            )?);
        }
        let next = match page_action(items.len(), page.next_page_token)? {
            DiscoveryPageAction::Continue(next) => {
                work.accept_next_token(&next, request.context().max_handle_payload_bytes())?;
                if work.exhausted() {
                    return Err(exhausted(
                        "Iceberg document discovery exhausted its internal work budget before finding a managed object",
                    ));
                }
                cursor = Some(next);
                continue;
            }
            DiscoveryPageAction::Return(next) => next,
        };
        let next_cursor = match next {
            Some(token) => {
                work.accept_next_token(&token, request.context().max_handle_payload_bytes())?;
                let remaining_items = request
                    .remaining_items()
                    .checked_sub(items.len())
                    .ok_or_else(|| exhausted("Iceberg discovery item budget underflowed"))?;
                Some(encode_cursor(&token, &work, remaining_items, request)?)
            }
            None => None,
        };
        let completeness = match next_cursor {
            Some(_) if items.len() == request.remaining_items() => {
                ConnectorDocumentDiscoveryCompleteness::Incomplete(
                    ConnectorDocumentDiscoveryIncompleteReason::ItemBudget,
                )
            }
            Some(_) => ConnectorDocumentDiscoveryCompleteness::Incomplete(
                ConnectorDocumentDiscoveryIncompleteReason::PageBoundary,
            ),
            None => ConnectorDocumentDiscoveryCompleteness::Complete,
        };
        return ConnectorDocumentDiscoveryPage::try_new(items, next_cursor, completeness, request);
    }
}

#[derive(Debug, Eq, PartialEq)]
enum DiscoveryPageAction {
    Continue(Arc<str>),
    Return(Option<Arc<str>>),
}

fn page_action(
    managed_items: usize,
    next: Option<Arc<str>>,
) -> Result<DiscoveryPageAction, ConnectorError> {
    if managed_items != 0 || next.is_none() {
        return Ok(DiscoveryPageAction::Return(next));
    }
    let next = next.expect("an empty managed page with no cursor returned above");
    Ok(DiscoveryPageAction::Continue(next))
}

fn encode_cursor(
    current: &str,
    work: &DiscoveryWorkBudget,
    remaining_items: usize,
    request: &ConnectorDocumentDiscoveryRequest,
) -> Result<Bytes, ConnectorError> {
    let seen_count = u32::try_from(work.seen_tokens.len())
        .map_err(|_| exhausted("Iceberg discovery cursor has too many visited pages"))?;
    let current_len = u32::try_from(current.len())
        .map_err(|_| exhausted("Iceberg discovery cursor token is too large"))?;
    let encoded_len = CURSOR_FIXED_BYTES
        .checked_add(work.seen_tokens.len().checked_mul(32).ok_or_else(|| {
            exhausted("Iceberg discovery cursor visited-page accounting overflowed")
        })?)
        .and_then(|size| size.checked_add(current.len()))
        .ok_or_else(|| exhausted("Iceberg discovery cursor size overflowed"))?;
    if encoded_len > request.context().max_handle_payload_bytes() {
        return Err(exhausted(
            "Iceberg discovery cursor exceeds the caller handle budget",
        ));
    }
    let mut bytes = Vec::with_capacity(encoded_len);
    bytes.extend_from_slice(CURSOR_MAGIC);
    bytes.extend_from_slice(&(work.remaining_pages.min(remaining_items) as u64).to_be_bytes());
    bytes
        .extend_from_slice(&(work.remaining_table_loads.min(remaining_items) as u64).to_be_bytes());
    bytes.extend_from_slice(&seen_count.to_be_bytes());
    bytes.extend_from_slice(&current_len.to_be_bytes());
    let mut seen = work.seen_tokens.iter().copied().collect::<Vec<_>>();
    seen.sort_unstable();
    for digest in seen {
        bytes.extend_from_slice(&digest);
    }
    bytes.extend_from_slice(current.as_bytes());
    Ok(Bytes::from(bytes))
}

fn decode_cursor(
    encoded: &Bytes,
    request: &ConnectorDocumentDiscoveryRequest,
) -> Result<(Option<Arc<str>>, DiscoveryWorkBudget), ConnectorError> {
    if encoded.len() > request.context().max_handle_payload_bytes() {
        return Err(exhausted(
            "Iceberg document discovery cursor exceeds the caller handle budget",
        ));
    }
    if encoded.len() < CURSOR_FIXED_BYTES || &encoded[..CURSOR_MAGIC.len()] != CURSOR_MAGIC {
        return Err(corrupt_cursor());
    }
    let mut offset = CURSOR_MAGIC.len();
    let remaining_pages = read_u64(encoded, &mut offset)?;
    let remaining_table_loads = read_u64(encoded, &mut offset)?;
    let seen_count = read_u32(encoded, &mut offset)? as usize;
    let current_len = read_u32(encoded, &mut offset)? as usize;
    let seen_bytes = seen_count.checked_mul(32).ok_or_else(corrupt_cursor)?;
    let expected_len = offset
        .checked_add(seen_bytes)
        .and_then(|size| size.checked_add(current_len))
        .ok_or_else(corrupt_cursor)?;
    if expected_len != encoded.len()
        || remaining_pages > request.remaining_items() as u64
        || remaining_table_loads > request.remaining_items() as u64
    {
        return Err(corrupt_cursor());
    }
    let mut seen_tokens = HashSet::with_capacity(seen_count);
    for _ in 0..seen_count {
        let mut digest = [0; 32];
        digest.copy_from_slice(&encoded[offset..offset + 32]);
        offset += 32;
        if !seen_tokens.insert(digest) {
            return Err(corrupt_cursor());
        }
    }
    let current = std::str::from_utf8(&encoded[offset..]).map_err(|_| corrupt_cursor())?;
    if current.is_empty() || !seen_tokens.contains(&token_digest(current)) {
        return Err(corrupt_cursor());
    }
    let remaining_pages = usize::try_from(remaining_pages).map_err(|_| corrupt_cursor())?;
    let remaining_table_loads =
        usize::try_from(remaining_table_loads).map_err(|_| corrupt_cursor())?;
    Ok((
        Some(Arc::from(current)),
        DiscoveryWorkBudget {
            remaining_pages,
            remaining_table_loads,
            seen_tokens,
        },
    ))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> Result<u64, ConnectorError> {
    let end = offset.checked_add(8).ok_or_else(corrupt_cursor)?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(corrupt_cursor)?
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| corrupt_cursor())?;
    *offset = end;
    Ok(value)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32, ConnectorError> {
    let end = offset.checked_add(4).ok_or_else(corrupt_cursor)?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(corrupt_cursor)?
        .try_into()
        .map(u32::from_be_bytes)
        .map_err(|_| corrupt_cursor())?;
    *offset = end;
    Ok(value)
}

fn token_digest(token: &str) -> [u8; 32] {
    use sha2::{Digest, Sha256};

    Sha256::digest(token.as_bytes()).into()
}

fn corrupt_cursor() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        "Iceberg document discovery cursor is malformed or outside its operation budget",
    )
}

fn exhausted(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorDocumentStorageBudget,
        ConnectorDocumentStorageLimits, ConnectorInstanceId, ConnectorProviderBindingKey,
        ConnectorRequestContext, ProviderBindingEpoch,
    };

    use super::*;

    fn request(max_handle_bytes: usize, max_items: usize) -> ConnectorDocumentDiscoveryRequest {
        let instance_id = ConnectorInstanceId::parse("catalog").unwrap();
        ConnectorDocumentDiscoveryRequest::try_new(
            ConnectorProviderBindingKey {
                instance_id: instance_id.clone(),
                incarnation: ProviderBindingEpoch::from_bytes([1; 16]),
            },
            CatalogHandle::new(instance_id, CatalogVersion::from_bytes([2; 32])),
            Some(Arc::from("db")),
            max_items,
            ConnectorDocumentStorageBudget::new(
                ConnectorDocumentStorageLimits::try_new(
                    16,
                    16,
                    1024 * 1024,
                    max_items,
                    max_items,
                    64,
                )
                .unwrap(),
            ),
            ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(30),
                novarocks_spi::connector::ConnectorStopOwner::new().view(),
                max_handle_bytes,
                max_handle_bytes.max(1024),
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn sparse_two_page_discovery_advances_past_an_empty_managed_page() {
        let first = page_action(0, Some(Arc::from("page-2"))).unwrap();
        assert_eq!(first, DiscoveryPageAction::Continue(Arc::from("page-2")));
        let second = page_action(1, None).unwrap();
        assert_eq!(second, DiscoveryPageAction::Return(None));
    }

    #[test]
    fn sparse_discovery_rejects_any_underlying_cursor_cycle() {
        let mut budget = DiscoveryWorkBudget {
            remaining_pages: 3,
            remaining_table_loads: 3,
            seen_tokens: HashSet::new(),
        };
        budget.accept_next_token("page-2", 1024).unwrap();
        budget.accept_next_token("page-3", 1024).unwrap();
        assert_eq!(
            budget.accept_next_token("page-2", 1024).unwrap_err().kind(),
            ConnectorErrorKind::CorruptData,
        );
    }

    #[test]
    fn sparse_discovery_work_budget_is_a_strict_operation_bound() {
        let mut budget = DiscoveryWorkBudget {
            remaining_pages: 1,
            remaining_table_loads: 1,
            seen_tokens: HashSet::new(),
        };
        budget.consume_page().unwrap();
        budget.consume_table_load().unwrap();
        assert_eq!(
            budget.consume_page().unwrap_err().kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(
            budget.consume_table_load().unwrap_err().kind(),
            ConnectorErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn continuation_carries_cycle_history_and_cannot_expand_its_budget() {
        let request = request(1024, 3);
        let mut work = DiscoveryWorkBudget {
            remaining_pages: 2,
            remaining_table_loads: 2,
            seen_tokens: HashSet::new(),
        };
        work.accept_next_token("page-2", 1024).unwrap();
        work.accept_next_token("page-3", 1024).unwrap();
        let encoded = encode_cursor("page-3", &work, 2, &request).unwrap();
        let (_, mut decoded) = decode_cursor(&encoded, &request).unwrap();
        assert_eq!(decoded.remaining_pages, 2);
        assert_eq!(
            decoded
                .accept_next_token("page-2", 1024)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData,
        );

        let mut expanded = encoded.to_vec();
        expanded[CURSOR_MAGIC.len()..CURSOR_MAGIC.len() + 8].copy_from_slice(&4u64.to_be_bytes());
        assert_eq!(
            decode_cursor(&Bytes::from(expanded), &request)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::InvalidRequest,
        );
    }

    #[test]
    fn continuation_is_sized_before_allocating_the_encoded_cursor() {
        let request = request(64, 3);
        let mut work = DiscoveryWorkBudget {
            remaining_pages: 2,
            remaining_table_loads: 2,
            seen_tokens: HashSet::new(),
        };
        work.accept_next_token("page-2", 64).unwrap();
        assert_eq!(
            encode_cursor("page-2", &work, 2, &request)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::ResourceExhausted,
        );
    }
}
