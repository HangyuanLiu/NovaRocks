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

mod common;

use arrow::array::{Array, Int32Array, StringArray};
use novarocks_fs::{
    CacheOptions, DataCacheManager, DataCachePageCacheOptions, FileErrorKind, FileFormat,
    FileProjection, FileReadRange, MinMaxPredicateOp, MinMaxPredicateValue, PhysicalPageSelection,
    ScanPredicate, ScanPredicateDomain, ScanPredicateSource, open_file_reader,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use common::{Fixture, collect};

#[test]
fn parquet_projects_all_root_columns() {
    let fixture = Fixture::parquet();
    let mut reader = open_file_reader(fixture.request(
        FileFormat::Parquet,
        FileProjection::All,
        1024,
        1024 * 1024,
    ))
    .expect("open reader");
    let batches = collect(reader.as_mut()).expect("read Parquet");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>(),
        8
    );
    assert_eq!(batches[0].batch.num_columns(), 2);
}

#[test]
fn parquet_projects_root_names() {
    let fixture = Fixture::parquet();
    let mut reader = open_file_reader(fixture.request(
        FileFormat::Parquet,
        FileProjection::RootNames(vec!["name".to_string()]),
        1024,
        1024 * 1024,
    ))
    .expect("open reader");
    let batches = collect(reader.as_mut()).expect("read Parquet");
    assert_eq!(batches[0].batch.num_columns(), 1);
    assert!(batches[0].batch.column(0).as_any().is::<StringArray>());
}

#[test]
fn parquet_projects_root_indices() {
    let fixture = Fixture::parquet();
    let mut reader = open_file_reader(fixture.request(
        FileFormat::Parquet,
        FileProjection::RootIndices(vec![0]),
        1024,
        1024 * 1024,
    ))
    .expect("open reader");
    let batches = collect(reader.as_mut()).expect("read Parquet");
    assert!(batches[0].batch.column(0).as_any().is::<Int32Array>());
}

#[test]
fn parquet_projects_field_ids() {
    let fixture = Fixture::parquet();
    let mut reader = open_file_reader(fixture.request(
        FileFormat::Parquet,
        FileProjection::FieldIds(vec![20]),
        1024,
        1024 * 1024,
    ))
    .expect("open reader");
    let batches = collect(reader.as_mut()).expect("read Parquet");
    assert_eq!(batches[0].batch.schema().field(0).name(), "name");
}

#[test]
fn parquet_range_selects_row_group_by_physical_offset() {
    let fixture = Fixture::parquet();
    let file = std::fs::File::open(fixture.file.location().path()).expect("open fixture");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("metadata");
    let second = builder.metadata().row_group(1);
    let start = second.columns()[0]
        .dictionary_page_offset()
        .unwrap_or_else(|| second.columns()[0].data_page_offset())
        .min(second.columns()[0].data_page_offset()) as u64;
    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    request.range = FileReadRange::bounded(start, fixture.file.identity().file_size() - start)
        .expect("bounded range");
    let mut reader = open_file_reader(request).expect("open reader");
    let batches = collect(reader.as_mut()).expect("read Parquet");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>(),
        4
    );
    assert_eq!(
        batches[0].physical_row_positions.as_ref().unwrap().value(0),
        4
    );
}

#[test]
fn parquet_predicate_prunes_row_groups() {
    let fixture = Fixture::parquet();
    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    request.predicates.push(ScanPredicate::new(
        "id",
        ScanPredicateDomain::Range {
            op: MinMaxPredicateOp::Ge,
            value: MinMaxPredicateValue::Int32(4),
        },
        ScanPredicateSource::Static,
    ));
    let mut reader = open_file_reader(request).expect("open reader");
    let batches = collect(reader.as_mut()).expect("read Parquet");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>(),
        4
    );
    assert_eq!(
        batches[0].physical_row_positions.as_ref().unwrap().value(0),
        4
    );
}

#[test]
fn parquet_honors_explicit_page_selection_and_positions() {
    let fixture = Fixture::parquet();
    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    request.pruning.row_groups = Some(vec![0]);
    request.pruning.pages.push(PhysicalPageSelection {
        row_group: 0,
        page_indices: vec![1],
    });
    let mut reader = open_file_reader(request).expect("open reader");
    let batches = collect(reader.as_mut()).expect("read Parquet");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>(),
        2
    );
    assert_eq!(
        batches[0].physical_row_positions.as_ref().unwrap().value(0),
        2
    );
}

#[test]
fn parquet_enforces_row_budget() {
    let fixture = Fixture::parquet();
    let mut reader =
        open_file_reader(fixture.request(FileFormat::Parquet, FileProjection::All, 3, 1024 * 1024))
            .expect("open reader");
    let batches = collect(reader.as_mut()).expect("read Parquet");
    assert!(batches.iter().all(|batch| batch.batch.num_rows() <= 3));
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>(),
        8
    );
}

#[test]
fn parquet_enforces_byte_budget_and_rejects_oversized_row() {
    let fixture = Fixture::parquet();
    let mut reader =
        open_file_reader(fixture.request(FileFormat::Parquet, FileProjection::All, 8, 260))
            .expect("open reader");
    let batches = collect(reader.as_mut()).expect("read within byte budget");
    assert!(
        batches
            .iter()
            .all(|batch| batch.batch.get_array_memory_size() <= 260)
    );

    let mut reader =
        open_file_reader(fixture.request(FileFormat::Parquet, FileProjection::All, 8, 1))
            .expect("open reader");
    assert_eq!(
        reader
            .next_batch()
            .expect_err("one row exceeds budget")
            .kind(),
        FileErrorKind::ResourceExhausted
    );
}

#[test]
fn parquet_positions_stay_aligned_across_budget_slices() {
    let fixture = Fixture::parquet();
    let mut reader =
        open_file_reader(fixture.request(FileFormat::Parquet, FileProjection::All, 3, 1024 * 1024))
            .expect("open reader");
    let batches = collect(reader.as_mut()).expect("read Parquet");
    let positions = batches
        .iter()
        .flat_map(|batch| {
            batch
                .physical_row_positions
                .as_ref()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(positions, (0..8).collect::<Vec<_>>());
}

#[test]
fn parquet_exact_ranges_use_foundation_page_cache() {
    let _ = DataCacheManager::instance().init_page_cache(DataCachePageCacheOptions {
        capacity: 1024 * 1024,
        evict_probability: 100,
    });
    let fixture = Fixture::parquet();
    let cache = DataCacheManager::instance().external_context(CacheOptions {
        enable_scan_datacache: true,
        enable_populate_datacache: true,
        enable_datacache_async_populate_mode: false,
        enable_datacache_io_adaptor: false,
        enable_cache_select: false,
        datacache_evict_probability: 100,
        datacache_priority: 0,
        datacache_ttl_seconds: 0,
        datacache_sharing_work_period: None,
    });
    let mut first = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    first.cache = Some(cache.clone());
    let mut first = open_file_reader(first).expect("first reader");
    collect(first.as_mut()).expect("first read");

    let mut second = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    second.cache = Some(cache);
    let mut second = open_file_reader(second).expect("second reader");
    collect(second.as_mut()).expect("second read");
    assert!(second.metrics_snapshot().cache_hits > 0);
}

#[test]
fn orc_projects_physical_columns_and_honors_row_budget() {
    let fixture = Fixture::orc();
    let mut reader = open_file_reader(fixture.request(
        FileFormat::Orc,
        FileProjection::RootNames(vec!["name".to_string()]),
        3,
        1024 * 1024,
    ))
    .expect("open ORC reader");
    let batches = collect(reader.as_mut()).expect("read ORC");
    assert!(batches.iter().all(|batch| batch.batch.num_rows() <= 3));
    assert!(batches[0].batch.column(0).as_any().is::<StringArray>());
    assert!(
        batches
            .iter()
            .all(|batch| batch.physical_row_positions.is_none())
    );
}
