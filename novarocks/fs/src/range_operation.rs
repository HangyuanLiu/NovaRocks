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

//! One supervised range read with separate result and actual-exit receipts.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::{
    BoundFile, FileCancellation, FileError, FileErrorKind, FileReadRange, FileResult, FileTask,
    FileTaskSpawner,
};

/// A completed result does not release the spawned operation's ownership.
/// The caller must also await `drained` or `stop_and_drain` before releasing a
/// request slot or a buffer reservation associated with that operation.
pub struct FileRangeOperation {
    cancellation: FileCancellation,
    result: Option<oneshot::Receiver<FileResult<Bytes>>>,
    task: FileTask,
}

impl FileRangeOperation {
    pub fn start(
        file: BoundFile,
        range: FileReadRange,
        cancellation: FileCancellation,
        spawner: &Arc<dyn FileTaskSpawner>,
    ) -> FileResult<Self> {
        cancellation.check()?;
        let cancellation = cancellation.child();
        let (sender, result) = oneshot::channel();
        let operation_cancellation = cancellation.clone();
        let task = spawner.spawn(Box::pin(async move {
            let outcome = file.read(range, &operation_cancellation).await;
            let _ = sender.send(outcome);
        }))?;
        Ok(Self {
            cancellation,
            result: Some(result),
            task,
        })
    }

    /// Return the read outcome once. A missing sender is an abnormal exit,
    /// never a successful empty range.
    pub async fn result_ready(&mut self) -> FileResult<Bytes> {
        let receiver = self.result.take().ok_or_else(|| {
            FileError::new(
                FileErrorKind::Invalid,
                "file range result was already consumed",
            )
        })?;
        receiver.await.map_err(|_| {
            FileError::new(
                FileErrorKind::Internal,
                "file range operation exited without a result",
            )
        })?
    }

    pub fn request_stop(&self) {
        self.cancellation.cancel();
    }

    /// Wait for the spawned operation's destructor path and task exit.
    pub async fn drained(self) -> FileResult<()> {
        self.task.drain().await
    }

    pub async fn stop_and_drain(self) -> FileResult<()> {
        self.request_stop();
        self.drained().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FileIdentity, FileTaskFuture, FsAccessResolver, TokioFileTaskSpawner};
    use novarocks_spi::connector::StorageAccessDomainId;

    fn local_file() -> (tempfile::TempDir, BoundFile) {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("range.parquet");
        std::fs::write(&path, b"range-data").expect("write fixture");
        let access = FsAccessResolver::new()
            .resolve_location(
                StorageAccessDomainId::from_bytes([9; 32]),
                path.to_string_lossy(),
                None,
            )
            .expect("local access");
        let file = access
            .bind(0, FileIdentity::new(path.to_string_lossy(), 10, None))
            .expect("bound file");
        (directory, file)
    }

    #[tokio::test]
    async fn successful_result_still_requires_an_exit_receipt() {
        let (_directory, file) = local_file();
        let spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(TokioFileTaskSpawner::new(tokio::runtime::Handle::current()));
        let mut operation = FileRangeOperation::start(
            file,
            FileReadRange::Bounded {
                offset: 1,
                length: 5,
            },
            FileCancellation::new(),
            &spawner,
        )
        .expect("range operation");

        assert_eq!(
            operation.result_ready().await.expect("result").as_ref(),
            b"ange-"
        );
        operation.drained().await.expect("actual task exit");
    }

    struct PanickingSpawner;

    impl FileTaskSpawner for PanickingSpawner {
        fn spawn(&self, task: FileTaskFuture) -> FileResult<FileTask> {
            Ok(FileTask::new(tokio::spawn(async move {
                drop(task);
                panic!("injected range task failure");
            })))
        }

        fn spawn_detached_blocking(&self, _job: Box<dyn FnOnce() + Send + 'static>) {
            unreachable!("range test has no credential refresh")
        }
    }

    #[tokio::test]
    async fn abnormal_task_exit_has_separate_result_and_drain_failures() {
        let (_directory, file) = local_file();
        let spawner: Arc<dyn FileTaskSpawner> = Arc::new(PanickingSpawner);
        let source = FileCancellation::new();
        let mut operation =
            FileRangeOperation::start(file, FileReadRange::WholeFile, source.clone(), &spawner)
                .expect("range operation started");

        assert_eq!(
            operation
                .result_ready()
                .await
                .expect_err("no result was sent")
                .kind(),
            FileErrorKind::Internal
        );
        assert_eq!(
            operation.drained().await.expect_err("task panicked").kind(),
            FileErrorKind::Internal
        );
        assert!(!source.is_cancelled());
    }
}
