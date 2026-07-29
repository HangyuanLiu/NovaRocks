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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use crate::{FileError, FileResult};

#[derive(Clone, Default)]
pub struct FileCancellation {
    cancelled: Arc<AtomicBool>,
}

impl FileCancellation {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub fn check(&self) -> FileResult<()> {
        if self.is_cancelled() {
            Err(FileError::cancelled("file operation cancelled"))
        } else {
            Ok(())
        }
    }
}

impl std::fmt::Debug for FileCancellation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileCancellation")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

pub type FileTaskFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub type FileBytesFuture = Pin<Box<dyn Future<Output = FileResult<Bytes>> + Send + 'static>>;

pub trait FileIoRuntime: Send + Sync {
    fn block_on_bytes(&self, future: FileBytesFuture) -> FileResult<Bytes>;
}

pub trait FileTaskSpawner: Send + Sync {
    fn spawn(&self, task: FileTaskFuture) -> FileResult<FileTask>;
}

pub struct FileTask {
    join: Option<JoinHandle<()>>,
}

impl FileTask {
    pub fn new(join: JoinHandle<()>) -> Self {
        Self { join: Some(join) }
    }

    pub fn abort(&mut self) {
        if let Some(join) = self.join.take() {
            join.abort();
        }
    }

    pub fn is_finished(&self) -> bool {
        self.join.as_ref().is_none_or(JoinHandle::is_finished)
    }
}

impl Drop for FileTask {
    fn drop(&mut self) {
        self.abort();
    }
}

#[derive(Clone)]
pub struct TokioFileIoRuntime {
    handle: Handle,
}

impl TokioFileIoRuntime {
    pub fn new(handle: Handle) -> Self {
        Self { handle }
    }
}

impl FileIoRuntime for TokioFileIoRuntime {
    fn block_on_bytes(&self, future: FileBytesFuture) -> FileResult<Bytes> {
        self.handle.block_on(future)
    }
}

#[derive(Clone)]
pub struct TokioFileTaskSpawner {
    handle: Handle,
}

impl TokioFileTaskSpawner {
    pub fn new(handle: Handle) -> Self {
        Self { handle }
    }
}

impl FileTaskSpawner for TokioFileTaskSpawner {
    fn spawn(&self, task: FileTaskFuture) -> FileResult<FileTask> {
        Ok(FileTask::new(self.handle.spawn(task)))
    }
}
